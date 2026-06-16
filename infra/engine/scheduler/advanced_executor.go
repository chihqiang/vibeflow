package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/tracing"
)

// ============================================================================
// AdvancedExecutor — 高级编排执行器
// 负责 Sub-Workflow（子工作流）、Fan-Out/Fan-In（动态并行）、While/Loop（循环）的执行
//
// 设计原则：
//   - 复用已有的 ParallelExecutor 和 TaskPusher 接口
//   - 子工作流通过 WorkflowCoordinator 的 ExecuteWorkflow 递归调度
//   - Fan-Out 将上游列表展开为多个并行任务，汇聚结果后作为单个输出
//   - Loop 在 SerialExecutor 的回调中循环执行任务组
// ============================================================================

// AdvancedExecutor 高级编排执行器
type AdvancedExecutor struct {
	wfStore          WorkflowStateAccessor
	taskPusher       TaskPusher
	snapshotMgr      SnapshotManager
	listenerMgr      TaskListenerManager
	eventBroadcaster EventBroadcaster
	coordinator      WorkflowCoordinator
	wg               GoroutineTracker
	parallelExec     *ParallelExecutor // 复用的 ParallelExecutor 实例
}

// NewAdvancedExecutor 创建高级编排执行器
func NewAdvancedExecutor(
	wfStore WorkflowStateAccessor,
	taskPusher TaskPusher,
	snapshotMgr SnapshotManager,
	listenerMgr TaskListenerManager,
	eventBroadcaster EventBroadcaster,
	coordinator WorkflowCoordinator,
	wg GoroutineTracker,
) *AdvancedExecutor {
	return &AdvancedExecutor{
		wfStore:          wfStore,
		taskPusher:       taskPusher,
		snapshotMgr:      snapshotMgr,
		listenerMgr:      listenerMgr,
		eventBroadcaster: eventBroadcaster,
		coordinator:      coordinator,
		wg:               wg,
		parallelExec:     NewParallelExecutor(taskPusher, listenerMgr),
	}
}

// ExecuteSubWorkflow 执行子工作流
// 在父工作流上下文中查找子工作流定义，深拷贝后提交执行
// 子工作流完成后，其输出合并到父工作流的输出中
//
// node: 子工作流节点（含 SubWorkflow UUID 和 SubWorkflowParams）
// workflowUUID: 父工作流 UUID
// upstreamOutputs: 上游任务输出
// callback: 子工作流执行完成后的回调，接收子工作流的所有输出
func (ae *AdvancedExecutor) ExecuteSubWorkflow(
	ctx context.Context,
	workflowUUID string,
	node model.TaskNode,
	upstreamOutputs []map[string]any,
	callback func(outputs []map[string]any, err error),
) {
	// 查找子工作流定义
	subWfDef := ae.wfStore.GetRegisteredWorkflow(node.SubWorkflow)
	if subWfDef == nil {
		callback(nil, fmt.Errorf("子工作流 %s 未注册或不存在", node.SubWorkflow))
		return
	}

	// 深拷贝子工作流定义
	subWf, err := subWfDef.DeepCopy()
	if err != nil {
		callback(nil, fmt.Errorf("深拷贝子工作流 %s 失败: %w", node.SubWorkflow, err))
		return
	}

	// 如果有传入参数，将参数作为子工作流的初始输入
	if len(node.SubWorkflowParams) > 0 {
		// 创建一个修改后的子工作流 UUID 以避免与原始定义冲突
		subWf.UUID = fmt.Sprintf("%s__sub__%s", workflowUUID, node.Name)
	}

	logger.Info("开始执行子工作流",
		"parent_workflow", workflowUUID,
		"sub_workflow", subWf.UUID,
		"task_name", node.Name,
	)

	// 创建子工作流的执行追踪
	taskEventCh := make(chan subWorkflowResult, 1)

	// 提交子工作流执行
	// 使用 coordinator.ExecuteWorkflow 异步执行子工作流
	// 通过轮询 WorkflowState 检测子工作流是否完成
	ae.wg.Add(1)
	go func() {
		defer ae.wg.Done()
		ae.executeAndTrackSubWorkflow(ctx, workflowUUID, node, subWf, upstreamOutputs, taskEventCh)
	}()

	// 等待子工作流完成
	ae.wg.Add(1)
	go func() {
		defer ae.wg.Done()
		result := <-taskEventCh
		if result.err != nil {
			logger.Error("子工作流执行失败",
				"parent_workflow", workflowUUID,
				"sub_workflow", node.SubWorkflow,
				"task_name", node.Name,
				"error", result.err,
			)
			callback(nil, result.err)
			return
		}

		logger.Info("子工作流执行完成",
			"parent_workflow", workflowUUID,
			"sub_workflow", node.SubWorkflow,
			"task_name", node.Name,
		)
		callback(result.outputs, nil)
	}()
}

// subWorkflowResult 子工作流执行结果
type subWorkflowResult struct {
	outputs []map[string]any
	err     error
}

// executeAndTrackSubWorkflow 执行子工作流并追踪其完成状态
func (ae *AdvancedExecutor) executeAndTrackSubWorkflow(
	ctx context.Context,
	parentUUID string,
	node model.TaskNode,
	subWf *model.Workflow,
	upstreamOutputs []map[string]any,
	resultCh chan<- subWorkflowResult,
) {
	// 如果有传入参数，合并到子工作流的第一个任务组
	if len(node.SubWorkflowParams) > 0 && len(subWf.TaskGroups) > 0 {
		// 将子工作流参数合并为额外的上游输出
		paramOutput := make(map[string]any, len(node.SubWorkflowParams))
		for k, v := range node.SubWorkflowParams {
			paramOutput[k] = v
		}
		mergedUpstream := make([]map[string]any, 0, len(upstreamOutputs)+1)
		mergedUpstream = append(mergedUpstream, upstreamOutputs...)
		mergedUpstream = append(mergedUpstream, paramOutput)

		// 通过 coordinator 提交子工作流
		ae.coordinator.ExecuteWorkflow(ctx, subWf.UUID, mergedUpstream, 0)
	} else {
		ae.coordinator.ExecuteWorkflow(ctx, subWf.UUID, upstreamOutputs, 0)
	}

	// 轮询子工作流状态直到完成
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	// 子工作流执行完毕后，清理子工作流的 per-workflow 并发计数器
	// 防止并发槽位泄漏影响父工作流的后续任务调度
	defer ae.coordinator.CleanupPerWfCounter(subWf.UUID)
	for {
		select {
		case <-ctx.Done():
			resultCh <- subWorkflowResult{err: ctx.Err()}
			return
		case <-ticker.C:
			state := ae.wfStore.GetWorkflowState(subWf.UUID)
			if state == nil {
				// 子工作流可能未被 WorkflowManager 管理
				// 尝试从历史记录中获取
				hist := ae.wfStore.GetHistory(subWf.UUID)
				if hist != nil {
					if hist.Status == model.WorkflowStatusCompleted {
						outputs := collectCompletedOutputs(hist.CompletedTasks)
						resultCh <- subWorkflowResult{outputs: outputs}
						return
					}
					resultCh <- subWorkflowResult{err: fmt.Errorf("子工作流 %s 已结束，状态: %s", subWf.UUID, hist.Status)}
					return
				}
				continue
			}

			switch state.Status {
			case model.WorkflowStatusCompleted:
				outputs := collectCompletedOutputs(state.CompletedTasks)
				resultCh <- subWorkflowResult{outputs: outputs}
				return
			case model.WorkflowStatusFailed, model.WorkflowStatusRolledBack:
				resultCh <- subWorkflowResult{err: fmt.Errorf("子工作流 %s 执行失败: %s", subWf.UUID, state.Error)}
				return
			}
		}
	}
}

// ExecuteFanOut 执行 Fan-Out 动态并行
// 从上游输出中提取列表，为每个元素并行执行任务模板，全部完成后汇聚结果
//
// node: Fan-Out 节点（含 FanOutDef）
// workflowUUID: 所属工作流 UUID
// upstreamOutputs: 上游任务输出
// callback: Fan-Out 完成后的回调，接收汇聚后的输出
func (ae *AdvancedExecutor) ExecuteFanOut(
	ctx context.Context,
	workflowUUID string,
	node model.TaskNode,
	upstreamOutputs []map[string]any,
	callback func(output map[string]any, err error),
) {
	fanOut := node.FanOut
	if fanOut == nil {
		callback(nil, fmt.Errorf("Fan-Out 定义为空"))
		return
	}

	// 从上游输出中提取迭代列表
	items := extractIteratorList(upstreamOutputs, fanOut.IteratorKey)
	if items == nil {
		callback(nil, fmt.Errorf("Fan-Out: 上游输出中未找到 key=%q 的列表", fanOut.IteratorKey))
		return
	}

	if len(items) == 0 {
		logger.Info("Fan-Out: 列表为空，跳过执行",
			"workflow", workflowUUID,
			"iterator_key", fanOut.IteratorKey,
		)
		callback(map[string]any{}, nil)
		return
	}

	logger.Info("开始 Fan-Out 动态并行执行",
		"workflow", workflowUUID,
		"task_name", node.Name,
		"iterator_key", fanOut.IteratorKey,
		"item_count", len(items),
		"max_parallel", fanOut.MaxParallel,
	)

	// 创建 span
	fanCtx, fanSpan := tracing.StartSpan(ctx, "scheduler.fan_out_execute",
		tracing.StringAttr("workflow.uuid", workflowUUID),
		tracing.StringAttr("fan_out.task", node.Name),
		tracing.IntAttr("fan_out.items", len(items)),
		tracing.IntAttr("fan_out.max_parallel", fanOut.MaxParallel),
	)
	defer fanSpan.End()

	// 限制并行度
	maxParallel := fanOut.MaxParallel
	if maxParallel <= 0 {
		maxParallel = len(items)
	}

	// 使用 semaphore 控制并行度
	sem := make(chan struct{}, maxParallel)
	var mu sync.Mutex
	var allResults []map[string]any
	var firstErr error

	var wg sync.WaitGroup
	for i, item := range items {
		wg.Add(1)
		go func(idx int, item any) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			// 为每个元素构建独立的上游输出
			itemOutput := buildItemOutput(idx, item, upstreamOutputs)

			// 执行任务模板
			resultCh := make(chan parallelTaskState, 1)

			// 使用共享的 ParallelExecutor 执行单个任务
			ae.parallelExec.Execute(fanCtx, workflowUUID, []model.TaskNode{fanOut.Task}, []map[string]any{itemOutput}, func(allOutputs []map[string]any, err error) {
				if err != nil {
					resultCh <- parallelTaskState{taskName: fmt.Sprintf("%s[%d]", node.Name, idx), err: err}
				} else {
					var output map[string]any
					if len(allOutputs) > 0 {
						output = allOutputs[0]
					}
					resultCh <- parallelTaskState{taskName: fmt.Sprintf("%s[%d]", node.Name, idx), output: output}
				}
			})

			result := <-resultCh
			if result.err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = result.err
					tracing.RecordError(fanCtx, result.err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			if result.output != nil {
				allResults = append(allResults, result.output)
			}
			mu.Unlock()
		}(i, item)
	}

	wg.Wait()

	if firstErr != nil {
		callback(nil, firstErr)
		return
	}

	// 汇聚结果
	outputKey := fanOut.OutputKey
	if outputKey == "" {
		outputKey = model.DefaultFanOutOutputKey
	}

	result := map[string]any{
		outputKey: allResults,
		"_count":   len(allResults),
	}

	logger.Info("Fan-Out 执行完成",
		"workflow", workflowUUID,
		"task_name", node.Name,
		"success_count", len(allResults),
		"total_items", len(items),
	)

	callback(result, nil)
}

// ExecuteLoop 执行循环任务组
// 在 SerialExecutor 的回调中调用，循环执行指定任务组直到条件不满足或达到最大迭代次数
//
// workflowUUID: 工作流 UUID
// groups: 完整的 TaskGroups（用于从 startGroupIdx 开始循环执行）
// startGroupIdx: 循环起始的任务组索引（0-based，循环体开始的组）
// loopEndGroupIdx: 循环体的结束组索引（包含），循环体从 startGroupIdx 到 loopEndGroupIdx
// loopDef: 循环定义
// initialUpstream: 初始上游输出
// afterLoop: 循环全部结束后的回调，传递最后一轮的输出
func (ae *AdvancedExecutor) ExecuteLoop(
	ctx context.Context,
	workflowUUID string,
	groups [][]model.TaskNode,
	startGroupIdx int,
	loopEndGroupIdx int,
	loopDef *model.LoopDef,
	initialUpstream []map[string]any,
	afterLoop func(outputs []map[string]any, err error),
) {
	logger.Info("开始循环执行",
		"workflow", workflowUUID,
		"start_group", startGroupIdx+1,
		"end_group", loopEndGroupIdx+1,
		"max_iterations", loopDef.MaxIterations,
		"condition_type", loopDef.ConditionType,
	)

	// 创建 span
	loopCtx, loopSpan := tracing.StartSpan(ctx, "scheduler.loop_execute",
		tracing.StringAttr("workflow.uuid", workflowUUID),
		tracing.IntAttr("loop.max_iterations", loopDef.MaxIterations),
		tracing.StringAttr("loop.condition_type", string(loopDef.ConditionType)),
	)
	defer loopSpan.End()

	// 启动异步循环执行
	ae.wg.Add(1)
	go func() {
		defer ae.wg.Done()
		ae.runLoop(loopCtx, workflowUUID, groups, startGroupIdx, loopEndGroupIdx, loopDef, initialUpstream, afterLoop)
	}()
}

// runLoop 循环执行的核心逻辑
func (ae *AdvancedExecutor) runLoop(
	ctx context.Context,
	workflowUUID string,
	groups [][]model.TaskNode,
	startGroupIdx int,
	loopEndGroupIdx int,
	loopDef *model.LoopDef,
	upstreamOutputs []map[string]any,
	afterLoop func(outputs []map[string]any, err error),
) {
	currentOutputs := upstreamOutputs

	for iteration := 0; iteration < loopDef.MaxIterations; iteration++ {
		select {
		case <-ctx.Done():
			afterLoop(currentOutputs, ctx.Err())
			return
		default:
		}

		logger.Info("循环迭代",
			"workflow", workflowUUID,
			"iteration", iteration+1,
			"max", loopDef.MaxIterations,
		)

		// 为每次迭代注入循环元数据到上游输出
		iterOutput := map[string]any{
			model.LoopIterationKey: iteration,
		}
		iterUpstream := make([]map[string]any, len(currentOutputs)+1)
		copy(iterUpstream, currentOutputs)
		iterUpstream[len(currentOutputs)] = iterOutput

		// 执行循环体（startGroupIdx 到 loopEndGroupIdx）
		loopDone := make(chan loopIterationResult, 1)

		ae.wg.Add(1)
		go func() {
			defer ae.wg.Done()
			ae.executeLoopBody(ctx, workflowUUID, groups, startGroupIdx, loopEndGroupIdx, iterUpstream, loopDone)
		}()

		var result loopIterationResult
		select {
		case <-ctx.Done():
			afterLoop(currentOutputs, ctx.Err())
			return
		case result = <-loopDone:
		}

		if result.err != nil {
			afterLoop(currentOutputs, result.err)
			return
		}

		// 合并本轮输出
		mergedOutputs := make([]map[string]any, 0, len(currentOutputs)+len(result.outputs))
		mergedOutputs = append(mergedOutputs, currentOutputs...)
		mergedOutputs = append(mergedOutputs, result.outputs...)
		currentOutputs = mergedOutputs

		// 检查循环条件
		if !shouldContinueLoop(result.lastOutput, loopDef, iteration+1) {
			logger.Info("循环条件不满足，退出循环",
				"workflow", workflowUUID,
				"iteration", iteration+1,
			)
			break
		}
	}

	logger.Info("循环执行完成",
		"workflow", workflowUUID,
		"final_outputs", len(currentOutputs),
	)

	afterLoop(currentOutputs, nil)
}

// loopIterationResult 单次循环迭代的执行结果
type loopIterationResult struct {
	outputs    []map[string]any
	lastOutput map[string]any // 最后一个任务组的输出（用于检查循环条件）
	err        error
}

// executeLoopBody 执行循环体（从 startGroupIdx 到 loopEndGroupIdx）
func (ae *AdvancedExecutor) executeLoopBody(
	ctx context.Context,
	workflowUUID string,
	groups [][]model.TaskNode,
	startGroupIdx int,
	loopEndGroupIdx int,
	upstreamOutputs []map[string]any,
	resultCh chan<- loopIterationResult,
) {
	// 构建循环体的任务组切片
	loopGroups := groups[startGroupIdx : loopEndGroupIdx+1]

	// 使用 SerialExecutor 串行执行循环体内的任务组
	state := ae.wfStore.GetWorkflowState(workflowUUID)
	if state == nil {
		resultCh <- loopIterationResult{err: fmt.Errorf("工作流 %s 不存在", workflowUUID)}
		return
	}

	// 手动执行循环体的各组
	var allOutputs []map[string]any
	var lastOutputMap map[string]any

	for gi, group := range loopGroups {
		groupDone := make(chan []map[string]any, 1)
		groupErr := make(chan error, 1)

		// 获取当前组的超时配置
		taskTimeoutSec := defaultTaskWatchdogTimeoutSec
		if state != nil && state.Workflow != nil {
			if t := state.Workflow.TaskTimeoutSec; t > 0 {
				if t*3 > taskTimeoutSec {
					taskTimeoutSec = t * 3
				}
			}
		}

		ae.parallelExec.ExecuteWithOptions(ctx, workflowUUID, group, allOutputs, ParallelOptions{
			Strategy:       ParallelStrategyAll,
			TaskTimeoutSec: taskTimeoutSec,
		}, func(grpOutputs []map[string]any, execErr error) {
			if execErr != nil {
				groupErr <- execErr
				return
			}
			groupDone <- grpOutputs
		})

		select {
		case <-ctx.Done():
			resultCh <- loopIterationResult{err: ctx.Err()}
			return
		case err := <-groupErr:
			resultCh <- loopIterationResult{err: err}
			return
		case grpOutputs := <-groupDone:
			merged := make([]map[string]any, 0, len(allOutputs)+len(grpOutputs))
			merged = append(merged, allOutputs...)
			merged = append(merged, grpOutputs...)
			allOutputs = merged
			// 获取最后一个任务的输出用于条件判断
			if len(grpOutputs) > 0 {
				lastOutputMap = grpOutputs[len(grpOutputs)-1]
			}
			_ = gi
		}

		// 检查组间控制流（审批、分支、跳过）— 简化处理，循环体内不支持分支
		if lastOutputMap != nil {
			wctx := model.NewContextFromMap(lastOutputMap)
			skipGroups := wctx.GetSkipGroups()
			if skipGroups > 0 {
				// 跳过循环体内后续组
				remaining := (loopEndGroupIdx + 1) - (startGroupIdx + gi + 1)
				if skipGroups >= remaining {
					break
				}
			}
		}
	}

	resultCh <- loopIterationResult{
		outputs:    allOutputs,
		lastOutput: lastOutputMap,
	}
}

// ============================================================================
// 辅助函数
// ============================================================================

// extractIteratorList 从上游输出中提取列表
// 支持从所有上游输出中查找指定的 key
func extractIteratorList(upstreamOutputs []map[string]any, key string) []any {
	// 先从最近的输出中查找（后写入的优先）
	for i := len(upstreamOutputs) - 1; i >= 0; i-- {
		if upstreamOutputs[i] == nil {
			continue
		}
		if val, ok := upstreamOutputs[i][key]; ok {
			if list, ok := val.([]any); ok {
				return list
			}
			// 处理 []interface{} 的情况
			if list, ok := val.([]interface{}); ok {
				result := make([]any, len(list))
				for i, v := range list {
					result[i] = v
				}
				return result
			}
		}
	}
	return nil
}

// buildItemOutput 为 Fan-Out 的每个元素构建独立的上游输出
// 包含当前元素的索引和值，以及原始上游输出的副本
func buildItemOutput(index int, item any, upstreamOutputs []map[string]any) map[string]any {
	output := make(map[string]any)
	// 注入元素索引
	output["_index"] = index
	// 注入元素值
	output["_item"] = item
	// 如果元素是 map，展开到顶层
	if m, ok := item.(map[string]any); ok {
		for k, v := range m {
			output[k] = v
		}
	}
	// 合并上游输出（上游数据作为背景）
	for _, up := range upstreamOutputs {
		for k, v := range up {
			if _, exists := output[k]; !exists {
				output[k] = v
			}
		}
	}
	return output
}

// shouldContinueLoop 检查是否应该继续下一次循环迭代
func shouldContinueLoop(lastOutput map[string]any, loopDef *model.LoopDef, currentIteration int) bool {
	if currentIteration >= loopDef.MaxIterations {
		return false
	}

	switch loopDef.ConditionType {
	case model.LoopConditionAlways:
		return true
	case model.LoopConditionKey:
		if loopDef.ConditionKey == "" {
			return true
		}
		if lastOutput == nil {
			return true
		}
		// 检查条件 key 是否存在且为 false
		if val, ok := lastOutput[loopDef.ConditionKey]; ok {
			if b, ok := val.(bool); ok {
				return b
			}
		}
		// 也检查 LoopContinueKey（任务可通过 taskCtx.Set(LoopContinueKey, false) 退出循环）
		if val, ok := lastOutput[model.LoopContinueKey]; ok {
			if b, ok := val.(bool); ok {
				return b
			}
		}
		return true
	default:
		return true
	}
}

// collectCompletedOutputs 从 CompletedTasks map 中收集所有输出为 []map[string]any
func collectCompletedOutputs(completed map[string]map[string]any) []map[string]any {
	outputs := make([]map[string]any, 0, len(completed))
	for _, output := range completed {
		outputs = append(outputs, output)
	}
	return outputs
}
