package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/tracing"
)

// ============================================================================
// ExecutionManager — 执行管理器
// 负责协调 SerialExecutor 和 ParallelExecutor，实现串并行混合编排
//
// 核心逻辑：
//   - 按 TaskGroups 顺序逐组执行（组间串行，组内并行）
//   - 统一处理所有控制流：审批暂停、条件分支、条件跳过、延迟执行
//   - 支持高级编排：Sub-Workflow（子工作流）、Fan-Out/Fan-In（动态并行）、While/Loop（循环）
//   - watchWorkflowStatus 仅负责通知和状态记录，不再直接推进工作流
// ============================================================================

// ExecutionManager 工作流执行管理器
type ExecutionManager struct {
	serial   *SerialExecutor
	parallel *ParallelExecutor
	advanced *AdvancedExecutor
}

// NewExecutionManager 创建执行管理器
// 注入窄接口而非 *Scheduler
func NewExecutionManager(
	wfStore WorkflowStateAccessor,
	taskPusher TaskPusher,
	snapshotMgr SnapshotManager,
	listenerMgr TaskListenerManager,
	eventBroadcaster EventBroadcaster,
	wg GoroutineTracker,
	coordinator WorkflowCoordinator,
	eventTrigger EventTriggerCoordinator,
) *ExecutionManager {
	parallel := NewParallelExecutor(taskPusher, listenerMgr)
	advanced := NewAdvancedExecutor(wfStore, taskPusher, snapshotMgr, listenerMgr, eventBroadcaster, coordinator, wg)
	return &ExecutionManager{
		serial:   NewSerialExecutor(wfStore, taskPusher, snapshotMgr, eventBroadcaster, wg, parallel, advanced, eventTrigger),
		parallel: parallel,
		advanced: advanced,
	}
}

// ExecuteWorkflow 启动工作流的完整执行流程
func (m *ExecutionManager) ExecuteWorkflow(ctx context.Context, workflowUUID string, upstreamOutputs []map[string]any, startGroupIdx int) {
	m.serial.Execute(ctx, workflowUUID, upstreamOutputs, startGroupIdx)
}

// ============================================================================
// SerialExecutor — 串行执行器
// ============================================================================

type SerialExecutor struct {
	wfStore          WorkflowStateAccessor
	taskPusher       TaskPusher
	snapshotMgr      SnapshotManager
	eventBroadcaster EventBroadcaster
	wg               GoroutineTracker
	parallel         *ParallelExecutor
	advanced         *AdvancedExecutor
	eventTrigger     EventTriggerCoordinator // 事件触发协调器（工作流完成/失败时触发事件链）
}

func NewSerialExecutor(
	wfStore WorkflowStateAccessor,
	taskPusher TaskPusher,
	snapshotMgr SnapshotManager,
	eventBroadcaster EventBroadcaster,
	wg GoroutineTracker,
	p *ParallelExecutor,
	adv *AdvancedExecutor,
	eventTrigger EventTriggerCoordinator,
) *SerialExecutor {
	return &SerialExecutor{
		wfStore:          wfStore,
		taskPusher:       taskPusher,
		snapshotMgr:      snapshotMgr,
		eventBroadcaster: eventBroadcaster,
		wg:               wg,
		parallel:         p,
		advanced:         adv,
		eventTrigger:     eventTrigger,
	}
}

// Execute 按 TaskGroup 顺序逐组执行
func (s *SerialExecutor) Execute(ctx context.Context, workflowUUID string, upstreamOutputs []map[string]any, startGroupIdx int) {
	state := s.wfStore.GetWorkflowState(workflowUUID)
	if state == nil {
		logger.Warn("串行执行器：工作流不存在", "workflow", workflowUUID)
		return
	}

	groups := state.Workflow.TaskGroups
	s.executeGroups(ctx, workflowUUID, groups, upstreamOutputs, startGroupIdx)
}

// advanceWorkflow 处理任务组完成后的控制流
func (s *SerialExecutor) advanceWorkflow(
	ctx context.Context,
	workflowUUID string,
	state *model.WorkflowState,
	groups [][]model.TaskNode,
	currentGroupIdx int,
	groupOutputs []map[string]any,
	mergedOutputs []map[string]any,
) {
	for _, output := range groupOutputs {
		wctx := model.NewContextFromMap(output)

		approvalMsg := wctx.GetApproval()
		if approvalMsg != "" {
			s.pauseForApproval(workflowUUID, state, currentGroupIdx, output, approvalMsg)
			return
		}

		branchName := wctx.GetBranch()
		if branchName != "" {
			currentNode := s.findTaskNodeWithBranches(groups, currentGroupIdx)
			if currentNode != nil {
				if branchGroups, exists := currentNode.Branches[branchName]; exists {
					logger.Info("条件分支：走指定分支",
						"workflow", workflowUUID,
						"from_group", currentGroupIdx+1,
						"branch", branchName,
						"branch_groups", len(branchGroups),
					)
					s.pushBranchGroups(ctx, workflowUUID, branchGroups, groupOutputs, 0)
					return
				}

				// 增强分支：如果指定分支不存在但有 DefaultBranch，走默认分支
				if currentNode.DefaultBranch != "" {
					if defaultGroups, exists := currentNode.Branches[currentNode.DefaultBranch]; exists {
						logger.Info("条件分支：走默认分支",
							"workflow", workflowUUID,
							"from_group", currentGroupIdx+1,
							"requested", branchName,
							"default", currentNode.DefaultBranch,
						)
						s.pushBranchGroups(ctx, workflowUUID, defaultGroups, groupOutputs, 0)
						return
					}
				}

				logger.Warn("条件分支：未找到指定分支，回退到默认下一组",
					"workflow", workflowUUID,
					"from_group", currentGroupIdx+1,
					"branch", branchName,
					"available", s.branchNames(currentNode.Branches),
				)
			}
		}

		skipGroups := wctx.GetSkipGroups()
		if skipGroups > 0 {
			nextIdx := currentGroupIdx + 1 + skipGroups
			if nextIdx >= len(groups) {
				logger.Info("条件跳过：工作流完成",
					"workflow", workflowUUID,
					"from_group", currentGroupIdx+1,
					"skipped_groups", skipGroups,
				)
				s.finishWorkflow(workflowUUID)
				return
			}
			logger.Info("条件跳过任务组",
				"workflow", workflowUUID,
				"from_group", currentGroupIdx+1,
				"skipped_groups", skipGroups,
				"to_group", nextIdx+1,
			)
			s.advanceToGroup(ctx, workflowUUID, groups, nextIdx, groupOutputs)
			return
		}
	}

	nextIdx := currentGroupIdx + 1
	if nextIdx >= len(groups) {
		s.finishWorkflow(workflowUUID)
		return
	}

	s.advanceToGroup(ctx, workflowUUID, groups, nextIdx, mergedOutputs)
}

func (s *SerialExecutor) advanceToGroup(
	ctx context.Context,
	workflowUUID string,
	groups [][]model.TaskNode,
	groupIdx int,
	upstreamOutputs []map[string]any,
) {
	nextGroup := groups[groupIdx]

	var delaySec int64
	for _, nt := range nextGroup {
		if nt.DelaySec > delaySec {
			delaySec = nt.DelaySec
		}
	}

	if delaySec > 0 {
		logger.Info("任务组延迟执行",
			"workflow", workflowUUID,
			"to_group", groupIdx+1,
			"delay_sec", delaySec,
		)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			select {
			case <-time.After(time.Duration(delaySec) * time.Second):
			case <-ctx.Done():
				return
			}
			s.executeGroups(ctx, workflowUUID, groups, upstreamOutputs, groupIdx)
		}()
	} else {
		s.executeGroups(ctx, workflowUUID, groups, upstreamOutputs, groupIdx)
	}
}

func (s *SerialExecutor) pauseForApproval(
	workflowUUID string,
	state *model.WorkflowState,
	groupIdx int,
	output map[string]any,
	approvalMsg string,
) {
	taskName := s.findTaskNameInOutput(state.Workflow.TaskGroups, groupIdx)

	entry := s.wfStore.LockEntry(workflowUUID)
	if entry == nil {
		return
	}
	entry.state.Status = model.WorkflowStatusPaused
	entry.state.PausedTaskName = taskName
	entry.state.PausedTaskOutput = output
	entry.state.PausedGroupIdx = groupIdx
	entry.mu.Unlock()

	s.snapshotMgr.SnapshotRunningWorkflow(workflowUUID)

	logger.Info("工作流暂停，等待人工审批",
		"workflow", workflowUUID,
		"task", taskName,
		"message", approvalMsg,
	)

	s.eventBroadcaster.BroadcastWorkflowEventToWorkflow(workflowUUID, model.EventWorkflowPaused, map[string]any{
		"workflow": workflowUUID,
		"task":     taskName,
		"message":  approvalMsg,
	})
}

func (s *SerialExecutor) finishWorkflow(workflowUUID string) {
	logger.Info("工作流完成", "workflow", workflowUUID)
	entry := s.wfStore.LockEntry(workflowUUID)
	if entry != nil {
		entry.state.Status = model.WorkflowStatusCompleted
		s.wfStore.PersistWorkflowLocked(workflowUUID, entry.state, entry)
		entry.mu.Unlock()
	}
	s.eventBroadcaster.BroadcastWorkflowEventToWorkflow(workflowUUID, model.EventWorkflowCompleted, workflowUUID)

	// 触发 workflow_completed 事件链
	if s.eventTrigger != nil {
		s.eventTrigger.FireWorkflowCompleted(workflowUUID, func(targetUUID string) {
			logger.Info("事件链：工作流完成触发目标工作流",
				"source", workflowUUID,
				"target", targetUUID,
			)
		})
	}
}

func (s *SerialExecutor) findTaskNodeWithBranches(groups [][]model.TaskNode, groupIdx int) *model.TaskNode {
	if groupIdx < 0 || groupIdx >= len(groups) {
		return nil
	}
	for i := range groups[groupIdx] {
		if groups[groupIdx][i].Branches != nil {
			return &groups[groupIdx][i]
		}
	}
	return nil
}

func (s *SerialExecutor) findTaskNameInOutput(groups [][]model.TaskNode, groupIdx int) string {
	if groupIdx < 0 || groupIdx >= len(groups) {
		return ""
	}
	if len(groups[groupIdx]) > 0 {
		return groups[groupIdx][0].Name
	}
	return ""
}

func (s *SerialExecutor) pushBranchGroups(ctx context.Context, workflowUUID string, branchGroups [][]model.TaskNode, upstreamOutputs []map[string]any, groupIdx int) {
	if groupIdx >= len(branchGroups) {
		logger.Info("分支执行完成", "workflow", workflowUUID, "branch_groups", len(branchGroups))
		s.finishWorkflow(workflowUUID)
		return
	}

	currentGroup := branchGroups[groupIdx]

	var delaySec int64
	for _, nt := range currentGroup {
		if nt.DelaySec > delaySec {
			delaySec = nt.DelaySec
		}
	}

	dispatchFn := func() {
		s.executeGroups(ctx, workflowUUID, branchGroups, upstreamOutputs, groupIdx)
	}

	if delaySec > 0 {
		logger.Info("分支任务组延迟执行",
			"workflow", workflowUUID,
			"branch_group", groupIdx+1,
			"delay_sec", delaySec,
		)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			select {
			case <-time.After(time.Duration(delaySec) * time.Second):
				dispatchFn()
			case <-ctx.Done():
			}
		}()
	} else {
		dispatchFn()
	}
}

func (s *SerialExecutor) executeGroups(ctx context.Context, workflowUUID string, groups [][]model.TaskNode, upstreamOutputs []map[string]any, startGroupIdx int) {
	if startGroupIdx >= len(groups) {
		s.finishWorkflow(workflowUUID)
		return
	}

	if upstreamOutputs == nil {
		upstreamOutputs = make([]map[string]any, 0)
	}

	// 创建 span：执行任务组
	groupCtx, groupSpan := tracing.StartSpan(ctx, "scheduler.execute_group",
		tracing.StringAttr("workflow.uuid", workflowUUID),
		tracing.IntAttr("group.index", startGroupIdx),
		tracing.IntAttr("group.total", len(groups)),
	)
	defer groupSpan.End()

	logger.Info("串行执行器：开始执行任务组",
		"workflow", workflowUUID,
		"group", startGroupIdx+1,
		"total_groups", len(groups),
	)

	// 从工作流状态获取任务超时配置，用于 Master 端超时监控
	taskTimeoutSec := defaultTaskWatchdogTimeoutSec
	if state := s.wfStore.GetWorkflowState(workflowUUID); state != nil && state.Workflow != nil {
		if t := state.Workflow.TaskTimeoutSec; t > 0 {
			// Worker 端超时 * 3 作为 Master 端兜底，给 Worker 充足的执行和重试时间
			if t*3 > taskTimeoutSec {
				taskTimeoutSec = t * 3
			}
		}
	}

	// 检查当前任务组是否包含高级节点类型（子工作流、Fan-Out、循环）
	currentGroup := groups[startGroupIdx]
	hasAdvancedNodes, advancedType := s.detectAdvancedNodes(currentGroup)

	if advancedType == "loop" {
		// 循环节点：整个任务组作为循环体
		s.executeLoopGroup(groupCtx, workflowUUID, groups, startGroupIdx, upstreamOutputs)
		return
	}

	if advancedType == "fan_out" || advancedType == "sub_workflow" {
		// Fan-Out 或 Sub-Workflow 节点：使用高级执行器
		s.executeAdvancedGroup(groupCtx, workflowUUID, groups, startGroupIdx, upstreamOutputs, currentGroup, taskTimeoutSec)
		return
	}

	// 如果组内混合了高级节点和普通节点，使用混合执行模式
	if hasAdvancedNodes {
		s.executeMixedGroup(groupCtx, workflowUUID, groups, startGroupIdx, upstreamOutputs, currentGroup, taskTimeoutSec)
		return
	}

	s.parallel.ExecuteWithOptions(groupCtx, workflowUUID, groups[startGroupIdx], upstreamOutputs, ParallelOptions{
		Strategy:       ParallelStrategyAll,
		TaskTimeoutSec: taskTimeoutSec,
	}, func(allOutputs []map[string]any, execErr error) {
		if execErr != nil {
			logger.Error("任务组执行失败，中止工作流",
				"workflow", workflowUUID,
				"group", startGroupIdx+1,
				"error", execErr,
			)
			entry := s.wfStore.LockEntry(workflowUUID)
			if entry != nil {
				entry.state.Status = model.WorkflowStatusFailed
				entry.state.Error = fmt.Sprintf("任务组 %d 执行失败: %v", startGroupIdx+1, execErr)
				s.wfStore.PersistWorkflowLocked(workflowUUID, entry.state, entry)
				entry.mu.Unlock()
			}
			s.eventBroadcaster.BroadcastWorkflowEventToWorkflow(workflowUUID, model.EventWorkflowFailed, workflowUUID)
			return
		}

		mergedOutputs := make([]map[string]any, 0, len(upstreamOutputs)+len(allOutputs))
		mergedOutputs = append(mergedOutputs, upstreamOutputs...)
		mergedOutputs = append(mergedOutputs, allOutputs...)

		state := s.wfStore.GetWorkflowState(workflowUUID)
		if state == nil {
			return
		}

		s.advanceWorkflow(ctx, workflowUUID, state, groups, startGroupIdx, allOutputs, mergedOutputs)
	})
}

// detectAdvancedNodes 检测任务组中是否包含高级节点类型
// 返回是否存在高级节点以及高级节点类型（优先返回 loop > fan_out > sub_workflow）
func (s *SerialExecutor) detectAdvancedNodes(group []model.TaskNode) (bool, string) {
	for _, node := range group {
		if node.Loop != nil {
			return true, "loop"
		}
		if model.GetTaskNodeType(node) == model.TaskNodeTypeFanOut && node.FanOut != nil {
			return true, "fan_out"
		}
		if model.GetTaskNodeType(node) == model.TaskNodeTypeSubWorkflow && node.SubWorkflow != "" {
			return true, "sub_workflow"
		}
	}
	return false, ""
}

// executeLoopGroup 执行包含循环的任务组
// 将循环体的输出合并后继续执行后续任务组
func (s *SerialExecutor) executeLoopGroup(
	ctx context.Context,
	workflowUUID string,
	groups [][]model.TaskNode,
	groupIdx int,
	upstreamOutputs []map[string]any,
) {
	currentGroup := groups[groupIdx]
	// 收集循环定义（组内可能有多个循环节点，取第一个）
	var loopDef *model.LoopDef
	for _, node := range currentGroup {
		if node.Loop != nil {
			loopDef = node.Loop
			break
		}
	}

	if loopDef == nil {
		// 没有找到循环定义，按普通任务组执行
		s.executeGroups(ctx, workflowUUID, groups, upstreamOutputs, groupIdx)
		return
	}

	// 循环体仅包含当前任务组（单组循环）
	s.advanced.ExecuteLoop(ctx, workflowUUID, groups, groupIdx, groupIdx, loopDef, upstreamOutputs,
		func(outputs []map[string]any, err error) {
			if err != nil {
				logger.Error("循环执行失败，中止工作流",
					"workflow", workflowUUID,
					"group", groupIdx+1,
					"error", err,
				)
				entry := s.wfStore.LockEntry(workflowUUID)
				if entry != nil {
					entry.state.Status = model.WorkflowStatusFailed
					entry.state.Error = fmt.Sprintf("循环任务组 %d 执行失败: %v", groupIdx+1, err)
					s.wfStore.PersistWorkflowLocked(workflowUUID, entry.state, entry)
					entry.mu.Unlock()
				}
				s.eventBroadcaster.BroadcastWorkflowEventToWorkflow(workflowUUID, model.EventWorkflowFailed, workflowUUID)
				return
			}

			mergedOutputs := make([]map[string]any, 0, len(upstreamOutputs)+len(outputs))
			mergedOutputs = append(mergedOutputs, upstreamOutputs...)
			mergedOutputs = append(mergedOutputs, outputs...)

			state := s.wfStore.GetWorkflowState(workflowUUID)
			if state == nil {
				return
			}

			s.advanceWorkflow(ctx, workflowUUID, state, groups, groupIdx, outputs, mergedOutputs)
		})
}

// executeAdvancedGroup 执行包含 Fan-Out 或 Sub-Workflow 的任务组
func (s *SerialExecutor) executeAdvancedGroup(
	ctx context.Context,
	workflowUUID string,
	groups [][]model.TaskNode,
	groupIdx int,
	upstreamOutputs []map[string]any,
	currentGroup []model.TaskNode,
	taskTimeoutSec int64,
) {
	// 分离高级节点和普通节点
	var normalNodes []model.TaskNode
	var advancedNodes []model.TaskNode
	for _, node := range currentGroup {
		nodeType := model.GetTaskNodeType(node)
		switch nodeType {
		case model.TaskNodeTypeSubWorkflow:
			advancedNodes = append(advancedNodes, node)
		case model.TaskNodeTypeFanOut:
			advancedNodes = append(advancedNodes, node)
		default:
			normalNodes = append(normalNodes, node)
		}
	}

	// 并行执行普通节点 + 高级节点
	var allOutputs []map[string]any
	var outputMu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	errOnce := sync.Once{}

	// 执行普通节点（如果有）
	if len(normalNodes) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.parallel.ExecuteWithOptions(ctx, workflowUUID, normalNodes, upstreamOutputs, ParallelOptions{
				Strategy:       ParallelStrategyAll,
				TaskTimeoutSec: taskTimeoutSec,
			}, func(outputs []map[string]any, err error) {
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				outputMu.Lock()
				if outputs != nil {
					allOutputs = append(allOutputs, outputs...)
				}
				outputMu.Unlock()
			})
		}()
	}

	// 执行高级节点
	for _, node := range advancedNodes {
		wg.Add(1)
		go func(n model.TaskNode) {
			defer wg.Done()
			nodeType := model.GetTaskNodeType(n)

			switch nodeType {
			case model.TaskNodeTypeSubWorkflow:
				s.advanced.ExecuteSubWorkflow(ctx, workflowUUID, n, upstreamOutputs, func(outputs []map[string]any, err error) {
					if err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
					outputMu.Lock()
					// 将子工作流输出合并，标记来源
					subOutput := map[string]any{
						"_sub_workflow": n.SubWorkflow,
						"_task_name":   n.Name,
					}
					for _, o := range outputs {
						for k, v := range o {
							subOutput[k] = v
						}
					}
					allOutputs = append(allOutputs, subOutput)
					outputMu.Unlock()
				})

			case model.TaskNodeTypeFanOut:
				s.advanced.ExecuteFanOut(ctx, workflowUUID, n, upstreamOutputs, func(output map[string]any, err error) {
					if err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
					outputMu.Lock()
					allOutputs = append(allOutputs, output)
					outputMu.Unlock()
				})
			}
		}(node)
	}

	// 等待所有节点完成
	go func() {
		wg.Wait()

		if firstErr != nil {
			logger.Error("高级任务组执行失败，中止工作流",
				"workflow", workflowUUID,
				"group", groupIdx+1,
				"error", firstErr,
			)
			entry := s.wfStore.LockEntry(workflowUUID)
			if entry != nil {
				entry.state.Status = model.WorkflowStatusFailed
				entry.state.Error = fmt.Sprintf("高级任务组 %d 执行失败: %v", groupIdx+1, firstErr)
				s.wfStore.PersistWorkflowLocked(workflowUUID, entry.state, entry)
				entry.mu.Unlock()
			}
			s.eventBroadcaster.BroadcastWorkflowEventToWorkflow(workflowUUID, model.EventWorkflowFailed, workflowUUID)
			return
		}

		mergedOutputs := make([]map[string]any, 0, len(upstreamOutputs)+len(allOutputs))
		mergedOutputs = append(mergedOutputs, upstreamOutputs...)
		mergedOutputs = append(mergedOutputs, allOutputs...)

		state := s.wfStore.GetWorkflowState(workflowUUID)
		if state == nil {
			return
		}

		s.advanceWorkflow(ctx, workflowUUID, state, groups, groupIdx, allOutputs, mergedOutputs)
	}()
}

// executeMixedGroup 执行混合了高级节点和普通节点的任务组
// 将所有节点统一通过高级执行路径处理
func (s *SerialExecutor) executeMixedGroup(
	ctx context.Context,
	workflowUUID string,
	groups [][]model.TaskNode,
	groupIdx int,
	upstreamOutputs []map[string]any,
	currentGroup []model.TaskNode,
	taskTimeoutSec int64,
) {
	// 统一使用 executeAdvancedGroup 处理
	s.executeAdvancedGroup(ctx, workflowUUID, groups, groupIdx, upstreamOutputs, currentGroup, taskTimeoutSec)
}

func (s *SerialExecutor) branchNames(branches model.BranchDef) []string {
	names := make([]string, 0, len(branches))
	for name := range branches {
		names = append(names, name)
	}
	return names
}

// ============================================================================
// ParallelExecutor — 并行执行器
// ============================================================================

// ParallelStrategy 并行执行策略
type ParallelStrategy int

const (
	// ParallelStrategyAll 所有任务都必须成功（默认，与原有行为一致）
	ParallelStrategyAll ParallelStrategy = iota
	// ParallelStrategyFailFast 第一个任务失败时立即取消其他任务
	ParallelStrategyFailFast
)

// ParallelOptions 并行执行选项
type ParallelOptions struct {
	// Strategy 并行执行策略，默认为 ParallelStrategyAll
	Strategy ParallelStrategy
	// MinSuccess 最小成功数：当成功任务数 >= MinSuccess 时视为整体成功
	// 0 表示不启用（必须全部成功）
	MinSuccess int
	// TaskTimeoutSec Master 端单任务超时监控秒数。当 Worker 崩溃或长时间未响应时，
	// Master 端通过此超时感知任务失败，触发重试。0 表示使用默认值。
	TaskTimeoutSec int64
}

type ParallelExecutor struct {
	taskPusher  TaskPusher
	listenerMgr TaskListenerManager
}

func NewParallelExecutor(taskPusher TaskPusher, listenerMgr TaskListenerManager) *ParallelExecutor {
	return &ParallelExecutor{
		taskPusher:  taskPusher,
		listenerMgr: listenerMgr,
	}
}

type parallelTaskState struct {
	taskName string
	output   map[string]any
	err      error
}

// Execute 并行执行一组任务（使用默认策略：所有任务必须成功）
func (p *ParallelExecutor) Execute(
	ctx context.Context,
	workflowName string,
	group []model.TaskNode,
	upstreamOutputs []map[string]any,
	onComplete func(allOutputs []map[string]any, err error),
) {
	p.ExecuteWithOptions(ctx, workflowName, group, upstreamOutputs, ParallelOptions{Strategy: ParallelStrategyAll}, onComplete)
}

// defaultTaskWatchdogTimeoutSec Master 端任务超时监控的默认超时（秒）
// Worker 端的 TaskExecutor.run 使用 context.WithTimeout 控制执行超时，
// 但如果 Worker 崩溃或网络断开，Master 无法感知。此值为 Master 端兜底超时。
// 设置为 Worker 端默认超时（30s）的 3 倍，给 Worker 充足的执行和重试时间。
const defaultTaskWatchdogTimeoutSec int64 = 90

// ExecuteWithOptions 使用指定策略并行执行一组任务
// 支持 failFast（第一个失败时取消其他任务）和 minSuccess（最小成功数）两种灵活策略
//
// Master 端超时监控：为每个任务添加独立的 watchdog goroutine。
// 当 Worker 崩溃或长时间未响应时，Master 端通过超时感知任务失败，触发重试。
// 超时时间 = max(任务配置的超时 * 3, defaultTaskWatchdogTimeoutSec)。
func (p *ParallelExecutor) ExecuteWithOptions(
	ctx context.Context,
	workflowName string,
	group []model.TaskNode,
	upstreamOutputs []map[string]any,
	opts ParallelOptions,
	onComplete func(allOutputs []map[string]any, err error),
) {
	if len(group) == 0 {
		onComplete(nil, nil)
		return
	}

	taskNames := make([]string, len(group))
	for i, n := range group {
		taskNames[i] = n.Name
	}

	// 创建 span：并行执行任务组
	parCtx, parSpan := tracing.StartSpan(ctx, "scheduler.execute_parallel",
		tracing.StringAttr("workflow.uuid", workflowName),
		tracing.IntAttr("group.task_count", len(group)),
	)
	defer parSpan.End()

	logger.Info("并行执行器：开始并行执行任务组",
		"workflow", workflowName,
		"tasks", taskNames,
		"count", len(group),
		"strategy", p.strategyLabel(opts),
	)

	// failFast 策略：创建可取消的子 context，第一个任务失败时取消所有其他任务
	var execCtx context.Context
	var cancel context.CancelFunc
	if opts.Strategy == ParallelStrategyFailFast {
		execCtx, cancel = context.WithCancel(parCtx)
		defer cancel()
	} else {
		execCtx = parCtx
	}

	resultCh := make(chan parallelTaskState, len(group))
	var wg sync.WaitGroup

	// 计算 Master 端单任务超时监控时间
	taskWatchdogSec := opts.TaskTimeoutSec
	if taskWatchdogSec <= 0 {
		taskWatchdogSec = defaultTaskWatchdogTimeoutSec
	}

	for _, node := range group {
		wg.Add(1)
		go func(n model.TaskNode) {
			defer wg.Done()
			p.executeSingleTask(execCtx, workflowName, n, upstreamOutputs, resultCh, taskWatchdogSec)
		}(node)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var allOutputs []map[string]any
	var firstErr error
	successCount := 0
	failedCount := 0
	total := len(group)

	minSuccess := opts.MinSuccess
	if minSuccess <= 0 {
		minSuccess = total // 默认全部成功
	}

	for result := range resultCh {
		if result.err != nil {
			failedCount++
			if firstErr == nil {
				firstErr = result.err
				tracing.RecordError(parCtx, result.err)
				logger.Error("并行任务执行失败",
					"workflow", workflowName,
					"task", result.taskName,
					"error", result.err,
				)

				// failFast：第一个任务失败时立即取消其他任务的 context
				if opts.Strategy == ParallelStrategyFailFast && cancel != nil {
					logger.Warn("快速失败：取消并行任务组中其他任务",
						"workflow", workflowName,
						"failed_task", result.taskName,
					)
					cancel()
				}
			}
		} else {
			successCount++
			if result.output != nil {
				allOutputs = append(allOutputs, result.output)
			}
		}
	}

	// 使用最小成功数判断整体是否成功
	if successCount >= minSuccess {
		logger.Info("并行执行器：任务组执行完成（满足最小成功数）",
			"workflow", workflowName,
			"success", successCount,
			"failed", failedCount,
			"total", total,
			"min_success", minSuccess,
			"outputs", len(allOutputs),
		)
		onComplete(allOutputs, nil)
	} else {
		err := firstErr
		if err == nil {
			err = fmt.Errorf("并行任务组执行失败：成功 %d/%d，需要至少 %d 个成功", successCount, total, minSuccess)
		}
		logger.Error("并行任务组执行失败（未达到最小成功数）",
			"workflow", workflowName,
			"success", successCount,
			"failed", failedCount,
			"total", total,
			"min_success", minSuccess,
		)
		onComplete(nil, err)
	}
}

func (p *ParallelExecutor) strategyLabel(opts ParallelOptions) string {
	switch opts.Strategy {
	case ParallelStrategyFailFast:
		return "failFast"
	default:
		return "all"
	}
}

func (p *ParallelExecutor) executeSingleTask(
	ctx context.Context,
	workflowName string,
	node model.TaskNode,
	upstreamOutputs []map[string]any,
	resultCh chan<- parallelTaskState,
	watchdogTimeoutSec int64,
) {
	if err := p.taskPusher.PushTask(ctx, workflowName, node, upstreamOutputs); err != nil {
		resultCh <- parallelTaskState{taskName: node.Name, err: err}
		return
	}

	taskEventCh := make(chan *store.TaskPayload, 1)
	p.listenerMgr.RegisterTaskListener(workflowName, node.Name, taskEventCh)
	defer p.listenerMgr.UnregisterTaskListener(workflowName, node.Name)

	// Master 端超时监控：Worker 端 TaskExecutor.run 使用 context.WithTimeout 控制执行超时，
	// 但如果 Worker 崩溃或网络断开，Master 无法感知。此 watchdog 提供兜底超时保护。
	// 超时时间由调用方通过 ParallelOptions.TaskTimeoutSec 传入，默认为 defaultTaskWatchdogTimeoutSec。
	watchdog := time.NewTimer(time.Duration(watchdogTimeoutSec) * time.Second)
	defer watchdog.Stop()

	select {
	case <-ctx.Done():
		resultCh <- parallelTaskState{
			taskName: node.Name,
			err:      fmt.Errorf("工作流上下文已取消: %w", ctx.Err()),
		}
	case <-watchdog.C:
		resultCh <- parallelTaskState{
			taskName: node.Name,
			err:      fmt.Errorf("Master 端任务超时：Worker 在 %d 秒内未响应", watchdogTimeoutSec),
		}
	case payload, ok := <-taskEventCh:
		if !ok {
			resultCh <- parallelTaskState{
				taskName: node.Name,
				err:      fmt.Errorf("任务监听通道已关闭"),
			}
			return
		}
		if payload.Status == store.StatusCompleted {
			resultCh <- parallelTaskState{
				taskName: node.Name,
				output:   payload.Output,
			}
		} else {
			resultCh <- parallelTaskState{
				taskName: node.Name,
				err:      fmt.Errorf("任务状态: %s, 错误: %s", payload.Status, payload.Result),
			}
		}
	}
}
