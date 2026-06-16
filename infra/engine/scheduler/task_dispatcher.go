package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/tracing"
)

// defaultPendingTaskTimeout 排队任务默认超时时间（秒），防止任务无限等待
const defaultPendingTaskTimeoutSec int64 = 300 // 5 分钟

// defaultTaskTimeoutSec 任务默认执行超时时间（秒），当工作流级别和全局配置均为 0 时兜底
const defaultTaskTimeoutSec int64 = 30

// defaultBaseBackoffSec 任务重试默认基础退避时间（秒），当工作流级别和全局配置均为 0 时兜底
const defaultBaseBackoffSec int64 = 1

// TaskDispatcher 任务下发器
// 职责：任务下发到 etcd、并发槽位管理（全局 + per-workflow 双层控制）、
//
//	优先队列待处理队列（含超时淘汰）、重试任务下发
//
// 依赖：store（etcd）、config（并发/超时配置）、workflowState（读取工作流状态）、
//
//	markFailed（标记工作流失败）、clearFailed（重试时清除失败标记）
//
// 并发设计：
//   - globalActive 使用 atomic.Int64，无锁原子操作
//   - perWfActive 使用 sync.Map[string]*atomic.Int64，per-workflow 独立原子计数器
//   - pendingPQ 使用独立的 pqMu 保护优先队列操作
//   - PushTask 的快速路径（无并发限制时）完全无锁
type TaskDispatcher struct {
	store  store.Store
	config ConfigProvider

	// 工作流状态查询（只读），由 WorkflowManager 注入
	workflowState WorkflowStateReader

	// 失败标记回调，由 Scheduler 协调层注入
	markWorkflowFailed func(payload *store.TaskPayload, errMsg string)

	// 重试时清除失败标记的回调
	clearFailedTask func(workflowUUID, taskName string)

	// --- 双层并发控制 ---
	// 全局计数器（无锁原子操作）
	globalActive atomic.Int64
	// per-workflow 计数器，每个 workflow 有独立的 atomic.Int64，无锁竞争
	perWfActive sync.Map // map[string]*atomic.Int64

	// --- 优先队列 ---
	pendingPQ pendingQueue
	pqMu      sync.Mutex // 仅保护 pendingPQ 的读写
}

// NewTaskDispatcher 创建任务下发器
func NewTaskDispatcher(s store.Store, cfg ConfigProvider, wfState WorkflowStateReader) *TaskDispatcher {
	return &TaskDispatcher{
		store:         s,
		config:        cfg,
		workflowState: wfState,
		pendingPQ:     make(pendingQueue, 0),
	}
}

// ============================================================================
// per-workflow 原子计数器辅助方法
// ============================================================================

// getOrCreatePerWfCounter 获取或创建 per-workflow 的原子计数器
func (td *TaskDispatcher) getOrCreatePerWfCounter(workflowUUID string) *atomic.Int64 {
	if v, ok := td.perWfActive.Load(workflowUUID); ok {
		return v.(*atomic.Int64)
	}
	counter := new(atomic.Int64)
	actual, _ := td.perWfActive.LoadOrStore(workflowUUID, counter)
	return actual.(*atomic.Int64)
}

// loadPerWfActive 读取 per-workflow 活跃计数，不存在则返回 0
func (td *TaskDispatcher) loadPerWfActive(workflowUUID string) int64 {
	if v, ok := td.perWfActive.Load(workflowUUID); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// incPerWfActive 增加 per-workflow 活跃计数，返回新值
func (td *TaskDispatcher) incPerWfActive(workflowUUID string) int64 {
	return td.getOrCreatePerWfCounter(workflowUUID).Add(1)
}

// decPerWfActive 减少 per-workflow 活跃计数，归零时删除条目以释放内存
func (td *TaskDispatcher) decPerWfActive(workflowUUID string) {
	counter := td.getOrCreatePerWfCounter(workflowUUID)
	newVal := counter.Add(-1)
	if newVal == 0 {
		td.perWfActive.Delete(workflowUUID)
	}
}

// CleanupPerWfCounter 工作流结束后强制清理 per-workflow 并发计数器
// 用于 PersistWorkflowLocked 中调用，确保即使计数器未归零（异常路径、槽位泄漏等），
// sync.Map 中的条目也会被清理，防止内存泄漏。
// 此方法是幂等的，可安全重复调用。
func (td *TaskDispatcher) CleanupPerWfCounter(workflowUUID string) {
	td.perWfActive.Delete(workflowUUID)
}

// SetFailureHandlers 注入失败处理回调（由 Scheduler 在初始化时调用，避免循环依赖）
func (td *TaskDispatcher) SetFailureHandlers(
	markFailed func(payload *store.TaskPayload, errMsg string),
	clearFailed func(workflowUUID, taskName string),
) {
	td.markWorkflowFailed = markFailed
	td.clearFailedTask = clearFailed
}

// ============================================================================
// TaskPusher 接口实现
// ============================================================================

// PushTask 尝试下发一个任务，若达到并发上限则入队等待
// 双层控制：
//   1. 先检查 per-workflow 并发上限（无锁原子读）
//   2. 再检查全局并发上限（无锁原子读）
//   3. 两个都通过则原子递增（无锁），失败则入队（仅 pqMu 保护）
// 优先级从 WorkflowState 中的 workflow 配置读取，或从 pending task 调用方传入
func (td *TaskDispatcher) PushTask(ctx context.Context, workflowUUID string, node model.TaskNode, upstreamOutputs []map[string]any) error {
	priority := td.getWorkflowPriority(workflowUUID)

	globalMax := td.config.MaxConcurrentTasks()
	perWfMax := td.config.PerWorkflowMaxConcurrency()

	if globalMax > 0 || perWfMax > 0 {
		// 快速路径：per-workflow 限制检查（无锁）
		if perWfMax > 0 {
			curWf := td.loadPerWfActive(workflowUUID)
			if curWf >= int64(perWfMax) {
				td.enqueuePending(workflowUUID, node, upstreamOutputs, priority)
				logger.Debug("任务已达工作流并发上限，进入等待队列",
					"workflow", workflowUUID, "task", node.Name,
					"per_wf_active", curWf, "per_wf_max", perWfMax,
				)
				return nil
			}
		}

		// 快速路径：全局限制检查（无锁）
		if globalMax > 0 {
			curGlobal := td.globalActive.Load()
			if curGlobal >= int64(globalMax) {
				td.enqueuePending(workflowUUID, node, upstreamOutputs, priority)
				logger.Debug("任务已达全局并发上限，进入等待队列",
					"workflow", workflowUUID, "task", node.Name,
					"global_active", curGlobal, "global_max", globalMax,
				)
				return nil
			}
		}

		// 尝试原子递增：先递增 per-wf，再递增 global
		// 如果 global 递增失败（达到上限），回退 per-wf
		newWf := td.incPerWfActive(workflowUUID)
		if perWfMax > 0 && newWf > int64(perWfMax) {
			// 其他 goroutine 抢先占用了 per-wf 槽位，回退并入队
			td.decPerWfActive(workflowUUID)
			td.enqueuePending(workflowUUID, node, upstreamOutputs, priority)
			logger.Debug("任务已达工作流并发上限（竞争），进入等待队列",
				"workflow", workflowUUID, "task", node.Name,
				"per_wf_active", newWf, "per_wf_max", perWfMax,
			)
			return nil
		}

		newGlobal := td.globalActive.Add(1)
		if globalMax > 0 && newGlobal > int64(globalMax) {
			// 其他 goroutine 抢先占用了全局槽位，回退并入队
			td.globalActive.Add(-1)
			td.decPerWfActive(workflowUUID)
			td.enqueuePending(workflowUUID, node, upstreamOutputs, priority)
			logger.Debug("任务已达全局并发上限（竞争），进入等待队列",
				"workflow", workflowUUID, "task", node.Name,
				"global_active", newGlobal, "global_max", globalMax,
			)
			return nil
		}
	}

	err := td.doPush(ctx, workflowUUID, node, upstreamOutputs)
	if err != nil {
		if globalMax > 0 {
			td.globalActive.Add(-1)
		}
		td.decPerWfActive(workflowUUID)
	}
	return err
}

// enqueuePending 将任务加入优先队列（pqMu 保护）
func (td *TaskDispatcher) enqueuePending(workflowUUID string, node model.TaskNode, upstreamOutputs []map[string]any, priority int) {
	td.pqMu.Lock()
	heap.Push(&td.pendingPQ, &pendingTask{
		workflowName:    workflowUUID,
		node:            node,
		upstreamOutputs: upstreamOutputs,
		priority:        priority,
		enqueued:        time.Now(),
	})
	td.pqMu.Unlock()
}

// getWorkflowPriority 获取工作流的优先级，默认为 0
func (td *TaskDispatcher) getWorkflowPriority(workflowUUID string) int {
	state := td.workflowState.GetWorkflowState(workflowUUID)
	if state != nil && state.Workflow != nil {
		return state.Workflow.Priority
	}
	return 0
}

// doPush 将任务写入 etcd（带 TTL）
func (td *TaskDispatcher) doPush(ctx context.Context, workflowUUID string, node model.TaskNode, upstreamOutputs []map[string]any) error {
	state := td.workflowState.GetWorkflowState(workflowUUID)
	if state == nil {
		return fmt.Errorf("工作流 %s 不存在", workflowUUID)
	}
	wf := state.Workflow

	timeoutSec := wf.TaskTimeoutSec
	if timeoutSec <= 0 {
		timeoutSec = td.config.DefaultTaskTimeoutSec()
	}
	if timeoutSec <= 0 {
		timeoutSec = defaultTaskTimeoutSec
	}
	maxRetries := wf.MaxRetries
	baseBackoff := wf.BaseBackoff
	if baseBackoff <= 0 {
		baseBackoff = td.config.DefaultBaseBackoffSec()
	}
	if baseBackoff <= 0 {
		baseBackoff = defaultBaseBackoffSec
	}

	traceID := NewTraceID()

	// 创建 span：任务下发
	taskCtx, taskSpan := tracing.StartSpan(ctx, "scheduler.dispatch_task",
		tracing.StringAttr("workflow.uuid", workflowUUID),
		tracing.StringAttr("task.name", node.Name),
		tracing.StringAttr("task.trace_id", traceID),
	)
	defer taskSpan.End()

	payload := &store.TaskPayload{
		WorkflowID:   workflowUUID,
		TaskName:     node.Name,
		TraceID:      traceID,
		TraceContext: tracing.InjectTraceContext(taskCtx),
		Status:       store.StatusPending,
		Priority:     wf.Priority,
		Params:       make(map[string]any),
		Input:        make(map[string]any),
		Output:       make(map[string]any),
		TimeoutSec:   timeoutSec,
		MaxRetries:   maxRetries,
		BaseBackoff:  baseBackoff,
	}

	// 输入映射优先：如果节点定义了 InputMapping，使用映射后的输入
	if mapped := model.ApplyInputMapping(node, upstreamOutputs); mapped != nil {
		for k, v := range mapped {
			payload.Input[k] = v
		}
	} else {
		// 默认全量合并
		for _, output := range upstreamOutputs {
			for k, v := range output {
				payload.Input[k] = v
			}
		}
	}

	if node.Params != nil {
		for k, v := range node.Params {
			payload.Params[k] = v
		}
	}

	val, err := store.Serialize(payload)
	if err != nil {
		tracing.RecordError(taskCtx, err)
		return fmt.Errorf("序列化任务失败 %s: %w", node.Name, err)
	}

	taskKey := taskKey(td.store.Prefixes().Tasks, workflowUUID, node.Name)
	if err := putWithTTL(td.store, ctx, taskKey, val, taskTTLSeconds); err != nil {
		tracing.RecordError(taskCtx, err)
		return fmt.Errorf("下发任务失败 %s: %w", node.Name, err)
	}

	taskSpan.SetAttributes(
		tracing.Int64Attr("task.timeout_sec", timeoutSec),
		tracing.IntAttr("task.max_retries", maxRetries),
		tracing.IntAttr("task.priority", wf.Priority),
	)

	logger.Info("任务已下发",
		"trace_id", payload.TraceID,
		"workflow", workflowUUID,
		"task", node.Name,
		"priority", wf.Priority,
	)
	return nil
}

// batchPush 批量下发多个任务到 etcd，共享同一个 lease
// 同一批任务使用一次 etcd Grant + Txn，相比逐个 doPush 减少 lease 管理开销
// 高并发场景下（大量工作流同时启动），此优化显著减少 etcd 压力
//
// 优化：按 workflowName 缓存 GetWorkflowState 查询结果，避免批量操作中
// 对同一工作流的多个任务重复 RLock 查询，减少锁竞争和 map 查找开销。
//
// 槽位释放修复：维护 occupied 切片记录每个任务是否成功占用了槽位，
// 批量写入失败时仅释放 occupied[i]==true 的任务，避免对已被 continue 跳过
// （工作流不存在、序列化失败）的任务重复调用 decPerWfActive/globalActive.Add(-1)。
func (td *TaskDispatcher) batchPush(ctx context.Context, tasks []*pendingTask, hasGlobalLimit bool) {
	if len(tasks) == 0 {
		return
	}

	kvs := make([]store.KeyValue, 0, len(tasks))
	// occupied[i] == true 表示 tasks[i] 成功占用了槽位（未在序列化阶段被 continue 跳过）
	occupied := make([]bool, len(tasks))
	taskInfos := make([]struct {
		workflowName string
		taskName     string
	}, len(tasks))

	// 按 workflowName 缓存 GetWorkflowState 查询结果，
	// 避免同一批次中对同一工作流的多个任务重复调用 GetWorkflowState（RLock + map 查找）
	wfStateCache := make(map[string]*model.Workflow)

	for i, pt := range tasks {
		wf, cached := wfStateCache[pt.workflowName]
		if !cached {
			state := td.workflowState.GetWorkflowState(pt.workflowName)
			if state == nil {
				logger.Error("批量下发：工作流不存在", "workflow", pt.workflowName)
				// 不释放槽位：此任务未占用槽位（槽位已在 tryDispatchPending 中预占）
				continue
			}
			wf = state.Workflow
			wfStateCache[pt.workflowName] = wf
		}

		timeoutSec := wf.TaskTimeoutSec
		if timeoutSec <= 0 {
			timeoutSec = td.config.DefaultTaskTimeoutSec()
		}
		if timeoutSec <= 0 {
			timeoutSec = defaultTaskTimeoutSec
		}
		maxRetries := wf.MaxRetries
		baseBackoff := wf.BaseBackoff
		if baseBackoff <= 0 {
			baseBackoff = td.config.DefaultBaseBackoffSec()
		}
		if baseBackoff <= 0 {
			baseBackoff = defaultBaseBackoffSec
		}

		traceID := NewTraceID()
		payload := &store.TaskPayload{
			WorkflowID:  pt.workflowName,
			TaskName:    pt.node.Name,
			TraceID:     traceID,
			Status:      store.StatusPending,
			Priority:    wf.Priority,
			Params:      make(map[string]any),
			Input:       make(map[string]any),
			Output:      make(map[string]any),
			TimeoutSec:  timeoutSec,
			MaxRetries:  maxRetries,
			BaseBackoff: baseBackoff,
		}

		// 输入映射优先：如果节点定义了 InputMapping，使用映射后的输入（与 doPush 保持一致）
		if mapped := model.ApplyInputMapping(pt.node, pt.upstreamOutputs); mapped != nil {
			for k, v := range mapped {
				payload.Input[k] = v
			}
		} else {
			// 默认全量合并
			for _, output := range pt.upstreamOutputs {
				for k, v := range output {
					payload.Input[k] = v
				}
			}
		}
		if pt.node.Params != nil {
			for k, v := range pt.node.Params {
				payload.Params[k] = v
			}
		}

		val, err := store.Serialize(payload)
		if err != nil {
			logger.Error("批量下发：序列化失败", "workflow", pt.workflowName, "task", pt.node.Name, "error", err)
			// 序列化失败：释放此任务的槽位（它在 tryDispatchPending 中已预占）
			if hasGlobalLimit {
				td.globalActive.Add(-1)
			}
			td.decPerWfActive(pt.workflowName)
			continue
		}

		taskKey := taskKey(td.store.Prefixes().Tasks, pt.workflowName, pt.node.Name)
		kvs = append(kvs, store.KeyValue{Key: taskKey, Value: val})
		taskInfos[i].workflowName = pt.workflowName
		taskInfos[i].taskName = pt.node.Name
		occupied[i] = true // 标记此任务成功占用槽位
	}

	if len(kvs) == 0 {
		return
	}

	if err := td.store.BatchPutWithTTL(ctx, kvs, taskTTLSeconds); err != nil {
		logger.Error("批量下发任务失败，释放槽位", "count", len(kvs), "error", err)
		for _, kv := range kvs {
			// 解析 workflow 和 task 信息用于日志
			logger.Error("批量下发失败：任务", "key", kv.Key)
		}
		// 批量失败时，仅释放 occupied[i]==true 的任务槽位
		for i, pt := range tasks {
			if !occupied[i] {
				continue // 此任务在序列化阶段已被跳过，未占用槽位
			}
			if hasGlobalLimit {
				td.globalActive.Add(-1)
			}
			td.decPerWfActive(pt.workflowName)
		}
		return
	}

	logger.Info("批量任务下发完成", "count", len(kvs))
}

// ReleaseSlot 释放一个并发槽位，从优先队列中取出最高优先级任务执行
// 仅释放全局槽位（不区分 workflow）
func (td *TaskDispatcher) ReleaseSlot(ctx context.Context) {
	if td.config.MaxConcurrentTasks() > 0 {
		td.globalActive.Add(-1)
	}
	td.tryDispatchPending(ctx)
}

// ReleaseSlotForWorkflow 释放指定工作流的一个并发槽位
func (td *TaskDispatcher) ReleaseSlotForWorkflow(ctx context.Context, workflowUUID string) {
	if td.config.MaxConcurrentTasks() > 0 {
		td.globalActive.Add(-1)
	}
	td.decPerWfActive(workflowUUID)
	td.tryDispatchPending(ctx)
}

// tryDispatchPending 尝试从优先队列中取出任务下发
// 使用 pqMu 保护队列操作，计数器的增减使用原子操作
//
// 对于 per-wf 上限达标的 workflow 任务：不弹出也不丢弃，
// 收集到 deferred 列表，循环结束后重新放回优先队列。
// 等对应 workflow 的槽位释放后，下一次 tryDispatchPending 会自然处理。
func (td *TaskDispatcher) tryDispatchPending(ctx context.Context) {
	globalMax := td.config.MaxConcurrentTasks()
	perWfMax := td.config.PerWorkflowMaxConcurrency()
	timeoutSec := td.getPendingTimeoutSec()

	var toPush []*pendingTask

	// 阶段 1：持有 pqMu，从队列中筛选可下发的任务
	td.pqMu.Lock()
	// 淘汰超时任务
	td.evictTimedOutLocked(timeoutSec)

	// blocked 的任务暂存于此，循环结束后重新入队
	var deferred []*pendingTask
	// 记录已达 per-wf 上限的工作流，用于跳过同 workflow 的后续任务
	blockedWf := make(map[string]bool)

	for td.pendingPQ.Len() > 0 {
		// 检查全局限制
		if globalMax > 0 && td.globalActive.Load() >= int64(globalMax) {
			break
		}

		// 查看队首任务（最高优先级）
		pt := td.pendingPQ[0]

		// 检查 per-workflow 限制
		if perWfMax > 0 && td.loadPerWfActive(pt.workflowName) >= int64(perWfMax) {
			// 该工作流已达上限：弹出任务并暂存到 deferred，等槽位释放后重新检查
			blockedWf[pt.workflowName] = true
			heap.Pop(&td.pendingPQ)
			deferred = append(deferred, pt)
			continue
		}

		// 如果队首任务所属工作流之前被标记为阻塞，弹出并暂存
		if blockedWf[pt.workflowName] {
			heap.Pop(&td.pendingPQ)
			deferred = append(deferred, pt)
			continue
		}

		// 可下发：弹出、占用槽位、加入 toPush
		heap.Pop(&td.pendingPQ)
		td.globalActive.Add(1)
		td.incPerWfActive(pt.workflowName)
		toPush = append(toPush, pt)
	}

	// 将 blocked 的任务重新放回优先队列，不丢失
	for _, pt := range deferred {
		heap.Push(&td.pendingPQ, pt)
	}

	td.pqMu.Unlock()

	// 阶段 2：批量写入 etcd（共享 lease，减少 etcd 压力）
	td.batchPush(ctx, toPush, globalMax > 0)
}

// evictTimedOutLocked 淘汰超时的排队任务，调用方必须持有 td.pqMu
// 采用惰性淘汰策略：仅检查堆顶元素是否超时（O(log n) pop），
// 堆顶不超时则跳过整次淘汰（大多数调用情况下无需任何操作）。
// 只有堆顶超时时才 pop 并递归检查新的堆顶，避免每次 dispatch 都做 O(n) 全量扫描。
func (td *TaskDispatcher) evictTimedOutLocked(timeoutSec int64) {
	if timeoutSec <= 0 || td.pendingPQ.Len() == 0 {
		return
	}
	now := time.Now()
	timeout := time.Duration(timeoutSec) * time.Second

	// 惰性淘汰：仅检查堆顶，堆顶不超时则直接返回
	// 由于堆按 priority 降序 + enqueued 升序排列，最老的入队时间不一定在堆顶。
	// 因此使用递归 pop 方式：检查堆顶 → 超时则弹出并处理 → 继续检查新堆顶
	var evicted []*pendingTask
	for td.pendingPQ.Len() > 0 {
		pt := td.pendingPQ[0] // 堆顶：最高优先级 + 最早入队
		if now.Sub(pt.enqueued) <= timeout {
			// 堆顶未超时，停止检查。注意：由于堆按 priority 优先排序，
			// 可能存在低优先级任务入队更早但不在堆顶的情况。
			// 不过超时淘汰的目的是防止任务无限等待，低优先级任务
			// 的超时由 Master 端整体排队超时机制兜底，此处不做全局扫描。
			break
		}
		heap.Pop(&td.pendingPQ)
		evicted = append(evicted, pt)
	}

	for _, pt := range evicted {
		logger.Warn("排队任务超时，标记失败",
			"workflow", pt.workflowName,
			"task", pt.node.Name,
			"enqueued", pt.enqueued.Format(time.RFC3339),
		)
		if td.markWorkflowFailed != nil {
			fakePayload := &store.TaskPayload{
				WorkflowID: pt.workflowName,
				TaskName:   pt.node.Name,
				TraceID:    NewTraceID(),
				Result:     fmt.Sprintf("排队超时（等待 %.0f 秒）", timeout.Seconds()),
				Status:     store.StatusFailed,
			}
			td.markWorkflowFailed(fakePayload, fakePayload.Result)
		}
	}
}

// getPendingTimeoutSec 获取排队超时时间（秒）
func (td *TaskDispatcher) getPendingTimeoutSec() int64 {
	t := td.config.PendingTaskTimeoutSec()
	if t <= 0 {
		return defaultPendingTaskTimeoutSec
	}
	return t
}

// ============================================================================
// 重试任务下发
// ============================================================================

// RetryTask 在退避延迟后重新下发一个失败的任务
func (td *TaskDispatcher) RetryTask(ctx context.Context, payload *store.TaskPayload, backoffSec int64) {
	select {
	case <-time.After(time.Duration(backoffSec) * time.Second):
	case <-ctx.Done():
		return
	}

	workflowUUID := payload.WorkflowID
	globalMax := td.config.MaxConcurrentTasks()
	perWfMax := td.config.PerWorkflowMaxConcurrency()

	if globalMax > 0 || perWfMax > 0 {
		if globalMax > 0 {
			td.globalActive.Add(1)
		}
		td.incPerWfActive(workflowUUID)
	}

	payload.RetryCount++
	payload.Status = store.StatusPending
	payload.Result = ""

	val, err := store.Serialize(payload)
	if err != nil {
		logger.Error("序列化重试任务负载失败，标记任务为永久失败",
			"trace_id", payload.TraceID, "workflow", payload.WorkflowID, "task", payload.TaskName, "error", err)
		if td.markWorkflowFailed != nil {
			td.markWorkflowFailed(payload, fmt.Sprintf("任务 %s 重试序列化失败: %v", payload.TaskName, err))
		}
		if globalMax > 0 {
			td.globalActive.Add(-1)
		}
		td.decPerWfActive(workflowUUID)
		td.tryDispatchPending(ctx)
		return
	}

	key := retryTaskKey(td.store.Prefixes().Tasks, payload.WorkflowID, payload.TaskName, payload.RetryCount)
	if err := putWithTTL(td.store, ctx, key, val, taskTTLSeconds); err != nil {
		logger.Error("重试任务失败，标记为永久失败",
			"trace_id", payload.TraceID, "workflow", payload.WorkflowID, "task", payload.TaskName, "error", err)
		if td.markWorkflowFailed != nil {
			td.markWorkflowFailed(payload, fmt.Sprintf("任务 %s 重试写入失败: %v", payload.TaskName, err))
		}
		if globalMax > 0 {
			td.globalActive.Add(-1)
		}
		td.decPerWfActive(workflowUUID)
		td.tryDispatchPending(ctx)
		return
	}

	logger.Info("任务已重试",
		"trace_id", payload.TraceID,
		"workflow", payload.WorkflowID,
		"task",     payload.TaskName,
		"attempt",  payload.RetryCount,
	)

	if td.clearFailedTask != nil {
		td.clearFailedTask(payload.WorkflowID, payload.TaskName)
	}
}

// PushRollbackTask 下发一个补偿回滚任务到 etcd
// 接受 context 参数，允许调用方通过 cancel 控制 etcd 写入的超时
func (td *TaskDispatcher) PushRollbackTask(ctx context.Context, workflowUUID string, node model.TaskNode, originalOutput map[string]any) {
	payload := &store.TaskPayload{
		WorkflowID: workflowUUID,
		TaskName:   node.Name,
		TraceID:    NewTraceID(),
		Status:     store.StatusPending,
		Params:     node.Params,
		Input:      originalOutput,
		Output:     make(map[string]any),
		TimeoutSec: td.config.DefaultTaskTimeoutSec(),
		Rollback:   true,
	}

	val, err := store.Serialize(payload)
	if err != nil {
		logger.Error("序列化回滚任务失败", "workflow", workflowUUID, "task", node.Name, "error", err)
		return
	}

	key := taskKey(td.store.Prefixes().Tasks, workflowUUID, node.Name)
	// 使用传入的 ctx 而非 context.Background()，确保回滚超时或 Shutdown 时能取消
	if err := putWithTTL(td.store, ctx, key, val, taskTTLSeconds); err != nil {
		logger.Error("下发回滚任务失败", "workflow", workflowUUID, "task", node.Name, "error", err)
		return
	}

	logger.Info("回滚任务已下发", "trace_id", payload.TraceID, "workflow", workflowUUID, "task", node.Name)
}

// ============================================================================
// 指标查询方法（供 BacklogMetrics 使用）
// ============================================================================

// PendingTaskCount 返回当前优先队列中排队的任务数
func (td *TaskDispatcher) PendingTaskCount() int {
	td.pqMu.Lock()
	defer td.pqMu.Unlock()
	return td.pendingPQ.Len()
}

// GlobalActiveCount 返回当前全局活跃任务数
func (td *TaskDispatcher) GlobalActiveCount() int64 {
	return td.globalActive.Load()
}
