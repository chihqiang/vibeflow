package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/tracing"
	"chihqiang/vibeflow/infra/ws"
)

// defaultMaxHistoryCache LRU 淘汰的默认缓存上限，与 config.DefaultConfig().Master.MaxHistoryCache 保持一致
const defaultMaxHistoryCache = 1000

// WorkflowManager 工作流管理器
// 职责：工作流生命周期管理（提交、取消、审批、驳回、重试）、Cron 调度、查询接口
// 从 Scheduler 拆分而来，独立管理 runningEntries / workflowHistory 等运行时状态
//
// 所有外部依赖通过接口注入（窄接口原则），不再直接依赖 *Scheduler，
// 实现组件间的完全解耦，便于单元测试和替换实现。
type WorkflowManager struct {
	// --- 注入的依赖（窄接口）---
	coordinator    WorkflowCoordinator      // 工作流协调（context、执行、Watch、超时、回滚）
	mysqlDeps      MySQLDeps                // MySQL 持久化依赖（historyStore、配置、buildTaskDefs）
	persistence    PersistenceOps           // 持久化操作（MySQL 写入、快照节流）
	eventBroadcast ExtendedEventBroadcaster // WebSocket 事件广播（含超时广播）
	cronScheduler  CronScheduler            // 定时任务调度

	// --- 工作流运行时状态 ---
	// runningEntries 按 UUID 索引的运行时条目，每个条目自带细粒度锁
	runningEntries map[string]*workflowEntry

	// --- 历史与注册 ---
	workflowHistory       map[uint]*model.WorkflowState
	workflowHistoryByUUID map[string][]uint // uuid → [execID1, execID2, ...] 保留最近 N 次执行
	registeredWorkflows   map[string]*model.Workflow

	// --- LRU 淘汰辅助结构 ---
	// historyOrder 按 ExecutionID 升序排列，用于在缓存超出上限时淘汰最旧的条目
	historyOrder []uint

	// --- 全局锁（仅保护 map 结构的增删，不保护 entry 内部状态）---
	mu sync.RWMutex

	// --- 统计计数器 ---
	completedCount atomic.Int64
	failedCount    atomic.Int64
}

// maxHistoryByName 同一工作流（按 UUID）在内存中最多保留的最近执行记录数
// 超出此数量时，最旧的记录在 LRU 淘汰时一并清理
const maxHistoryByUUID = 10

// workflowEntry 单个工作流的运行时条目
// 自带细粒度读写锁，避免所有工作流争抢全局锁
type workflowEntry struct {
	state  *model.WorkflowState
	mu     sync.RWMutex
	cancel context.CancelFunc
	// recovered 标记此工作流是否为恢复的（recoverRunningWorkflows 恢复的）
	// watchWorkflowStatus 可据此特殊处理恢复期间到达的旧事件
	recovered bool
}

// NewWorkflowManager 创建工作流管理器
// 所有外部依赖通过窄接口注入，不再依赖 *Scheduler
func NewWorkflowManager(
	coordinator WorkflowCoordinator,
	mysqlDeps MySQLDeps,
	persistence PersistenceOps,
	eventBroadcast ExtendedEventBroadcaster,
	cronScheduler CronScheduler,
) *WorkflowManager {
	return &WorkflowManager{
		coordinator:           coordinator,
		mysqlDeps:             mysqlDeps,
		persistence:           persistence,
		eventBroadcast:        eventBroadcast,
		cronScheduler:         cronScheduler,
		runningEntries:        make(map[string]*workflowEntry),
		workflowHistory:       make(map[uint]*model.WorkflowState),
		workflowHistoryByUUID: make(map[string][]uint),
		registeredWorkflows:   make(map[string]*model.Workflow),
	}
}

// ============================================================================
// 查询接口
// ============================================================================

// ============================================================================
// WorkflowStateReader / WorkflowStateAccessor 接口实现
// ============================================================================

// LockEntry 获取指定工作流条目的写锁，返回条目及是否需要释放
// 调用方应 defer entry.mu.Unlock()
//
// 【并发安全说明】
// 此方法存在 TOCTOU 窗口：在 RUnlock 全局读锁后、Lock entry 细粒度锁之前，
// entry 可能已被另一个 goroutine 从 runningEntries 中移除（例如被 PersistWorkflowLocked
// 转移到历史记录）。由于 Go 不会 GC 仍在被引用的对象，entry 本身不会成为悬垂指针；
// 但 entry.state 的状态可能已变更（例如 Status 已变为 Completed/Failed）。
// 调用方应检查 entry.state.Status 是否仍为预期状态，不应假设获取锁后状态不变。
// 对于只读场景，RLockEntry 的行为类似，且 entry 在释放锁后随时可能被转移。
func (wm *WorkflowManager) LockEntry(uuid string) *workflowEntry {
	wm.mu.RLock()
	entry := wm.runningEntries[uuid]
	wm.mu.RUnlock()
	if entry == nil {
		return nil
	}
	entry.mu.Lock()
	return entry
}

// RLockEntry 获取指定工作流条目的读锁，返回条目及是否需要释放
// 调用方应 defer entry.mu.RUnlock()
//
// 【并发安全说明】
// 参见 LockEntry 的文档。在全局读锁释放后到获取 entry 读锁之间，entry 可能已被
// 转移到历史记录。entry 对象本身不会悬垂，但其内部状态可能已变化。
// 典型用法：调用方获取读锁后应检查 entry.state 的状态是否仍符合预期，
// 并在发现状态变更时安全降级处理。
func (wm *WorkflowManager) RLockEntry(uuid string) *workflowEntry {
	wm.mu.RLock()
	entry := wm.runningEntries[uuid]
	wm.mu.RUnlock()
	if entry == nil {
		return nil
	}
	entry.mu.RLock()
	return entry
}

// Lock 获取全局写锁（仅用于保护 history/registered maps 的批量操作）
func (wm *WorkflowManager) Lock() { wm.mu.Lock() }

// Unlock 释放全局写锁
func (wm *WorkflowManager) Unlock() { wm.mu.Unlock() }

// RLock 获取全局读锁
func (wm *WorkflowManager) RLock() { wm.mu.RLock() }

// RUnlock 释放全局读锁
func (wm *WorkflowManager) RUnlock() { wm.mu.RUnlock() }

// GetWorkflowState 查询正在运行中的工作流状态
func (wm *WorkflowManager) GetWorkflowState(uuid string) *model.WorkflowState {
	entry := wm.RLockEntry(uuid)
	if entry == nil {
		return nil
	}
	defer entry.mu.RUnlock()
	return entry.state
}

// ListWorkflowStates 返回所有正在运行的工作流
func (wm *WorkflowManager) ListWorkflowStates() []*model.WorkflowState {
	wm.mu.RLock()
	entries := make([]*workflowEntry, 0, len(wm.runningEntries))
	for _, entry := range wm.runningEntries {
		entries = append(entries, entry)
	}
	wm.mu.RUnlock()

	states := make([]*model.WorkflowState, 0, len(entries))
	for _, entry := range entries {
		entry.mu.RLock()
		states = append(states, entry.state)
		entry.mu.RUnlock()
	}
	return states
}

// ListHistory 返回所有已结束的工作流历史记录
// 返回内存缓存中的条目（最近 max_history_cache 条），历史数据依赖 MySQL 分页查询
func (wm *WorkflowManager) ListHistory() []*model.WorkflowState {
	return wm.sortedHistory()
}

// GetHistory 按 UUID 查询单个历史工作流（返回最新的一条记录）
// 优先查内存缓存（O(1) 索引），未命中时回退到 MySQL
func (wm *WorkflowManager) GetHistory(uuid string) *model.WorkflowState {
	wm.mu.RLock()
	execIDs, ok := wm.workflowHistoryByUUID[uuid]
	if ok && len(execIDs) > 0 {
		// 最新记录在切片末尾
		latestID := execIDs[len(execIDs)-1]
		if st, ok2 := wm.workflowHistory[latestID]; ok2 {
			wm.mu.RUnlock()
			return st
		}
	}
	wm.mu.RUnlock()

	// 缓存未命中，回退到 MySQL
	return wm.loadHistoryFromMySQL(uuid)
}

// loadHistoryFromMySQL 从 MySQL 加载单条历史记录
// 使用 GetExecutionByUUID 精确查询，WHERE workflow_uuid = ? ORDER BY id DESC LIMIT 1
// 替代原来的全表扫描 + 客户端过滤方案
func (wm *WorkflowManager) loadHistoryFromMySQL(uuid string) *model.WorkflowState {
	hs := wm.mysqlDeps.HistoryStore()
	if hs == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(wm.mysqlDeps.StoreTimeoutSec())*time.Second)
	defer cancel()

	rec, err := hs.GetExecutionByUUID(ctx, uuid)
	if err != nil {
		logger.Warn("从 MySQL 查询历史记录失败", "uuid", uuid, "error", err)
		return nil
	}
	if rec == nil {
		return nil
	}
	var state model.WorkflowState
	if err := json.Unmarshal(rec.Data, &state); err != nil {
		logger.Warn("解析历史记录失败", "uuid", uuid, "error", err)
		return nil
	}
	return &state
}

// ListWorkflowDefs 返回所有已注册的工作流定义
func (wm *WorkflowManager) ListWorkflowDefs() []*model.WorkflowState {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	states := make([]*model.WorkflowState, 0, len(wm.registeredWorkflows))
	for _, wf := range wm.registeredWorkflows {
		states = append(states, &model.WorkflowState{
			Workflow: wf,
			Status:   model.WorkflowStatusPending,
		})
	}
	return states
}

// GetRegisteredWorkflow 按 UUID 查询单个已注册的工作流定义
func (wm *WorkflowManager) GetRegisteredWorkflow(uuid string) *model.Workflow {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.registeredWorkflows[uuid]
}

// RunWorkflow 按 UUID 查找已注册的工作流定义并提交执行
func (wm *WorkflowManager) RunWorkflow(ctx context.Context, uuid string) error {
	wm.mu.RLock()
	wf, ok := wm.registeredWorkflows[uuid]
	wm.mu.RUnlock()

	if !ok {
		return fmt.Errorf("工作流 %s 不存在", uuid)
	}

	if wm.GetWorkflowState(uuid) != nil {
		return fmt.Errorf("工作流 %s 正在运行中", uuid)
	}

	wfCopy, err := wf.DeepCopy()
	if err != nil {
		return fmt.Errorf("深拷贝工作流 %s 失败: %w", uuid, err)
	}
	return wm.SubmitWorkflow(ctx, wfCopy)
}

// CompletedCount 返回已完成工作流的原子计数器值（O(1)）
func (wm *WorkflowManager) CompletedCount() int64 {
	return wm.completedCount.Load()
}

// FailedCount 返回已失败工作流的原子计数器值（O(1)）
func (wm *WorkflowManager) FailedCount() int64 {
	return wm.failedCount.Load()
}

// sortedHistory 将 workflowHistory map 转为按 ExecutionID 降序排列的切片
func (wm *WorkflowManager) sortedHistory() []*model.WorkflowState {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return sortedHistoryLocked(wm.workflowHistory)
}

// sortedHistoryLocked 对已持锁的 workflowHistory map 排序（按 ExecutionID 降序）
func sortedHistoryLocked(history map[uint]*model.WorkflowState) []*model.WorkflowState {
	list := make([]*model.WorkflowState, 0, len(history))
	for _, st := range history {
		list = append(list, st)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ExecutionID > list[j].ExecutionID
	})
	return list
}

// ============================================================================
// 工作流生命周期
// ============================================================================

// SubmitWorkflow 提交工作流执行
func (wm *WorkflowManager) SubmitWorkflow(ctx context.Context, wf *model.Workflow) error {
	if err := validateWorkflowName(wf.Name); err != nil {
		return err
	}

	groups := wf.TaskGroups
	if len(groups) == 0 {
		return fmt.Errorf("工作流 %s 没有定义任务", wf.Name)
	}

	if err := validateWorkflowTasks(wf); err != nil {
		return fmt.Errorf("工作流 %s 任务定义不合法: %w", wf.Name, err)
	}

	// 创建 span：工作流提交
	// 使用 RootContext 而非 HTTP request context，因为 HTTP handler 返回后 ctx 会被 cancel，
	// 导致后续异步 goroutine（ExecuteWorkflow、WatchWorkflowStatus 等）无法正常执行
	wfCtx, wfSpan := tracing.StartSpan(wm.coordinator.RootContext(), "scheduler.submit_workflow",
		tracing.StringAttr("workflow.uuid", wf.UUID),
		tracing.StringAttr("workflow.name", wf.Name),
		tracing.IntAttr("workflow.task_groups", len(groups)),
		tracing.StringAttr("workflow.trigger", string(wf.Trigger)),
	)
	defer wfSpan.End()

	logger.Info("提交工作流", "workflow", wf.Name, "uuid", wf.UUID, "groups", len(groups))

	wfCancelCtx, wfCancel := context.WithCancel(wm.coordinator.RootContext())

	now := time.Now()
	entry := &workflowEntry{
		state: &model.WorkflowState{
			Workflow:        wf,
			WorkflowUUID:    wf.UUID,
			Status:          model.WorkflowStatusRunning,
			StartedAt:       now,
			CompletedTasks:  make(map[string]map[string]any),
			FailedTasks:     make(map[string]string),
			TaskGroupIndex:  wf.BuildTaskGroupIndex(),
			BranchTaskIndex: wf.BuildBranchTaskIndex(),
		},
		cancel: wfCancel,
	}
	wm.mu.Lock()
	wm.runningEntries[wf.UUID] = entry
	wm.mu.Unlock()

	// 异步写入 MySQL
	if wm.mysqlDeps.HistoryStore() != nil {
		wm.coordinator.GoroutineTracker().Add(1)
		go wm.asyncInitMySQL(wfCtx, wf, now)
	}

	wm.eventBroadcast.BroadcastWorkflowEvent(model.EventWorkflowSubmitted, wf)

	// 启动超时监控
	if wf.TimeoutSec > 0 {
		wm.coordinator.GoroutineTracker().Add(1)
		go func() {
			defer wm.coordinator.GoroutineTracker().Done()
			wm.coordinator.TimeoutAfter(wfCancelCtx, wf.UUID, time.Duration(wf.TimeoutSec)*time.Second)
		}()
	}

	// 启动状态监听
	wm.coordinator.GoroutineTracker().Add(1)
	go func() {
		defer wm.coordinator.GoroutineTracker().Done()
		wm.coordinator.WatchWorkflowStatus(wfCancelCtx, wf.UUID)
	}()

	// 启动执行
	wm.coordinator.GoroutineTracker().Add(1)
	go func() {
		defer wm.coordinator.GoroutineTracker().Done()
		wm.coordinator.ExecuteWorkflow(wfCtx, wf.UUID, nil, 0)
	}()

	return nil
}

// CancelWorkflow 取消一个正在运行的工作流
func (wm *WorkflowManager) CancelWorkflow(uuid string) error {
	entry := wm.LockEntry(uuid)
	if entry == nil {
		return fmt.Errorf("工作流 %s 不存在或已结束", uuid)
	}
	state := entry.state
	if state.Status != model.WorkflowStatusRunning && state.Status != model.WorkflowStatusPaused {
		entry.mu.Unlock()
		return fmt.Errorf("工作流 %s 不在运行中", uuid)
	}
	wasPaused := state.Status == model.WorkflowStatusPaused
	pausedTaskName := state.PausedTaskName
	state.Error = "用户已取消"
	wm.PersistWorkflowLocked(uuid, state, entry)
	entry.mu.Unlock()

	wm.eventBroadcast.BroadcastWorkflowEventToWorkflowWithTimeout(uuid, model.EventWorkflowCancelled, uuid, ws.DefaultBroadcastTimeout)

	taskName := "__cancelled__"
	if wasPaused && pausedTaskName != "" {
		taskName = pausedTaskName
	}
	fakePayload := &store.TaskPayload{
		WorkflowID: uuid,
		TaskName:   taskName,
		TraceID:    NewTraceID(),
		Result:     "用户已取消",
		RetryCount: 0,
		MaxRetries: 0,
	}
	wm.coordinator.StartSagaRollback(uuid, state, fakePayload)
	return nil
}

// ApproveWorkflow 审批通过一个暂停中的工作流
func (wm *WorkflowManager) ApproveWorkflow(uuid string) error {
	entry := wm.LockEntry(uuid)
	if entry == nil {
		return fmt.Errorf("工作流 %s 不存在或已结束", uuid)
	}
	state := entry.state
	if state.Status != model.WorkflowStatusPaused {
		entry.mu.Unlock()
		return fmt.Errorf("工作流 %s 不在暂停状态，当前状态: %s", uuid, state.Status)
	}

	state.Status = model.WorkflowStatusRunning
	pausedTaskName := state.PausedTaskName
	pausedGroupIdx := state.PausedGroupIdx
	pausedOutput := state.PausedTaskOutput
	groups := state.Workflow.TaskGroups
	entry.mu.Unlock()

	logger.Info("工作流审批通过，继续执行",
		"uuid", uuid,
		"task", pausedTaskName,
	)

	wm.eventBroadcast.BroadcastWorkflowEventToWorkflow(uuid, model.EventWorkflowApproved, map[string]any{
		"workflow": uuid,
		"task":     pausedTaskName,
	})

	if pausedGroupIdx >= 0 && pausedGroupIdx < len(groups)-1 {
		cleanOutput := make(map[string]any, len(pausedOutput))
		for k, v := range pausedOutput {
			if k != model.SkipGroupsKey && k != model.BranchKey && k != model.ApprovalKey {
				cleanOutput[k] = v
			}
		}
		upstreamOutputs := []map[string]any{cleanOutput}

		wm.coordinator.GoroutineTracker().Add(1)
		go func() {
			defer wm.coordinator.GoroutineTracker().Done()
			wm.coordinator.ExecuteWorkflow(wm.coordinator.RootContext(), uuid, upstreamOutputs, pausedGroupIdx+1)
		}()
	} else if pausedGroupIdx == len(groups)-1 {
		entry2 := wm.LockEntry(uuid)
		if entry2 != nil {
			entry2.state.Status = model.WorkflowStatusCompleted
			wm.PersistWorkflowLocked(uuid, entry2.state, entry2)
			entry2.mu.Unlock()
		}
		wm.eventBroadcast.BroadcastWorkflowEventToWorkflow(uuid, model.EventWorkflowCompleted, uuid)
	}

	return nil
}

// RejectWorkflow 审批驳回一个暂停中的工作流
func (wm *WorkflowManager) RejectWorkflow(uuid string, reason string) error {
	entry := wm.LockEntry(uuid)
	if entry == nil {
		return fmt.Errorf("工作流 %s 不存在或已结束", uuid)
	}
	state := entry.state
	if state.Status != model.WorkflowStatusPaused {
		entry.mu.Unlock()
		return fmt.Errorf("工作流 %s 不在暂停状态，当前状态: %s", uuid, state.Status)
	}

	pausedTaskName := state.PausedTaskName
	entry.mu.Unlock()

	if reason == "" {
		reason = "审批被驳回"
	}

	logger.Warn("工作流审批被驳回",
		"uuid", uuid,
		"task", pausedTaskName,
		"reason", reason,
	)

	wm.eventBroadcast.BroadcastWorkflowEventToWorkflow(uuid, model.EventWorkflowRejected, map[string]any{
		"workflow": uuid,
		"task":     pausedTaskName,
		"reason":   reason,
	})

	failedPayload := &store.TaskPayload{
		WorkflowID: uuid,
		TaskName:   pausedTaskName,
		TraceID:    NewTraceID(),
		Status:     store.StatusFailed,
		Result:     reason,
		RetryCount: 0,
		MaxRetries: 0,
	}

	wm.coordinator.HandleTaskFailed(wm.coordinator.RootContext(), uuid, state, failedPayload)
	return nil
}

// RetryWorkflow 从历史记录中重新提交一个已结束的工作流
func (wm *WorkflowManager) RetryWorkflow(ctx context.Context, uuid string) error {
	wm.mu.Lock()
	if _, ok := wm.runningEntries[uuid]; ok {
		wm.mu.Unlock()
		return fmt.Errorf("工作流 %s 正在运行中", uuid)
	}

	execIDs, ok := wm.workflowHistoryByUUID[uuid]
	if !ok || len(execIDs) == 0 {
		wm.mu.Unlock()
		return fmt.Errorf("工作流 %s 不存在于历史记录中", uuid)
	}

	// 获取最新一条历史记录
	latestID := execIDs[len(execIDs)-1]
	state, ok := wm.workflowHistory[latestID]
	if !ok {
		wm.mu.Unlock()
		return fmt.Errorf("工作流 %s 的历史记录不一致", uuid)
	}

	wf, err := state.Workflow.DeepCopy()
	if err != nil {
		wm.mu.Unlock()
		return fmt.Errorf("深拷贝工作流 %s 失败: %w", uuid, err)
	}

	delete(wm.workflowHistory, latestID)
	wm.removeHistoryByUUIDEntry(uuid, latestID)
	wm.removeFromHistoryOrder(latestID)
	wm.mu.Unlock()

	return wm.SubmitWorkflow(ctx, wf)
}

// removeHistoryByUUIDEntry 从 workflowHistoryByUUID 中移除指定 execID
// 调用方必须持有 wm.mu.Lock()
func (wm *WorkflowManager) removeHistoryByUUIDEntry(uuid string, execID uint) {
	execIDs, ok := wm.workflowHistoryByUUID[uuid]
	if !ok {
		return
	}
	for i, id := range execIDs {
		if id == execID {
			wm.workflowHistoryByUUID[uuid] = append(execIDs[:i], execIDs[i+1:]...)
			if len(wm.workflowHistoryByUUID[uuid]) == 0 {
				delete(wm.workflowHistoryByUUID, uuid)
			}
			return
		}
	}
}

// removeFromHistoryOrder 从 historyOrder 中移除指定 ExecutionID
// 调用方必须已持有 wm.mu.Lock()（全局锁保护 historyOrder）
func (wm *WorkflowManager) removeFromHistoryOrder(execID uint) {
	for i, id := range wm.historyOrder {
		if id == execID {
			wm.historyOrder = append(wm.historyOrder[:i], wm.historyOrder[i+1:]...)
			return
		}
	}
}

// ============================================================================
// 持久化（内部方法）
// ============================================================================

// PersistWorkflowLocked 将工作流转入历史并持久化到 MySQL
// 调用方必须已持有 entry.mu.Lock()
//
// 【状态转移设计说明】
// 本方法先将 state 加入 workflowHistory、从 runningEntries 删除（完成"运行→历史"的
// 内存状态转移），再序列化并写入 MySQL。即使序列化失败，内存状态转移已经完成且是正确的
// （工作流已从运行集移除，进入历史集），MySQL 持久化只是辅助恢复手段，不会影响内存一致性。
func (wm *WorkflowManager) PersistWorkflowLocked(uuid string, state *model.WorkflowState, entry *workflowEntry) {
	wm.mu.Lock()
	wm.workflowHistory[state.ExecutionID] = state
	// 追加到同 UUID 工作流历史列表，保留最近 maxHistoryByUUID 条
	wm.workflowHistoryByUUID[uuid] = append(wm.workflowHistoryByUUID[uuid], state.ExecutionID)
	if len(wm.workflowHistoryByUUID[uuid]) > maxHistoryByUUID {
		// 淘汰最旧的条目
		wm.workflowHistoryByUUID[uuid] = wm.workflowHistoryByUUID[uuid][len(wm.workflowHistoryByUUID[uuid])-maxHistoryByUUID:]
	}
	wm.historyOrder = append(wm.historyOrder, state.ExecutionID)
	delete(wm.runningEntries, uuid)
	wm.mu.Unlock()

	wm.persistence.CleanSnapshotThrottle(uuid)
	wm.coordinator.CleanupPerWfCounter(uuid)
	if entry.cancel != nil {
		entry.cancel()
	}
	switch state.Status {
	case model.WorkflowStatusCompleted:
		wm.completedCount.Add(1)
	case model.WorkflowStatusFailed, model.WorkflowStatusRolledBack:
		wm.failedCount.Add(1)
	}

	// 拷贝持久化所需的字段，避免 go 协程在 entry 锁释放后读取被修改的 state
	workflowID := state.WorkflowID
	execID := state.ExecutionID
	status := string(state.Status)
	errMsg := state.Error
	val, err := json.Marshal(state)
	if err != nil {
		logger.Error("序列化工作流状态失败", "uuid", uuid, "error", err)
		// 序列化失败不影响内存状态转移，跳过 MySQL 持久化
	} else {
		wm.persistence.PersistToMySQL(uuid, workflowID, execID, status, errMsg, val)
	}

	// LRU 淘汰：超出缓存上限时，移除最旧的条目
	wm.mu.Lock()
	maxCache := wm.mysqlDeps.MaxHistoryCache()
	if maxCache <= 0 {
		maxCache = defaultMaxHistoryCache
	}
	for len(wm.historyOrder) > maxCache {
		oldestID := wm.historyOrder[0]
		wm.historyOrder = wm.historyOrder[1:]
		if oldState, ok := wm.workflowHistory[oldestID]; ok {
			oldUUID := oldState.WorkflowUUID
			wm.removeHistoryByUUIDEntry(oldUUID, oldestID)
		}
		delete(wm.workflowHistory, oldestID)
	}
	wm.mu.Unlock()
}

// ============================================================================
// Cron 调度
// ============================================================================

// ScheduleCronWorkflow 注册一个 Cron 定时工作流
func (wm *WorkflowManager) ScheduleCronWorkflow(wf *model.Workflow) error {
	if wf.Trigger != model.TriggerCron || wf.CronExpr == "" {
		return fmt.Errorf("工作流 %s 不是 cron 类型或缺少 cron 表达式", wf.Name)
	}

	name := wf.Name
	err := wm.cronScheduler.AddCronFunc(wf.CronExpr, func() {
		wfCopy, err := wf.DeepCopy()
		if err != nil {
			logger.Error("深拷贝工作流失败", "workflow", name, "error", err)
			return
		}
		// 使用 RootContext() 派生，Shutdown 时 cron 触发的工作流提交也能被取消
		if err := wm.SubmitWorkflow(wm.coordinator.RootContext(), wfCopy); err != nil {
			logger.Error("执行定时工作流失败", "workflow", name, "error", err)
		}
	})
	if err != nil {
		return fmt.Errorf("注册 cron 工作流 %s 失败: %w", wf.Name, err)
	}

	logger.Info("Cron 工作流已注册", "workflow", wf.Name, "cron", wf.CronExpr)
	return nil
}

// ============================================================================
// 注册
// ============================================================================

// RegisterWorkflow 注册一个工作流定义
func (wm *WorkflowManager) RegisterWorkflow(wf *model.Workflow) {
	if err := validateWorkflowName(wf.Name); err != nil {
		logger.Error("工作流注册失败：名称不合法", "name", wf.Name, "error", err)
		return
	}
	if err := validateWorkflowTasks(wf); err != nil {
		logger.Error("工作流注册失败：任务定义不合法", "name", wf.Name, "error", err)
		return
	}

	wm.mu.Lock()
	if _, exists := wm.registeredWorkflows[wf.UUID]; exists {
		logger.Warn("工作流 UUID 重复，将覆盖旧定义", "uuid", wf.UUID)
	}
	wfCopy, err := wf.DeepCopy()
	if err != nil {
		logger.Error("深拷贝工作流失败，无法注册", "workflow", wf.Name, "error", err)
		return
	}
	wm.registeredWorkflows[wf.UUID] = wfCopy
	wm.mu.Unlock()

	hs := wm.mysqlDeps.HistoryStore()
	if hs == nil {
		logger.Warn("HistoryStore 未初始化，跳过工作流 MySQL 持久化", "name", wf.Name)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(wm.mysqlDeps.StoreTimeoutSec())*time.Second)
	defer cancel()

	taskDefs := wm.mysqlDeps.BuildTaskDefs(wf.AllTaskNames())
	workflowID, err := hs.UpsertWorkflowDef(ctx, wf.UUID, wf.Name, taskDefs,
		wf.TimeoutSec, wf.TaskTimeoutSec, wf.MaxRetries, wf.BaseBackoff,
		string(wf.Trigger), wf.CronExpr)
	if err != nil {
		logger.Error("工作流注册失败", "name", wf.Name, "error", err)
		return
	}
	logger.Info("工作流已注册", "name", wf.Name, "uuid", wf.UUID, "workflow_id", workflowID)
}

// ============================================================================
// MySQL 异步初始化
// ============================================================================

// asyncInitMySQL 异步初始化 MySQL 中的工作流记录
// 熔断器保护：MySQL 不可用时快速失败并清理工作流状态
func (wm *WorkflowManager) asyncInitMySQL(ctx context.Context, wf *model.Workflow, startedAt time.Time) {
	defer wm.coordinator.GoroutineTracker().Done()

	hs := wm.mysqlDeps.HistoryStore()

	// 注意：asyncInitMySQL 使用的熔断器在 MySQLDeps 接口中未暴露，
	// 因为熔断器的决策应由协调层统一管理。当前实现中，如果 HistoryStore 为 nil
	// 或保存失败，则直接清理工作流状态。
	// TODO: 未来可将熔断器决策移至 WorkflowCoordinator 接口

	hctx, hcancel := context.WithTimeout(ctx, time.Duration(wm.mysqlDeps.StoreTimeoutSec())*time.Second)
	defer hcancel()

	taskDefs := wm.mysqlDeps.BuildTaskDefs(wf.AllTaskNames())
	workflowID, err := hs.SaveWorkflowDef(hctx, wf.UUID, wf.Name, taskDefs,
		wf.TimeoutSec, wf.TaskTimeoutSec, wf.MaxRetries, wf.BaseBackoff,
		string(wf.Trigger), wf.CronExpr)
	if err != nil {
		logger.Error("异步保存工作流定义失败", "workflow", wf.Name, "error", err)
		wm.cleanupWorkflowState(wf.UUID)
		return
	}

	entry := wm.LockEntry(wf.UUID)
	if entry == nil {
		logger.Warn("工作流已在 MySQL 初始化前被取消", "workflow", wf.Name)
		return
	}
	entry.state.WorkflowID = workflowID
	// 在锁内拷贝 state 并序列化，避免锁释放后 state 被并发修改导致数据不一致
	copied := *entry.state
	entry.mu.Unlock()

	val, err := json.Marshal(&copied)
	if err != nil {
		logger.Error("异步序列化工作流状态失败", "workflow", wf.Name, "error", err)
		wm.cleanupWorkflowState(wf.UUID)
		return
	}

	execID, err := hs.CreateExecution(hctx, workflowID, val, string(model.WorkflowStatusRunning), startedAt, "")
	if err != nil {
		logger.Error("异步创建执行记录失败", "workflow", wf.Name, "error", err)
		wm.cleanupWorkflowState(wf.UUID)
		return
	}

	entry2 := wm.LockEntry(wf.UUID)
	if entry2 != nil {
		entry2.state.ExecutionID = execID
		entry2.mu.Unlock()
	}

	if err := hs.UpdateWorkflowStatus(hctx, workflowID, store.WorkflowStatusRunning); err != nil {
		logger.Warn("更新工作流状态失败（不影响执行）", "workflow", wf.Name, "error", err)
	}
}

// clearFailedTask 清除失败任务标记（供 TaskDispatcher 重试成功后调用）
func (wm *WorkflowManager) clearFailedTask(workflowUUID, taskName string) {
	entry := wm.LockEntry(workflowUUID)
	if entry != nil {
		delete(entry.state.FailedTasks, taskName)
		// 重置快照标记，确保重试成功后该任务的新状态会被增量快照捕获
		if entry.state.Snapshotted != nil {
			delete(entry.state.Snapshotted, taskName)
		}
		entry.mu.Unlock()
	}
}

// cleanupWorkflowState 清理工作流的内存状态
// 工作流异常退出时做强制兜底清理：
//   1. 从 runningEntries 移除
//   2. 取消工作流 context（通知 watchWorkflowStatus goroutine 退出）
//   3. 强制关闭 wfWatchers channel（防止 unregister 在 goroutine 退出前未被调用）
//   4. 强制清理 taskListeners（关闭所有 per-task channel）
//   5. 清理快照节流记录
func (wm *WorkflowManager) cleanupWorkflowState(uuid string) {
	wm.mu.Lock()
	entry, ok := wm.runningEntries[uuid]
	if ok {
		delete(wm.runningEntries, uuid)
	}
	wm.mu.Unlock()

	if ok && entry.cancel != nil {
		entry.cancel()
	}
	// 强制清理 Watch 通道和任务监听器（兜底，watchWorkflowStatus 的 defer 也会执行）
	wm.coordinator.CleanupWatcher(uuid)
	wm.coordinator.CleanupTaskListeners(uuid)
	wm.persistence.CleanSnapshotThrottle(uuid)
	logger.Warn("已清理工作流内存状态", "uuid", uuid)
}

// ============================================================================
// SaveWorkflow（持久化工作流定义）
// ============================================================================

// SaveWorkflow 持久化工作流定义（不执行）
func (wm *WorkflowManager) SaveWorkflow(ctx context.Context, wf *model.Workflow) error {
	hs := wm.mysqlDeps.HistoryStore()
	if hs != nil {
		taskDefs := wm.mysqlDeps.BuildTaskDefs(wf.AllTaskNames())
		if _, err := hs.SaveWorkflowDef(ctx, wf.UUID, wf.Name, taskDefs,
			wf.TimeoutSec, wf.TaskTimeoutSec, wf.MaxRetries, wf.BaseBackoff,
			string(wf.Trigger), wf.CronExpr); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================================
// 分页历史查询
// ============================================================================

// maxHistoryPageSize 历史记录分页查询的每页数量上限，作为防御性编程的兜底保护
const maxHistoryPageSize = 100

// ListHistoryPaged 分页返回已结束的工作流历史记录
// 优先使用 MySQL 分页查询（支持全量历史），缓存不可用时降级为内存分页
// pageSize 会被限制在 maxHistoryPageSize 以内，防止调用方传入极大值导致性能问题
func (wm *WorkflowManager) ListHistoryPaged(page, pageSize int) ([]*model.WorkflowState, int) {
	if pageSize > maxHistoryPageSize {
		pageSize = maxHistoryPageSize
	}
	hs := wm.mysqlDeps.HistoryStore()
	if hs != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(wm.mysqlDeps.StoreTimeoutSec())*time.Second)
		defer cancel()
		records, total, err := hs.LoadExecutions(ctx, (page-1)*pageSize, pageSize)
		if err == nil {
			states := make([]*model.WorkflowState, 0, len(records))
			for _, r := range records {
				var state model.WorkflowState
				if err := json.Unmarshal(r.Data, &state); err != nil {
					continue
				}
				states = append(states, &state)
			}
			return states, int(total)
		}
		logger.Warn("从 MySQL 分页查询历史记录失败，降级为内存分页", "error", err)
	}

	// 降级：仅返回内存缓存中的数据（已受 LRU 上限约束）
	all := wm.sortedHistory()
	total := len(all)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return all[start:end], total
}
