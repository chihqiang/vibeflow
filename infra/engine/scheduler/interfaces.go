package scheduler

import (
	"context"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/store"
)

// defaultStoreTimeoutSec etcd/MySQL 操作默认超时时间（秒），当配置值 <= 0 时兜底。
// 同时被 schedulerConfig 和 schedulerMySQLDeps 两个适配器使用。
const defaultStoreTimeoutSec int64 = 5

// ============================================================================
// 跨组件窄接口 — 消除子组件对 *Scheduler 的直接依赖
// ============================================================================

// WorkflowStateReader WorkflowManager 对外暴露的只读状态查询接口
type WorkflowStateReader interface {
	GetWorkflowState(uuid string) *model.WorkflowState
}

// WorkflowStateAccessor WorkflowManager 对外暴露的读写接口
// 使用 per-workflow 细粒度锁替代全局锁
type WorkflowStateAccessor interface {
	WorkflowStateReader
	WorkflowDefReader
	WorkflowHistoryReader
	Lock()
	Unlock()
	LockEntry(uuid string) *workflowEntry
	PersistWorkflowLocked(uuid string, state *model.WorkflowState, entry *workflowEntry)
}

// WorkflowDefReader 工作流定义查询接口
// 用于 AdvancedExecutor 查找子工作流定义
type WorkflowDefReader interface {
	GetRegisteredWorkflow(uuid string) *model.Workflow
}

// WorkflowHistoryReader 工作流历史查询接口
// 用于 AdvancedExecutor 查询子工作流的执行历史
type WorkflowHistoryReader interface {
	GetHistory(uuid string) *model.WorkflowState
}

// ConfigProvider 配置访问接口（只暴露各组件需要的字段）
type ConfigProvider interface {
	MaxConcurrentTasks() int
	PerWorkflowMaxConcurrency() int
	PendingTaskTimeoutSec() int64
	DefaultTaskTimeoutSec() int64
	DefaultBaseBackoffSec() int64
	StaleCheckIntervalSec() int64
	HeartbeatStaleSec() int64
	StoreTimeoutSec() int64
	MaxHistoryCache() int
}

// TaskPusher 任务下发接口
type TaskPusher interface {
	PushTask(ctx context.Context, workflowUUID string, node model.TaskNode, upstreamOutputs []map[string]any) error
	ReleaseSlot(ctx context.Context)
	RetryTask(ctx context.Context, payload *store.TaskPayload, backoffSec int64)
	PushRollbackTask(ctx context.Context, workflowUUID string, node model.TaskNode, originalOutput map[string]any)
}

// SnapshotManager 快照与任务记录持久化接口
type SnapshotManager interface {
	SnapshotRunningWorkflow(uuid string)
	SaveTaskRecord(workflowUUID string, payload *store.TaskPayload, execID, workflowID uint)
}

// TaskListenerManager 任务监听器管理接口
type TaskListenerManager interface {
	RegisterTaskListener(workflowUUID, taskName string, ch chan *store.TaskPayload)
	UnregisterTaskListener(workflowUUID, taskName string)
}

// EventBroadcaster 事件广播接口
type EventBroadcaster interface {
	BroadcastWorkflowEvent(eventType model.WSEventType, data any)
	BroadcastWorkflowEventToWorkflow(workflow string, eventType model.WSEventType, data any)
}

// GoroutineTracker goroutine 生命周期追踪接口
type GoroutineTracker interface {
	Add(delta int)
	Done()
}

// ============================================================================
// WorkflowManager 专用接口 — 消除 WorkflowManager 对 *Scheduler 的剩余直接依赖
// ============================================================================

// WorkflowCoordinator 工作流协调接口
// 提供 WorkflowManager 所需的工作流生命周期协调能力：
//   - 根 context（用于派生子工作流的上下文）
//   - goroutine 追踪
//   - 工作流执行推进
//   - 状态监听（Watch）、超时监控
//   - Saga 回滚、任务失败处理
type WorkflowCoordinator interface {
	// RootContext 返回 Scheduler 的根 context，用于派生工作流生命周期 context
	RootContext() context.Context

	// GoroutineTracker 返回 goroutine 生命周期追踪器
	GoroutineTracker() GoroutineTracker

	// ExecuteWorkflow 启动/继续工作流执行
	ExecuteWorkflow(ctx context.Context, workflowUUID string, upstreamOutputs []map[string]any, startGroupIdx int)

	// WatchWorkflowStatus 启动工作流状态监听 goroutine
	WatchWorkflowStatus(ctx context.Context, workflowUUID string)

	// TimeoutAfter 启动工作流超时监控 goroutine
	// timeout 为工作流级别的超时时长
	TimeoutAfter(ctx context.Context, workflowUUID string, timeout time.Duration)

	// StartSagaRollback 触发 Saga 补偿回滚
	StartSagaRollback(workflowUUID string, state *model.WorkflowState, failedPayload *store.TaskPayload)

	// HandleTaskFailed 处理任务执行失败事件（重试 / 回滚 / 释放槽位）
	HandleTaskFailed(ctx context.Context, workflowUUID string, state *model.WorkflowState, payload *store.TaskPayload)

	// CleanupWatcher 强制注销工作流的全局事件监听通道，关闭 channel
	// 用于工作流异常退出时的兜底清理，防止 channel 泄漏
	CleanupWatcher(workflowUUID string)

	// CleanupTaskListeners 强制清理工作流的所有任务监听通道
	// 用于工作流异常退出时的兜底清理
	CleanupTaskListeners(workflowUUID string)

	// CleanupPerWfCounter 工作流结束后清理 per-workflow 并发计数器
	// 防止 sync.Map 中的条目在计数器未归零时（如异常路径）永久残留，造成内存泄漏
	CleanupPerWfCounter(workflowUUID string)
}

// MySQLDeps WorkflowManager 对 MySQL 持久化的依赖集合
// 将 historyStore、storeTimeout、maxHistoryCache、mysqlBreaker、buildTaskDefs 合并为一个接口，
// 减少 WorkflowManager 构造函数的参数数量
type MySQLDeps interface {
	// HistoryStore 返回 MySQL 历史存储
	HistoryStore() store.HistoryStore

	// StoreTimeoutSec 返回 MySQL 操作超时秒数
	StoreTimeoutSec() int64

	// MaxHistoryCache 返回内存历史缓存最大条数
	MaxHistoryCache() int

	// BuildTaskDefs 根据任务名称列表构建 TaskDef 列表
	BuildTaskDefs(taskNames []string) []store.TaskDef
}

// PersistenceOps WorkflowManager 对持久化操作（快照节流、MySQL 写入）的依赖
type PersistenceOps interface {
	// PersistToMySQL 将工作流状态异步写入 MySQL
	PersistToMySQL(uuid string, workflowID, execID uint, status, errMsg string, val []byte)

	// CleanSnapshotThrottle 工作流结束后清理快照节流记录
	CleanSnapshotThrottle(uuid string)
}

// CronScheduler 定时任务调度接口
type CronScheduler interface {
	// AddCronFunc 注册一个 cron 定时任务
	// 返回 error 表示注册失败
	AddCronFunc(cronExpr string, fn func()) error
}

// ============================================================================
// Scheduler 上的窄接口适配器（内部实现，将 *Scheduler 的方法转为接口）
// ============================================================================

// schedulerConfig 将 *config.MasterConfig 适配为 ConfigProvider
type schedulerConfig struct {
	s *Scheduler
}

func (c *schedulerConfig) MaxConcurrentTasks() int       { return c.s.conf.MaxConcurrentTasks }
func (c *schedulerConfig) PerWorkflowMaxConcurrency() int { return c.s.conf.PerWorkflowMaxConcurrency }
func (c *schedulerConfig) PendingTaskTimeoutSec() int64   { return int64(c.s.conf.PendingTaskTimeout.ToDuration().Seconds()) }
func (c *schedulerConfig) DefaultTaskTimeoutSec() int64   { return int64(c.s.conf.DefaultTaskTimeout.ToDuration().Seconds()) }
func (c *schedulerConfig) DefaultBaseBackoffSec() int64   { return int64(c.s.conf.DefaultBaseBackoff.ToDuration().Seconds()) }
func (c *schedulerConfig) StaleCheckIntervalSec() int64   { return int64(c.s.conf.StaleCheckInterval.ToDuration().Seconds()) }
func (c *schedulerConfig) HeartbeatStaleSec() int64       { return int64(c.s.conf.HeartbeatStale.ToDuration().Seconds()) }
func (c *schedulerConfig) StoreTimeoutSec() int64 {
	t := int64(c.s.conf.StoreTimeout.ToDuration().Seconds())
	if t <= 0 {
		return defaultStoreTimeoutSec
	}
	return t
}
func (c *schedulerConfig) MaxHistoryCache() int            { return c.s.conf.MaxHistoryCache }

// schedulerEventBroadcaster 将 Scheduler 的 wsEvent 适配为 EventBroadcaster
type schedulerEventBroadcaster struct {
	s *Scheduler
}

func (b *schedulerEventBroadcaster) BroadcastWorkflowEvent(eventType model.WSEventType, data any) {
	b.s.wsEvent.Broadcast(model.WSMessage{Type: eventType, Data: data})
}

func (b *schedulerEventBroadcaster) BroadcastWorkflowEventToWorkflow(workflow string, eventType model.WSEventType, data any) {
	b.s.wsEvent.BroadcastToWorkflow(workflow, model.WSMessage{Type: eventType, Data: data})
}

// schedulerGoroutineTracker 将 Scheduler 的 wg 适配为 GoroutineTracker
type schedulerGoroutineTracker struct {
	s *Scheduler
}

func (t *schedulerGoroutineTracker) Add(delta int) { t.s.wg.Add(delta) }
func (t *schedulerGoroutineTracker) Done()          { t.s.wg.Done() }

// schedulerSnapshotManager 将 Scheduler 的 repository + snapshotThrottle 适配为 SnapshotManager
type schedulerSnapshotManager struct {
	s *Scheduler
}

func (m *schedulerSnapshotManager) SnapshotRunningWorkflow(uuid string) {
	m.s.repository.snapshotRunningWorkflow(uuid)
}
func (m *schedulerSnapshotManager) SaveTaskRecord(workflowUUID string, payload *store.TaskPayload, execID, workflowID uint) {
	m.s.repository.saveTaskRecord(workflowUUID, payload, execID, workflowID)
}

// schedulerListenerManager 将 Scheduler 的 taskListeners 适配为 TaskListenerManager
type schedulerListenerManager struct {
	s *Scheduler
}

func (m *schedulerListenerManager) RegisterTaskListener(workflowUUID, taskName string, ch chan *store.TaskPayload) {
	m.s.registerTaskListener(workflowUUID, taskName, ch)
}
func (m *schedulerListenerManager) UnregisterTaskListener(workflowUUID, taskName string) {
	m.s.unregisterTaskListener(workflowUUID, taskName)
}

// ============================================================================
// WorkflowCoordinator 适配器
// ============================================================================

// schedulerWorkflowCoordinator 将 *Scheduler 适配为 WorkflowCoordinator
type schedulerWorkflowCoordinator struct {
	s *Scheduler
}

func (c *schedulerWorkflowCoordinator) RootContext() context.Context { return c.s.ctx }
func (c *schedulerWorkflowCoordinator) GoroutineTracker() GoroutineTracker {
	return &schedulerGoroutineTracker{s: c.s}
}
func (c *schedulerWorkflowCoordinator) ExecuteWorkflow(ctx context.Context, workflowUUID string, upstreamOutputs []map[string]any, startGroupIdx int) {
	c.s.execMgr.ExecuteWorkflow(ctx, workflowUUID, upstreamOutputs, startGroupIdx)
}
func (c *schedulerWorkflowCoordinator) WatchWorkflowStatus(ctx context.Context, workflowUUID string) {
	c.s.watchWorkflowStatus(ctx, workflowUUID)
}
func (c *schedulerWorkflowCoordinator) TimeoutAfter(ctx context.Context, workflowUUID string, timeout time.Duration) {
	c.s.timeoutAfter(ctx, workflowUUID, timeout)
}
func (c *schedulerWorkflowCoordinator) StartSagaRollback(workflowUUID string, state *model.WorkflowState, failedPayload *store.TaskPayload) {
	c.s.startSagaRollback(workflowUUID, state, failedPayload)
}
func (c *schedulerWorkflowCoordinator) HandleTaskFailed(ctx context.Context, workflowUUID string, state *model.WorkflowState, payload *store.TaskPayload) {
	c.s.handleTaskFailed(ctx, workflowUUID, state, payload)
}
func (c *schedulerWorkflowCoordinator) CleanupWatcher(workflowUUID string) {
	c.s.wfWatchers.unregister(workflowUUID)
}
func (c *schedulerWorkflowCoordinator) CleanupTaskListeners(workflowUUID string) {
	c.s.cleanupTaskListeners(workflowUUID)
}
func (c *schedulerWorkflowCoordinator) CleanupPerWfCounter(workflowUUID string) {
	c.s.td.CleanupPerWfCounter(workflowUUID)
}

// ============================================================================
// MySQLDeps 适配器
// ============================================================================

// schedulerMySQLDeps 将 *Scheduler 适配为 MySQLDeps
type schedulerMySQLDeps struct {
	s *Scheduler
}

func (m *schedulerMySQLDeps) HistoryStore() store.HistoryStore     { return m.s.historyStore }
func (m *schedulerMySQLDeps) StoreTimeoutSec() int64 {
	t := int64(m.s.conf.StoreTimeout.ToDuration().Seconds())
	if t <= 0 {
		return defaultStoreTimeoutSec
	}
	return t
}
func (m *schedulerMySQLDeps) MaxHistoryCache() int                  { return m.s.conf.MaxHistoryCache }
func (m *schedulerMySQLDeps) BuildTaskDefs(taskNames []string) []store.TaskDef {
	return m.s.buildTaskDefs(taskNames)
}

// ============================================================================
// PersistenceOps 适配器
// ============================================================================

// schedulerPersistenceOps 将 *Scheduler 适配为 PersistenceOps
type schedulerPersistenceOps struct {
	s *Scheduler
}

func (p *schedulerPersistenceOps) PersistToMySQL(uuid string, workflowID, execID uint, status, errMsg string, val []byte) {
	// persistToMySQL 内部已通过 worker pool 异步执行，不再需要额外的 go func()
	p.s.repository.persistToMySQL(uuid, workflowID, execID, status, errMsg, val)
}
func (p *schedulerPersistenceOps) CleanSnapshotThrottle(uuid string) {
	p.s.cleanSnapshotThrottle(uuid)
}

// ============================================================================
// CronScheduler 适配器
// ============================================================================

// schedulerCronScheduler 将 *Scheduler 的 cronScheduler 适配为 CronScheduler
type schedulerCronScheduler struct {
	s *Scheduler
}

func (c *schedulerCronScheduler) AddCronFunc(cronExpr string, fn func()) error {
	_, err := c.s.cronScheduler.AddFunc(cronExpr, fn)
	return err
}

// ============================================================================
// EventTriggerCoordinator 事件触发协调接口
// ============================================================================

// EventTriggerCoordinator 事件触发协调接口
// WorkflowManager 在工作流完成/失败时通过此接口触发事件链
type EventTriggerCoordinator interface {
	FireWorkflowCompleted(sourceUUID string, callback func(targetUUID string))
	FireTaskFailed(sourceUUID, taskName string, callback func(targetUUID string))
}

// schedulerEventTriggerCoordinator 将 *Scheduler 的 eventTriggerMgr 适配为 EventTriggerCoordinator
type schedulerEventTriggerCoordinator struct {
	s *Scheduler
}

func (c *schedulerEventTriggerCoordinator) FireWorkflowCompleted(sourceUUID string, callback func(targetUUID string)) {
	c.s.eventTriggerMgr.FireWorkflowCompleted(sourceUUID, callback)
}

func (c *schedulerEventTriggerCoordinator) FireTaskFailed(sourceUUID, taskName string, callback func(targetUUID string)) {
	c.s.eventTriggerMgr.FireTaskFailed(sourceUUID, taskName, callback)
}

// ============================================================================
// ExtendedEventBroadcaster 扩展事件广播接口（支持超时广播）
// 与基础 EventBroadcaster 分开定义，WorkflowManager 的部分操作需要超时广播能力
// ============================================================================

// ExtendedEventBroadcaster 扩展事件广播接口，包含超时广播能力
// WorkflowManager 在 Cancel/Reject/回滚等操作中使用带超时的广播，
// 防止 channel 满时 goroutine 永久阻塞
type ExtendedEventBroadcaster interface {
	EventBroadcaster
	// BroadcastWorkflowEventToWorkflowWithTimeout 带超时的定向广播
	BroadcastWorkflowEventToWorkflowWithTimeout(workflow string, eventType model.WSEventType, data any, timeout time.Duration)
}

// schedulerExtendedEventBroadcaster 将 *Scheduler 的 wsEvent 适配为 ExtendedEventBroadcaster
type schedulerExtendedEventBroadcaster struct {
	s *Scheduler
}

func (b *schedulerExtendedEventBroadcaster) BroadcastWorkflowEvent(eventType model.WSEventType, data any) {
	b.s.wsEvent.Broadcast(model.WSMessage{Type: eventType, Data: data})
}

func (b *schedulerExtendedEventBroadcaster) BroadcastWorkflowEventToWorkflow(workflow string, eventType model.WSEventType, data any) {
	b.s.wsEvent.BroadcastToWorkflow(workflow, model.WSMessage{Type: eventType, Data: data})
}

func (b *schedulerExtendedEventBroadcaster) BroadcastWorkflowEventToWorkflowWithTimeout(workflow string, eventType model.WSEventType, data any, timeout time.Duration) {
	b.s.wsEvent.BroadcastTimeoutToWorkflow(workflow, model.WSMessage{Type: eventType, Data: data}, timeout)
}
