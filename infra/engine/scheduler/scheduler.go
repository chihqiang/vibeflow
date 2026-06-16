package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/circuit"
	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/ws"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Scheduler 工作流调度器 — 协调层
// 负责组合 WorkflowManager、TaskDispatcher、WorkerManager、WorkflowRepository，
// 提供统一入口并协调各组件之间的交互。
// 拆分后的 Scheduler 自身仅持有跨组件共享的基础设施依赖（etcd、MySQL、配置、WS），
// 不再直接管理运行时状态。
type Scheduler struct {
	// --- 基础设施（跨组件共享）---
	store        store.Store
	historyStore store.HistoryStore
	conf         *config.MasterConfig
	wsEvent      *ws.WSEvent
	cronScheduler *cron.Cron

	// --- 熔断器 ---
	mysqlBreaker *circuit.Breaker // MySQL 持久化操作熔断器
	etcdBreaker  *circuit.Breaker // etcd 操作熔断器（供 Reporter 等组件使用）

	// --- 子组件 ---
	wm         *WorkflowManager    // 工作流生命周期管理
	td         *TaskDispatcher     // 任务下发与并发控制
	wkm        *WorkerManager      // Worker 管理
	repository *WorkflowRepository // MySQL 持久化

	// --- 执行管理器 ---
	execMgr *ExecutionManager

	// --- 全局 Watch 分发 ---
	wfWatchers *workflowWatchers // 管理全局 Watch → 各工作流的事件分发

	// --- 任务监听器（跨 WorkflowManager / ExecutionManager 共享）---
	// 按工作流分组的细粒度锁，避免全局锁成为热点
	taskListeners   map[string]*workflowListeners
	taskListenersMu sync.RWMutex // 仅保护 map 的增删

	// --- 事件触发管理器 ---
	eventTriggerMgr *eventTriggerManager

	// --- 错误策略处理器 ---
	errorPolicyHandler *ErrorPolicyHandler

	// --- 快照节流 ---
	// 使用 sync.Map 实现无锁的 per-workflow 节流检查，避免所有工作流竞争同一把全局锁。
	// sync.Map 内部按 key 分片，不同 workflow 的节流检查互不阻塞。
	snapshotLastTime sync.Map // key: uuid(string), value: time.Time

	// --- Saga 回滚并发控制 ---
	rollbackSem *rollbackSemaphore // 回滚并发信号量，限制同时回滚的任务数

	// --- 积压指标（用于监控和自动扩缩容）---
	metrics *BacklogMetrics

	// --- 生命周期 ---
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// circuit breaker 默认配置
const (
	// mysqlBreakerMaxFailures MySQL 操作连续失败多少次后熔断
	mysqlBreakerMaxFailures = 5
	// mysqlBreakerCooldown MySQL 熔断后的冷却时间
	mysqlBreakerCooldown = 30 * time.Second
	// etcdBreakerMaxFailures etcd 操作连续失败多少次后熔断
	etcdBreakerMaxFailures = 5
	// etcdBreakerCooldown etcd 熔断后的冷却时间
	etcdBreakerCooldown = 15 * time.Second

	// defaultShutdownTimeout Scheduler 优雅关闭的默认超时时间，当配置值 <= 0 时兜底使用
	defaultShutdownTimeout = 10 * time.Second
)

// NewScheduler 创建 Scheduler 实例
func NewScheduler(s store.Store, hs store.HistoryStore, masterCfg *config.MasterConfig, wsEvent *ws.WSEvent) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	sch := &Scheduler{
		store:            s,
		historyStore:     hs,
		conf:             masterCfg,
		wsEvent:          wsEvent,
		cronScheduler:    cron.New(cron.WithSeconds()),
		mysqlBreaker:     circuit.NewBreaker("mysql-persist", mysqlBreakerMaxFailures, mysqlBreakerCooldown),
		etcdBreaker:      circuit.NewBreaker("etcd", etcdBreakerMaxFailures, etcdBreakerCooldown),
		wfWatchers:       newWorkflowWatchers(),
		taskListeners:    make(map[string]*workflowListeners),
		eventTriggerMgr:  newEventTriggerManager(),
		rollbackSem:      newRollbackSemaphore(maxRollbackConcurrency),
		metrics:          &BacklogMetrics{},
		ctx:              ctx,
		cancel:           cancel,
	}

	// 构建窄接口适配器
	cfgProvider := &schedulerConfig{s: sch}
	tracker := &schedulerGoroutineTracker{s: sch}
	broadcaster := &schedulerEventBroadcaster{s: sch}
	snapshotMgr := &schedulerSnapshotManager{s: sch}
	listenerMgr := &schedulerListenerManager{s: sch}

	// 构建 WorkflowManager 专用接口适配器
	coordinator := &schedulerWorkflowCoordinator{s: sch}
	mysqlDeps := &schedulerMySQLDeps{s: sch}
	persistence := &schedulerPersistenceOps{s: sch}
	extBroadcaster := &schedulerExtendedEventBroadcaster{s: sch}
	cronSched := &schedulerCronScheduler{s: sch}
	eventTriggerCoord := &schedulerEventTriggerCoordinator{s: sch}

	// 初始化子组件（顺序很重要：wm → td → wkm → execMgr）
	sch.wm = NewWorkflowManager(coordinator, mysqlDeps, persistence, extBroadcaster, cronSched)
	sch.td = NewTaskDispatcher(s, cfgProvider, sch.wm)
	sch.wkm = NewWorkerManager(ctx, s, cfgProvider)
	sch.repository = NewWorkflowRepository(sch)

	// TaskDispatcher 的失败处理回调（避免循环依赖）
	sch.td.SetFailureHandlers(
		func(payload *store.TaskPayload, errMsg string) { sch.markWorkflowFailed(payload, errMsg) },
		func(workflowUUID, taskName string) { sch.wm.clearFailedTask(workflowUUID, taskName) },
	)

	sch.execMgr = NewExecutionManager(sch.wm, sch.td, snapshotMgr, listenerMgr, broadcaster, tracker, coordinator, eventTriggerCoord)

	// 初始化错误策略处理器
	sch.errorPolicyHandler = NewErrorPolicyHandler(
		func(workflowUUID string, state *model.WorkflowState, failedPayload *store.TaskPayload) {
			sch.startSagaRollback(workflowUUID, state, failedPayload)
		},
		func(payload *store.TaskPayload, errMsg string) {
			sch.markWorkflowFailed(payload, errMsg)
		},
		func(workflow string, eventType model.WSEventType, data any) {
			sch.wsEvent.BroadcastToWorkflow(workflow, model.WSMessage{Type: eventType, Data: data})
		},
		func(workflow string, eventType model.WSEventType, data any, timeout time.Duration) {
			sch.wsEvent.BroadcastTimeoutToWorkflow(workflow, model.WSMessage{Type: eventType, Data: data}, timeout)
		},
		func(workflowUUID string) {
			sch.td.ReleaseSlotForWorkflow(sch.ctx, workflowUUID)
		},
	)

	sch.cronScheduler.Start()
	go sch.wkm.watchWorkerHeartbeats()

	// 启动全局任务事件 Watcher，单连接监听所有工作流事件
	sch.wg.Add(1)
	go func() {
		defer sch.wg.Done()
		sch.globalTaskWatcher(ctx)
	}()

	// 并发加载历史记录和恢复运行中的工作流
	// 使用 Scheduler 根 context，Shutdown 时能及时取消正在进行的加载/恢复
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sch.repository.loadHistory(sch.ctx)
	}()
	go func() {
		defer wg.Done()
		sch.repository.recoverRunningWorkflows(sch.ctx)
	}()
	wg.Wait()
	return sch
}

// Shutdown 优雅关闭 Scheduler
func (s *Scheduler) Shutdown() {
	logger.Info("Master 正在关闭，等待工作流执行完毕...")
	s.cronScheduler.Stop()
	s.cancel()

	shutdownTimeout := s.conf.ShutdownTimeout.ToDuration()
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		logger.Info("所有后台 goroutine 已正常退出")
	case <-time.After(shutdownTimeout):
		logger.Warn("关闭超时，强制退出", "timeout", shutdownTimeout)
	}
	logger.Info("Master 已停止")
}

// ============================================================================
// 任务监听器管理（ParallelExecutor 使用）
// ============================================================================

// workflowListeners 单个工作流的任务监听器集合
// 自带细粒度锁，避免所有工作流争抢全局锁
type workflowListeners struct {
	mu    sync.Mutex
	chans map[string]chan *store.TaskPayload // key: taskName
}

func newWorkflowListeners() *workflowListeners {
	return &workflowListeners{
		chans: make(map[string]chan *store.TaskPayload),
	}
}

// getOrCreate 获取或创建工作流的监听器集合
func (s *Scheduler) getOrCreateListeners(workflowUUID string) *workflowListeners {
	s.taskListenersMu.RLock()
	wl, ok := s.taskListeners[workflowUUID]
	s.taskListenersMu.RUnlock()
	if ok {
		return wl
	}
	s.taskListenersMu.Lock()
	// 双重检查
	if wl, ok = s.taskListeners[workflowUUID]; ok {
		s.taskListenersMu.Unlock()
		return wl
	}
	wl = newWorkflowListeners()
	s.taskListeners[workflowUUID] = wl
	s.taskListenersMu.Unlock()
	return wl
}

func (s *Scheduler) registerTaskListener(workflowUUID, taskName string, ch chan *store.TaskPayload) {
	wl := s.getOrCreateListeners(workflowUUID)
	wl.mu.Lock()
	wl.chans[taskName] = ch
	wl.mu.Unlock()
}

// unregisterTaskListener 注销任务监听器并关闭通道
// 关闭 channel 让任何仍在等待的读取方收到零值并退出，
// 防止通道对象泄漏（GC 最终会回收，但显式关闭更符合 Go 惯例）。
func (s *Scheduler) unregisterTaskListener(workflowUUID, taskName string) {
	s.taskListenersMu.RLock()
	wl, ok := s.taskListeners[workflowUUID]
	s.taskListenersMu.RUnlock()
	if !ok {
		return
	}
	wl.mu.Lock()
	ch, chOk := wl.chans[taskName]
	delete(wl.chans, taskName)
	// 如果该工作流已无监听器，清理条目
	if len(wl.chans) == 0 {
		wl.mu.Unlock()
		s.taskListenersMu.Lock()
		if wl2, ok2 := s.taskListeners[workflowUUID]; ok2 && len(wl2.chans) == 0 {
			delete(s.taskListeners, workflowUUID)
		}
		s.taskListenersMu.Unlock()
	} else {
		wl.mu.Unlock()
	}
	// 关闭通道（安全：此时读取方已通过 defer 退出，发送方使用非阻塞 select）
	if chOk {
		close(ch)
	}
}

func (s *Scheduler) notifyTaskListener(workflowUUID, taskName string, payload *store.TaskPayload) bool {
	s.taskListenersMu.RLock()
	wl, ok := s.taskListeners[workflowUUID]
	s.taskListenersMu.RUnlock()
	if !ok {
		return false
	}
	wl.mu.Lock()
	ch, ok := wl.chans[taskName]
	wl.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- payload:
		return true
	default:
		return false
	}
}

// cleanupTaskListeners 强制清理工作流的所有任务监听器
// 工作流异常退出时，兜底清理残留的 per-task channel，
// 防止 channel 对象泄漏。安全幂等：已清理的 channel 不会被重复关闭。
func (s *Scheduler) cleanupTaskListeners(workflowUUID string) {
	s.taskListenersMu.Lock()
	wl, ok := s.taskListeners[workflowUUID]
	if ok {
		delete(s.taskListeners, workflowUUID)
	}
	s.taskListenersMu.Unlock()

	if !ok {
		return
	}

	wl.mu.Lock()
	for taskName, ch := range wl.chans {
		delete(wl.chans, taskName)
		close(ch)
	}
	wl.mu.Unlock()
}

// ============================================================================
// 快照节流
// ============================================================================

func (s *Scheduler) snapshotThrottle(uuid string, interval time.Duration) bool {
	now := time.Now()
	// LoadOrStore 原子地返回已有值，或存入新值。首次调用直接放行。
	actual, loaded := s.snapshotLastTime.LoadOrStore(uuid, now)
	if !loaded {
		return true // 首次快照，直接允许
	}

	lastTime := actual.(time.Time)
	if now.Sub(lastTime) < interval {
		return false
	}
	// 间隔已过，更新最后快照时间
	s.snapshotLastTime.Store(uuid, now)
	return true
}

// cleanSnapshotThrottle 工作流结束后清理对应的节流记录，防止 map 无限增长
func (s *Scheduler) cleanSnapshotThrottle(uuid string) {
	s.snapshotLastTime.Delete(uuid)
}

// ============================================================================
// buildTaskDefs（供 WorkflowManager.RegisterWorkflow 和 SaveWorkflow 使用）
// ============================================================================

func (s *Scheduler) buildTaskDefs(taskNames []string) []store.TaskDef {
	defs := make([]store.TaskDef, 0, len(taskNames))
	for _, name := range taskNames {
		defs = append(defs, store.TaskDef{Name: name})
	}
	return defs
}

// ============================================================================
// 对外委托方法 — WorkflowManager
// 将外部调用透明代理到子组件，保持 Scheduler 作为统一入口
// ============================================================================

// SubmitWorkflow 提交工作流执行
func (s *Scheduler) SubmitWorkflow(ctx context.Context, wf *model.Workflow) error {
	return s.wm.SubmitWorkflow(ctx, wf)
}

// SaveWorkflow 持久化工作流定义（不执行）
func (s *Scheduler) SaveWorkflow(ctx context.Context, wf *model.Workflow) error {
	return s.wm.SaveWorkflow(ctx, wf)
}

// ScheduleCronWorkflow 注册一个 Cron 定时工作流
func (s *Scheduler) ScheduleCronWorkflow(wf *model.Workflow) error {
	return s.wm.ScheduleCronWorkflow(wf)
}

// ListWorkflowStates 返回所有正在运行的工作流
func (s *Scheduler) ListWorkflowStates() []*model.WorkflowState {
	return s.wm.ListWorkflowStates()
}

// ListWorkflowDefs 返回所有已注册的工作流定义
func (s *Scheduler) ListWorkflowDefs() []*model.WorkflowState {
	return s.wm.ListWorkflowDefs()
}

// GetWorkflowState 查询正在运行中的工作流状态
func (s *Scheduler) GetWorkflowState(uuid string) *model.WorkflowState {
	return s.wm.GetWorkflowState(uuid)
}

// GetRegisteredWorkflow 按 UUID 查询单个已注册的工作流定义
func (s *Scheduler) GetRegisteredWorkflow(uuid string) *model.Workflow {
	return s.wm.GetRegisteredWorkflow(uuid)
}

// GetHistory 按 UUID 查询单个历史工作流
func (s *Scheduler) GetHistory(uuid string) *model.WorkflowState {
	return s.wm.GetHistory(uuid)
}

// ListHistory 返回所有已结束的工作流历史记录
func (s *Scheduler) ListHistory() []*model.WorkflowState {
	return s.wm.ListHistory()
}

// ListHistoryPaged 分页返回已结束的工作流历史记录
func (s *Scheduler) ListHistoryPaged(page, pageSize int) ([]*model.WorkflowState, int) {
	return s.wm.ListHistoryPaged(page, pageSize)
}

// CancelWorkflow 取消一个正在运行的工作流
func (s *Scheduler) CancelWorkflow(uuid string) error {
	return s.wm.CancelWorkflow(uuid)
}

// RetryWorkflow 从历史记录中重新提交一个已结束的工作流
func (s *Scheduler) RetryWorkflow(ctx context.Context, uuid string) error {
	return s.wm.RetryWorkflow(ctx, uuid)
}

// RunWorkflow 按 UUID 查找已注册的工作流定义并提交执行
func (s *Scheduler) RunWorkflow(ctx context.Context, uuid string) error {
	return s.wm.RunWorkflow(ctx, uuid)
}

// ApproveWorkflow 审批通过一个暂停中的工作流
func (s *Scheduler) ApproveWorkflow(uuid string) error {
	return s.wm.ApproveWorkflow(uuid)
}

// RejectWorkflow 审批驳回一个暂停中的工作流
func (s *Scheduler) RejectWorkflow(uuid string, reason string) error {
	return s.wm.RejectWorkflow(uuid, reason)
}

// CompletedCount 返回已完成工作流的原子计数器值
func (s *Scheduler) CompletedCount() int64 {
	return s.wm.CompletedCount()
}

// FailedCount 返回已失败工作流的原子计数器值
func (s *Scheduler) FailedCount() int64 {
	return s.wm.FailedCount()
}

// RegisterWorkflow 注册一个工作流定义
func (s *Scheduler) RegisterWorkflow(wf *model.Workflow) {
	s.wm.RegisterWorkflow(wf)
	// 注册事件触发监听
	s.eventTriggerMgr.RegisterEventTrigger(wf)
}

// ============================================================================
// 对外委托方法 — WorkerManager
// ============================================================================

// ListWorkers 返回所有 Worker 的当前状态快照
func (s *Scheduler) ListWorkers() []*model.WorkerState {
	return s.wkm.ListWorkers()
}

// RegisterTaskType 注册一个任务类型描述
func (s *Scheduler) RegisterTaskType(tt *model.TaskType) {
	s.wkm.RegisterTaskType(tt)
}

// ListTaskTypes 返回所有已注册的任务类型
func (s *Scheduler) ListTaskTypes() []*model.TaskType {
	return s.wkm.ListTaskTypes()
}

// NewWorkflowUUID 生成一个新的工作流 UUID
func (s *Scheduler) NewWorkflowUUID() string {
	return uuid.New().String()
}

// GetBacklogMetrics 返回积压指标快照，用于监控和自动扩缩容
func (s *Scheduler) GetBacklogMetrics() BacklogMetricsSnapshot {
	s.metrics.Refresh(s.td, s.wkm)
	return s.metrics.Snapshot()
}

// TriggerWebhook 触发一个 webhook 类型的工作流
// workflowUUID: 目标工作流 UUID
// payload: webhook 传入的参数（可选）
func (s *Scheduler) TriggerWebhook(ctx context.Context, workflowUUID string, payload map[string]any) error {
	listener, err := s.eventTriggerMgr.FireWebhook(workflowUUID)
	if err != nil {
		return err
	}

	wf := s.wm.GetRegisteredWorkflow(workflowUUID)
	if wf == nil {
		return fmt.Errorf("工作流 %s 未注册", workflowUUID)
	}

	wfCopy, err := wf.DeepCopy()
	if err != nil {
		return fmt.Errorf("深拷贝工作流失败: %w", err)
	}

	if len(payload) > 0 && len(wfCopy.TaskGroups) > 0 {
		wfCopy.TaskGroups[0][0].Params = payload
	}

	logger.Info("Webhook 触发工作流",
		"workflow", listener.WorkflowName,
		"uuid", workflowUUID,
	)

	return s.wm.SubmitWorkflow(ctx, wfCopy)
}

// ListWebhookWorkflows 列出所有注册了 Webhook 触发的工作流
func (s *Scheduler) ListWebhookWorkflows() []EventListener {
	return s.eventTriggerMgr.ListWebhookWorkflows()
}

