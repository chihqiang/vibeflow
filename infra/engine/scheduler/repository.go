package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"math"
	"sync"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
)

const (
	persistBaseBackoff        = 500 * time.Millisecond
	persistMaxBackoff         = 10 * time.Second
	defaultMaxHistoryInMemory = 500 // 内存历史缓存默认最大条数，可被 config.MaxHistoryCache 覆盖

	// persistWorkerPoolSize MySQL 持久化 goroutine 池大小。
	// 限制同时进行 MySQL 写入的 goroutine 数量，避免 MySQL 慢查询时 goroutine 爆炸。
	// 当池满时，新的持久化请求在 channel 中排队等待（带超时兜底）。
	persistWorkerPoolSize = 32

	// persistEnqueueTimeout 持久化请求入队超时时间。
	// 当 worker pool 的 channel 满时，等待此时间后放弃持久化（非关键路径，可降级）。
	persistEnqueueTimeout = 5 * time.Second
)

// persistJob MySQL 持久化任务
type persistJob struct {
	uuid       string
	workflowID uint
	execID     uint
	status     string
	errMsg     string
	val        []byte
}

// persistWorkerPool 固定大小的持久化 goroutine 池
// 替代每次 go func() 的方案，限制并发持久化 goroutine 数量，
// 避免 MySQL 慢查询或不可用时 goroutine 堆积导致 OOM。
type persistWorkerPool struct {
	jobs    chan persistJob
	ctx     context.Context
	process func(job persistJob)
	wg      sync.WaitGroup
}

// newPersistWorkerPool 创建持久化 goroutine 池
func newPersistWorkerPool(ctx context.Context, size int, process func(job persistJob)) *persistWorkerPool {
	pool := &persistWorkerPool{
		jobs:    make(chan persistJob, size*4), // 缓冲区为池大小的 4 倍，平滑瞬时突发
		ctx:     ctx,
		process: process,
	}
	for i := 0; i < size; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}
	return pool
}

// submit 提交一个持久化任务到 worker pool
// 使用非阻塞发送 + 超时兜底：池满时短暂等待，超时后丢弃本次持久化（非关键路径可降级）
func (p *persistWorkerPool) submit(job persistJob) bool {
	select {
	case p.jobs <- job:
		return true
	default:
	}

	// channel 满，等待一段时间
	timer := time.NewTimer(persistEnqueueTimeout)
	select {
	case p.jobs <- job:
		timer.Stop()
		return true
	case <-timer.C:
		// 超时，放弃本次持久化
		return false
	case <-p.ctx.Done():
		timer.Stop()
		return false
	}
}

func (p *persistWorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case job := <-p.jobs:
			p.process(job)
		case <-p.ctx.Done():
			return
		}
	}
}

// shutdown 等待所有 worker 完成当前任务后关闭
func (p *persistWorkerPool) shutdown() {
	close(p.jobs)
	p.wg.Wait()
}

// WorkflowRepository 工作流持久化仓库
// 职责：MySQL 持久化逻辑（写历史、快照、恢复）、与 HistoryStore 交互
// 从 Scheduler 拆分而来，独立管理所有 MySQL 读写
type WorkflowRepository struct {
	scheduler   *Scheduler
	persistPool *persistWorkerPool // 固定大小的持久化 goroutine 池
}

// NewWorkflowRepository 创建持久化仓库
func NewWorkflowRepository(s *Scheduler) *WorkflowRepository {
	repo := &WorkflowRepository{scheduler: s}
	// 初始化持久化 goroutine 池，使用固定 worker 数量避免 goroutine 爆炸
	repo.persistPool = newPersistWorkerPool(s.ctx, persistWorkerPoolSize, func(job persistJob) {
		repo.persistToMySQLInternal(job.uuid, job.workflowID, job.execID, job.status, job.errMsg, job.val)
	})
	return repo
}

// backoffWithJitter 计算指数退避 + 抖动的等待时间
// 使用 crypto/rand 替代 math/rand，避免伪随机数预测
func backoffWithJitter(attempt int, base, max time.Duration) time.Duration {
	exp := time.Duration(math.Min(
		float64(base)*math.Pow(2, float64(attempt)),
		float64(max),
	))
	var b [8]byte
	jitterN := int64(exp) / 2
	if jitterN <= 0 {
		return exp
	}
	if _, err := rand.Read(b[:]); err != nil {
		return exp
	}
	jitter := time.Duration(int64(binary.BigEndian.Uint64(b[:])) % jitterN)
	return exp + jitter
}

// persistToMySQL 将工作流状态异步写入 MySQL（通过固定大小的 goroutine 池）
// 替代原来的 go func() 方式，避免 MySQL 慢查询时 goroutine 爆炸。
// 参数为调用方在锁内拷贝的值，确保异步协程不访问可能被并发修改的 state。
// 池满或超时时会丢弃本次持久化（非关键路径可降级，etcd 中有完整状态）。
func (r *WorkflowRepository) persistToMySQL(uuid string, workflowID, execID uint, status, errMsg string, val []byte) {
	job := persistJob{
		uuid:       uuid,
		workflowID: workflowID,
		execID:     execID,
		status:     status,
		errMsg:     errMsg,
		val:        val,
	}
	if !r.persistPool.submit(job) {
		logger.Warn("持久化任务提交失败（池满或超时），降级跳过",
			"uuid", uuid,
			"pool_size", persistWorkerPoolSize,
		)
	}
}

// persistToMySQLInternal 将工作流状态写入 MySQL，失败后各自独立重试 3 次
// 熔断器保护：当 MySQL 连续不可用时快速失败，避免重试 goroutine 堆积
// 此方法由 persistWorkerPool 中的 worker goroutine 调用，不再额外创建 goroutine
func (r *WorkflowRepository) persistToMySQLInternal(uuid string, workflowID, execID uint, status, errMsg string, val []byte) {
	const maxRetries = 3
	hs := r.scheduler.historyStore
	breaker := r.scheduler.mysqlBreaker

	// 熔断检查
	if !breaker.Allow() {
		logger.Warn("MySQL 熔断器已打开，跳过持久化", "uuid", uuid, "state", breaker.State())
		return
	}

	statusOk := false
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(r.scheduler.ctx, r.scheduler.conf.StoreTimeout.ToDuration())
		err := hs.UpdateWorkflowStatus(ctx, workflowID, status)
		cancel()
		if err == nil {
			statusOk = true
			break
		}
		logger.Error("更新工作流状态失败", "uuid", uuid, "error", err, "attempt", i+1)
		if i == maxRetries-1 {
			logger.Error("UpdateWorkflowStatus 重试耗尽", "uuid", uuid, "error", err)
		} else {
			time.Sleep(backoffWithJitter(i, persistBaseBackoff, persistMaxBackoff))
		}
	}

	if !statusOk {
		breaker.Failure()
	}

	var lastErr error
	execOk := false
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(r.scheduler.ctx, r.scheduler.conf.StoreTimeout.ToDuration())
		err := hs.UpdateExecution(ctx, execID, val, status, errMsg)
		cancel()
		if err == nil {
			execOk = true
			break
		}
		logger.Error("更新执行记录失败", "uuid", uuid, "error", err, "attempt", i+1)
		lastErr = err
		if i < maxRetries-1 {
			time.Sleep(backoffWithJitter(i, persistBaseBackoff, persistMaxBackoff))
		}
	}

	if execOk {
		breaker.Success()
	} else {
		logger.Error("UpdateExecution 重试耗尽", "uuid", uuid, "error", lastErr)
		breaker.Failure()
	}
}

// snapshotThrottleInterval 快照节流默认间隔。
// 增量快照仅记录变更的任务（TaskDelta），单次开销很小，但高并发下任务完成事件
// 可能非常密集。将间隔从 2s 提高到 5s 可有效减少 MySQL 写入频率，同时不影响
// 恢复精度（5s 的增量丢失在可接受范围内）。
const snapshotThrottleInterval = 5 * time.Second

// snapshotRunningWorkflow 将运行中工作流的增量变更写入 MySQL
// 增量快照：每次任务完成/失败/回滚仅记录变更的任务（TaskDelta），而非整个 WorkflowState
// 快照大小从 O(总任务数) 降为 O(1)，大幅减少 MySQL 写入开销
// 熔断器保护：MySQL 不可用时跳过快照，避免 goroutine 堆积
func (r *WorkflowRepository) snapshotRunningWorkflow(uuid string) {
	if !r.scheduler.snapshotThrottle(uuid, snapshotThrottleInterval) {
		return
	}

	// 熔断检查：MySQL 不可用时直接跳过
	breaker := r.scheduler.mysqlBreaker
	if !breaker.Allow() {
		return
	}

	entry := r.scheduler.wm.RLockEntry(uuid)
	if entry == nil {
		return
	}
	execID := entry.state.ExecutionID
	if execID == 0 || r.scheduler.historyStore == nil {
		entry.mu.RUnlock()
		return
	}

	// 只序列化变更的任务（增量），而非整个 WorkflowState
	snapshot := model.WorkflowSnapshot{
		WorkflowUUID: uuid,
		ExecutionID:  execID,
		Status:       string(entry.state.Status),
		Error:        entry.state.Error,
		UpdatedAt:    time.Now(),
		ChangedTasks: r.collectTaskDeltasLocked(entry.state),
	}
	entry.mu.RUnlock()

	val, err := json.Marshal(&snapshot)
	if err != nil {
		logger.Error("快照增量序列化失败", "uuid", uuid, "error", err)
		return
	}

	// 从 Scheduler 根 context 派生，Shutdown 时能及时取消
	// 同时用 scheduler.wg 追踪 goroutine 生命周期
	r.scheduler.wg.Add(1)
	go func() {
		defer r.scheduler.wg.Done()
		ctx, cancel := context.WithTimeout(r.scheduler.ctx, r.scheduler.conf.StoreTimeout.ToDuration())
		defer cancel()
		if err := r.scheduler.historyStore.SaveTaskDeltas(ctx, execID, val); err != nil {
			logger.Error("增量快照写入失败", "uuid", uuid, "error", err)
			breaker.Failure()
		} else {
			breaker.Success()
		}
	}()
}

// collectTaskDeltasLocked 从 WorkflowState 中提取自上次快照以来新增/变更的任务作为增量
// 调用方必须持有 entry.mu.RLock() 或 entry.mu.Lock()
// 维护 snapshotSeq 计数器，通过 snapshotted map 判断哪些任务是新的变更，
// 避免每次快照都序列化所有历史任务，真正实现 O(增量) 而非 O(总数)
func (r *WorkflowRepository) collectTaskDeltasLocked(state *model.WorkflowState) []model.TaskDelta {
	// 初始化追踪字段（首次快照或从 MySQL 恢复后）
	if state.Snapshotted == nil {
		state.Snapshotted = make(map[string]uint64)
	}

	// 预估容量：本轮新增的任务数
	deltas := make([]model.TaskDelta, 0, 4)

	for taskName, output := range state.CompletedTasks {
		if state.Snapshotted[taskName] == 0 {
			deltas = append(deltas, model.TaskDelta{
				TaskName: taskName,
				Action:   "completed",
				Output:   output,
			})
		}
	}
	for taskName, result := range state.FailedTasks {
		if state.Snapshotted[taskName] == 0 {
			deltas = append(deltas, model.TaskDelta{
				TaskName: taskName,
				Action:   "failed",
				Result:   result,
			})
		}
	}
	for taskName := range state.RolledBack {
		if state.Snapshotted[taskName] == 0 {
			deltas = append(deltas, model.TaskDelta{
				TaskName: taskName,
				Action:   "rolled_back",
			})
		}
	}

	// 递增版本号，并将本轮新快照的任务标记为已快照
	state.SnapshotSeq++
	for _, d := range deltas {
		state.Snapshotted[d.TaskName] = state.SnapshotSeq
	}

	// 清理 snapshotted 中已不存在于任何集合的旧条目（任务可能被重试清除）
	// 注意：此清理仅在有 deltas 时才执行，避免每次快照都全量扫描
	if len(deltas) > 0 && state.SnapshotSeq%10 == 0 {
		for name := range state.Snapshotted {
			_, inCompleted := state.CompletedTasks[name]
			_, inFailed := state.FailedTasks[name]
			_, inRolledBack := state.RolledBack[name]
			if !inCompleted && !inFailed && !inRolledBack {
				delete(state.Snapshotted, name)
			}
		}
	}

	return deltas
}

// ============================================================================
// 启动时恢复
// ============================================================================

// loadHistory 启动时从 MySQL 加载历史记录到内存
func (r *WorkflowRepository) loadHistory(ctx context.Context) {
	hs := r.scheduler.historyStore
	if hs == nil {
		return
	}
	maxInMemory := r.scheduler.conf.MaxHistoryCache
	if maxInMemory <= 0 {
		maxInMemory = defaultMaxHistoryInMemory
	}
	records, total, err := hs.LoadExecutions(ctx, 0, maxInMemory)
	if err != nil {
		logger.Warn("从 MySQL 加载历史记录失败", "error", err)
		return
	}
	for _, rec := range records {
		var state model.WorkflowState
		if err := json.Unmarshal(rec.Data, &state); err != nil {
			logger.Warn("解析历史记录失败", "uuid", rec.UUID, "error", err)
			continue
		}
		r.scheduler.wm.mu.Lock()
		r.scheduler.wm.workflowHistory[rec.ExecutionID] = &state
		r.scheduler.wm.workflowHistoryByUUID[rec.UUID] = append(r.scheduler.wm.workflowHistoryByUUID[rec.UUID], rec.ExecutionID)
		r.scheduler.wm.historyOrder = append(r.scheduler.wm.historyOrder, rec.ExecutionID)
		r.scheduler.wm.mu.Unlock()
	}
	if int64(maxInMemory) < total {
		logger.Info("已加载工作流历史（截断）", "loaded", len(records), "total", total, "limit", maxInMemory)
	} else {
		logger.Info("已加载工作流历史", "count", total)
	}
}

// recoverRunningWorkflows 从 MySQL 恢复状态为 RUNNING 的工作流
// 完全依赖 MySQL 的增量快照（task_deltas）和 WorkflowState 来判断恢复进度，
// 不再依赖 etcd 中的任务数据（etcd 任务有 24h TTL，过期后无法用于恢复）
//
// 并行恢复：批量加载增量快照 + 并行恢复多个工作流。
//   - 使用 BatchLoadTaskDeltas 一次 SQL 查询加载所有工作流的增量数据
//   - 通过 goroutine + WaitGroup 并行恢复多个工作流
//   - 工作流数量多时显著减少启动时间
//
// 幂等保护：
//   - 恢复前检查 etcd 中是否已有该工作流的 PENDING 任务，跳过已由 Worker 处理的任务组
//   - 为恢复的工作流设置恢复标志（Recovered），watchWorkflowStatus 特殊处理恢复期间的事件
//   - 防止 taskListeners 被重复注册（通过检查 runningEntries 中是否已有相同 workflowUUID）
func (r *WorkflowRepository) recoverRunningWorkflows(ctx context.Context) {
	hs := r.scheduler.historyStore
	if hs == nil {
		return
	}

	records, err := hs.LoadRunningExecutions(ctx)
	if err != nil {
		logger.Warn("加载运行中的工作流失败", "error", err)
		return
	}

	if len(records) == 0 {
		logger.Info("无需要恢复的运行中工作流")
		return
	}

	logger.Info("开始并行恢复运行中的工作流", "count", len(records))

	// 批量加载所有工作流的增量快照，一次 SQL 查询替代 N 次
	executionIDs := make([]uint, 0, len(records))
	for _, rec := range records {
		executionIDs = append(executionIDs, rec.ExecutionID)
	}
	allDeltas, batchErr := hs.BatchLoadTaskDeltas(ctx, executionIDs)
	if batchErr != nil {
		logger.Warn("批量加载增量快照失败，回退到逐个加载", "error", batchErr)
		allDeltas = nil // 回退到逐个加载
	} else {
		logger.Info("批量加载增量快照完成", "executions", len(executionIDs), "with_deltas", len(allDeltas))
	}

	// 并行恢复多个工作流
	// 使用带缓冲的信号量控制并发度，避免同时打开过多 goroutine
	const maxParallelRecovery = 10 // 最多并行恢复 10 个工作流
	sem := make(chan struct{}, maxParallelRecovery)

	var recoverWg sync.WaitGroup
	for _, rec := range records {
		recoverWg.Add(1)
		go func(rec store.ExecutionRecord) {
			defer recoverWg.Done()

			// 获取并发恢复槽位
			sem <- struct{}{}
			defer func() { <-sem }()

			r.recoverSingleWorkflow(ctx, rec, allDeltas)
		}(rec)
	}
	recoverWg.Wait()
	logger.Info("并行恢复运行中工作流完成", "count", len(records))
}

// recoverSingleWorkflow 恢复单个工作流
// 从 recoverRunningWorkflows 中提取，支持并行调用
func (r *WorkflowRepository) recoverSingleWorkflow(ctx context.Context, rec store.ExecutionRecord, allDeltas map[uint][]byte) {
	var state model.WorkflowState
	if err := json.Unmarshal(rec.Data, &state); err != nil {
		logger.Warn("解析运行中工作流失败", "uuid", rec.UUID, "error", err)
		return
	}

	workflowUUID := state.WorkflowUUID
	if workflowUUID == "" && state.Workflow != nil {
		workflowUUID = state.Workflow.UUID
	}
	if workflowUUID == "" {
		logger.Warn("恢复工作流失败：缺少 UUID", "execution_id", rec.ExecutionID)
		return
	}

	wm := r.scheduler.wm
	hs := r.scheduler.historyStore

	// ====================================================================
	// 幂等保护 1：检查 etcd 中是否有该工作流的 PENDING 任务
	// ====================================================================
	etcdPendingTasks := r.scanEtcdPendingTasks(ctx, workflowUUID)

	// 应用增量快照：优先使用批量加载的数据，回退到逐个加载
	if allDeltas != nil {
		if raw, ok := allDeltas[rec.ExecutionID]; ok {
			var deltas []model.TaskDelta
			if err := json.Unmarshal(raw, &deltas); err == nil {
				state.ApplyDeltas(deltas)
				logger.Debug("已应用增量快照（批量）", "execution_id", rec.ExecutionID, "deltas", len(deltas))
			}
		}
	} else {
		r.applyDeltasToState(ctx, hs, rec.ExecutionID, &state)
	}

	// 基于 state.CompletedTasks（由 deltas 恢复）判断各任务组的完成情况
	groups := state.Workflow.TaskGroups
	var startGroupIdx int
	var upstreamOutputs []map[string]any
	for gi, g := range groups {
		allCompleted := true
		for _, node := range g {
			if _, ok := state.CompletedTasks[node.Name]; !ok {
				if _, pending := etcdPendingTasks[node.Name]; pending {
					logger.Info("恢复幂等保护：etcd 中存在 PENDING 任务，跳过该任务组",
						"workflow", workflowUUID,
						"task", node.Name,
						"group", gi+1,
					)
					allCompleted = false
					break
				}
				allCompleted = false
				break
			}
		}
		if !allCompleted {
			startGroupIdx = gi
			break
		}
		for _, node := range g {
			if out, ok := state.CompletedTasks[node.Name]; ok {
				upstreamOutputs = append(upstreamOutputs, out)
			}
		}
		startGroupIdx = gi + 1
	}

	wfCtx, wfCancel := context.WithCancel(r.scheduler.ctx)
	state.Status = model.WorkflowStatusRunning
	if state.TaskGroupIndex == nil {
		state.TaskGroupIndex = state.Workflow.BuildTaskGroupIndex()
	}
	if state.BranchTaskIndex == nil {
		state.BranchTaskIndex = state.Workflow.BuildBranchTaskIndex()
	}

	// ====================================================================
	// 幂等保护 2：检查 runningEntries 中是否已有该工作流
	// ====================================================================
	wm.mu.Lock()
	if existingEntry, exists := wm.runningEntries[workflowUUID]; exists {
		wm.mu.Unlock()
		logger.Warn("恢复幂等保护：工作流已在运行中，跳过重复恢复",
			"uuid", workflowUUID,
			"existing_status", existingEntry.state.Status,
		)
		wfCancel()
		return
	}

	entry := &workflowEntry{
		state:     &state,
		cancel:    wfCancel,
		recovered: true,
	}
	wm.runningEntries[workflowUUID] = entry
	wm.mu.Unlock()

	r.scheduler.wg.Add(1)
	go func(uuid string, wfCtx context.Context) {
		defer r.scheduler.wg.Done()
		r.scheduler.watchWorkflowStatus(wfCtx, uuid)
	}(workflowUUID, wfCtx)

	if state.Workflow.TimeoutSec > 0 {
		r.scheduler.wg.Add(1)
		go func(uuid string, wfCtx context.Context) {
			defer r.scheduler.wg.Done()
			elapsed := time.Since(state.StartedAt)
			remaining := time.Duration(state.Workflow.TimeoutSec)*time.Second - elapsed
			if remaining > 0 {
				r.scheduler.timeoutAfter(wfCtx, uuid, remaining)
			} else {
				r.scheduler.timeoutAfter(wfCtx, uuid, 0)
			}
		}(workflowUUID, wfCtx)
	}

	if startGroupIdx < len(groups) {
		logger.Info("恢复工作流，从任务组继续执行",
			"uuid", workflowUUID,
			"start_group", startGroupIdx+1,
			"total_groups", len(groups),
		)
		r.scheduler.wg.Add(1)
		go func(uuid string, wfCtx context.Context) {
			defer r.scheduler.wg.Done()
			r.scheduler.execMgr.ExecuteWorkflow(wfCtx, uuid, upstreamOutputs, startGroupIdx)
		}(workflowUUID, wfCtx)
	} else {
		if state.Status == model.WorkflowStatusPaused {
			logger.Info("恢复的工作流处于暂停状态，等待审批",
				"uuid", workflowUUID,
				"paused_task", state.PausedTaskName,
			)
		} else {
			logger.Info("恢复工作流完成，所有任务已执行",
				"uuid", workflowUUID)
		}
	}

	logger.Info("已恢复运行中的工作流", "uuid", workflowUUID)
}

// scanEtcdPendingTasks 扫描 etcd 中指定工作流的 PENDING 任务
// 返回 taskName → true 的映射，用于恢复时的幂等保护
//
// 优化：通过 {prefix}PENDING/{workflowUUID}/ 前缀直接扫描，无需 JSON 反序列化判断 status。
func (r *WorkflowRepository) scanEtcdPendingTasks(ctx context.Context, workflowUUID string) map[string]bool {
	result := make(map[string]bool)
	taskPrefix := r.scheduler.store.Prefixes().Tasks
	// 构造 {prefix}PENDING/{workflowUUID}/ 前缀，直接定位到指定工作流的 PENDING 任务
	prefix := taskPrefix + string(store.StatusPending) + "/" + workflowUUID + "/"

	kvs, err := r.scheduler.store.List(ctx, prefix)
	if err != nil {
		logger.Warn("恢复幂等保护：扫描 etcd PENDING 任务失败，跳过检查",
			"workflow", workflowUUID, "error", err)
		return result
	}

	for _, kv := range kvs {
		payload, err := store.Deserialize(kv.Value)
		if err != nil {
			continue
		}
		result[payload.TaskName] = true
	}

	if len(result) > 0 {
		logger.Info("恢复幂等保护：发现 etcd 中仍有 PENDING 任务",
			"workflow", workflowUUID,
			"pending_count", len(result),
		)
	}
	return result
}

// applyDeltasToState 从 MySQL 加载增量快照并应用到 WorkflowState
func (r *WorkflowRepository) applyDeltasToState(ctx context.Context, hs store.HistoryStore, execID uint, state *model.WorkflowState) {
	raw, err := hs.LoadTaskDeltas(ctx, execID)
	if err != nil {
		logger.Warn("加载增量快照失败，跳过增量恢复", "execution_id", execID, "error", err)
		return
	}
	if raw == nil {
		return
	}
	// raw 是 JSON 数组: [{"task_name":..., "action":..., ...}, ...]
	var deltas []model.TaskDelta
	if err := json.Unmarshal(raw, &deltas); err != nil {
		logger.Warn("解析增量快照失败", "execution_id", execID, "error", err)
		return
	}
	state.ApplyDeltas(deltas)
	logger.Info("已应用增量快照", "execution_id", execID, "deltas", len(deltas))
}

// saveTaskRecord 异步保存任务执行记录到 MySQL
// 熔断器保护：MySQL 不可用时跳过保存，避免 goroutine 堆积
// 从 Scheduler 根 context 派生，Shutdown 时能及时取消
func (r *WorkflowRepository) saveTaskRecord(workflowUUID string, payload *store.TaskPayload, execID, workflowID uint) {
	hs := r.scheduler.historyStore
	if hs == nil || execID == 0 {
		return
	}

	breaker := r.scheduler.mysqlBreaker
	if !breaker.Allow() {
		return
	}

	r.scheduler.wg.Add(1)
	go func(p *store.TaskPayload) {
		defer r.scheduler.wg.Done()
		ctx, cancel := context.WithTimeout(r.scheduler.ctx, r.scheduler.conf.StoreTimeout.ToDuration())
		defer cancel()
		if err := hs.SaveTask(ctx, execID, workflowID, p.TaskName, string(p.Status),
			p.Params, p.Output, "", p.RetryCount, p.MaxRetries); err != nil {
			logger.Error("保存任务执行记录失败", "uuid", workflowUUID, "task", p.TaskName, "error", err)
			breaker.Failure()
		} else {
			breaker.Success()
		}
	}(payload)
}
