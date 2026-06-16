package scheduler

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
)

const (
	heartbeatWatchReconnectDelay    = 3 * time.Second
	heartbeatWatchMaxReconnectDelay = 30 * time.Second

	// defaultStaleCheckIntervalSec Worker 心跳检查默认间隔（秒），当配置值 <= 0 时兜底
	defaultStaleCheckIntervalSec = 15
	// defaultHeartbeatStaleSec Worker 心跳过期默认阈值（秒），当配置值 <= 0 时兜底
	defaultHeartbeatStaleSec = 15
)

// WorkerManager Worker 管理器
// 职责：Worker 心跳监控、注册/注销、存活性检测、任务类型注册
// 不依赖 *Scheduler，仅注入 context、store、配置
//
// 心跳优化：Worker 常规心跳仅携带 TaskTypesHash，不携带完整 TaskTypes 定义。
// workerTypeHash 记录每个 Worker 上次接收到的 hash，hash 不变时跳过 RegisterTaskType，
// 减少不必要的锁竞争和 map 写入。
//
// 统计优化：使用原子计数器（aliveCount、deadCount）替代每次遍历全部 Worker，
// 在 Worker 状态变更时增量更新，BacklogMetrics.Refresh 直接读取 O(1)。
type WorkerManager struct {
	ctx    context.Context
	store  store.Store
	config ConfigProvider

	workers        map[string]*model.WorkerState
	taskRegistry   map[string]*model.TaskType
	workerTypeHash map[string]string // workerID → 上次接收到的 TaskTypesHash

	// 原子计数器：在 Worker 状态变更时增量更新，避免 BacklogMetrics.Refresh 全量遍历
	aliveCount atomic.Int64
	deadCount  atomic.Int64

	mu sync.RWMutex
}

// NewWorkerManager 创建 Worker 管理器
func NewWorkerManager(ctx context.Context, s store.Store, cfg ConfigProvider) *WorkerManager {
	return &WorkerManager{
		ctx:            ctx,
		store:          s,
		config:         cfg,
		workers:        make(map[string]*model.WorkerState),
		taskRegistry:   make(map[string]*model.TaskType),
		workerTypeHash: make(map[string]string),
	}
}

// ============================================================================
// 心跳监控
// ============================================================================

// watchWorkerHeartbeats 持续监听 Worker 心跳事件并更新 Worker 状态
func (wm *WorkerManager) watchWorkerHeartbeats() {
	wm.scanExistingWorkers(wm.ctx)

	staleCheckInterval := wm.config.StaleCheckIntervalSec()
	if staleCheckInterval <= 0 {
		staleCheckInterval = defaultStaleCheckIntervalSec
	}
	staleTicker := time.NewTicker(time.Duration(staleCheckInterval) * time.Second)
	defer staleTicker.Stop()

	reconnectDelay := heartbeatWatchReconnectDelay

	for {
		eventChan, err := wm.store.Watch(wm.ctx, wm.store.Prefixes().Heartbeats)
		if err != nil {
			if wm.ctx.Err() != nil {
				return
			}
			logger.Error("监听心跳失败，准备重连", "error", err, "delay", reconnectDelay)
			select {
			case <-time.After(reconnectDelay):
				reconnectDelay = min(reconnectDelay*2, heartbeatWatchMaxReconnectDelay)
				continue
			case <-wm.ctx.Done():
				return
			}
		}

		reconnectDelay = heartbeatWatchReconnectDelay
		logger.Info("Worker 心跳监听已建立")

		wm.processHeartbeatEvents(eventChan, staleTicker)
	}
}

// processHeartbeatEvents 处理心跳事件循环
func (wm *WorkerManager) processHeartbeatEvents(eventChan <-chan store.Event, staleTicker *time.Ticker) {
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				logger.Warn("Worker 心跳 Watch 通道已关闭，准备重连")
				return
			}
		switch event.Type {
		case store.EventPut:
			hb, err := store.DeserializeHeartbeat(event.Value)
			if err != nil {
				continue
			}
			wm.mu.Lock()
			if existing, exists := wm.workers[hb.WorkerID]; exists {
				// 已有 Worker：状态可能从 Dead 变为 Alive
				if existing.Status == model.WorkerStatusDead {
					wm.deadCount.Add(-1)
				}
				existing.Status = model.WorkerStatusAlive
				existing.AliveAt = hb.AliveAt
				existing.Tasks = hb.Tasks
			} else {
				// 新 Worker
				wm.workers[hb.WorkerID] = &model.WorkerState{
					WorkerID:  hb.WorkerID,
					Tasks:     hb.Tasks,
					Status:    model.WorkerStatusAlive,
					StartedAt: hb.StartedAt,
					AliveAt:   hb.AliveAt,
				}
				wm.aliveCount.Add(1)
			}
			wm.mu.Unlock()

			// 心跳优化：仅当 hash 变化或首次收到该 Worker 心跳时才更新任务类型注册表
			prevHash := wm.workerTypeHash[hb.WorkerID]
			if hb.TaskTypesHash != "" && hb.TaskTypesHash == prevHash && prevHash != "" {
				// hash 未变化，跳过注册（TaskTypes 在常规心跳中为空，也不需处理）
				continue
			}
			wm.workerTypeHash[hb.WorkerID] = hb.TaskTypesHash

			if len(hb.TaskTypes) > 0 {
				for i := range hb.TaskTypes {
					wm.RegisterTaskType(&hb.TaskTypes[i])
				}
			}

		case store.EventDelete:
			workerID := extractWorkerID(event.Key, wm.store.Prefixes().Heartbeats)
			if workerID == "" {
				continue
			}
			wm.mu.Lock()
			if w, ok := wm.workers[workerID]; ok {
				switch w.Status {
				case model.WorkerStatusAlive:
					wm.aliveCount.Add(-1)
				case model.WorkerStatusDead:
					wm.deadCount.Add(-1)
				}
				delete(wm.workers, workerID)
			}
			wm.mu.Unlock()
			logger.Info("Worker 已离线", "worker_id", workerID)
		}

		case <-staleTicker.C:
			wm.markStaleWorkers()

		case <-wm.ctx.Done():
			return
		}
	}
}

// scanExistingWorkers 启动时扫描 etcd 中已有的 Worker 心跳记录
// 同时初始化 aliveCount 原子计数器
func (wm *WorkerManager) scanExistingWorkers(ctx context.Context) {
	kvs, err := wm.store.List(ctx, wm.store.Prefixes().Heartbeats)
	if err != nil {
		logger.Warn("扫描已有 Worker 失败", "error", err)
		return
	}
	wm.mu.Lock()
	for _, kv := range kvs {
		hb, err := store.DeserializeHeartbeat(kv.Value)
		if err != nil {
			continue
		}
		workerID := extractWorkerID(kv.Key, wm.store.Prefixes().Heartbeats)
		if workerID == "" {
			continue
		}
		wm.workers[workerID] = &model.WorkerState{
			WorkerID:  hb.WorkerID,
			Tasks:     hb.Tasks,
			Status:    model.WorkerStatusAlive,
			StartedAt: hb.StartedAt,
			AliveAt:   hb.AliveAt,
		}
		wm.workerTypeHash[workerID] = hb.TaskTypesHash
		if len(hb.TaskTypes) > 0 {
			for i := range hb.TaskTypes {
				wm.RegisterTaskType(&hb.TaskTypes[i])
			}
		}
	}
	wm.aliveCount.Store(int64(len(wm.workers)))
	wm.mu.Unlock()
	logger.Info("扫描已有 Worker 完成", "count", len(kvs))
}

// markStaleWorkers 检查所有 Worker 的最后心跳时间，超时的标记为 dead
// 同时增量更新 aliveCount / deadCount 原子计数器
// 使用两阶段策略：先用读锁快速遍历找出需要标记的 Worker，再用写锁批量更新
// 减少写锁持有时间，避免阻塞 RegisterTaskType、ListWorkers 等读操作
func (wm *WorkerManager) markStaleWorkers() {
	now := time.Now()
	staleThresholdSec := wm.config.HeartbeatStaleSec()
	if staleThresholdSec <= 0 {
		staleThresholdSec = defaultHeartbeatStaleSec
	}
	staleThreshold := time.Duration(staleThresholdSec) * time.Second

	// 阶段 1：读锁遍历，收集需要标记的 Worker
	wm.mu.RLock()
	staleIDs := make([]string, 0)
	for id, w := range wm.workers {
		if w.Status == model.WorkerStatusDead {
			continue
		}
		if now.Sub(w.AliveAt) > staleThreshold {
			staleIDs = append(staleIDs, id)
		}
	}
	wm.mu.RUnlock()

	if len(staleIDs) == 0 {
		return
	}

	// 阶段 2：写锁批量更新
	wm.mu.Lock()
	for _, id := range staleIDs {
		if w, ok := wm.workers[id]; ok && w.Status != model.WorkerStatusDead {
			w.Status = model.WorkerStatusDead
			wm.aliveCount.Add(-1)
			wm.deadCount.Add(1)
			logger.Warn("Worker 已失联", "worker_id", id, "last_seen", w.AliveAt)
		}
	}
	wm.mu.Unlock()
}

// ============================================================================
// 查询接口
// ============================================================================

// ListWorkers 返回所有 Worker 的当前状态快照
func (wm *WorkerManager) ListWorkers() []*model.WorkerState {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	list := make([]*model.WorkerState, 0, len(wm.workers))
	for _, w := range wm.workers {
		list = append(list, w)
	}
	return list
}

// AliveCount 返回当前存活的 Worker 数量（O(1) 原子读取）
func (wm *WorkerManager) AliveCount() int64 {
	return wm.aliveCount.Load()
}

// DeadCount 返回当前失联的 Worker 数量（O(1) 原子读取）
func (wm *WorkerManager) DeadCount() int64 {
	return wm.deadCount.Load()
}

// TotalCount 返回 Worker 总数（O(1) 原子读取）
func (wm *WorkerManager) TotalCount() int64 {
	return wm.aliveCount.Load() + wm.deadCount.Load()
}

// ============================================================================
// 任务类型注册
// ============================================================================

// extractWorkerID 从 etcd key 中提取 Worker ID（去掉前缀）
func extractWorkerID(key, prefix string) string {
	if len(key) <= len(prefix) {
		return ""
	}
	return key[len(prefix):]
}

// RegisterTaskType 注册一个任务类型描述
func (wm *WorkerManager) RegisterTaskType(tt *model.TaskType) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if existing, exists := wm.taskRegistry[tt.Name]; exists {
		if existing.Description == tt.Description && reflect.DeepEqual(existing.Params, tt.Params) {
			// 完全一致，跳过（使用已有的注册，避免无意义的 map 写入）
			return
		}
		logger.Debug("任务类型已存在，更新定义", "task", tt.Name)
	}
	wm.taskRegistry[tt.Name] = tt
	logger.Info("注册任务类型", "task", tt.Name)
}

// ListTaskTypes 返回所有已注册的任务类型
func (wm *WorkerManager) ListTaskTypes() []*model.TaskType {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	types := make([]*model.TaskType, 0, len(wm.taskRegistry))
	for _, tt := range wm.taskRegistry {
		types = append(types, tt)
	}
	return types
}
