package scheduler

import (
	"sync/atomic"
)

// BacklogMetrics 积压指标，用于暴露给外部监控系统（如 K8s HPA、Prometheus）
// 提供 pendingPQ 长度、Worker 负载等关键指标，支持基于指标的自动扩缩容
type BacklogMetrics struct {
	// PendingTasks 当前排队等待下发的任务数（Master 端 pendingPQ 长度）
	PendingTasks atomic.Int64
	// ActiveTasks 当前正在执行的任务数（全局并发计数器值）
	ActiveTasks atomic.Int64
	// TotalWorkers 当前存活的 Worker 数量
	TotalWorkers atomic.Int64
	// AliveWorkers 当前心跳正常的 Worker 数量
	AliveWorkers atomic.Int64
	// DeadWorkers 当前失联的 Worker 数量
	DeadWorkers atomic.Int64
	// QueueDepthPerWorker 平均每个 Worker 的排队深度（PendingTasks / max(AliveWorkers, 1)）
	QueueDepthPerWorker atomic.Int64
}

// Snapshot 返回当前指标的快照
func (m *BacklogMetrics) Snapshot() BacklogMetricsSnapshot {
	return BacklogMetricsSnapshot{
		PendingTasks:       m.PendingTasks.Load(),
		ActiveTasks:        m.ActiveTasks.Load(),
		TotalWorkers:       m.TotalWorkers.Load(),
		AliveWorkers:       m.AliveWorkers.Load(),
		DeadWorkers:        m.DeadWorkers.Load(),
		QueueDepthPerWorker: m.QueueDepthPerWorker.Load(),
	}
}

// BacklogMetricsSnapshot 指标快照（值类型，并发安全）
type BacklogMetricsSnapshot struct {
	PendingTasks       int64 `json:"pending_tasks"`
	ActiveTasks        int64 `json:"active_tasks"`
	TotalWorkers       int64 `json:"total_workers"`
	AliveWorkers       int64 `json:"alive_workers"`
	DeadWorkers        int64 `json:"dead_workers"`
	QueueDepthPerWorker int64 `json:"queue_depth_per_worker"`
}

// Refresh 刷新积压指标
// 由 Scheduler 定期调用（或通过 HTTP handler 按需获取）
// Worker 统计使用 O(1) 原子计数器，不再每次遍历全部 Worker 快照
func (m *BacklogMetrics) Refresh(td *TaskDispatcher, wm *WorkerManager) {
	// 更新排队任务数
	m.PendingTasks.Store(int64(td.PendingTaskCount()))

	// 更新活跃任务数
	m.ActiveTasks.Store(td.GlobalActiveCount())

	// 更新 Worker 统计（O(1) 原子读取，无需遍历全部 Worker）
	alive := wm.AliveCount()
	dead := wm.DeadCount()
	total := alive + dead

	m.TotalWorkers.Store(total)
	m.AliveWorkers.Store(alive)
	m.DeadWorkers.Store(dead)

	// 计算平均排队深度
	if alive > 0 {
		m.QueueDepthPerWorker.Store(m.PendingTasks.Load() / alive)
	} else {
		m.QueueDepthPerWorker.Store(m.PendingTasks.Load())
	}
}
