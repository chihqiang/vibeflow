package handlers

import (
	"context"
	"net/http"
	"time"

	"chihqiang/vibeflow/infra/engine/scheduler"
	"chihqiang/vibeflow/infra/store"

	"github.com/gin-gonic/gin"
)

// Response 通用 API 响应结构，所有 API 端点统一使用此格式
type Response struct {
	Code int    `json:"code"`           // 业务状态码：0=成功，非0=失败
	Msg  string `json:"msg"`            // 状态描述，失败时包含错误信息
	Data any    `json:"data,omitempty"` // 成功时的响应数据
}

// healthCheckTimeout 各子检查的超时时间
const healthCheckTimeout = 3 * time.Second

// HealthStatus 深度健康检查响应
type HealthStatus struct {
	Status  string        `json:"status"`  // "ok" 或 "unhealthy"
	Details HealthDetails `json:"details"`
}

// HealthDetails 各组件检查详情
type HealthDetails struct {
	Etcd  EtcdHealth  `json:"etcd"`
	MySQL MySQLHealth `json:"mysql"`
	Queue QueueHealth `json:"queue"`
}

// EtcdHealth etcd 连接检查
type EtcdHealth struct {
	OK      bool   `json:"ok"`
	Latency string `json:"latency"`          // 延迟，如 "3.2ms"
	Error   string `json:"error,omitempty"`  // 失败时的错误信息
}

// MySQLHealth MySQL 连接检查
type MySQLHealth struct {
	OK        bool   `json:"ok"`
	Latency   string `json:"latency"`
	OpenConns int    `json:"open_conns"`     // 当前打开的连接数
	InUse     int    `json:"in_use"`         // 正在使用的连接数
	Idle      int    `json:"idle"`           // 空闲连接数
	MaxOpen   int    `json:"max_open"`       // 最大连接数配置
	Error     string `json:"error,omitempty"`
}

// QueueHealth 任务队列深度
type QueueHealth struct {
	OK                 bool  `json:"ok"`
	PendingTasks       int64 `json:"pending_tasks"`         // 排队等待下发的任务数
	ActiveTasks        int64 `json:"active_tasks"`          // 正在执行的任务数
	QueueDepthPerWorker int64 `json:"queue_depth_per_worker"` // 平均每 Worker 排队深度
}

// Health 深度健康检查端点。
// 检查 etcd 连接延迟、MySQL 连接池状态、任务队列深度，
// 适用于 K8s liveness/readiness probe。
// 任一组件不健康时返回 503。
func Health(sch *scheduler.Scheduler, etcdStore store.Store, historyStore store.HistoryStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		details := HealthDetails{}
		overallOK := true

		// 1. etcd 连接延迟检查
		details.Etcd = checkEtcd(etcdStore)
		if !details.Etcd.OK {
			overallOK = false
		}

		// 2. MySQL 连接池状态检查
		details.MySQL = checkMySQL(historyStore)
		if !details.MySQL.OK {
			overallOK = false
		}

		// 3. 任务队列深度（只读，不标记 unhealthy）
		details.Queue = checkQueue(sch)

		status := "ok"
		httpStatus := http.StatusOK
		if !overallOK {
			status = "unhealthy"
			httpStatus = http.StatusServiceUnavailable
		}

		c.JSON(httpStatus, HealthStatus{
			Status:  status,
			Details: details,
		})
	}
}

// checkEtcd 通过 Ping 测量 etcd 读写延迟
func checkEtcd(s store.Store) EtcdHealth {
	h := EtcdHealth{OK: false}

	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()

	latency, err := s.Ping(ctx)
	if err != nil {
		h.Error = err.Error()
		h.Latency = latency.String()
		return h
	}

	h.OK = true
	h.Latency = latency.String()
	return h
}

// checkMySQL 检查 MySQL 连接和连接池状态
func checkMySQL(hs store.HistoryStore) MySQLHealth {
	h := MySQLHealth{OK: false}

	// HistoryStore 可能为 nil（未配置 MySQL）
	if hs == nil {
		h.OK = true
		h.Error = "mysql not configured"
		return h
	}

	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()

	latency, err := hs.Ping(ctx)
	if err != nil {
		h.Error = err.Error()
		h.Latency = latency.String()
		return h
	}

	stats := hs.Stats()
	h.OK = true
	h.Latency = latency.String()
	h.OpenConns = stats.OpenConnections
	h.InUse = stats.InUse
	h.Idle = stats.Idle
	h.MaxOpen = stats.MaxOpenConnections
	return h
}

// checkQueue 读取任务队列深度指标
func checkQueue(sch *scheduler.Scheduler) QueueHealth {
	h := QueueHealth{OK: true}

	m := sch.GetBacklogMetrics()
	h.PendingTasks = m.PendingTasks
	h.ActiveTasks = m.ActiveTasks
	h.QueueDepthPerWorker = m.QueueDepthPerWorker

	return h
}
