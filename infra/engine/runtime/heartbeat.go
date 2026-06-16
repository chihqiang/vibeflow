package runtime

import (
	"context"
	"fmt"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/circuit"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
)

const (
	// heartbeatTimeout 单次心跳上报的超时时间
	heartbeatTimeout = 10 * time.Second
	// heartbeatMaxRetries 心跳上报失败时的最大重试次数
	heartbeatMaxRetries = 3
	// heartbeatRetryBackoff 心跳上报重试的基础退避时间
	heartbeatRetryBackoff = time.Second
	// heartbeatCleanupTimeout 清理心跳 key 的超时时间
	heartbeatCleanupTimeout = 5 * time.Second
	// heartbeatTTLFactor 心跳 TTL = 心跳间隔 * TTLFactor，确保 Worker 异常退出后 etcd 自动清理
	// 设置为心跳间隔的 3 倍，即使偶发网络抖动也不会误删
	heartbeatTTLFactor = 3
)

// HeartbeatSender Worker 心跳发送器
// 定期向 etcd 写入心跳数据，Master 通过 Watch 心跳前缀检测 Worker 存活性
// 熔断器保护：etcd 连续不可用时跳过一次心跳周期，等待半开探测恢复
//
// 心跳优化：常规心跳仅携带 Tasks 列表 + TaskTypesHash，不携带完整 TaskTypes。
// 仅首次心跳或任务类型 hash 变化时才携带完整 TaskTypes 定义，减少数据传输量。
type HeartbeatSender struct {
	store           store.Store                                              // etcd 存储后端
	workerID        string                                                   // 当前 Worker 的唯一标识
	taskNames       func() []string                              // 获取已注册任务名称列表（轻量，用于常规心跳）
	namesAndTypes   func() ([]string, []model.TaskType)          // 获取名称+类型定义（用于首次心跳）
	typesHash       func() string                                 // 获取当前 TaskTypes 的 hash 值
	interval        time.Duration                                            // 心跳上报间隔
	heartbeatPrefix string                                                   // 心跳在 etcd 中的 key 前缀
	breaker         *circuit.Breaker                                         // etcd 熔断器

	// lastFullHash 上次携带完整 TaskTypes 时的 hash，用于判断是否需要再次发送完整定义
	lastFullHash string
}

// NewHeartbeatSender 创建心跳发送器
func NewHeartbeatSender(s store.Store, workerID string, taskNamesFn func() []string, namesAndTypesFn func() ([]string, []model.TaskType), typesHashFn func() string, interval time.Duration, heartbeatPrefix string, breaker *circuit.Breaker) *HeartbeatSender {
	return &HeartbeatSender{
		store:           s,
		workerID:        workerID,
		taskNames:       taskNamesFn,
		namesAndTypes:   namesAndTypesFn,
		typesHash:       typesHashFn,
		interval:        interval,
		heartbeatPrefix: heartbeatPrefix,
		breaker:         breaker,
	}
}

// Start 启动心跳上报循环
// 启动时立即发送一次心跳（含完整 TaskType 定义），之后按 interval 间隔定时发送
// 常规心跳仅携带任务名称列表，首次心跳携带完整 TaskType 定义
// ctx 取消时清理 etcd 中的心跳 key，通知 Master 该 Worker 已离线
func (h *HeartbeatSender) Start(ctx context.Context) {
	key := fmt.Sprintf("%s%s", h.heartbeatPrefix, h.workerID)
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	startedAt := time.Now()

	// 启动后立即上报一次（携带完整 TaskType 定义，供 Master 注册任务类型）
	h.report(ctx, key, startedAt, true)

	for {
		select {
		case <-ctx.Done():
			// Worker 退出时清理心跳 key，Master 感知后标记为离线
			h.cleanup(key)
			return
		case <-ticker.C:
			// 常规心跳仅携带任务名称列表，减少数据传输量
			h.report(ctx, key, startedAt, false)
		}
	}
}

// report 向 etcd 写入一条心跳记录，支持失败重试与熔断保护
// fullPayload 为 true 时强制携带完整 TaskType 定义（首次心跳），否则仅当 hash 变化时携带
func (h *HeartbeatSender) report(ctx context.Context, key string, startedAt time.Time, forceFull bool) {
	// 熔断检查：etcd 不可用时跳过本次心跳，等待下一次 tick 再探测
	if !h.breaker.Allow() {
		logger.Warn("etcd 熔断器已打开，跳过本次心跳", "worker_id", h.workerID)
		return
	}

	// 带超时的 context，防止网络异常时 goroutine 泄漏
	reportCtx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()

	// 计算当前 TaskTypes 的 hash
	currentHash := h.typesHash()

	// 判断是否需要发送完整 TaskTypes：
	// - 首次心跳（forceFull）：必须发送
	// - hash 变化：任务类型定义发生了变更，需要通知 Master 更新
	needFull := forceFull || (currentHash != "" && currentHash != h.lastFullHash)

	var taskTypes []model.TaskType
	var names []string
	if needFull {
		// 一次锁操作同时获取名称和类型定义
		names, taskTypes = h.namesAndTypes()
		h.lastFullHash = currentHash
	} else {
		// 常规心跳：只获取名称列表，减少锁持有时间和序列化开销
		names = h.taskNames()
	}

	payload := &store.HeartbeatPayload{
		WorkerID:      h.workerID,
		Tasks:         names,
		StartedAt:     startedAt,
		AliveAt:       time.Now(),
		TaskTypes:     taskTypes,
		TaskTypesHash: currentHash,
	}
	val, err := store.SerializeHeartbeat(payload)
	if err != nil {
		logger.Warn("序列化心跳失败", "error", err)
		return
	}

	// 心跳上报失败时进行指数退避重试，使用 TTL 防止 Worker 异常退出后 etcd 残留
	heartbeatTTL := int64(h.interval.Seconds()) * heartbeatTTLFactor
	for attempt := 0; attempt < heartbeatMaxRetries; attempt++ {
		// 同时检查外层 ctx 和 reportCtx，确保外层取消时立即响应
		if ctx.Err() != nil {
			logger.Warn("心跳上报被取消（外层 ctx）", "worker_id", h.workerID, "error", ctx.Err())
			return
		}
		if reportCtx.Err() != nil {
			logger.Warn("心跳上报被取消", "worker_id", h.workerID, "error", reportCtx.Err())
			return
		}
		if err := h.store.PutWithTTL(reportCtx, key, val, heartbeatTTL); err != nil {
			if attempt < heartbeatMaxRetries-1 {
				backoff := heartbeatRetryBackoff * time.Duration(1<<attempt)
				logger.Warn("上报心跳失败，准备重试",
					"worker_id", h.workerID,
					"attempt", attempt+1,
					"backoff", backoff,
					"error", err,
				)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				case <-reportCtx.Done():
					return
				}
			} else {
				logger.Error("上报心跳失败，已达最大重试次数",
					"worker_id", h.workerID,
					"attempts", heartbeatMaxRetries,
					"error", err,
				)
				h.breaker.Failure()
			}
		} else {
			h.breaker.Success()
			return // 上报成功
		}
	}
}

// cleanup 删除 etcd 中的心跳 key，通知 Master 本 Worker 已离线
func (h *HeartbeatSender) cleanup(key string) {
	// 清理操作使用带超时的 context，防止阻塞退出流程
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatCleanupTimeout)
	defer cancel()

	if err := h.store.Delete(ctx, key); err != nil {
		logger.Warn("退出时删除心跳 key 失败", "worker_id", h.workerID, "error", err)
	} else {
		logger.Info("心跳 key 已清理", "worker_id", h.workerID)
	}
}
