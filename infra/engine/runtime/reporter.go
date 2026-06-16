package runtime

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"math"
	"time"

	"chihqiang/vibeflow/infra/circuit"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
)

const (
	// reportMaxRetries 状态上报的最大重试次数
	reportMaxRetries = 3
	// reportBaseBackoff 状态上报重试的基础退避时间
	reportBaseBackoff = 500 * time.Millisecond
	// reportMaxBackoff 状态上报重试的最大退避时间
	reportMaxBackoff = 5 * time.Second
)

// StatusReporter 任务状态报告器
// 将任务的最新状态（PENDING / RUNNING / COMPLETED / FAILED）写回 etcd
// Master 通过 Watch 任务前缀感知状态变更，驱动工作流调度
// 上报失败时进行指数退避重试，熔断器保护防止 etcd 不可用时无限重试
type StatusReporter struct {
	store   store.Store
	breaker *circuit.Breaker // etcd 熔断器
}

// NewStatusReporter 创建状态报告器
func NewStatusReporter(s store.Store, breaker *circuit.Breaker) *StatusReporter {
	return &StatusReporter{store: s, breaker: breaker}
}

// Report 将任务负载（含最新状态）写入 etcd，失败时指数退避重试
// key 为任务在 etcd 中的完整路径，payload 包含状态、输出、错误信息等
// 熔断器保护：etcd 连续不可用时快速失败，避免无限重试
// 返回 true 表示上报成功，false 表示序列化或所有重试均失败（含熔断拒绝）
func (r *StatusReporter) Report(ctx context.Context, key string, payload *store.TaskPayload) bool {
	val, err := store.Serialize(payload)
	if err != nil {
		logger.Error("序列化任务状态失败", "error", err, "key", key)
		return false
	}

	// 熔断检查
	if !r.breaker.Allow() {
		logger.Warn("etcd 熔断器已打开，跳过状态上报", "key", key)
		return false
	}

	for attempt := 0; attempt < reportMaxRetries; attempt++ {
		if ctx.Err() != nil {
			logger.Warn("状态上报被取消", "key", key, "error", ctx.Err())
			return false
		}
		if err := r.store.Put(ctx, key, val); err != nil {
			if attempt < reportMaxRetries-1 {
				// 指数退避 + 抖动，避免惊群效应
				backoff := time.Duration(math.Min(
					float64(reportBaseBackoff)*math.Pow(2, float64(attempt)),
					float64(reportMaxBackoff),
				))
				jitter := time.Duration(cryptoRandInt63n(int64(backoff) / 2))
				sleepDuration := backoff + jitter
				logger.Warn("更新任务状态失败，准备重试",
					"key", key,
					"attempt", attempt+1,
					"max_attempts", reportMaxRetries,
					"backoff", sleepDuration,
					"error", err,
				)
				select {
				case <-time.After(sleepDuration):
				case <-ctx.Done():
					return false
				}
			} else {
				logger.Error("更新任务状态失败，已达最大重试次数",
					"key", key,
					"attempts", reportMaxRetries,
					"error", err,
				)
				r.breaker.Failure()
			}
		} else {
			r.breaker.Success()
			return true
		}
	}
	return false
}

// cryptoRandInt63n 使用 crypto/rand 生成 [0, n) 范围内的随机 int64
// 替代 math/rand.Int63n，与项目中其他 crypto/rand 使用保持一致性
func cryptoRandInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		// crypto/rand 在 Linux 上极少失败，fallback 用时间戳取模
		return time.Now().UnixNano() % n
	}
	return int64(binary.BigEndian.Uint64(b[:])) % n
}
