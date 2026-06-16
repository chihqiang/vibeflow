package runtime

import (
	"context"
	"time"

	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
)

// casTaskLock 基于 etcd CAS 乐观锁的任务排他控制
// 相比基于 concurrency.Mutex 的互斥锁，CAS 方式无需创建 etcd session + lease，
// 大幅减少 etcd 资源开销，适合每秒数千任务的高并发场景。
//
// 工作原理：
//   1. 读取任务的 etcd key，获取当前值和 ModRevision
//   2. 修改任务状态（PENDING → RUNNING），使用 CASPut 原子写入
//   3. 如果 CASPut 成功，表示该 Worker 成功抢占任务
//   4. 如果 CASPut 失败（版本冲突），表示其他 Worker 已抢先处理，当前 Worker 放弃
//
// CAS 重试：最多重试 3 次，每次间隔随机退避（10-50ms），
// 减少多 Worker 同时竞争时的冲突概率。
type casTaskLock struct {
	store      store.Store
	maxRetries int
}

// newCASTaskLock 创建 CAS 任务锁
func newCASTaskLock(s store.Store) *casTaskLock {
	return &casTaskLock{
		store:      s,
		maxRetries: 3,
	}
}

// tryAcquire 尝试通过 CAS 获取任务执行权
// 返回 (payload, true) 表示成功抢占任务，payload 为最新的任务数据
// 返回 (nil, false) 表示任务已被其他 Worker 抢占
func (l *casTaskLock) tryAcquire(ctx context.Context, taskKey string) (*store.TaskPayload, bool) {
	for attempt := 0; attempt < l.maxRetries; attempt++ {
		// 1. 读取当前值及其版本号
		val, revision, err := l.store.GetWithRevision(ctx, taskKey)
		if err != nil {
			logger.Warn("CAS 读取任务失败", "key", taskKey, "error", err)
			return nil, false
		}
		if val == "" {
			// key 不存在（可能已过期或被其他 Worker 删除）
			return nil, false
		}

		// 2. 解析任务负载
		payload, err := store.Deserialize(val)
		if err != nil {
			logger.Warn("CAS 解析任务负载失败", "key", taskKey, "error", err)
			return nil, false
		}

		// 3. 检查任务是否仍为 PENDING
		if payload.Status != store.StatusPending {
			// 已被其他 Worker 处理，放弃
			return nil, false
		}

		// 4. 修改状态为 RUNNING 并尝试 CAS 写入
		payload.Status = store.StatusRunning
		newVal, err := store.Serialize(payload)
		if err != nil {
			logger.Warn("CAS 序列化任务失败", "key", taskKey, "error", err)
			return nil, false
		}

		success, newRevision, err := l.store.CASPut(ctx, taskKey, newVal, revision)
		if err != nil {
			logger.Warn("CAS 写入任务失败", "key", taskKey, "error", err, "attempt", attempt+1)
			return nil, false
		}

		if success {
			// CAS 成功，本 Worker 抢占到任务
			// 将 etcd ModRevision 存入 payload 用于后续状态上报时的 CAS 更新
			payload.EtcdModRevision = newRevision
			return payload, true
		}

		// CAS 冲突：其他 Worker 抢先修改了任务状态
		if attempt < l.maxRetries-1 {
			// 随机退避后重试
			backoff := time.Duration(10+attempt*20) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, false
			}
		}
	}

	logger.Debug("CAS 获取任务失败（超过最大重试次数）", "key", taskKey, "max_retries", l.maxRetries)
	return nil, false
}

// updateStatus 通过 CAS 更新任务状态（RUNNING → COMPLETED/FAILED）
// 使用 payload 中存储的 EtcdModRevision 作为版本号进行 CAS 写入。
// CAS 失败时不再无条件回退到 Put 覆盖，而是读取最新值判断任务是否已被其他 Worker 完成：
//   - 如果任务已处于终态（COMPLETED/FAILED），说明其他 Worker 已处理完毕，放弃上报
//   - 如果任务状态异常（PENDING/RUNNING），由 Master watchdog 超时机制兜底
func (l *casTaskLock) updateStatus(ctx context.Context, taskKey string, payload *store.TaskPayload) bool {
	val, err := store.Serialize(payload)
	if err != nil {
		logger.Warn("CAS 序列化状态更新失败", "key", taskKey, "error", err)
		return false
	}

	if payload.EtcdModRevision == 0 {
		// 没有版本号（异常情况），不执行 Put 避免无条件覆盖，直接返回 false
		// 由 executor 层的 fallback 日志记录即可，Master watchdog 超时机制兜底
		logger.Warn("CAS 状态更新失败：缺少 EtcdModRevision，放弃写入",
			"key", taskKey, "status", payload.Status)
		return false
	}

	success, _, err := l.store.CASPut(ctx, taskKey, val, payload.EtcdModRevision)
	if err != nil {
		logger.Warn("CAS 状态更新失败", "key", taskKey, "error", err)
		return false
	}

	if success {
		return true
	}

	// CAS 冲突：版本号不匹配，说明 etcd 中的数据已被其他 Writer 修改。
	// 读取最新值判断任务是否已被其他 Worker 标记为终态。
	latestVal, _, readErr := l.store.GetWithRevision(ctx, taskKey)
	if readErr != nil {
		logger.Warn("CAS 冲突后读取最新值失败", "key", taskKey, "error", readErr)
		return false
	}
	if latestVal == "" {
		// key 不存在（可能已被 Master 清理），放弃上报
		logger.Debug("CAS 冲突后任务 key 已不存在，放弃上报", "key", taskKey)
		return false
	}

	latestPayload, parseErr := store.Deserialize(latestVal)
	if parseErr != nil {
		logger.Warn("CAS 冲突后解析最新值失败", "key", taskKey, "error", parseErr)
		return false
	}

	// 如果最新状态已经是终态（COMPLETED 或 FAILED），说明任务已被其他 Worker 完成
	if latestPayload.Status == store.StatusCompleted || latestPayload.Status == store.StatusFailed {
		logger.Info("CAS 冲突：任务已被其他 Worker 完成，放弃状态上报",
			"key", taskKey,
			"our_status", payload.Status,
			"latest_status", latestPayload.Status)
		return true // 返回 true 表示不需要再 fallback，避免 executor 层重复处理
	}

	// 任务状态不是终态（仍在 PENDING 或 RUNNING），属于异常情况
	// 不执行无条件 Put 覆盖，由 Master watchdog 超时机制兜底
	logger.Warn("CAS 冲突：任务状态异常，放弃状态上报，由 Master watchdog 兜底",
		"key", taskKey,
		"our_status", payload.Status,
		"latest_status", latestPayload.Status)
	return false
}
