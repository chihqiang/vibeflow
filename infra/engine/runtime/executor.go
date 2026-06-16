package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/tracing"
)

const (
	// defaultLockTimeout 获取分布式锁的默认超时时间
	defaultLockTimeout = 30 * time.Second
	// defaultReportTimeout 状态上报的默认超时时间
	defaultReportTimeout = 10 * time.Second
)

// LockMode 任务锁模式
type LockMode int

const (
	// LockModeMutex 使用 etcd 互斥锁（concurrency.Mutex + session + lease）
	// 适合低并发场景，语义清晰，自动过期防止死锁
	LockModeMutex LockMode = iota
	// LockModeCAS 使用 etcd CAS 乐观锁
	// 适合高并发场景（每秒数千任务），无 session/lease 开销
	// 通过版本号冲突检测实现排他控制，冲突时重试
	LockModeCAS
)

// TaskExecutor 任务执行器，负责从 etcd 获取任务、加锁、执行并报告结果
type TaskExecutor struct {
	store       store.Store     // etcd 存储后端
	prefix      string          // 任务在 etcd 中的 key 前缀，用于构造锁 key
	registry    *TaskRegistry   // 任务注册表
	reporter    *StatusReporter // 状态报告器
	lockTimeout time.Duration   // 获取分布式锁的超时时间，默认 defaultLockTimeout
	lockMode    LockMode        // 锁模式，默认 LockModeMutex
	casLock     *casTaskLock    // CAS 锁实现（lockMode == LockModeCAS 时使用）
}

// NewTaskExecutor 创建任务执行器
func NewTaskExecutor(s store.Store, prefix string, reg *TaskRegistry, rep *StatusReporter) *TaskExecutor {
	return &TaskExecutor{
		store:       s,
		prefix:      prefix,
		registry:    reg,
		reporter:    rep,
		lockTimeout: defaultLockTimeout,
		lockMode:    LockModeMutex,
		casLock:     newCASTaskLock(s),
	}
}

// SetLockTimeout 设置获取分布式锁的超时时间
func (e *TaskExecutor) SetLockTimeout(d time.Duration) {
	if d > 0 {
		e.lockTimeout = d
	}
}

// SetLockMode 设置锁模式
func (e *TaskExecutor) SetLockMode(mode LockMode) {
	e.lockMode = mode
}

// Execute 执行一个来自 etcd 事件的任务
// 流程：反序列化（优先使用 filter 预解析的 Payload）→ 获取分布式锁 → 标记运行中 → 执行业务逻辑 → 报告结果
//
// 锁模式选择：
//   - LockModeMutex（默认）：使用 etcd concurrency.Mutex，创建 session + lease
//   - LockModeCAS（高并发推荐）：使用 CAS 乐观锁，无 session/lease 开销
func (e *TaskExecutor) Execute(ctx context.Context, event store.Event) {
	var payload *store.TaskPayload
	var err error
	if event.Payload != nil {
		payload = event.Payload
	} else {
		payload, err = store.Deserialize(event.Value)
		if err != nil {
			logger.Error("反序列化任务事件失败", "error", err)
			return
		}
	}

	// 从 TraceContext 恢复跨进程链路上下文
	execCtx := tracing.ExtractTraceContext(ctx, payload.TraceContext)

	// 创建 span：任务执行
	execCtx, execSpan := tracing.StartSpan(execCtx, "worker.execute_task",
		tracing.StringAttr("workflow.uuid", payload.WorkflowID),
		tracing.StringAttr("task.name", payload.TaskName),
		tracing.StringAttr("task.trace_id", payload.TraceID),
		tracing.IntAttr("task.attempt", payload.RetryCount+1),
		tracing.BoolAttr("task.rollback", payload.Rollback),
		tracing.Int64Attr("task.timeout_sec", payload.TimeoutSec),
		tracing.IntAttr("task.priority", payload.Priority),
	)
	defer execSpan.End()

	taskKey := event.Key
	lockKey := taskLockKey(e.prefix, taskKey)

	if e.lockMode == LockModeCAS {
		// CAS 乐观锁模式：通过版本号冲突实现排他控制
		e.executeWithCAS(execCtx, taskKey, payload)
		return
	}

	// 互斥锁模式（默认）：通过 etcd concurrency.Mutex 实现排他控制
	e.executeWithMutex(execCtx, taskKey, lockKey, payload)
}

// executeWithCAS 使用 CAS 乐观锁执行任务
func (e *TaskExecutor) executeWithCAS(ctx context.Context, taskKey string, payload *store.TaskPayload) {
	// 通过 CAS 抢占任务（PENDING → RUNNING）
	acquiredPayload, ok := e.casLock.tryAcquire(ctx, taskKey)
	if !ok {
		tracing.AddSpanEvent(ctx, "cas_acquire.failed",
			tracing.StringAttr("task.key", taskKey),
		)
		logger.Debug("CAS 抢占任务失败（已被其他 Worker 处理）",
			"trace_id", payload.TraceID,
			"task", payload.TaskName,
			"workflow", payload.WorkflowID,
		)
		return
	}

	// 使用 CAS 抢占到的 payload（包含最新版本号）
	payload = acquiredPayload
	tracing.AddSpanEvent(ctx, "cas_acquire.success",
		tracing.StringAttr("task.key", taskKey),
	)

	// 初始化任务上下文
	taskCtx := model.NewContext()
	for k, v := range payload.Input {
		taskCtx.Set(k, v)
	}

	logger.Info("正在执行任务（CAS 模式）",
		"trace_id", payload.TraceID,
		"task", payload.TaskName,
		"workflow", payload.WorkflowID,
		"timeout", payload.TimeoutSec,
		"attempt", payload.RetryCount+1,
		"priority", payload.Priority,
	)

	execErr := e.run(ctx, payload, taskCtx)

	if execErr != nil {
		logger.Error("任务执行失败", "trace_id", payload.TraceID, "task", payload.TaskName, "error", execErr)
		tracing.RecordError(ctx, execErr)
		payload.Status = store.StatusFailed
		payload.Result = execErr.Error()
		if errors.Is(execErr, model.ErrNoRetry) {
			payload.NoRetry = true
			logger.Warn("任务标记为不可重试", "trace_id", payload.TraceID, "task", payload.TaskName, "error", execErr)
		}
	} else {
		logger.Info("任务执行完成", "trace_id", payload.TraceID, "task", payload.TaskName)
		payload.Status = store.StatusCompleted
		payload.Result = "ok"
		payload.Output = taskCtx.GetAll()
	}

	// 通过 CAS 更新最终状态
	if !e.casLock.updateStatus(ctx, taskKey, payload) {
		// CAS 更新失败：回退到直接 Report
		logger.Warn("CAS 状态更新失败，回退到直接写入",
			"trace_id", payload.TraceID, "task", payload.TaskName, "status", payload.Status)
		finalCtx, finalCancel := context.WithTimeout(context.Background(), defaultReportTimeout)
		defer finalCancel()
		e.reporter.Report(finalCtx, taskKey, payload)
	}
}

// executeWithMutex 使用 etcd 互斥锁执行任务（原有逻辑）
func (e *TaskExecutor) executeWithMutex(ctx context.Context, taskKey, lockKey string, payload *store.TaskPayload) {
	// 使用带超时的 context 获取锁，避免 goroutine 永久阻塞
	lockCtx, lockCancel := context.WithTimeout(ctx, e.lockTimeout)
	defer lockCancel()

	unlock, err := e.store.Lock(lockCtx, lockKey)
	if err != nil {
		tracing.AddSpanEvent(ctx, "acquire_lock.failed",
			tracing.StringAttr("lock.key", lockKey),
			tracing.StringAttr("error", err.Error()),
		)
		logger.Warn("获取任务分布式锁失败（可能已被其他 Worker 抢占）",
			"trace_id", payload.TraceID,
			"task", payload.TaskName,
			"workflow", payload.WorkflowID,
			"key", taskKey,
			"error", err,
		)
		return
	}
	tracing.AddSpanEvent(ctx, "acquire_lock.success",
		tracing.StringAttr("lock.key", lockKey),
	)

	// 释放锁时记录错误日志，帮助排查分布式锁泄漏问题
	defer func() {
		if unlockErr := unlock(); unlockErr != nil {
			logger.Warn("释放分布式锁失败", "task", payload.TaskName, "key", lockKey, "error", unlockErr)
		}
	}()

	// 将任务状态更新为 RUNNING 并写入 etcd（使用带超时的 context）
	reportCtx, reportCancel := context.WithTimeout(ctx, defaultReportTimeout)
	defer reportCancel()

	payload.Status = store.StatusRunning
	if !e.reporter.Report(reportCtx, taskKey, payload) {
		tracing.AddSpanEvent(ctx, "report_running.failed")
		logger.Error("上报 RUNNING 状态失败，放弃执行任务", "trace_id", payload.TraceID, "task", payload.TaskName, "key", taskKey)
		return
	}
	tracing.AddSpanEvent(ctx, "report_running.success")

	// 初始化任务上下文
	taskCtx := model.NewContext()
	for k, v := range payload.Input {
		taskCtx.Set(k, v)
	}

	logger.Info("正在执行任务（互斥锁模式）",
		"trace_id", payload.TraceID,
		"task", payload.TaskName,
		"workflow", payload.WorkflowID,
		"timeout", payload.TimeoutSec,
		"attempt", payload.RetryCount+1,
	)

	execErr := e.run(ctx, payload, taskCtx)

	if execErr != nil {
		logger.Error("任务执行失败", "trace_id", payload.TraceID, "task", payload.TaskName, "error", execErr)
		tracing.RecordError(ctx, execErr)
		payload.Status = store.StatusFailed
		payload.Result = execErr.Error()
		if errors.Is(execErr, model.ErrNoRetry) {
			payload.NoRetry = true
			logger.Warn("任务标记为不可重试", "trace_id", payload.TraceID, "task", payload.TaskName, "error", execErr)
		}
	} else {
		logger.Info("任务执行完成", "trace_id", payload.TraceID, "task", payload.TaskName)
		payload.Status = store.StatusCompleted
		payload.Result = "ok"
		payload.Output = taskCtx.GetAll()
	}

	// 将最终状态写回 etcd，Master 通过 Watch 感知状态变更
	finalCtx, finalCancel := context.WithTimeout(context.Background(), defaultReportTimeout)
	defer finalCancel()
	if !e.reporter.Report(finalCtx, taskKey, payload) {
		logger.Error("上报最终状态失败", "trace_id", payload.TraceID, "task", payload.TaskName, "status", payload.Status, "key", taskKey)
	}
}

// run 执行注册的业务逻辑，包含超时控制和 panic 保护
// 如果 payload.Rollback == true，则调用 OnRollback 而非 Execute
func (e *TaskExecutor) run(parentCtx context.Context, payload *store.TaskPayload, taskCtx *model.Context) (err error) {
	task, exists := e.registry.Get(payload.TaskName)
	if !exists {
		return fmt.Errorf("未注册的任务处理器: %s", payload.TaskName)
	}

	// 如果任务配置了超时，使用 context.WithTimeout 自动取消
	execCtx := parentCtx
	cancel := func() {}
	if payload.TimeoutSec > 0 {
		execCtx, cancel = context.WithTimeout(parentCtx, time.Duration(payload.TimeoutSec)*time.Second)
	}
	defer cancel()

	// 捕获 panic，防止单个任务崩溃导致整个 Worker 退出
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("任务 %s 发生 panic: %v", payload.TaskName, r)
		}
	}()

	paramCtx := model.NewContextWith(payload.Params)

	if payload.Rollback {
		// 补偿回滚：调用 OnRollback
		rb, ok := task.(model.Rollbackable)
		if !ok {
			return fmt.Errorf("任务 %s 不支持补偿回滚", payload.TaskName)
		}
		return rb.OnRollback(execCtx, paramCtx, taskCtx)
	}

	return task.Execute(execCtx, paramCtx, taskCtx)
}

// taskLockKey 构造 etcd 中任务分布式锁 key
// 锁 key 格式：{prefix}lock/{relativeKey}
// 例如：/vibeflow/tasks/lock/fetch_data/123456789
func taskLockKey(prefix, key string) string {
	// 去掉 key 中的前缀部分，避免路径冗余
	relativeKey := strings.TrimPrefix(key, prefix)
	return fmt.Sprintf("%slock/%s", prefix, relativeKey)
}
