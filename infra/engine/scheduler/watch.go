package scheduler

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/tracing"
	"chihqiang/vibeflow/infra/ws"
)

// workflowEventChanBuf 每个工作流事件通道的缓冲区大小
// 增大缓冲区以减少高并发下 per-workflow 事件丢弃。
// 从 1024 提升至 4096，进一步适应超大规模工作流并发场景。
const workflowEventChanBuf = 4096

// dispatchBlockTimeout dispatch 阻塞发送的超时时间。
// 当 per-workflow channel 满时，阻塞等待而非立即丢弃，
// 给消费端短暂的处理窗口。超时后走积压队列兜底。
const dispatchBlockTimeout = 200 * time.Millisecond

// fanOutWorkers Fan-Out 分发模型中的 worker pool 大小。
// 主 Watch goroutine 仅做轻量 workflowUUID 提取后投递到 fan-out channel，
// 由 worker pool 并行处理反序列化和 dispatch，消除单 goroutine 串行瓶颈。
const fanOutWorkers = 8

// fanOutChanBuf fan-out 分发 channel 的缓冲区大小。
// 主 goroutine 非阻塞写入，缓冲区满时丢弃事件（由 rescanAndRedispatchTasks 兜底）。
const fanOutChanBuf = 8192

// fanOutTask 主 Watch goroutine 投递给 worker pool 的轻量任务
type fanOutTask struct {
	workflowUUID string
	key          string // etcd 完整 key 路径，用于任务完成后主动删除
	value        string // 原始 JSON 字符串，由 worker pool 反序列化
}

// workflowWatchers 管理工作流事件分发：全局 Watch → 按 workflowUUID 分发到各工作流的 channel
// 内置本地积压队列（backlog），当 per-workflow channel 满时暂存事件，
// 由后台 goroutine 定期重试发送，减少静默丢弃
type workflowWatchers struct {
	mu      sync.RWMutex
	chans   map[string]chan *store.TaskPayload // key: workflowUUID

	// 本地积压队列：当 per-workflow channel 满时，事件暂存于此
	backlogMu sync.Mutex
	backlog   *list.List // 元素为 *backlogEntry
	dropped   atomic.Int64 // 最终丢弃计数（积压队列也满时）
}

// backlogEntry 积压队列条目
type backlogEntry struct {
	workflowUUID string
	payload       *store.TaskPayload
	addedAt       time.Time
}

// maxBacklogSize 积压队列最大长度，超过则最终丢弃
const maxBacklogSize = 2048

// backlogRetryInterval 积压重试间隔
const backlogRetryInterval = 1 * time.Second

// backlogReportInterval 积压队列长度及丢弃计数的定期上报间隔
const backlogReportInterval = 30 * time.Second

func newWorkflowWatchers() *workflowWatchers {
	return &workflowWatchers{
		chans:   make(map[string]chan *store.TaskPayload),
		backlog: list.New(),
	}
}

// register 注册一个工作流的事件通道，返回该通道
func (ww *workflowWatchers) register(workflowUUID string) chan *store.TaskPayload {
	ch := make(chan *store.TaskPayload, workflowEventChanBuf)
	ww.mu.Lock()
	ww.chans[workflowUUID] = ch
	ww.mu.Unlock()
	return ch
}

// unregister 注销工作流的事件通道并关闭它
func (ww *workflowWatchers) unregister(workflowUUID string) {
	ww.mu.Lock()
	ch, ok := ww.chans[workflowUUID]
	if ok {
		delete(ww.chans, workflowUUID)
		close(ch)
	}
	ww.mu.Unlock()
}

// dispatch 将事件分发到对应工作流的通道。
// 优先尝试阻塞发送（带短超时），给消费端一个短暂的处理窗口；
// 超时后尝试放入积压队列；积压队列也满时才最终丢弃。
// 相比纯 select default 方案，大幅减少正常连接下的事件丢弃。
func (ww *workflowWatchers) dispatch(workflowUUID string, payload *store.TaskPayload) {
	ww.mu.RLock()
	ch, ok := ww.chans[workflowUUID]
	ww.mu.RUnlock()
	if !ok {
		return
	}

	// 阶段 1：优先尝试阻塞发送（带超时），而非立即丢弃
	// 大多数情况下，消费端只需几百微秒即可处理完当前事件，
	// 200ms 的阻塞窗口足以覆盖绝大多数短暂堆积场景
	timer := time.NewTimer(dispatchBlockTimeout)
	select {
	case ch <- payload:
		timer.Stop()
		return
	case <-timer.C:
		// 超时，进入阶段 2
	}

	// 阶段 2：尝试放入积压队列
	ww.backlogMu.Lock()
	if ww.backlog.Len() < maxBacklogSize {
		ww.backlog.PushBack(&backlogEntry{
			workflowUUID: workflowUUID,
			payload:      payload,
			addedAt:      time.Now(),
		})
		ww.backlogMu.Unlock()
		return
	}
	// 积压队列也满了，最终丢弃
	ww.dropped.Add(1)
	ww.backlogMu.Unlock()

	logger.Warn("工作流事件积压队列已满，最终丢弃事件",
		"workflow", workflowUUID,
		"task", payload.TaskName,
		"status", string(payload.Status),
		"trace_id", payload.TraceID,
		"backlog_size", maxBacklogSize,
	)
}

// flushBacklog 尝试将积压队列中的事件重新发送到对应工作流通道
// 超时条目（超过 30 秒仍未成功发送）会被丢弃
func (ww *workflowWatchers) flushBacklog() {
	const maxAge = 30 * time.Second

	ww.backlogMu.Lock()
	if ww.backlog.Len() == 0 {
		ww.backlogMu.Unlock()
		return
	}

	now := time.Now()
	var remaining []*backlogEntry

	for e := ww.backlog.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*backlogEntry)
		if now.Sub(entry.addedAt) > maxAge {
			ww.dropped.Add(1)
			logger.Warn("积压事件超时，最终丢弃",
				"workflow", entry.workflowUUID,
				"task", entry.payload.TaskName,
				"age_sec", now.Sub(entry.addedAt).Seconds(),
			)
			continue
		}

		// 尝试发送
		ww.mu.RLock()
		ch, ok := ww.chans[entry.workflowUUID]
		ww.mu.RUnlock()
		if !ok {
			// 工作流已结束，丢弃该事件
			continue
		}
		select {
		case ch <- entry.payload:
			// 成功发送，不保留
		default:
			remaining = append(remaining, entry)
		}
	}

	// 重建积压队列（保留未成功发送的条目）
	ww.backlog.Init()
	for _, entry := range remaining {
		ww.backlog.PushBack(entry)
	}
	ww.backlogMu.Unlock()
}

// backlogLen 返回当前积压队列长度（用于监控）
func (ww *workflowWatchers) backlogLen() int {
	ww.backlogMu.Lock()
	defer ww.backlogMu.Unlock()
	return ww.backlog.Len()
}

// globalTaskWatcher 全局 Watch 整个 Tasks 前缀，反序列化后按 workflowName 分发到各工作流
// 替代原来每个工作流独立的 Watch goroutine，减少 etcd 连接数和内存开销
//
// Fan-Out 分发模型：主 Watch goroutine 仅做轻量的 workflowUUID 提取（字符串扫描），
// 然后将原始事件投递到 fan-out channel，由 worker pool 并行处理反序列化和 dispatch。
// 消除单 goroutine 串行处理瓶颈，避免数千并发工作流时 watchEventChanBuf 溢出。
//
// 断连恢复机制：当 Watch 通道关闭（etcd 连接断开）并重新连接后，
// 会执行一次全量扫描，重新下发在断连期间写入但未被 Worker 消费的 PENDING 任务，
// 防止因 etcd 断连导致任务"丢失"（任务 key 虽有 24h TTL，但断连期间无 Worker 获取）。
//
// 本地积压重试：启动后台 goroutine 定期重试积压队列中的事件，
// 减少因 per-workflow channel 瞬时满导致的静默丢弃。
func (s *Scheduler) globalTaskWatcher(ctx context.Context) {
	const (
		initialBackoff = 2 * time.Second
		maxBackoff     = 30 * time.Second
	)
	backoff := initialBackoff
	taskPrefix := s.store.Prefixes().Tasks

	// 启动积压队列刷新 goroutine（与 Watch 生命周期独立）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(backlogRetryInterval)
		defer ticker.Stop()
		// 同时定期输出积压队列长度和最终丢弃计数
		reportTicker := time.NewTicker(backlogReportInterval)
		defer reportTicker.Stop()
		for {
			select {
			case <-ticker.C:
				s.wfWatchers.flushBacklog()
			case <-reportTicker.C:
				if n := s.wfWatchers.dropped.Swap(0); n > 0 {
					logger.Warn("工作流事件最终丢弃（积压队列溢出或超时）", "dropped", n, "backlog_len", s.wfWatchers.backlogLen())
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Fan-Out: 启动 worker pool，并行处理反序列化和 dispatch
	fanOutCh := make(chan fanOutTask, fanOutChanBuf)
	var fanOutWg sync.WaitGroup
	for i := 0; i < fanOutWorkers; i++ {
		fanOutWg.Add(1)
		go func() {
			defer fanOutWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-fanOutCh:
					if !ok {
						return
					}
					// Worker pool 负责：反序列化 + tracing + dispatch
					payload, err := store.Deserialize(task.value)
					if err != nil {
						logger.Warn("fan-out worker 反序列化事件失败，跳过",
							"workflow", task.workflowUUID, "error", err)
						continue
					}
					// 记录 etcd key，供后续任务完成/失败时主动删除
					payload.EtcdKey = task.key
					// 创建 span：etcd watch 事件分发
					evtCtx, evtSpan := tracing.StartSpan(ctx, "scheduler.watch_event_dispatch",
						tracing.StringAttr("workflow.uuid", payload.WorkflowID),
						tracing.StringAttr("task.name", payload.TaskName),
						tracing.StringAttr("task.status", string(payload.Status)),
					)
					evtSpan.End()
					_ = evtCtx

					s.wfWatchers.dispatch(payload.WorkflowID, payload)
				}
			}
		}()
	}

	// 主 Watch 循环：仅做轻量 workflowUUID 提取后投递到 fan-out channel
	for {
		if ctx.Err() != nil {
			break
		}

		eventChan, err := s.store.Watch(ctx, taskPrefix)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			logger.Error("全局任务监听失败，准备重试", "error", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			case <-ctx.Done():
				break
			}
		}

		backoff = initialBackoff

		// Watch 成功建立后，执行一次全量扫描，重新下发断连期间遗漏的 PENDING 任务
		s.rescanAndRedispatchTasks(ctx, taskPrefix)

		for event := range eventChan {
			// 优先使用 filter 预解析的 Payload（Worker Watch 场景），
			// 否则从原始 value 中快速提取 workflowUUID
			var workflowUUID string
			if event.Payload != nil {
				workflowUUID = event.Payload.WorkflowID
			} else {
				workflowUUID = store.ExtractWorkflowUUID(event.Value)
			}
			if workflowUUID == "" {
				logger.Warn("无法从事件中提取 workflowUUID，跳过", "key", event.Key)
				continue
			}

			// 投递到 fan-out channel（非阻塞），由 worker pool 处理
			task := fanOutTask{workflowUUID: workflowUUID, key: event.Key, value: event.Value}
			select {
			case fanOutCh <- task:
			default:
				// fan-out channel 满时丢弃，由 rescanAndRedispatchTasks 兜底
				logger.Warn("fan-out channel 满，丢弃事件",
					"workflow", workflowUUID, "key", event.Key)
			}
		}

		if ctx.Err() != nil {
			break
		}
		logger.Warn("全局任务 Watch 通道已关闭，准备重连")
	}

	// 清理：关闭 fan-out channel，等待 worker pool 退出
	close(fanOutCh)
	fanOutWg.Wait()
}

// rescanAndRedispatchTasks 扫描 etcd 中所有 PENDING 状态的任务，对于仍在运行中的工作流，
// 如果任务尚未被记录为完成或失败，则重新下发该任务。用于 etcd Watch 断连恢复场景，
// 防止断连期间已写入 etcd 的任务因没有 Watch 事件而被"遗忘"。
//
// 优化：直接扫描 {prefix}PENDING/ 前缀，通过 key 路径前缀过滤，无需 JSON 反序列化判断 status。
func (s *Scheduler) rescanAndRedispatchTasks(ctx context.Context, taskPrefix string) {
	// 仅扫描 PENDING/ 子前缀，避免列出所有状态的任务并逐个 JSON 反序列化
	pendingPrefix := taskPrefix + string(store.StatusPending) + "/"
	kvs, err := s.store.List(ctx, pendingPrefix)
	if err != nil {
		logger.Warn("全量扫描 PENDING 任务失败，跳过断连恢复", "error", err)
		return
	}

	if len(kvs) == 0 {
		return
	}

	redispatchCount := 0
	for _, kv := range kvs {
		if ctx.Err() != nil {
			return
		}

		payload, err := store.Deserialize(kv.Value)
		if err != nil {
			continue
		}

		workflowUUID := payload.WorkflowID
		wm := s.wm

		// 检查工作流是否仍在运行
		entry := wm.RLockEntry(workflowUUID)
		if entry == nil {
			continue
		}

		// 检查任务是否已被处理（已完成或已失败）
		_, completed := entry.state.CompletedTasks[payload.TaskName]
		_, failed := entry.state.FailedTasks[payload.TaskName]
		entry.mu.RUnlock()

		if completed || failed {
			continue
		}

		// 重新分发到工作流的事件通道
		s.wfWatchers.dispatch(workflowUUID, payload)
		redispatchCount++
	}

	if redispatchCount > 0 {
		logger.Info("断连恢复：重新下发遗漏的 PENDING 任务", "count", redispatchCount)
	}
}

// watchWorkflowStatus 监听任务状态变更（从全局 Watch 分发通道读取）
// 每个工作流启动一个 goroutine，从注册的通道读取事件并处理
// 职责：记录状态 → 持久化快照 → 保存历史 → 广播事件 → 通知 ExecutionManager 的监听器
//
// 恢复模式处理：
//   当 entry.recovered == true 时，对已完成/已失败的任务事件做幂等检查，
//   避免恢复期间仍在执行的 Worker 发来的旧事件导致重复执行或状态覆盖
func (s *Scheduler) watchWorkflowStatus(ctx context.Context, workflowUUID string) {
	ch := s.wfWatchers.register(workflowUUID)
	defer s.wfWatchers.unregister(workflowUUID)

	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-ch:
			if !ok {
				return
			}

			wm := s.wm
			entry := wm.RLockEntry(workflowUUID)
			if entry == nil {
				return
			}

			// 幂等性检查：如果任务已在 FailedTasks 中，忽略旧的完成/失败事件
			// 与 state 读取合并到同一次读锁内，避免 TOCTOU：两次读锁之间
			// entry 可能已被 PersistWorkflowLocked 转移到历史
			if !payload.Rollback {
				if _, failed := entry.state.FailedTasks[payload.TaskName]; failed {
					entry.mu.RUnlock()
					logger.Warn("忽略旧任务事件（任务已被标记为失败）",
						"workflow", workflowUUID,
						"task", payload.TaskName,
						"status", payload.Status,
					)
					continue
				}
			}

			// 恢复模式幂等保护：如果工作流是恢复的，检查事件是否已被 deltas 覆盖
			// 恢复期间 Worker 可能仍在处理旧任务并发送完成事件，
			// 但 deltas 已经记录了该任务的状态，应忽略重复事件
			isRecovered := entry.recovered
			if isRecovered {
				if _, alreadyCompleted := entry.state.CompletedTasks[payload.TaskName]; alreadyCompleted {
					entry.mu.RUnlock()
					logger.Warn("恢复模式：忽略已由 deltas 记录的任务完成事件",
						"workflow", workflowUUID,
						"task", payload.TaskName,
						"status", payload.Status,
					)
					continue
				}
				if _, alreadyFailed := entry.state.FailedTasks[payload.TaskName]; alreadyFailed && !payload.Rollback {
					entry.mu.RUnlock()
					logger.Warn("恢复模式：忽略已由 deltas 记录的任务失败事件",
						"workflow", workflowUUID,
						"task", payload.TaskName,
						"status", payload.Status,
					)
					continue
				}
			}

			state := entry.state
			entry.mu.RUnlock()

			switch payload.Status {
			case store.StatusCompleted:
				if payload.Rollback {
					s.handleRollbackCompleted(workflowUUID, state, payload)
				} else {
					s.recordTaskCompleted(ctx, workflowUUID, payload)
					s.notifyTaskListener(workflowUUID, payload.TaskName, payload)
					s.td.ReleaseSlotForWorkflow(ctx, workflowUUID)
				}
			case store.StatusFailed:
				if payload.Rollback {
					logger.Error("补偿回滚任务失败", "trace_id", payload.TraceID, "workflow", workflowUUID, "task", payload.TaskName, "error", payload.Result)
					s.handleRollbackCompleted(workflowUUID, state, payload)
				} else {
					s.recordTaskFailed(ctx, workflowUUID, payload)
					s.notifyTaskListener(workflowUUID, payload.TaskName, payload)
					s.handleTaskFailed(ctx, workflowUUID, state, payload)
				}
			}
		}
	}
}

// recordTaskCompleted 记录任务完成
// 接受 ctx 参数，tracing span 与工作流生命周期绑定
func (s *Scheduler) recordTaskCompleted(ctx context.Context, workflowUUID string, payload *store.TaskPayload) {
	// 从 TraceContext 恢复上下文，创建 span
	taskCtx := tracing.ExtractTraceContext(ctx, payload.TraceContext)
	taskCtx, taskSpan := tracing.StartSpan(taskCtx, "scheduler.task_completed",
		tracing.StringAttr("workflow.uuid", workflowUUID),
		tracing.StringAttr("task.name", payload.TaskName),
		tracing.StringAttr("task.trace_id", payload.TraceID),
		tracing.IntAttr("task.attempt", payload.RetryCount+1),
		tracing.IntAttr("task.output_keys", len(payload.Output)),
	)
	defer taskSpan.End()

	wm := s.wm
	entry := wm.LockEntry(workflowUUID)
	if entry == nil {
		return
	}
	entry.state.CompletedTasks[payload.TaskName] = payload.Output
	execID := entry.state.ExecutionID
	workflowID := entry.state.WorkflowID
	entry.mu.Unlock()

	s.repository.snapshotRunningWorkflow(workflowUUID)
	s.repository.saveTaskRecord(workflowUUID, payload, execID, workflowID)

	s.wsEvent.BroadcastToWorkflow(workflowUUID, model.WSMessage{
		Type: model.EventTaskCompleted,
		Data: map[string]any{
			"workflow": workflowUUID,
			"task":     payload.TaskName,
			"output":   payload.Output,
		},
	})

	s.cleanupTaskEtcdKey(taskCtx, payload, "completed")
}

// recordTaskFailed 记录任务失败
// 接受 ctx 参数，tracing span 与工作流生命周期绑定
func (s *Scheduler) recordTaskFailed(ctx context.Context, workflowUUID string, payload *store.TaskPayload) {
	// 从 TraceContext 恢复上下文，创建 span
	taskCtx := tracing.ExtractTraceContext(ctx, payload.TraceContext)
	taskCtx, taskSpan := tracing.StartSpan(taskCtx, "scheduler.task_failed",
		tracing.StringAttr("workflow.uuid", workflowUUID),
		tracing.StringAttr("task.name", payload.TaskName),
		tracing.StringAttr("task.trace_id", payload.TraceID),
		tracing.IntAttr("task.attempt", payload.RetryCount+1),
		tracing.StringAttr("task.error", payload.Result),
	)
	defer taskSpan.End()

	wm := s.wm
	entry := wm.LockEntry(workflowUUID)
	if entry != nil {
		entry.state.FailedTasks[payload.TaskName] = payload.Result
		entry.mu.Unlock()
	}

	s.repository.snapshotRunningWorkflow(workflowUUID)

	logger.Warn("任务失败", "workflow", workflowUUID, "task", payload.TaskName, "error", payload.Result)
	s.wsEvent.BroadcastToWorkflow(workflowUUID, model.WSMessage{
		Type: model.EventTaskFailed,
		Data: map[string]any{
			"workflow": workflowUUID,
			"task":     payload.TaskName,
			"error":    payload.Result,
		},
	})

	s.cleanupTaskEtcdKey(taskCtx, payload, "failed")
}

// handleTaskFailed 处理任务执行失败事件
func (s *Scheduler) handleTaskFailed(ctx context.Context, workflowUUID string, state *model.WorkflowState, payload *store.TaskPayload) {
	// 检查 ErrorPolicy：如果有自定义策略，使用 ErrorPolicyHandler 处理
	if state.Workflow != nil && state.Workflow.ErrorPolicy != nil {
		ep := state.Workflow.ErrorPolicy
		taskPolicy := ep.GetTaskErrorPolicy(payload.TaskName)

		switch taskPolicy {
		case model.ErrorPolicyFailFast:
			// 立即失败，不重试不回滚
			s.errorPolicyHandler.ExecuteFailFast(workflowUUID, payload)
			s.td.ReleaseSlotForWorkflow(ctx, workflowUUID)
			return

		case model.ErrorPolicyRollback:
			// 立即回滚，不重试
			s.errorPolicyHandler.ExecuteRollback(workflowUUID, state, payload)
			s.td.ReleaseSlotForWorkflow(ctx, workflowUUID)
			return

		case model.ErrorPolicySkip:
			if ep.IsSkippable(payload.TaskName) {
				// 跳过失败任务，将跳过结果写入 completedTasks 使后续任务组继续
				entry := s.wm.LockEntry(workflowUUID)
				if entry != nil {
					entry.state.CompletedTasks[payload.TaskName] = map[string]any{
						"_skipped":     true,
						"_error":       payload.Result,
						"_skip_reason": "error_policy_skip",
					}
					// 从 FailedTasks 中移除（已在 recordTaskFailed 中添加）
					delete(entry.state.FailedTasks, payload.TaskName)
					s.wm.PersistWorkflowLocked(workflowUUID, entry.state, entry)
					entry.mu.Unlock()
				}
				s.errorPolicyHandler.ExecuteSkip(workflowUUID, payload)
				s.notifyTaskListener(workflowUUID, payload.TaskName, &store.TaskPayload{
					WorkflowID: workflowUUID,
					TaskName:   payload.TaskName,
					TraceID:    payload.TraceID,
					Status:     store.StatusCompleted,
					Output: map[string]any{
						"_skipped":     true,
						"_error":       payload.Result,
						"_skip_reason": "error_policy_skip",
					},
				})
				s.td.ReleaseSlotForWorkflow(ctx, workflowUUID)
				return
			}
			// 任务不可跳过，回退到默认重试行为
			logger.Warn("任务不在可跳过列表中，回退到默认重试策略",
				"workflow", workflowUUID,
				"task", payload.TaskName,
			)
		}
	}

	// 默认行为（原有逻辑）
	if payload.NoRetry {
		logger.Error("任务不可重试，直接失败",
			"workflow", workflowUUID,
			"task", payload.TaskName,
			"error", payload.Result,
		)
		s.wsEvent.BroadcastToWorkflow(workflowUUID, model.WSMessage{
			Type: model.EventWorkflowFailed,
			Data: map[string]any{
				"workflow": workflowUUID,
				"task":     payload.TaskName,
				"error":    payload.Result,
			},
		})
		s.startSagaRollback(workflowUUID, state, payload)
		s.td.ReleaseSlotForWorkflow(ctx, workflowUUID)
		return
	}

	if payload.RetryCount < payload.MaxRetries {
		backoff := time.Duration(payload.BaseBackoff) * time.Second * (1 << payload.RetryCount)
		logger.Info("即将重试任务",
			"workflow", workflowUUID,
			"task", payload.TaskName,
			"attempt", payload.RetryCount+1,
			"max", payload.MaxRetries,
			"backoff", backoff,
		)

		// 释放当前任务占用的槽位，重试 goroutine 会自行获取新槽位
		s.td.ReleaseSlotForWorkflow(ctx, workflowUUID)

		p := *payload
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.td.RetryTask(ctx, &p, int64(backoff.Seconds()))
		}()
	} else {
		logger.Error("工作流失败", "workflow", workflowUUID, "task", payload.TaskName, "error", payload.Result)
		s.wsEvent.BroadcastToWorkflow(workflowUUID, model.WSMessage{
			Type: model.EventWorkflowFailed,
			Data: map[string]any{
				"workflow": workflowUUID,
				"task":     payload.TaskName,
				"error":    payload.Result,
			},
		})
		s.startSagaRollback(workflowUUID, state, payload)
		s.td.ReleaseSlotForWorkflow(ctx, workflowUUID)
	}
}

// markWorkflowFailed 标记工作流为失败并持久化
func (s *Scheduler) markWorkflowFailed(payload *store.TaskPayload, errMsg string) {
	wm := s.wm
	entry := wm.LockEntry(payload.WorkflowID)
	if entry != nil {
		entry.state.FailedTasks[payload.TaskName] = errMsg
		entry.state.Status = model.WorkflowStatusFailed
		entry.state.Error = errMsg
		wm.PersistWorkflowLocked(payload.WorkflowID, entry.state, entry)
		entry.mu.Unlock()
	}
	s.wsEvent.BroadcastTimeoutToWorkflow(payload.WorkflowID, model.WSMessage{Type: model.EventWorkflowFailed, Data: payload.WorkflowID}, ws.DefaultBroadcastTimeout)

	// 触发 task_failed 事件链
	s.eventTriggerMgr.FireTaskFailed(payload.WorkflowID, payload.TaskName, func(targetUUID string) {
		go func() {
			if wf := wm.GetRegisteredWorkflow(targetUUID); wf != nil {
				wfCopy, err := wf.DeepCopy()
				if err != nil {
					logger.Error("事件链：深拷贝目标工作流失败", "target", targetUUID, "error", err)
					return
				}
				if err := wm.SubmitWorkflow(s.ctx, wfCopy); err != nil {
					logger.Error("事件链：提交目标工作流失败", "target", targetUUID, "error", err)
				}
			}
		}()
	})
}

// timeoutAfter 等待指定时间后，如果工作流还未完成则根据 ErrorPolicy 处理
func (s *Scheduler) timeoutAfter(ctx context.Context, workflowUUID string, timeout time.Duration) {
	select {
	case <-time.After(timeout):
	case <-ctx.Done():
		return
	}

	wm := s.wm
	entry := wm.LockEntry(workflowUUID)
	if entry == nil || (entry.state.Status != model.WorkflowStatusRunning && entry.state.Status != model.WorkflowStatusPaused) {
		if entry != nil {
			entry.mu.Unlock()
		}
		return
	}
	state := entry.state
	errMsg := fmt.Sprintf("工作流执行超时（%v）", timeout)
	state.Error = errMsg
	wm.PersistWorkflowLocked(workflowUUID, state, entry)
	entry.mu.Unlock()

	// 检查 ErrorPolicy 的超时策略
	if s.errorPolicyHandler.HandleTimeoutPolicy(workflowUUID, state, timeout) {
		logger.Warn("工作流超时，ErrorPolicy=fail（不回滚）", "workflow", workflowUUID, "timeout", timeout)
		return
	}

	logger.Warn("工作流超时，开始补偿回滚", "workflow", workflowUUID, "timeout", timeout)

	fakePayload := &store.TaskPayload{
		WorkflowID: workflowUUID,
		TaskName:   "__timeout__",
		TraceID:    NewTraceID(),
		Result:     errMsg,
		RetryCount: 0,
		MaxRetries: 0,
	}
	s.startSagaRollback(workflowUUID, state, fakePayload)
}

// rollback 并发控制常量
const (
	// maxRollbackConcurrency 同时回滚的最大任务数，防止大量已完成任务时瞬间下发几十个回滚任务
	maxRollbackConcurrency = 5
	// rollbackOverallTimeout 回滚整体超时，超时后强制标记 ROLLED_BACK
	rollbackOverallTimeout = 5 * time.Minute
	// rollbackStallWarnThreshold 单个回滚任务停滞告警阈值
	rollbackStallWarnThreshold = 2 * time.Minute
)

// rollbackSemaphore 回滚并发信号量（全局，所有工作流共享）
type rollbackSemaphore struct {
	mu    sync.Mutex
	cond  *sync.Cond
	count int
	max   int
}

func newRollbackSemaphore(max int) *rollbackSemaphore {
	rs := &rollbackSemaphore{max: max}
	rs.cond = sync.NewCond(&rs.mu)
	return rs
}

// acquire 获取一个回滚槽位，阻塞直到有空闲槽位
func (rs *rollbackSemaphore) acquire() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for rs.count >= rs.max {
		rs.cond.Wait()
	}
	rs.count++
}

// release 释放一个回滚槽位
func (rs *rollbackSemaphore) release() {
	rs.mu.Lock()
	rs.count--
	rs.cond.Signal()
	rs.mu.Unlock()
}

// startSagaRollback 触发 Saga 补偿回滚
// 增加回滚并发限制（maxRollbackConcurrency）和回滚整体超时（rollbackOverallTimeout）
func (s *Scheduler) startSagaRollback(workflowUUID string, state *model.WorkflowState, failedPayload *store.TaskPayload) {
	wm := s.wm
	entry := wm.LockEntry(workflowUUID)
	if entry == nil {
		return
	}
	if entry.state.Status == model.WorkflowStatusRollingBack || entry.state.Status == model.WorkflowStatusRolledBack || entry.state.Status == model.WorkflowStatusFailed {
		entry.mu.Unlock()
		return
	}
	entry.state.Status = model.WorkflowStatusRollingBack
	entry.state.Error = fmt.Sprintf("任务 %s 重试 %d 次后仍然失败: %s",
		failedPayload.TaskName, failedPayload.RetryCount, failedPayload.Result)
	if entry.state.RolledBack == nil {
		entry.state.RolledBack = make(map[string]bool)
	}
	wm.PersistWorkflowLocked(workflowUUID, entry.state, entry)
	// 获取 state 快照用于后续回滚逻辑
	wf := entry.state.Workflow
	groups := wf.TaskGroups
	failedGroupIdx := -1
	if entry.state.TaskGroupIndex != nil {
		if gi, ok := entry.state.TaskGroupIndex[failedPayload.TaskName]; ok {
			failedGroupIdx = gi
		}
	} else {
		for gi, g := range groups {
			for _, node := range g {
				if node.Name == failedPayload.TaskName {
					failedGroupIdx = gi
					break
				}
			}
			if failedGroupIdx >= 0 {
				break
			}
		}
	}
	entry.mu.Unlock()

	s.wsEvent.BroadcastTimeoutToWorkflow(workflowUUID, model.WSMessage{
		Type: model.EventWorkflowRollingBack,
		Data: map[string]any{
			"workflow":     workflowUUID,
			"failed_task":  failedPayload.TaskName,
			"failed_group": failedGroupIdx + 1,
			"error":        failedPayload.Result,
		},
	}, ws.DefaultBroadcastTimeout)

	if failedGroupIdx <= 0 {
		logger.Info("无已完成任务需要回滚，工作流失败",
			"workflow", workflowUUID,
			"failed_group", failedGroupIdx+1,
		)
		entry2 := wm.LockEntry(workflowUUID)
		if entry2 != nil {
			entry2.state.Status = model.WorkflowStatusRolledBack
			wm.PersistWorkflowLocked(workflowUUID, entry2.state, entry2)
			entry2.mu.Unlock()
		}
		s.wsEvent.BroadcastTimeoutToWorkflow(workflowUUID, model.WSMessage{Type: model.EventWorkflowRolledBack, Data: workflowUUID}, ws.DefaultBroadcastTimeout)
		return
	}

	// 收集所有需要回滚的任务
	type rollbackTask struct {
		node   model.TaskNode
		output map[string]any
		group  int
	}
	var rollbackTasks []rollbackTask
	for gi := failedGroupIdx - 1; gi >= 0; gi-- {
		for _, node := range groups[gi] {
			entry3 := wm.RLockEntry(workflowUUID)
			var output map[string]any
			var alreadyRolledBack bool
			wasCompleted := false
			if entry3 != nil {
				output, wasCompleted = entry3.state.CompletedTasks[node.Name]
				_, alreadyRolledBack = entry3.state.RolledBack[node.Name]
				entry3.mu.RUnlock()
			}

			if !wasCompleted || alreadyRolledBack {
				continue
			}

			rollbackTasks = append(rollbackTasks, rollbackTask{
				node:   node,
				output: output,
				group:  gi + 1,
			})
		}
	}

	if len(rollbackTasks) == 0 {
		logger.Info("无待回滚任务（可能均已回滚）", "workflow", workflowUUID)
		entry2 := wm.LockEntry(workflowUUID)
		if entry2 != nil {
			entry2.state.Status = model.WorkflowStatusRolledBack
			wm.PersistWorkflowLocked(workflowUUID, entry2.state, entry2)
			entry2.mu.Unlock()
		}
		s.wsEvent.BroadcastTimeoutToWorkflow(workflowUUID, model.WSMessage{Type: model.EventWorkflowRolledBack, Data: workflowUUID}, ws.DefaultBroadcastTimeout)
		return
	}

	logger.Info("开始下发回滚任务",
		"workflow", workflowUUID,
		"rollback_task_count", len(rollbackTasks),
		"max_concurrency", maxRollbackConcurrency,
	)

	// 异步下发回滚任务（受并发限制），并启动整体超时监控
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// 从 Scheduler 根 context 派生，Shutdown 时回滚操作也能及时取消
		rollbackCtx, rollbackCancel := context.WithTimeout(s.ctx, rollbackOverallTimeout)
		defer rollbackCancel()

		// 使用信号量控制并发回滚数
		var dispatchWg sync.WaitGroup

		for _, rt := range rollbackTasks {
			dispatchWg.Add(1)
			go func(rt rollbackTask) {
				defer dispatchWg.Done()

				// 获取回滚槽位（阻塞等待）
				s.rollbackSem.acquire()
				defer s.rollbackSem.release()

				logger.Info("下发补偿回滚任务",
					"workflow", workflowUUID,
					"task", rt.node.Name,
					"group", rt.group,
				)
				s.td.PushRollbackTask(rollbackCtx, workflowUUID, rt.node, rt.output)

				// 记录回滚开始时间，监控长时间未响应的回滚任务
				rollbackStartTime := time.Now()

				// 等待回滚任务完成或超时
				rollbackDone := make(chan struct{})
				go func() {
					// 轮询检查回滚状态
					ticker := time.NewTicker(2 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							entry := wm.RLockEntry(workflowUUID)
							if entry == nil {
								return
							}
							_, rolledBack := entry.state.RolledBack[rt.node.Name]
							entry.mu.RUnlock()
							if rolledBack {
								close(rollbackDone)
								return
							}
						case <-rollbackCtx.Done():
							return
						}
					}
				}()

				select {
				case <-rollbackDone:
					logger.Info("回滚任务完成",
						"workflow", workflowUUID,
						"task", rt.node.Name,
					)
				case <-rollbackCtx.Done():
					logger.Error("回滚整体超时，强制标记回滚完成",
						"workflow", workflowUUID,
						"task", rt.node.Name,
						"overall_timeout", rollbackOverallTimeout,
					)
					// 超时时强制标记该任务已回滚
					entry := wm.LockEntry(workflowUUID)
					if entry != nil {
						if entry.state.RolledBack == nil {
							entry.state.RolledBack = make(map[string]bool)
						}
						entry.state.RolledBack[rt.node.Name] = true
						wm.PersistWorkflowLocked(workflowUUID, entry.state, entry)
						entry.mu.Unlock()
					}
				case <-time.After(rollbackStallWarnThreshold):
					elapsed := time.Since(rollbackStartTime)
					logger.Warn("回滚任务长时间未响应",
						"workflow", workflowUUID,
						"task", rt.node.Name,
						"elapsed", elapsed,
					)
					// 继续等待，但已发出告警
					select {
					case <-rollbackDone:
					case <-rollbackCtx.Done():
						entry := wm.LockEntry(workflowUUID)
						if entry != nil {
							if entry.state.RolledBack == nil {
								entry.state.RolledBack = make(map[string]bool)
							}
							entry.state.RolledBack[rt.node.Name] = true
							wm.PersistWorkflowLocked(workflowUUID, entry.state, entry)
							entry.mu.Unlock()
						}
					}
				}
			}(rt)
		}

		dispatchWg.Wait()
		logger.Info("所有回滚任务已下发（或超时强制完成）", "workflow", workflowUUID)
	}()
}

// handleRollbackCompleted 处理回滚任务完成事件
func (s *Scheduler) handleRollbackCompleted(workflowUUID string, state *model.WorkflowState, payload *store.TaskPayload) {
	wm := s.wm
	entry := wm.LockEntry(workflowUUID)
	if entry == nil {
		return
	}
	if entry.state.RolledBack == nil {
		entry.state.RolledBack = make(map[string]bool)
	}
	entry.state.RolledBack[payload.TaskName] = true
	wm.PersistWorkflowLocked(workflowUUID, entry.state, entry)

	groups := entry.state.Workflow.TaskGroups
	allRolledBack := true
	for _, g := range groups {
		for _, node := range g {
			if _, completed := entry.state.CompletedTasks[node.Name]; completed {
				if !entry.state.RolledBack[node.Name] {
					allRolledBack = false
					break
				}
			}
		}
		if !allRolledBack {
			break
		}
	}
	entry.mu.Unlock()

	logger.Info("补偿回滚完成", "workflow", workflowUUID, "task", payload.TaskName)

	s.wsEvent.BroadcastToWorkflow(workflowUUID, model.WSMessage{
		Type: model.EventTaskRolledBack,
		Data: map[string]any{
			"workflow": workflowUUID,
			"task":     payload.TaskName,
		},
	})

	if allRolledBack {
		entry2 := wm.LockEntry(workflowUUID)
		if entry2 != nil {
			entry2.state.Status = model.WorkflowStatusRolledBack
			wm.PersistWorkflowLocked(workflowUUID, entry2.state, entry2)
			entry2.mu.Unlock()
		}
		s.wsEvent.BroadcastTimeoutToWorkflow(workflowUUID, model.WSMessage{Type: model.EventWorkflowRolledBack, Data: workflowUUID}, ws.DefaultBroadcastTimeout)
		logger.Info("所有补偿回滚已完成", "workflow", workflowUUID)
	}
}

// cleanupTaskEtcdKey 任务完成后主动从 etcd 中删除任务 key
// 避免 etcd 中堆积已完成/失败的任务数据，减少 etcd 内存压力
// 仅当 payload.EtcdKey 非空时执行，key 由 fan-out worker 在
// 事件分发阶段设置（来源于 Watch 事件中的 event.Key）
func (s *Scheduler) cleanupTaskEtcdKey(ctx context.Context, payload *store.TaskPayload, status string) {
	if payload.EtcdKey == "" {
		return
	}
	_, cleanSpan := tracing.StartSpan(ctx, "scheduler.cleanup_task_etcd_key",
		tracing.StringAttr("workflow.uuid", payload.WorkflowID),
		tracing.StringAttr("task.name", payload.TaskName),
		tracing.StringAttr("task.status", status),
		tracing.StringAttr("etcd.key", payload.EtcdKey),
	)
	defer cleanSpan.End()

	if err := s.store.Delete(ctx, payload.EtcdKey); err != nil {
		logger.Warn("删除 etcd 任务 key 失败（不影响工作流执行）",
			"workflow", payload.WorkflowID,
			"task", payload.TaskName,
			"key", payload.EtcdKey,
			"error", err,
		)
		return
	}
	logger.Debug("已删除 etcd 任务 key",
		"workflow", payload.WorkflowID,
		"task", payload.TaskName,
		"key", payload.EtcdKey,
		"status", status,
	)
}
