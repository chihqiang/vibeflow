package scheduler

import (
	"fmt"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/ws"
)

// ============================================================================
// ErrorPolicyHandler — 全局错误处理策略处理器
// 根据 Workflow.ErrorPolicy 中定义的策略，决定任务失败时的处理方式
//
// 支持的策略：
//   - retry（默认）：重试耗尽后触发 Saga 回滚（原有行为）
//   - rollback：任务失败时立即触发 Saga 回滚（不重试）
//   - skip：任务失败时跳过该任务，继续执行后续任务组
//   - fail_fast：任务失败时立即终止工作流（不回滚不重试）
//
// TaskPolicies 支持为特定任务覆盖全局策略
// ============================================================================

// ErrorPolicyAction 错误处理动作
type ErrorPolicyAction int

const (
	// ErrorActionRetry 重试任务
	ErrorActionRetry ErrorPolicyAction = iota
	// ErrorActionRollback 触发 Saga 回滚
	ErrorActionRollback
	// ErrorActionSkip 跳过任务
	ErrorActionSkip
	// ErrorActionFailFast 立即终止工作流
	ErrorActionFailFast
)

// ErrorPolicyHandler 错误处理策略处理器
type ErrorPolicyHandler struct {
	// 回调函数（由 Scheduler 注入）
	startSagaRollback func(workflowUUID string, state *model.WorkflowState, failedPayload *store.TaskPayload)
	markFailed        func(payload *store.TaskPayload, errMsg string)
	broadcastEvent    func(workflow string, eventType model.WSEventType, data any)
	broadcastTimeout  func(workflow string, eventType model.WSEventType, data any, timeout time.Duration)
	releaseSlot       func(workflowUUID string)
}

// NewErrorPolicyHandler 创建错误处理策略处理器
func NewErrorPolicyHandler(
	startSaga func(workflowUUID string, state *model.WorkflowState, failedPayload *store.TaskPayload),
	markFailed func(payload *store.TaskPayload, errMsg string),
	broadcastEvent func(workflow string, eventType model.WSEventType, data any),
	broadcastTimeout func(workflow string, eventType model.WSEventType, data any, timeout time.Duration),
	releaseSlot func(workflowUUID string),
) *ErrorPolicyHandler {
	return &ErrorPolicyHandler{
		startSagaRollback: startSaga,
		markFailed:        markFailed,
		broadcastEvent:    broadcastEvent,
		broadcastTimeout:  broadcastTimeout,
		releaseSlot:       releaseSlot,
	}
}

// ResolveErrorPolicy 解析任务失败时应采取的动作
// 根据 Workflow.ErrorPolicy 和 TaskPolicies 决定具体策略
func (h *ErrorPolicyHandler) ResolveErrorPolicy(
	workflowUUID string,
	state *model.WorkflowState,
	payload *store.TaskPayload,
) ErrorPolicyAction {
	if state == nil || state.Workflow == nil || state.Workflow.ErrorPolicy == nil {
		// 无 ErrorPolicy 配置，回退到原有行为
		return h.defaultAction(payload)
	}

	ep := state.Workflow.ErrorPolicy
	taskPolicy := ep.GetTaskErrorPolicy(payload.TaskName)

	switch taskPolicy {
	case model.ErrorPolicyRollback:
		return ErrorActionRollback
	case model.ErrorPolicySkip:
		if ep.IsSkippable(payload.TaskName) {
			return ErrorActionSkip
		}
		// 任务不在可跳过列表中，回退到重试
		logger.Warn("任务不在可跳过列表中，回退到重试策略",
			"workflow", workflowUUID,
			"task", payload.TaskName,
		)
		return ErrorActionRetry
	case model.ErrorPolicyFailFast:
		return ErrorActionFailFast
	case model.ErrorPolicyRetry:
		fallthrough
	default:
		return h.defaultAction(payload)
	}
}

// defaultAction 默认的错误处理逻辑（与原有行为一致）
func (h *ErrorPolicyHandler) defaultAction(payload *store.TaskPayload) ErrorPolicyAction {
	if payload.NoRetry {
		return ErrorActionRollback
	}
	if payload.RetryCount < payload.MaxRetries {
		return ErrorActionRetry
	}
	return ErrorActionRollback
}

// ExecuteRollback 执行回滚动作
// 供 handleTaskFailed 调用
func (h *ErrorPolicyHandler) ExecuteRollback(
	workflowUUID string,
	state *model.WorkflowState,
	payload *store.TaskPayload,
) {
	errMsg := fmt.Sprintf("任务 %s 失败（策略=rollback）: %s", payload.TaskName, payload.Result)
	logger.Warn("错误策略：立即回滚",
		"workflow", workflowUUID,
		"task", payload.TaskName,
		"error", errMsg,
	)

	h.broadcastEvent(workflowUUID, model.EventWorkflowFailed, map[string]any{
		"workflow": workflowUUID,
		"task":     payload.TaskName,
		"error":    errMsg,
	})

	failedPayload := &store.TaskPayload{
		WorkflowID: workflowUUID,
		TaskName:   payload.TaskName,
		TraceID:    NewTraceID(),
		Result:     errMsg,
		RetryCount: 0,
		MaxRetries: 0,
	}
	h.startSagaRollback(workflowUUID, state, failedPayload)
}

// ExecuteFailFast 执行快速失败动作
// 立即标记工作流为失败，不触发回滚
func (h *ErrorPolicyHandler) ExecuteFailFast(
	workflowUUID string,
	payload *store.TaskPayload,
) {
	errMsg := fmt.Sprintf("任务 %s 失败（策略=fail_fast）: %s", payload.TaskName, payload.Result)
	logger.Warn("错误策略：快速失败",
		"workflow", workflowUUID,
		"task", payload.TaskName,
		"error", errMsg,
	)

	h.markFailed(payload, errMsg)

	h.broadcastTimeout(workflowUUID, model.EventWorkflowFailed, map[string]any{
		"workflow": workflowUUID,
		"task":     payload.TaskName,
		"error":    errMsg,
		"policy":   "fail_fast",
	}, ws.DefaultBroadcastTimeout)
}

// ExecuteSkip 执行跳过动作
// 将失败任务标记为已完成（带跳过标记），使 SerialExecutor 继续推进
func (h *ErrorPolicyHandler) ExecuteSkip(
	workflowUUID string,
	payload *store.TaskPayload,
) {
	logger.Warn("错误策略：跳过任务",
		"workflow", workflowUUID,
		"task", payload.TaskName,
		"error", payload.Result,
	)

	h.broadcastEvent(workflowUUID, model.EventTaskSkipped, map[string]any{
		"workflow": workflowUUID,
		"task":     payload.TaskName,
		"error":    payload.Result,
		"reason":   "error_policy_skip",
	})
}

// HandleTimeoutPolicy 处理工作流超时策略
// 根据 Workflow.ErrorPolicy.OnTimeout 决定超时后的处理方式
// 返回 true 表示已由 ErrorPolicyHandler 处理，调用方无需继续
func (h *ErrorPolicyHandler) HandleTimeoutPolicy(
	workflowUUID string,
	state *model.WorkflowState,
	timeout time.Duration,
) bool {
	if state == nil || state.Workflow == nil || state.Workflow.ErrorPolicy == nil {
		return false // 无策略，由原有 timeoutAfter 处理（回滚）
	}

	policy := state.Workflow.ErrorPolicy.GetTimeoutPolicy()
	if policy == "fail" {
		// 超时时直接标记失败，不回滚
		errMsg := fmt.Sprintf("工作流执行超时（策略=fail，%v）", timeout)
		logger.Warn("超时策略：直接失败（不回滚）",
			"workflow", workflowUUID,
			"timeout", timeout,
		)
		h.markFailed(&store.TaskPayload{
			WorkflowID: workflowUUID,
			TaskName:   "__timeout__",
			TraceID:    NewTraceID(),
			Result:     errMsg,
		}, errMsg)
		return true // 已处理，调用方无需继续
	}

	return false // 回退到原有回滚行为
}

// calculateBackoff 计算退避时间
func calculateBackoff(payload *store.TaskPayload) time.Duration {
	backoff := time.Duration(payload.BaseBackoff) * time.Second
	return backoff * time.Duration(1<<payload.RetryCount)
}
