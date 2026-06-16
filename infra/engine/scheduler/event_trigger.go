package scheduler

import (
	"fmt"
	"sync"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/logger"
)

// ============================================================================
// EventTriggerManager — 事件触发管理器
// 管理事件驱动的工作流触发：Webhook、工作流完成触发、任务失败触发
//
// 工作原理：
//   - 工作流注册时（trigger=event），通过 RegisterEventTrigger 注册事件监听
//   - 工作流完成/任务失败时，通过 FireWorkflowCompleted / FireTaskFailed 触发匹配的事件监听器
//   - Webhook 触发由 HTTP handler 直接调用 FireWebhook
// ============================================================================

// EventListener 已注册的事件监听器
type EventListener struct {
	WorkflowUUID  string                 // 目标工作流 UUID
	WorkflowName  string                 // 目标工作流名称
	EventType     model.EventTriggerType  // 监听的事件类型
	Filter        string                 // 过滤条件
	WebhookSecret string                 // Webhook 签名密钥
}

// eventTriggerManager 事件触发管理器
type eventTriggerManager struct {
	mu         sync.RWMutex
	listeners  map[string][]EventListener // key: 事件类型（"workflow_completed" / "task_failed"）
	webhookWfs map[string]EventListener   // key: workflow UUID → webhook 监听器（用于快速查找 webhook 触发的工作流）
}

func newEventTriggerManager() *eventTriggerManager {
	return &eventTriggerManager{
		listeners:  make(map[string][]EventListener),
		webhookWfs: make(map[string]EventListener),
	}
}

// RegisterEventTrigger 注册事件触发的监听器
// wf: 要注册的工作流定义（必须 trigger=event 且 event_trigger 非空）
func (etm *eventTriggerManager) RegisterEventTrigger(wf *model.Workflow) {
	if wf.Trigger != model.TriggerEvent || wf.EventTrigger == nil {
		return
	}

	et := wf.EventTrigger
	listener := EventListener{
		WorkflowUUID:  wf.UUID,
		WorkflowName:  wf.Name,
		EventType:     et.EventType,
		Filter:        et.Filter,
		WebhookSecret: et.WebhookSecret,
	}

	etm.mu.Lock()
	defer etm.mu.Unlock()

	eventKey := string(et.EventType)
	etm.listeners[eventKey] = append(etm.listeners[eventKey], listener)

	// Webhook 类型额外记录到快速查找 map
	if et.EventType == model.EventTriggerWebhook {
		etm.webhookWfs[wf.UUID] = listener
	}

	logger.Info("事件触发已注册",
		"workflow", wf.Name,
		"uuid", wf.UUID,
		"event_type", et.EventType,
		"filter", et.Filter,
	)
}

// UnregisterEventTrigger 注销事件触发的监听器
func (etm *eventTriggerManager) UnregisterEventTrigger(workflowUUID string) {
	etm.mu.Lock()
	defer etm.mu.Unlock()

	delete(etm.webhookWfs, workflowUUID)

	for eventKey, listeners := range etm.listeners {
		for i, l := range listeners {
			if l.WorkflowUUID == workflowUUID {
				etm.listeners[eventKey] = append(listeners[:i], listeners[i+1:]...)
				break
			}
		}
	}
}

// FireWorkflowCompleted 触发 workflow_completed 事件
// sourceUUID: 完成的工作流 UUID
// callback: 匹配到目标工作流时的回调，传入目标工作流 UUID
func (etm *eventTriggerManager) FireWorkflowCompleted(sourceUUID string, callback func(targetUUID string)) {
	etm.mu.RLock()
	listeners := etm.listeners[string(model.EventTriggerWorkflowCompleted)]
	snapshot := make([]EventListener, len(listeners))
	copy(snapshot, listeners)
	etm.mu.RUnlock()

	for _, l := range snapshot {
		// Filter 匹配：Filter 为源工作流 UUID
		if l.Filter != "" && l.Filter != sourceUUID {
			continue
		}
		logger.Info("事件触发：工作流完成",
			"source", sourceUUID,
			"target", l.WorkflowUUID,
			"target_name", l.WorkflowName,
		)
		callback(l.WorkflowUUID)
	}
}

// FireTaskFailed 触发 task_failed 事件
// sourceUUID: 任务失败的工作流 UUID
// taskName: 失败的任务名
// callback: 匹配到目标工作流时的回调，传入目标工作流 UUID
func (etm *eventTriggerManager) FireTaskFailed(sourceUUID, taskName string, callback func(targetUUID string)) {
	etm.mu.RLock()
	listeners := etm.listeners[string(model.EventTriggerTaskFailed)]
	snapshot := make([]EventListener, len(listeners))
	copy(snapshot, listeners)
	etm.mu.RUnlock()

	for _, l := range snapshot {
		// Filter 匹配：Filter 格式为 "workflow_uuid:task_name"
		if l.Filter != "" {
			expectedFilter := fmt.Sprintf("%s:%s", sourceUUID, taskName)
			if l.Filter != sourceUUID && l.Filter != expectedFilter {
				continue
			}
		}
		logger.Info("事件触发：任务失败",
			"source_workflow", sourceUUID,
			"source_task", taskName,
			"target", l.WorkflowUUID,
			"target_name", l.WorkflowName,
		)
		callback(l.WorkflowUUID)
	}
}

// FireWebhook 触发 webhook 事件
// workflowUUID: 目标工作流 UUID（由 HTTP handler 从 URL 路径获取）
// 返回匹配的监听器信息
func (etm *eventTriggerManager) FireWebhook(workflowUUID string) (*EventListener, error) {
	etm.mu.RLock()
	defer etm.mu.RUnlock()

	listener, ok := etm.webhookWfs[workflowUUID]
	if !ok {
		return nil, fmt.Errorf("工作流 %s 未注册 Webhook 触发器", workflowUUID)
	}
	return &listener, nil
}

// ListWebhookWorkflows 列出所有注册了 Webhook 触发的工作流
func (etm *eventTriggerManager) ListWebhookWorkflows() []EventListener {
	etm.mu.RLock()
	defer etm.mu.RUnlock()

	result := make([]EventListener, 0, len(etm.webhookWfs))
	for _, l := range etm.webhookWfs {
		result = append(result, l)
	}
	return result
}
