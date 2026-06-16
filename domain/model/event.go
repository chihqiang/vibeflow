package model

// WSEventType WebSocket 事件类型
type WSEventType string

const (
	EventWorkflowSubmitted   WSEventType = "workflow_submitted"    // 工作流已提交
	EventTaskCompleted       WSEventType = "task_completed"        // 任务执行成功
	EventTaskFailed          WSEventType = "task_failed"           // 任务执行失败
	EventTaskRolledBack      WSEventType = "task_rolled_back"      // 任务补偿回滚完成
	EventWorkflowRollingBack WSEventType = "workflow_rolling_back" // 工作流开始补偿回滚
	EventWorkflowRolledBack  WSEventType = "workflow_rolled_back"  // 工作流补偿回滚完成
	EventWorkflowCompleted   WSEventType = "workflow_completed"    // 工作流全部完成
	EventWorkflowFailed      WSEventType = "workflow_failed"       // 工作流执行失败
	EventWorkflowCancelled   WSEventType = "workflow_cancelled"    // 工作流被取消
	EventWorkflowPaused      WSEventType = "workflow_paused"       // 工作流暂停等待审批
	EventWorkflowApproved    WSEventType = "workflow_approved"     // 工作流审批通过
	EventWorkflowRejected    WSEventType = "workflow_rejected"     // 工作流审批驳回
	EventTaskSkipped         WSEventType = "task_skipped"            // 任务被跳过（ErrorPolicy=skip）
)

// WSMessage WebSocket 推送的事件消息
type WSMessage struct {
	Type WSEventType `json:"type"` // 事件类型
	Data any         `json:"data"` // 事件数据，具体结构由 type 决定
}
