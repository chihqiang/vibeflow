package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/engine/scheduler"
	"chihqiang/vibeflow/domain/model"

	"github.com/gin-gonic/gin"
)

// CreateWorkflowRequest 创建工作流的 API 请求体
type CreateWorkflowRequest struct {
	Name           string              `json:"name"`                       // 工作流名称
	TaskGroups     [][]model.TaskNode  `json:"task_groups,omitempty"`      // 任务分组：组内并行，组间串行，每个 TaskNode 含 name 和 params
	Trigger        string              `json:"trigger"`                    // 触发方式：manual / cron
	CronExpr       string              `json:"cron_expr,omitempty"`        // Cron 表达式，trigger=cron 时必填
	TimeoutSec     *int64              `json:"timeout_sec,omitempty"`      // 工作流超时秒数，nil 表示不限制
	TaskTimeoutSec *int64              `json:"task_timeout_sec,omitempty"` // 任务默认超时秒数
	MaxRetries     *int                `json:"max_retries,omitempty"`      // 任务默认重试次数
	BaseBackoff    *int64              `json:"base_backoff,omitempty"`     // 任务默认基础退避秒数
	ErrorPolicy    *model.ErrorPolicy  `json:"error_policy,omitempty"`   // 全局错误处理策略
}

// WorkflowUUIDRequest URI 路径参数结构体
// 用于绑定路由中 :uuid 路径参数（工作流 UUID）
type WorkflowUUIDRequest struct {
	UUID string `uri:"uuid" binding:"required"` // 工作流 UUID
}

// WorkflowResponse 创建工作流响应，返回 UUID 和 Name
type WorkflowResponse struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

// NameResponse 包含 uuid 和 name 的通用响应
type NameResponse struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

// MessageResponse 通用消息响应
type MessageResponse struct {
	Message string `json:"message"`
}

// CreateWorkflow 提交一个新的工作流（支持串并行混合编排）
func CreateWorkflow(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreateWorkflowRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的 JSON: " + err.Error()})
			return
		}
		if req.Name == "" || len(req.TaskGroups) == 0 {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "name 和 task_groups 为必填项"})
			return
		}

		wf := model.Workflow{
			Name:       req.Name,
			TaskGroups: req.TaskGroups,
		}
		if req.Trigger == string(model.TriggerCron) {
			wf.Trigger = model.TriggerCron
			wf.CronExpr = req.CronExpr
		} else {
			wf.Trigger = model.TriggerManual
		}
		if req.TimeoutSec != nil {
			wf.TimeoutSec = *req.TimeoutSec
		}
		if req.TaskTimeoutSec != nil {
			wf.TaskTimeoutSec = *req.TaskTimeoutSec
		}
		if req.MaxRetries != nil {
			wf.MaxRetries = *req.MaxRetries
		}
		if req.BaseBackoff != nil {
			wf.BaseBackoff = *req.BaseBackoff
		}
		if req.ErrorPolicy != nil {
			wf.ErrorPolicy = req.ErrorPolicy
		}

		// 分配 UUID
		wf.UUID = sch.NewWorkflowUUID()

		if wf.Trigger == model.TriggerCron {
			if err := sch.SaveWorkflow(c.Request.Context(), &wf); err != nil {
				c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
				return
			}
			if err := sch.ScheduleCronWorkflow(&wf); err != nil {
				c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
				return
			}
		} else {
			if err := sch.SubmitWorkflow(c.Request.Context(), &wf); err != nil {
				c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
				return
			}
		}

		c.JSON(http.StatusCreated, Response{Code: 0, Data: WorkflowResponse{UUID: wf.UUID, Name: wf.Name}})
	}
}

// ListWorkflows 列出所有正在运行的工作流 + PENDING 状态的工作流定义
func ListWorkflows(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		running := sch.ListWorkflowStates()
		pending := sch.ListWorkflowDefs()

		// 去重：运行中的工作流优先，PENDING 中同 UUID 的不再重复显示
		seen := make(map[string]struct{}, len(running))
		for _, s := range running {
			seen[s.Workflow.UUID] = struct{}{}
		}
		all := make([]*model.WorkflowState, 0, len(running)+len(pending))
		all = append(all, running...)
		for _, s := range pending {
			if _, ok := seen[s.Workflow.UUID]; !ok {
				all = append(all, s)
			}
		}
		c.JSON(http.StatusOK, Response{Code: 0, Data: all})
	}
}

// GetWorkflow 查询单个工作流详情（优先查运行中，再查 PENDING 定义，最后查历史记录）
func GetWorkflow(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req WorkflowUUIDRequest
		if err := c.ShouldBindUri(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}
		// 1. 优先查运行中
		state := sch.GetWorkflowState(req.UUID)
		// 2. 再查 PENDING 定义
		if state == nil {
			if wf := sch.GetRegisteredWorkflow(req.UUID); wf != nil {
				state = &model.WorkflowState{
					Workflow:     wf,
					WorkflowUUID: wf.UUID,
					Status:       model.WorkflowStatusPending,
				}
			}
		}
		// 3. 最后查历史记录
		if state == nil {
			state = sch.GetHistory(req.UUID)
		}
		if state == nil {
			c.JSON(http.StatusNotFound, Response{Code: 1, Msg: "工作流不存在"})
			return
		}
		c.JSON(http.StatusOK, Response{Code: 0, Data: state})
	}
}

// CancelWorkflow 取消一个正在运行的工作流
func CancelWorkflow(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req WorkflowUUIDRequest
		if err := c.ShouldBindUri(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}
		if err := sch.CancelWorkflow(req.UUID); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
			return
		}
		c.JSON(http.StatusOK, Response{Code: 0, Data: MessageResponse{Message: "已取消"}})
	}
}

// RetryWorkflow 从历史记录中重试一个已结束的工作流
func RetryWorkflow(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req WorkflowUUIDRequest
		if err := c.ShouldBindUri(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}
		if err := sch.RetryWorkflow(c.Request.Context(), req.UUID); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
			return
		}
		c.JSON(http.StatusCreated, Response{Code: 0, Data: NameResponse{UUID: req.UUID}})
	}
}

// RunWorkflow 执行一个 PENDING 状态的工作流
func RunWorkflow(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req WorkflowUUIDRequest
		if err := c.ShouldBindUri(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}
		if err := sch.RunWorkflow(c.Request.Context(), req.UUID); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
			return
		}
		c.JSON(http.StatusCreated, Response{Code: 0, Data: NameResponse{UUID: req.UUID}})
	}
}

// ApproveWorkflow 审批通过一个暂停中的工作流
func ApproveWorkflow(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req WorkflowUUIDRequest
		if err := c.ShouldBindUri(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}
		if err := sch.ApproveWorkflow(req.UUID); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
			return
		}
		c.JSON(http.StatusOK, Response{Code: 0, Data: MessageResponse{Message: "已审批通过"}})
	}
}

// RejectWorkflow 驳回一个暂停中的工作流
func RejectWorkflow(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req WorkflowUUIDRequest
		if err := c.ShouldBindUri(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}
		var body struct {
			Reason string `json:"reason"`
		}
		_ = c.ShouldBindJSON(&body)
		if err := sch.RejectWorkflow(req.UUID, body.Reason); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
			return
		}
		c.JSON(http.StatusOK, Response{Code: 0, Data: MessageResponse{Message: "已驳回"}})
	}
}
