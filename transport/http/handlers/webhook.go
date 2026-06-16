package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/engine/scheduler"

	"github.com/gin-gonic/gin"
)

// WebhookTriggerRequest Webhook 触发请求体
type WebhookTriggerRequest struct {
	// Webhook 传入的参数，将作为工作流第一个任务的输入
	Payload map[string]any `json:"payload,omitempty"`
}

// TriggerWebhook 通过 Webhook 触发一个事件驱动的工作流
// POST /api/v1/webhooks/:uuid
func TriggerWebhook(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var uriReq struct {
			UUID string `uri:"uuid" binding:"required"`
		}
		if err := c.ShouldBindUri(&uriReq); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}

		var body WebhookTriggerRequest
		// body 是可选的
		_ = c.ShouldBindJSON(&body)

		if err := sch.TriggerWebhook(c.Request.Context(), uriReq.UUID, body.Payload); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: err.Error()})
			return
		}

		c.JSON(http.StatusCreated, Response{Code: 0, Data: NameResponse{UUID: uriReq.UUID}})
	}
}

// ListWebhookWorkflows 列出所有注册了 Webhook 触发的工作流
func ListWebhookWorkflows(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		webhooks := sch.ListWebhookWorkflows()
		c.JSON(http.StatusOK, Response{Code: 0, Data: webhooks})
	}
}
