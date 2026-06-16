package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/engine/scheduler"

	"github.com/gin-gonic/gin"
)

// ListWorkers 返回所有 Worker 的当前状态
func ListWorkers(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		workers := sch.ListWorkers()
		c.JSON(http.StatusOK, Response{Code: 0, Data: workers})
	}
}
