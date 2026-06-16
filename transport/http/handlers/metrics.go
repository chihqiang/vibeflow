package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/engine/scheduler"

	"github.com/gin-gonic/gin"
)

// Metrics 返回调度器核心运行指标，包括任务队列深度、并发数和 Worker 统计
func Metrics(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		m := sch.GetBacklogMetrics()
		c.JSON(http.StatusOK, Response{Code: 0, Data: m})
	}
}
