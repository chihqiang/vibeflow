package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/engine/scheduler"

	"github.com/gin-gonic/gin"
)

// ListTaskTypes 返回所有已注册的任务类型
func ListTaskTypes(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		types := sch.ListTaskTypes()
		c.JSON(http.StatusOK, Response{Code: 0, Data: types})
	}
}
