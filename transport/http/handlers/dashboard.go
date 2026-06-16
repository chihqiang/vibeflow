package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/engine/scheduler"
	"chihqiang/vibeflow/domain/model"

	"github.com/gin-gonic/gin"
)

// DashboardStats 控制台统计数据
type DashboardStats struct {
	RunningCount   int                     `json:"running_count"`
	PausedCount    int                     `json:"paused_count"`
	CompletedCount int                     `json:"completed_count"`
	FailedCount    int                     `json:"failed_count"`
	AliveWorkers   int                     `json:"alive_workers"`
	TotalWorkers   int                     `json:"total_workers"`
	Running        []*model.WorkflowState  `json:"running"`
	Workers        []*model.WorkerState    `json:"workers"`
}

// DashboardPage 控制台页面，只渲染 HTML 骨架，数据由前端 JS 通过 API 获取
func DashboardPage(c *gin.Context) {
	c.HTML(http.StatusOK, "dashboard.html", gin.H{
		"ActiveNav": "dashboard",
	})
}

// DashboardAPI 控制台数据接口
// 使用原子计数器获取 completed/failed 统计，避免每次全量遍历历史记录
func DashboardAPI(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		workflows := sch.ListWorkflowStates()
		workers := sch.ListWorkers()

		var running, paused int
		for _, s := range workflows {
			switch s.Status {
			case model.WorkflowStatusRunning:
				running++
			case model.WorkflowStatusPaused:
				paused++
			}
		}

		alive := 0
		for _, w := range workers {
			if w.Status == model.WorkerStatusAlive {
				alive++
			}
		}

		c.JSON(http.StatusOK, Response{Code: 0, Data: DashboardStats{
			RunningCount:   running,
			PausedCount:    paused,
			CompletedCount: int(sch.CompletedCount()),
			FailedCount:    int(sch.FailedCount()),
			AliveWorkers:   alive,
			TotalWorkers:   len(workers),
			Running:        workflows,
			Workers:        workers,
		}})
	}
}
