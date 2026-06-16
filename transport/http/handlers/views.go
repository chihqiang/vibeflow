package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// TasksPage 任务类型列表页面，只渲染 HTML 骨架，数据由前端 JS 通过 API 获取
func TasksPage(c *gin.Context) {
	c.HTML(http.StatusOK, "tasks.html", gin.H{
		"ActiveNav": "tasks",
	})
}

// WorkflowsPage 工作流列表页面，只渲染 HTML 骨架，数据由前端 JS 通过 API 获取
func WorkflowsPage(c *gin.Context) {
	c.HTML(http.StatusOK, "workflows.html", gin.H{
		"ActiveNav": "workflows",
	})
}

// WorkflowDetail 工作流详情页面，只渲染 HTML 骨架，数据由前端 JS 通过 API 获取
func WorkflowDetail(c *gin.Context) {
	c.HTML(http.StatusOK, "workflow.html", gin.H{
		"ActiveNav": "workflows",
	})
}

// HistoryPage 历史记录页面，只渲染 HTML 骨架，数据由前端 JS 通过 API 获取
func HistoryPage(c *gin.Context) {
	c.HTML(http.StatusOK, "history.html", gin.H{
		"ActiveNav": "history",
	})
}
