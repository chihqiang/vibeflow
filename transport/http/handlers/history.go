package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/engine/scheduler"
	"chihqiang/vibeflow/domain/model"

	"github.com/gin-gonic/gin"
)

// ListHistoryRequest 历史记录列表请求参数
type ListHistoryRequest struct {
	Page     int `form:"page" binding:"omitempty,min=1"`              // 页码，从 1 开始
	PageSize int `form:"page_size" binding:"omitempty,min=1,max=100"` // 每页数量，最大 100
}

// ListHistoryResponse 分页历史记录响应
type ListHistoryResponse struct {
	List     []*model.WorkflowState `json:"list"`
	Total    int                   `json:"total"`
	Page     int                   `json:"page"`
	PageSize int                  `json:"page_size"`
}

// ListHistory 返回所有已结束的工作流历史记录
func ListHistory(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		history := sch.ListHistory()
		c.JSON(http.StatusOK, Response{Code: 0, Data: history})
	}
}

// maxPageSize 分页查询的每页数量上限，防止调用方传入极大值导致一次性从 MySQL 加载大量数据
const maxPageSize = 100

// ListHistoryPaged 分页返回已结束的工作流历史记录
// 请求参数通过 ShouldBindQuery 绑定到 ListHistoryRequest 结构体
// page_size 上限为 maxPageSize（100），超出部分自动截断
func ListHistoryPaged(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ListHistoryRequest
		if err := c.ShouldBindQuery(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的请求参数: " + err.Error()})
			return
		}
		if req.Page == 0 {
			req.Page = 1
		}
		if req.PageSize == 0 {
			req.PageSize = 10
		}
		// 安全上限：即使 binding 标签的 max=100 被绕过（例如框架 bug 或直接调用内部方法），
		// 也在此处做二次截断，确保不会一次性加载过多数据
		if req.PageSize > maxPageSize {
			req.PageSize = maxPageSize
		}
		list, total := sch.ListHistoryPaged(req.Page, req.PageSize)
		c.JSON(http.StatusOK, Response{Code: 0, Data: ListHistoryResponse{
			List:     list,
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		}})
	}
}

// GetHistory 查询单个历史工作流详情
func GetHistory(sch *scheduler.Scheduler) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req WorkflowUUIDRequest
		if err := c.ShouldBindUri(&req); err != nil {
			c.JSON(http.StatusBadRequest, Response{Code: 1, Msg: "无效的工作流 UUID: " + err.Error()})
			return
		}
		state := sch.GetHistory(req.UUID)
		if state == nil {
			c.JSON(http.StatusNotFound, Response{Code: 1, Msg: "历史记录不存在"})
			return
		}
		c.JSON(http.StatusOK, Response{Code: 0, Data: state})
	}
}
