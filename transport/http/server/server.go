// Package server 提供 Gin HTTP 服务端实现，包含 API 路由和 Web 管理界面
package server

import (
	"context"
	"html/template"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/engine/scheduler"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/tracing"
	"chihqiang/vibeflow/infra/ws"
	"chihqiang/vibeflow/transport/http/handlers"
	"chihqiang/vibeflow/transport/http/views"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// Server Gin HTTP 服务，提供 RESTful API 和 Tailwind CSS 管理页面
type Server struct {
	sch             *scheduler.Scheduler
	addr            string
	ginEngine       *gin.Engine
	srv             *http.Server
	shutdownTimeout time.Duration
}

// defaultShutdownTimeout HTTP Server 优雅关闭的默认超时时间，当配置值 <= 0 时兜底使用
const defaultShutdownTimeout = 10 * time.Second

// NewServer 创建并配置 HTTP 服务
// 注册所有 API 路由、Web 页面路由和 WebSocket 端点
func NewServer(sch *scheduler.Scheduler, wsEvent *ws.WSEvent, etcdStore store.Store, historyStore store.HistoryStore, cfg *config.MasterConfig) *Server {
	gin.SetMode(gin.ReleaseMode)
	ginEngine := gin.New()
	ginEngine.Use(gin.Recovery())
	for _, mw := range tracing.GinMiddlewares() {
		ginEngine.Use(mw)
	}
	ginEngine.Use(cors.New(cors.Config{
		AllowAllOrigins:  true,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
		MaxAge:           cfg.CORSMaxAge.ToDuration(),
	}))

	// 从嵌入式文件系统加载 HTML 模板
	// 使用 {% %} 作为 Go 模板定界符，避免与 Vue 的 {{ }} 冲突
	ginEngine.SetHTMLTemplate(template.Must(template.New("").Delims("{%", "%}").ParseFS(views.ViewFS, "*.html")))

	// Web 管理页面路由（仅渲染 HTML 骨架，数据通过 API 接口获取）
	ginEngine.GET("/", handlers.DashboardPage)
	ginEngine.GET("/tasks", handlers.TasksPage)
	ginEngine.GET("/history", handlers.HistoryPage)

	workflows := ginEngine.Group("/workflows")
	{
		workflows.GET("", handlers.WorkflowsPage)
		workflows.GET("/:uuid", handlers.WorkflowDetail)
	}

	// RESTful API 路由
	v1 := ginEngine.Group("/api/v1")
	{
		v1.GET("/health", handlers.Health(sch, etcdStore, historyStore))
		v1.GET("/metrics", handlers.Metrics(sch))
		v1.GET("/dashboard", handlers.DashboardAPI(sch))
		v1.GET("/tasks", handlers.ListTaskTypes(sch))

		v1.GET("/workers", handlers.ListWorkers(sch))

		v1.POST("/workflows", handlers.CreateWorkflow(sch))
		v1.GET("/workflows", handlers.ListWorkflows(sch))
		v1.GET("/workflows/:uuid", handlers.GetWorkflow(sch))
		v1.DELETE("/workflows/:uuid", handlers.CancelWorkflow(sch))
		v1.POST("/workflows/:uuid/retry", handlers.RetryWorkflow(sch))
		v1.POST("/workflows/:uuid/run", handlers.RunWorkflow(sch))
		v1.POST("/workflows/:uuid/approve", handlers.ApproveWorkflow(sch))
		v1.POST("/workflows/:uuid/reject", handlers.RejectWorkflow(sch))

		v1.GET("/history", handlers.ListHistory(sch))
		v1.GET("/history/paged", handlers.ListHistoryPaged(sch))
		v1.GET("/history/:uuid", handlers.GetHistory(sch))

		v1.POST("/webhooks/:uuid", handlers.TriggerWebhook(sch))
		v1.GET("/webhooks", handlers.ListWebhookWorkflows(sch))
	}

	ginEngine.GET("/ws", handlers.HandleWebSocket(wsEvent))

	return &Server{
		sch:       sch,
		addr:      cfg.HTTPAddr,
		ginEngine: ginEngine,
		shutdownTimeout: func() time.Duration {
			t := cfg.ShutdownTimeout.ToDuration()
			if t <= 0 {
				return defaultShutdownTimeout
			}
			return t
		}(),
	}
}

// Start 启动 HTTP 服务，监听直到 ctx 取消后优雅关闭
func (s *Server) Start(ctx context.Context) error {
	s.srv = &http.Server{Addr: s.addr, Handler: s.ginEngine}

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("HTTP 服务已启动", "addr", s.addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		shutdownErr := s.srv.Shutdown(shutdownCtx)
		// 等待 ListenAndServe goroutine 完全退出
		wg.Wait()
		return shutdownErr
	}
}
