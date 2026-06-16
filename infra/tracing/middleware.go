package tracing

import (
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// GinMiddlewares 返回 Gin 的 tracing 中间件列表
// 若 tracing 未启用，返回一个透传的空操作中间件
// 功能：
//   - 自动创建 HTTP span，提取 W3C TraceContext
//   - 在响应 header 中返回 X-Trace-Id，方便调用方排查
func GinMiddlewares() []gin.HandlerFunc {
	if !enabled {
		return []gin.HandlerFunc{func(c *gin.Context) { c.Next() }}
	}

	// 1. otelgin 中间件：创建 span、提取 TraceContext
	otelMw := otelgin.Middleware(
		"vibeflow",
		otelgin.WithSpanNameFormatter(func(c *gin.Context) string {
			return c.Request.Method + " " + c.FullPath()
		}),
	)

	// 2. trace-id 响应头中间件：
	//    otelgin 在 c.Next() 之前创建 span 并注入 ctx，
	//    此中间件在 otelgin 之后注册，其 c.Next() 在 otelgin 的 c.Next() 内部执行，
	//    因此此时 c.Request.Context() 已包含 span。
	traceIDMw := func(c *gin.Context) {
		// c.Next() 之前，otelgin 已创建 span 并注入 ctx
		span := oteltrace.SpanFromContext(c.Request.Context())
		if span.SpanContext().HasTraceID() {
			c.Header("X-Trace-Id", span.SpanContext().TraceID().String())
		}
		c.Next()
	}

	return []gin.HandlerFunc{otelMw, traceIDMw}
}

// StartWorkflowSpan 从 gin.Context 创建带工作流属性的 span
func StartWorkflowSpan(c *gin.Context, name string, workflowUUID string, workflowName string) (*gin.Context, oteltrace.Span) {
	ctx, sp := StartSpan(
		c.Request.Context(),
		name,
		StringAttr("workflow.uuid", workflowUUID),
		StringAttr("workflow.name", workflowName),
	)
	c.Request = c.Request.WithContext(ctx)
	return c, sp
}

// TaskSpanAttributes 返回任务 span 的通用属性
func TaskSpanAttributes(workflowUUID, taskName, traceID string, attempt int) []attribute.KeyValue {
	return []attribute.KeyValue{
		StringAttr("workflow.uuid", workflowUUID),
		StringAttr("task.name", taskName),
		StringAttr("task.trace_id", traceID),
		IntAttr("task.attempt", attempt),
	}
}

// LogWorkflowLifecycle 返回工作流生命周期事件的 span 属性
func LogWorkflowLifecycle(workflowUUID, workflowName, event string) []attribute.KeyValue {
	return []attribute.KeyValue{
		StringAttr("workflow.uuid", workflowUUID),
		StringAttr("workflow.name", workflowName),
		StringAttr("workflow.event", event),
	}
}
