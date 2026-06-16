// Package tracing 提供基于 OpenTelemetry 的分布式链路追踪能力
// 可选启用：若配置中 enabled=false 或未配置，所有操作退化为空操作，不影响程序运行
package tracing

import (
	"context"
	"fmt"

	"chihqiang/vibeflow/infra/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var (
	// tracer 全局 tracer 实例，未初始化时为空操作 tracer
	tracer oteltrace.Tracer
	// tp 全局 TracerProvider，用于 Shutdown
	tp *sdktrace.TracerProvider
	// enabled 是否已成功初始化
	enabled bool
)

// Init 初始化 OpenTelemetry tracing
// 若 cfg.Enabled=false 或配置无效，退化为空操作，不影响程序运行
func Init(cfg config.TracingConfig) error {
	if !cfg.Enabled {
		return nil
	}

	if cfg.Endpoint == "" {
		return nil
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "vibeflow"
	}
	sampleRate := cfg.SampleRate
	if sampleRate <= 0 {
		sampleRate = 1.0
	}

	// 创建 OTLP gRPC exporter，连接到 Jaeger
	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("创建 OTLP trace exporter 失败: %w", err)
	}

	// 构建 Resource，标识服务信息
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return fmt.Errorf("创建 trace resource 失败: %w", err)
	}

	// 创建 TracerProvider
	tp = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate))),
	)

	// 设置全局 TracerProvider 和 Propagator
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer = tp.Tracer(serviceName)
	enabled = true

	return nil
}

// Shutdown 优雅关闭 tracing，刷新缓冲区中的 span
func Shutdown() {
	if tp != nil {
		_ = tp.Shutdown(context.Background())
	}
}

// IsEnabled 返回 tracing 是否已启用
func IsEnabled() bool {
	return enabled
}

// Tracer 返回全局 tracer 实例
func Tracer() oteltrace.Tracer {
	if tracer == nil {
		return otel.Tracer("vibeflow-noop")
	}
	return tracer
}

// StartSpan 从 context 开始一个新的 span
// 若 tracing 未启用，返回原始 context 和空 span
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	if !enabled {
		return ctx, oteltrace.SpanFromContext(ctx)
	}
	return Tracer().Start(ctx, name, oteltrace.WithAttributes(attrs...))
}

// AddSpanEvent 向当前 span 添加事件
func AddSpanEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := oteltrace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent(name, oteltrace.WithAttributes(attrs...))
	}
}

// RecordError 记录错误到当前 span
func RecordError(ctx context.Context, err error) {
	span := oteltrace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.RecordError(err)
	}
}

// SetSpanStatus 设置当前 span 的状态
func SetSpanStatus(ctx context.Context, code codes.Code, description string) {
	span := oteltrace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(code, description)
	}
}

// StringAttr 创建字符串属性
func StringAttr(k, v string) attribute.KeyValue {
	return attribute.String(k, v)
}

// IntAttr 创建整数属性
func IntAttr(k string, v int) attribute.KeyValue {
	return attribute.Int(k, v)
}

// Int64Attr 创建 int64 属性
func Int64Attr(k string, v int64) attribute.KeyValue {
	return attribute.Int64(k, v)
}

// BoolAttr 创建布尔属性
func BoolAttr(k string, v bool) attribute.KeyValue {
	return attribute.Bool(k, v)
}

// ============================================================================
// TraceContext 跨进程传播
// ============================================================================

// TraceContextCarrier 用于序列化/反序列化 W3C TraceContext
// 实现 otel propagation.TextMapCarrier 接口
type TraceContextCarrier map[string]string

// Get 实现 TextMapCarrier.Get
func (c TraceContextCarrier) Get(key string) string {
	return c[key]
}

// Set 实现 TextMapCarrier.Set
func (c TraceContextCarrier) Set(key, value string) {
	c[key] = value
}

// Keys 实现 TextMapCarrier.Keys
func (c TraceContextCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectTraceContext 将当前 context 中的 trace 信息注入到 carrier map 中
// 用于跨进程传播（Master → etcd → Worker）
func InjectTraceContext(ctx context.Context) map[string]string {
	if !enabled {
		return nil
	}
	carrier := make(TraceContextCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

// ExtractTraceContext 从 carrier map 中提取 trace 信息并设置到 context
// 用于跨进程恢复（Worker 从 etcd 读取后恢复）
func ExtractTraceContext(ctx context.Context, carrier map[string]string) context.Context {
	if !enabled || carrier == nil {
		return ctx
	}
	tc := TraceContextCarrier(carrier)
	return otel.GetTextMapPropagator().Extract(ctx, tc)
}
