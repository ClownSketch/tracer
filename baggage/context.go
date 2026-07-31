package baggage

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/trace/noop"
	"github.com/ClownSketch/tracer/types"
)

// contextKey 定义一个上下文键，用于在上下文中存储Span
// 主要用于在上下文中存储Span，用于上下文传播
type contextKey struct{}

// spanContextKey 定义一个上下文键，用于在上下文中存储SpanContext
// 主要用于在上下文中存储SpanContext，用于上下文传播
type spanContextKey struct{}

// WithSpanContext 将Span添加到上下文中
// 主要用于将Span添加到上下文中，用于上下文传播
func WithSpanContext(ctx context.Context, span trace.Span) context.Context {
	// 将Span添加到上下文中
	return context.WithValue(ctx, contextKey{}, span)
}

// GetSpanContext 从上下文中获取Span
// 如果Span不存在，返回noopSpan
func GetSpanContext(ctx context.Context) trace.Span {
	// 从上下文中获取Span
	if span, ok := ctx.Value(contextKey{}).(trace.Span); ok {
		// 如果Span存在，则返回Span
		return span
	}

	// 如果Span不存在，则返回noopSpan
	return new(noop.NoopSpan)
}

// WithContextSpanContext 将SpanContext添加到上下文中
// 主要用于将SpanContext添加到上下文中，用于上下文传播
func WithContextSpanContext(ctx context.Context, spanContext types.SpanContext) context.Context {
	// 将SpanContext添加到上下文中
	return context.WithValue(ctx, spanContextKey{}, spanContext)
}

// SpanContextFromContext 从上下文中获取SpanContext
// 主要用于从上下文中获取SpanContext，用于上下文传播
// 如果SpanContext不存在，返回空的SpanContext
func SpanContextFromContext(ctx context.Context) types.SpanContext {
	// 从上下文中获取SpanContext
	if spanContext, ok := ctx.Value(spanContextKey{}).(types.SpanContext); ok {
		// 如果SpanContext存在，则返回SpanContext
		return spanContext
	}

	// 如果SpanContext不存在，则返回空的SpanContext
	return types.SpanContext{}
}

// GetTraceIDFromContext 从上下文中获取TraceID
// 主要用于从上下文中获取TraceID，用于上下文传播
// 优先从当前活跃的 Span 获取 TraceID，如果 Span 不存在，则从 SpanContext 获取
// 如果TraceID不存在，返回空字符串
func GetTraceIDFromContext(ctx context.Context) string {
	// 优先从上下文中获取 Span，如果 Span 存在，则返回 Span 的 TraceID
	if span := GetSpanContext(ctx); span != nil {
		// 使用 GetSpanTraceID 方法获取 TraceID（更直接，避免创建 SpanContext）
		if traceID := span.GetSpanTraceID(); traceID != "" {
			return traceID
		}
		// 如果 GetSpanTraceID 返回空，尝试从 SpanContext 获取
		if spanCtx := span.SpanContext(); spanCtx.TraceID != "" {
			return spanCtx.TraceID
		}
	}

	// 如果 Span 不存在或 Span 中没有 TraceID，从 SpanContext 获取
	spanContext := SpanContextFromContext(ctx)
	// 如果 SpanContext 存在，则返回 SpanContext 的 TraceID，否则返回空的 TraceID
	return spanContext.TraceID
}

// GetSpanIDFromContext 从上下文中获取SpanID
// 主要用于从上下文中获取SpanID，用于上下文传播
// 如果SpanID不存在，返回空字符串
func GetSpanIDFromContext(ctx context.Context) string {
	// 从上下文中获取Span，如果Span存在，则返回Span的SpanID
	if span := GetSpanContext(ctx); span != nil {
		// 如果Span存在，则返回Span的SpanID
		return span.SpanContext().SpanID
	}

	// 如果Span不存在，从SpanContext获取
	spanContext := SpanContextFromContext(ctx)
	// 如果SpanContext存在，则返回SpanContext的SpanID，否则返回空的SpanID
	return spanContext.SpanID
}

// StartAsyncSpan 在异步场景（如goroutine）中创建子span
// 该方法会自动从父context中提取父span，并创建新的子span
// @param parentCtx 父上下文，包含父span
// @param tracer 追踪器实例
// @param spanName 子span的名称
// @param options Span配置选项（如SpanKind、ForceRecord等）
// @return 新的上下文和子span
//
// 性能优化：
// - 在协程外部调用此方法，确保父span还未结束，可以获取属性
// - 同服务内传播：只需要将parentSpan写入新context，Start方法会直接从parentSpan获取属性
// - 不需要将属性写入baggage（只有跨服务传播时才需要）
//
// 示例:
//
//	// 在协程外部创建子span，确保父span还未结束，可以获取属性
//	// tracer.Start 会自动从 newCtx 中的父span获取继承属性和全局属性，并设置到子span中
//	asyncCtx, asyncSpan := baggage.StartAsyncSpan(
//		ctx.Request.Context(),
//		global.Trace,
//		"span_name",
//		tracer.WithSpanKind(tracer.SpanKindAsync),
//	)
//
//	// 在协程中使用
//	go func() {
//		defer asyncSpan.End() // 在协程内部结束span，确保在异步操作完成时结束
//		// 使用 asyncCtx 和 asyncSpan
//		// asyncSpan 会自动继承父span的上下文和属性
//	}()
func StartAsyncSpan(parentCtx context.Context, tracer trace.Tracer, spanName string, options ...types.SpanOptions) (context.Context, trace.Span) {
	// 从父context中获取父span
	parentSpan := GetSpanContext(parentCtx)

	// 创建一个不继承取消信号的新 context，但保留原 context 的值链
	// context.WithoutCancel 会创建一个新的 context，它不继承取消信号，但保留原 context 的值链
	// 这样既保留了中间件设置的数据和自定义的 context 值，又避免了原 context 取消信号的影响
	// 注意：Go 1.21+ 才支持 context.WithoutCancel
	baseCtx := context.WithoutCancel(parentCtx)

	// 将父span写入新context（同服务内传播）
	// Start方法会直接从parentSpan获取继承属性和全局属性，不需要写入baggage
	newCtx := WithSpanContext(baseCtx, parentSpan)

	// 在协程外部创建子span，确保父span还未结束，可以获取属性
	// tracer.Start 会自动从 newCtx 中的 parentSpan 获取继承属性和全局属性，并设置到子span中
	return tracer.Start(newCtx, spanName, options...)
}
