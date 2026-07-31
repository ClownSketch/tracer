package processor

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
)

// NoopSpanProcessor 是一个空Span处理器实现
type NoopSpanProcessor struct{}

// OnStart 实现trace.SpanProcessor接口
// 主要用于在Span开始时执行，可以用于记录Span的开始时间、设置Span的标签、属性等
// @param ctx 上下文
// @param span Span实例
func (n *NoopSpanProcessor) OnStart(ctx context.Context, span trace.Span) {}

// OnEnd 释放不需要继续处理的 Span 快照。
// @param span Span实例
func (n *NoopSpanProcessor) OnEnd(span trace.SpanSnapshot) {
	if span != nil {
		span.Release()
	}
}

// Shutdown 实现trace.SpanProcessor接口
// 主要用于在追踪器关闭时执行，可以用于关闭Span处理器、关闭资源等
// @param ctx 上下文
// @return error 错误，如果关闭失败，则返回错误
func (n *NoopSpanProcessor) Shutdown(ctx context.Context) error { return nil }
