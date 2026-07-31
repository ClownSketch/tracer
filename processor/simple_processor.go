package processor

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
)

// SimpleSpanProcessor 是一个简单的Span处理器实现
type SimpleSpanProcessor struct {
	// exporter 导出器
	exporter trace.SpanExporter
}

// NewSimpleSpanProcessor 创建一个新的SimpleSpanProcessor
func NewSimpleSpanProcessor(exporter trace.SpanExporter) trace.SpanProcessor {
	return &SimpleSpanProcessor{exporter: exporter}
}

// OnStart 实现trace.SpanProcessor接口
// 主要用于在Span开始时执行，可以用于记录Span的开始时间、设置Span的标签、属性等
// @param ctx 上下文
// @param span Span实例
func (p *SimpleSpanProcessor) OnStart(ctx context.Context, span trace.Span) {
	// 简单处理器在Span开始的时候不做任何操作
}

// OnEnd 实现trace.SpanProcessor接口
// 主要用于在Span结束时执行，可以用于记录Span的结束时间、设置Span的标签、属性等
// @param span Span实例
func (p *SimpleSpanProcessor) OnEnd(span trace.SpanSnapshot) {
	if span == nil {
		return
	}
	defer span.Release()
	if p.exporter != nil {
		_ = p.exporter.ExportSpan(span)
	}
}

// Shutdown 实现trace.SpanProcessor接口
// 主要用于在追踪器关闭时执行，可以用于关闭Span处理器、关闭资源等
// @param ctx 上下文
// @return error 错误，如果关闭失败，则返回错误
func (p *SimpleSpanProcessor) Shutdown(ctx context.Context) error {
	// 如果导出器不为空，则关闭导出器
	if p.exporter != nil {
		return p.exporter.Shutdown(ctx)
	}

	return nil

}
