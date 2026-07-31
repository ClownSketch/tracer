package http

import (
	"context"

	"github.com/ClownSketch/tracer/propagation"
	"github.com/ClownSketch/tracer/propagation/text"
	"github.com/ClownSketch/tracer/trace"
)

// HTTPPropagator HTTP 传播器实现
// 提供 HTTP 协议特定的上下文传播功能
type HTTPPropagator struct {
	// 内部使用 text 包的实现（HTTP 本质上是文本协议）
	textPropagator *text.TextMapPropagator
}

// NewHTTPPropagator 创建 HTTP 传播器
func NewHTTPPropagator() propagation.Propagation {
	return &HTTPPropagator{
		textPropagator: text.NewTextMapPropagator(),
	}
}

// Extract 从 HTTP 请求中提取 Span 上下文和 baggage
// 实现 Propagator 接口
// 返回更新后的 context（包含提取的 SpanContext 和 Baggage）
func (p *HTTPPropagator) Extract(ctx context.Context, carrier trace.ContextCarrier) context.Context {
	// 提取 Span 上下文和 baggage
	return p.textPropagator.Extract(ctx, carrier)
}

// Inject 将 Span 上下文注入到 HTTP 响应中
// 实现 Propagator 接口
func (p *HTTPPropagator) Inject(ctx context.Context, carrier trace.ContextCarrier) error {
	return p.textPropagator.Inject(ctx, carrier)
}
