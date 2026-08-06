package noop

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// NoopTracer 实现不创建真实链路数据的 Tracer。
type NoopTracer struct{}

// Start 返回原上下文和无操作 Span。
// @param ctx context.Context 上下文
// @param spanName string Span 名称
// @param options ...types.SpanOptions Span 配置项
// @return resultCtx context.Context 原上下文
// @return resultSpan trace.Span 无操作 Span
func (n *NoopTracer) Start(ctx context.Context, spanName string, options ...types.SpanOptions) (context.Context, trace.Span) {
	return ctx, &NoopSpan{}
}
