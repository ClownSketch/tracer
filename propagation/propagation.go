package propagation

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
)

// Propagation 定义传播接口
type Propagation interface {
	// Inject 将追踪上下文和baggage注入到载体中
	Inject(ctx context.Context, carrier trace.ContextCarrier) error

	// Extract 从载体中提取追踪上下文和baggage
	// 返回更新后的 context（包含提取的 SpanContext 和 Baggage）
	Extract(ctx context.Context, carrier trace.ContextCarrier) context.Context
}
