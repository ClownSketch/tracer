package trace

import (
	"context"

	"github.com/ClownSketch/tracer/types"
)

// Tracer 是用于创建和管理 Span 的接口。
//
// Tracer 用于创建 Span，Span 表示分布式系统中的操作。
// 每个服务应该有自己的 Tracer 实例，通常从 TracerProvider 获取。
//
// 示例:
//
//	tracer := provider.GetTracer("my-service")
//	ctx, span := tracer.Start(ctx, "my-operation",
//		tracer.WithSpanKind(tracer.SpanKindServer),
//	)
//	defer span.End()
type Tracer interface {
	// Start 创建一个新的 Span 并返回包含该 Span 的 context。
	//
	// 返回的 context 应该用于同一追踪中的所有后续操作。
	// 当操作完成时，必须调用 span.End() 来结束 Span。
	//
	// 参数:
	//   - ctx: 父级 context。如果它包含 SpanContext，新 Span 将成为该 Span 的子 Span。
	//   - spanName: 此 Span 表示的操作名称。
	//   - options: Span 的可选配置（例如 SpanKind、属性等）。
	//
	// 返回值:
	//   - context.Context: 包含创建的 Span 的 SpanContext 的新 context。
	//   - Span: 新创建的 Span 实例。
	//
	// 示例:
	//
	//	ctx, span := tracer.Start(ctx, "database.query",
	//		tracer.WithSpanKind(tracer.SpanKindClient),
	//	)
	//	defer span.End()
	//
	//	// 使用 context 进行后续操作
	//	result, err := db.Query(ctx, "SELECT * FROM users")
	Start(ctx context.Context, spanName string, options ...types.SpanOptions) (context.Context, Span)
}
