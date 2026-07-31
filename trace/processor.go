package trace

import "context"

// SpanProcessor 定义处理器接口
type SpanProcessor interface {
	// OnStart 在Span开始时调用
	OnStart(ctx context.Context, span Span)

	// OnEnd 在Span结束时调用；处理器取得快照所有权，处理完成后必须调用 Release。
	OnEnd(span SpanSnapshot)

	// Shutdown 在追踪器提供者关闭时调用
	Shutdown(ctx context.Context) error
}
