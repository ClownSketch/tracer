package trace

import "context"

// SpanExporter 是用于将 Span 数据同步导出到外部系统的接口。
//
// 当前设计中，异步调度、fallback 和快照释放都由 processor 统一负责，
// exporter 只负责“收到一批就直接写后端”。
//
// 约定：
//   - Exporter 不负责调用 span.Release()
//   - Exporter 不负责 fallback
//   - 返回 nil 表示这一批数据已成功写出
//   - 返回 error 表示这一批数据写出失败，由上层 processor 决定是否 fallback
type SpanExporter interface {
	// ExportSpan 同步导出单个 Span。
	ExportSpan(span SpanSnapshot) error

	// ExportSpans 同步批量导出多个 Span。
	ExportSpans(spans []SpanSnapshot) error

	// Shutdown 优雅地关闭导出器并释放所有资源。
	Shutdown(ctx context.Context) error
}

// SyncSpanExporter 是可选接口，用于支持带确认语义的同步导出。
//
// 与 SpanExporter 不同，这组方法会在数据真正完成导出后返回结果，
// 适合 WAL / durable processor 这类需要“成功后 ACK，失败后保留本地日志”的场景。
//
// 约定：
//   - 实现方负责在方法返回前释放传入的 SpanSnapshot 资源
//   - 返回 nil 表示这一批数据已经被后端成功接收，可以推进 ACK
//   - 返回 error 表示导出失败，调用方不应删除本地 WAL 记录
type SyncSpanExporter interface {
	// ExportSpanSync 同步导出单个 Span。
	ExportSpanSync(ctx context.Context, span SpanSnapshot) error

	// ExportSpansSync 同步批量导出多个 Span。
	ExportSpansSync(ctx context.Context, spans []SpanSnapshot) error
}
