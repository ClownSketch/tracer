package trace

import "context"

// FallbackWriter 定义 fallback 写入器接口
// 用于在导出器失败时，将 Span 快照写入本地存储，保证数据不丢失
// 所有导出器都使用统一的 FallbackSpanData 格式写入本地磁盘
type FallbackWriter interface {
	// Fallback 将单个 span 可靠写入 fallback 存储。
	Fallback(data []byte) error

	// FallbackBatch 将一批 spans 可靠写入 fallback 存储。
	FallbackBatch(dataList [][]byte) error

	// Recover 恢复已完整落盘的 fallback 数据。
	Recover(exporter SpanExporter) error

	// Shutdown 关闭 fallback writer 并清理资源
	Shutdown(ctx context.Context) error
}
