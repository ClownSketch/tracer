package types

// SpanLogSeverity 表示日志级别
type SpanLogSeverity string

const (
	SpanLogSeverityDebug  SpanLogSeverity = "debug"  // 调试
	SpanLogSeverityInfo   SpanLogSeverity = "info"   // 信息
	SpanLogSeverityWarn   SpanLogSeverity = "warn"   // 警告
	SpanLogSeverityError  SpanLogSeverity = "error"  // 错误
	SpanLogSeverityFatal  SpanLogSeverity = "fatal"  // 严重
	SpanLogSeverityPanic  SpanLogSeverity = "panic"  // 恐慌
	SpanLogSeverityTrace  SpanLogSeverity = "trace"  // 追踪
	SpanLogSeverityMetric SpanLogSeverity = "metric" // 指标
)

func (s SpanLogSeverity) String() string {
	return string(s)
}

// SpanLog 定义日志列表
type SpanLog struct {
	Timestamp  string          `json:"timestamp"`  // 日志时间，格式为ISO 8601，如2021-01-01T00:00:00.000Z
	Message    string          `json:"message"`    // 日志消息
	Fields     any             `json:"fields"`     // 日志字段，可用于存储自定义数据
	Severity   SpanLogSeverity `json:"severity"`   // 日志级别，如debug、info、warn、error
	EventType  string          `json:"event_type"` // 事件类型
	Attributes map[string]any  `json:"attributes"` // 日志属性
}
