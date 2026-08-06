package types

// SpanOptions 定义Span配置选项
// 可以设置Span的名称、标签、属性、事件、状态等
type SpanOptions func(spanConfig *SpanConfig)

// Span 导出策略（写入 spanState.forceRecord，End 时据此决定是否生成快照）。
const (
	RecordPolicyNone    uint32 = 0 // 默认不导出
	RecordPolicyAlways  uint32 = 1 // 始终导出（WithForceRecord）
	RecordPolicyOnError uint32 = 2 // 仅在有错误时导出（WithRecordOnError）
)

// SpanConfig 定义Span配置
// 可以设置Span的名称、标签、属性、事件、状态等
type SpanConfig struct {
	SpanKind        SpanKind // Span的类型
	ForceRecord     uint32   // 导出策略，见 RecordPolicy* 常量
	MongoCollection string   // MongoDB 导出集合名，空表示使用导出器默认集合
}

// SpanKind 表示 Span 的类型
type SpanKind string

const (
	SpanKindInternal SpanKind = "internal" // 内部 Span
	SpanKindClient   SpanKind = "client"   // 客户端 Span
	SpanKindServer   SpanKind = "server"   // 服务端 Span
	SpanKindProducer SpanKind = "producer" // 生产者 Span
	SpanKindConsumer SpanKind = "consumer" // 消费者 Span
	SpanKindCron     SpanKind = "corn"     // 定时任务Span
	SpanKindAsync    SpanKind = "async"    // 异步Span
)

// String 返回 Span 类型的字符串值。
// @return result string Span 类型
func (s SpanKind) String() string {
	return string(s)
}

// StatusCode 表示span的执行状态码
type StatusCode string

const (
	StatusCodeUnset   StatusCode = "0"     // 未设置
	StatusCodeOk      StatusCode = "200"   // 成功
	StatusCodeError   StatusCode = "50000" // 错误
	StatusCodeWarning StatusCode = "50001" // 警告
	StatusCodeInfo    StatusCode = "50002" // 信息
	StatusCodeDebug   StatusCode = "50003" // 调试
	StatusCodeTrace   StatusCode = "50004" // 追踪
	StatusCodeMetric  StatusCode = "50005" // 指标
	StatusCodeUnknown StatusCode = "50006" // 未知
)

// String 返回状态码的字符串值。
// @return result string 状态码
func (s StatusCode) String() string {
	return string(s)
}

// SpanStatus 表示span的执行状态
type SpanStatus struct {
	Code        StatusCode // 状态码
	Description string     // 状态描述
}
