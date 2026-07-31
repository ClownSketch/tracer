package types

// SamplingDecision 定义采样决策
type SamplingDecision int

const (
	SamplingDecisionRecordAndSample SamplingDecision = iota // 记录并采样
	SamplingDecisionRecordOnly                              // 仅记录
	SamplingDecisionDrop                                    // 丢弃
)

// SamplingParameters 定义采样参数
type SamplingParameters struct {
	Name         string   // 名称
	TraceID      string   // 追踪ID
	ParentSpanID string   // 父追踪ID
	SpanKind     SpanKind // 跨度类型
}

// SamplingResult 定义采样结果
type SamplingResult struct {
	Decision         SamplingDecision // 采样决策
	Attributes       map[string]any   // 自定义属性
	TracerStateBytes []byte           // 追踪器状态
}
