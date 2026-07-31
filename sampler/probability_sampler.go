package sampler

import (
	"math/rand/v2"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// AlwaysSampler 是一个总是采样的Span采样器实现（无锁，高并发优化）
type AlwaysSampler struct{}

// ShouldSample 实现sampler.Sampler接口，总是返回采样
func (s *AlwaysSampler) ShouldSample(params types.SamplingParameters) types.SamplingResult {
	return types.SamplingResult{
		Decision: types.SamplingDecisionRecordAndSample,
		Attributes: map[string]any{
			"sampling.probability": 1.0,
		},
	}
}

// ProbabilitySampler 是一个概率采样的Span采样器实现
// 使用 math/rand/v2 的全局函数（线程安全），无需 Mutex
type ProbabilitySampler struct {
	probability float64 // 采样概率（只读，不需要锁）
}

// NewProbabilitySampler 创建一个概率采样器
func NewProbabilitySampler(probability float64) trace.SpanSampler {
	// 如果概率小于0.0，则设置为0.0
	if probability < 0.0 {
		probability = 0.0
	}

	// 如果概率大于1.0，则设置为1.0
	if probability > 1.0 {
		probability = 1.0
	}

	// 优化：概率为 1.0 或 0.0 时，使用专门的 sampler（无锁）
	if probability == 1.0 {
		return &AlwaysSampler{}
	}
	if probability == 0.0 {
		return NewNeverSampler()
	}

	// 创建一个概率采样器
	return &ProbabilitySampler{
		probability: probability, // 采样概率
	}
}

// ShouldSample 实现sampler.Sampler接口
// 主要用于根据采样参数返回采样结果
// 使用 math/rand/v2 的全局函数 rand.Float64()（线程安全），无需 Mutex
// @param params 采样参数
// @return SamplingResult 采样结果
func (s *ProbabilitySampler) ShouldSample(params types.SamplingParameters) types.SamplingResult {
	// 使用全局 rand.Float64()（线程安全，无需锁）
	// math/rand/v2 的全局函数是线程安全的
	shouldSample := rand.Float64() < s.probability

	// 如果小于采样概率，则采样
	if shouldSample {
		return types.SamplingResult{
			// 记录并采样
			Decision: types.SamplingDecisionRecordAndSample,
			// 自定义属性，用于记录采样概率
			Attributes: map[string]any{
				"sampling.probability": s.probability,
			},
		}
	}

	return types.SamplingResult{
		// 未命中采样概率时直接丢弃，避免构建不会导出的完整 Span。
		Decision: types.SamplingDecisionDrop,
		// 自定义属性，用于记录采样概率
		Attributes: map[string]any{
			"sampling.probability": s.probability,
		},
	}
}

// NewAlwaysSampleSampler 主要用于创建一个总是采样器（无锁实现）
// @return Sampler 采样器
func NewAlwaysSampleSampler() trace.SpanSampler {
	return &AlwaysSampler{}
}

// NewNeverSampleSampler 主要用于创建一个从不采样器（无锁实现）
// @return Sampler 采样器
func NewNeverSampleSampler() trace.SpanSampler {
	return NewNeverSampler()
}
