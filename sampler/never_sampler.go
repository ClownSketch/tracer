package sampler

import (
	"github.com/ClownSketch/tracer/types"
)

// NeverSampler 是一个从不采样的Span采样器实现
type NeverSampler struct {
}

// NewNeverSampler 创建一个新的NeverSampler
func NewNeverSampler() *NeverSampler {
	return &NeverSampler{}
}

// ShouldSample 实现trace.SpanSampler接口
// 主要用于从不采样
// @param params types.SamplingParameters 采样参数
// @return types.SamplingResult 采样结果
func (s *NeverSampler) ShouldSample(params types.SamplingParameters) types.SamplingResult {
	return types.SamplingResult{
		Decision: types.SamplingDecisionDrop,
		Attributes: map[string]any{
			"sampling.type": "never",
		},
	}
}
