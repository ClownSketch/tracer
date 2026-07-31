package trace

import "github.com/ClownSketch/tracer/types"

// SpanSampler 定义采样器接口
type SpanSampler interface {
	// ShouldSample 根据采样参数返回采样结果
	// 返回采样决策、自定义属性、追踪器状态
	ShouldSample(params types.SamplingParameters) types.SamplingResult
}
