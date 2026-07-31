package sampler

import (
	"sync"
	"time"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// RateLimitingSampler 是一个限制采样的Span采样器实现
type RateLimitingSampler struct {
	mu                 sync.Mutex // 互斥锁
	maxTracesPerSecond float64    // 每秒最大追踪数
	lastResetTime      time.Time  // 上次重置时间
	tracesPerSecond    float64    // 每秒追踪数
}

// NewRateLimitingSampler 主要用于创建一个速率限制采样器
// @param maxTracesPerSecond 每秒最大追踪数
// @return Sampler 采样器
func NewRateLimitingSampler(maxTracesPerSecond float64) trace.SpanSampler {

	// 如果每秒最大追踪数小于等于0，则设置为1
	if maxTracesPerSecond <= 0 {
		maxTracesPerSecond = 1
	}

	// 创建一个速率限制采样器
	return &RateLimitingSampler{
		maxTracesPerSecond: maxTracesPerSecond, // 每秒最大追踪数
		lastResetTime:      time.Now(),         // 上次重置时间
	}
}

// ShouldSample 实现trace.SpanSampler接口
// 主要用于根据速率限制采样器返回采样结果
// @param params 采样参数
// @return types.SamplingResult 采样结果
func (s *RateLimitingSampler) ShouldSample(params types.SamplingParameters) types.SamplingResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()                             // 当前时间
	elapsed := now.Sub(s.lastResetTime).Seconds() // 距离上次重置时间的差值
	s.lastResetTime = now                         // 更新上次重置时间

	// 计算当前可用的追踪数，每秒最大追踪数 - 距离上次重置时间的差值 * 每秒最大追踪数
	s.tracesPerSecond = s.tracesPerSecond - (elapsed * s.maxTracesPerSecond)

	// 如果当前可用的追踪数小于0，则丢弃
	if s.tracesPerSecond < 0 {
		s.tracesPerSecond = 0
	}

	// 如果当前可用的追踪数大于等于每秒最大追踪数，则丢弃
	if s.tracesPerSecond >= s.maxTracesPerSecond {
		return types.SamplingResult{
			Decision: types.SamplingDecisionDrop,
			Attributes: map[string]any{
				"sampling.type":       "rate_limiting",      // 采样类型
				"sampling.rate_limit": s.maxTracesPerSecond, // 每秒最大追踪数
			},
		}
	}

	s.tracesPerSecond++ // 增加当前可用的追踪数

	// 如果当前可用的追踪数小于每秒最大追踪数，则记录并采样
	return types.SamplingResult{
		Decision: types.SamplingDecisionRecordAndSample,
		Attributes: map[string]any{
			"sampling.type":       "rate_limiting",      // 采样类型
			"sampling.rate_limit": s.maxTracesPerSecond, // 每秒最大追踪数
		},
	}

}
