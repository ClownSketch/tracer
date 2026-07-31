package sampler

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// DistributedSampler 分布式采样器
// 基于TraceID的一致性哈希进行采样，确保同一Trace的所有Spans采样决策一致
// 采样决策直接由 TraceID 计算，不保存无界缓存，避免长期运行后内存持续增长。
type DistributedSampler struct {
	sampleRate float64 // 采样率（0.0-1.0）

	// 统计信息
	totalDecisions int64 // 总决策数
	sampledCount   int64 // 采样数
	droppedCount   int64 // 丢弃数
}

// DistributedSamplerOption 分布式采样器选项
type DistributedSamplerOption func(*DistributedSampler)

// WithDistributedSampleRate 设置采样率
func WithDistributedSampleRate(rate float64) DistributedSamplerOption {
	return func(s *DistributedSampler) {
		if rate < 0.0 {
			rate = 0.0
		}
		if rate > 1.0 {
			rate = 1.0
		}
		s.sampleRate = rate
	}
}

// NewDistributedSampler 创建分布式采样器
func NewDistributedSampler(sampleRate float64, opts ...DistributedSamplerOption) trace.SpanSampler {
	if sampleRate < 0 {
		sampleRate = 0
	}
	if sampleRate > 1 {
		sampleRate = 1
	}
	s := &DistributedSampler{
		sampleRate: sampleRate,
	}

	// 应用选项
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// ShouldSample 判断是否应该采样
// 基于TraceID的一致性哈希，确保同一Trace的所有Spans采样决策一致
func (s *DistributedSampler) ShouldSample(params types.SamplingParameters) types.SamplingResult {
	// 更新统计信息
	atomic.AddInt64(&s.totalDecisions, 1)

	// 如果采样率为0，直接丢弃
	if s.sampleRate == 0.0 {
		atomic.AddInt64(&s.droppedCount, 1)
		return types.SamplingResult{
			Decision: types.SamplingDecisionDrop,
		}
	}

	// 如果采样率为1，直接采样
	if s.sampleRate >= 1.0 {
		atomic.AddInt64(&s.sampledCount, 1)
		return types.SamplingResult{
			Decision: types.SamplingDecisionRecordAndSample,
		}
	}

	// 计算TraceID的哈希值
	hash := s.hashTraceID(params.TraceID)

	// 使用哈希高 53 位映射到 [0,1)，避免 uint64 阈值计算溢出。
	const hashScale = float64(uint64(1) << 53)
	shouldSample := float64(hash>>11)/hashScale < s.sampleRate

	// 更新统计信息
	if shouldSample {
		atomic.AddInt64(&s.sampledCount, 1)
		return types.SamplingResult{
			Decision: types.SamplingDecisionRecordAndSample,
		}
	} else {
		atomic.AddInt64(&s.droppedCount, 1)
		return types.SamplingResult{
			Decision: types.SamplingDecisionDrop,
		}
	}
}

// hashTraceID 计算TraceID的哈希值
// 使用SHA256哈希，确保分布均匀
func (s *DistributedSampler) hashTraceID(traceID string) uint64 {
	// 计算SHA256哈希
	hash := sha256.Sum256([]byte(traceID))
	return binary.BigEndian.Uint64(hash[:8])
}

// GetStats 获取统计信息
func (s *DistributedSampler) GetStats() map[string]int64 {
	return map[string]int64{
		"total_decisions": atomic.LoadInt64(&s.totalDecisions),
		"sampled_count":   atomic.LoadInt64(&s.sampledCount),
		"dropped_count":   atomic.LoadInt64(&s.droppedCount),
		"cache_size":      0,
	}
}

// getCacheSize 获取缓存大小
func (s *DistributedSampler) getCacheSize() int {
	return 0
}

// ClearCache 保留兼容入口；当前实现不再维护采样缓存。
func (s *DistributedSampler) ClearCache() {
}

// Description 返回采样器描述
func (s *DistributedSampler) Description() string {
	return fmt.Sprintf("DistributedSampler{sampleRate=%.2f}", s.sampleRate)
}
