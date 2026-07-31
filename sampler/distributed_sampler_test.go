package sampler

import (
	"fmt"
	"math"
	"testing"

	"github.com/ClownSketch/tracer/types"
)

// TestDistributedSampler_ShouldSample 测试采样决策
func TestDistributedSampler_ShouldSample(t *testing.T) {
	tests := []struct {
		name       string
		sampleRate float64
		traceID    string
		expect     types.SamplingDecision
	}{
		{
			name:       "采样率0.0，应该丢弃",
			sampleRate: 0.0,
			traceID:    "12345678901234567890123456789012",
			expect:     types.SamplingDecisionDrop,
		},
		{
			name:       "采样率1.0，应该采样",
			sampleRate: 1.0,
			traceID:    "12345678901234567890123456789012",
			expect:     types.SamplingDecisionRecordAndSample,
		},
		{
			name:       "采样率0.5，应该根据TraceID决定",
			sampleRate: 0.5,
			traceID:    "12345678901234567890123456789012",
			expect:     types.SamplingDecisionRecordAndSample, // 或Drop，取决于哈希值
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampler := NewDistributedSampler(tt.sampleRate).(*DistributedSampler)

			params := types.SamplingParameters{
				TraceID: tt.traceID,
				Name:    "test-span",
			}

			result := sampler.ShouldSample(params)

			// 验证决策类型
			if tt.sampleRate == 0.0 {
				if result.Decision != types.SamplingDecisionDrop {
					t.Errorf("期望丢弃，实际决策: %v", result.Decision)
				}
			} else if tt.sampleRate >= 1.0 {
				if result.Decision != types.SamplingDecisionRecordAndSample {
					t.Errorf("期望采样，实际决策: %v", result.Decision)
				}
			}
			// 对于0.0 < sampleRate < 1.0的情况，决策取决于哈希值，不做严格验证
		})
	}
}

// TestDistributedSampler_Consistency 测试同一TraceID的一致性
func TestDistributedSampler_Consistency(t *testing.T) {
	sampler := NewDistributedSampler(0.5).(*DistributedSampler)
	traceID := "12345678901234567890123456789012"

	params := types.SamplingParameters{
		TraceID: traceID,
		Name:    "test-span",
	}

	// 多次采样同一TraceID，应该得到相同的结果
	firstResult := sampler.ShouldSample(params)
	for i := 0; i < 100; i++ {
		result := sampler.ShouldSample(params)
		if result.Decision != firstResult.Decision {
			t.Errorf("第%d次采样结果不一致: 期望%v，实际%v", i+1, firstResult.Decision, result.Decision)
		}
	}
}

// TestDistributedSampler_NoUnboundedCache 验证采样器不会按 TraceID 持续占用内存。
func TestDistributedSampler_NoUnboundedCache(t *testing.T) {
	sampler := NewDistributedSampler(0.5).(*DistributedSampler)

	for i := 0; i < 10000; i++ {
		sampler.ShouldSample(types.SamplingParameters{
			TraceID: fmt.Sprintf("trace-%d", i),
			Name:    "test-span",
		})
	}
	if sampler.getCacheSize() != 0 {
		t.Fatalf("采样器不应保存 TraceID 缓存: %d", sampler.getCacheSize())
	}
}

// TestDistributedSampler_RateDistribution 验证中间采样率不会退化成 50% 或 100%。
func TestDistributedSampler_RateDistribution(t *testing.T) {
	const total = 100000
	for _, rate := range []float64{0.25, 0.5, 0.75} {
		sampler := NewDistributedSampler(rate).(*DistributedSampler)
		sampled := 0
		for i := 0; i < total; i++ {
			result := sampler.ShouldSample(types.SamplingParameters{TraceID: fmt.Sprintf("trace-%d", i)})
			if result.Decision == types.SamplingDecisionRecordAndSample {
				sampled++
			}
		}
		actual := float64(sampled) / total
		if math.Abs(actual-rate) > 0.015 {
			t.Fatalf("采样率偏差过大: 配置 %.2f，实际 %.4f", rate, actual)
		}
	}
}

// TestDistributedSampler_Stats 测试统计信息
func TestDistributedSampler_Stats(t *testing.T) {
	sampler := NewDistributedSampler(0.5).(*DistributedSampler)

	// 执行多次采样
	for i := 0; i < 100; i++ {
		params := types.SamplingParameters{
			TraceID: "12345678901234567890123456789012",
			Name:    "test-span",
		}
		sampler.ShouldSample(params)
	}

	stats := sampler.GetStats()

	// 验证统计信息
	if stats["total_decisions"] != 100 {
		t.Errorf("期望100次决策，实际%d次", stats["total_decisions"])
	}

	if stats["sampled_count"]+stats["dropped_count"] != 100 {
		t.Errorf("采样数+丢弃数应该等于总决策数: %d + %d != 100", stats["sampled_count"], stats["dropped_count"])
	}
}

// BenchmarkDistributedSampler_ShouldSample 基准测试：采样决策性能
func BenchmarkDistributedSampler_ShouldSample(b *testing.B) {
	sampler := NewDistributedSampler(0.5).(*DistributedSampler)

	params := types.SamplingParameters{
		TraceID: "12345678901234567890123456789012",
		Name:    "test-span",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sampler.ShouldSample(params)
	}
}

// BenchmarkDistributedSampler_ShouldSample_Parallel 基准测试：高并发采样决策性能
func BenchmarkDistributedSampler_ShouldSample_Parallel(b *testing.B) {
	sampler := NewDistributedSampler(0.5).(*DistributedSampler)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := 0
		for pb.Next() {
			params := types.SamplingParameters{
				TraceID: "12345678901234567890123456789012",
				Name:    "test-span",
			}
			sampler.ShouldSample(params)
			id++
		}
	})
}
