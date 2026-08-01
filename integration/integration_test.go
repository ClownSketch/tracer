package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tracerpkg "github.com/ClownSketch/tracer"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/providers"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// mustNewBatchSpanProcessor 创建测试使用的批处理器，初始化失败时立即终止当前测试。
func mustNewBatchSpanProcessor(tb testing.TB, exporter trace.SpanExporter, opts ...processor.BatchSpanProcessorOption) *processor.BatchSpanProcessor {
	tb.Helper()

	spanProcessor, err := processor.NewBatchSpanProcessor(exporter, opts...)
	if err != nil {
		tb.Fatalf("创建批处理器失败: %v", err)
	}

	batchProcessor, ok := spanProcessor.(*processor.BatchSpanProcessor)
	if !ok {
		tb.Fatalf("批处理器类型错误: %T", spanProcessor)
	}

	return batchProcessor
}

// TestIntegration_FullLifecycle 集成测试：完整生命周期
func TestIntegration_FullLifecycle(t *testing.T) {
	// 创建mock导出器
	exporter := newMockExporter()

	// 创建批处理器
	batchProcessor := mustNewBatchSpanProcessor(t, exporter,
		processor.WithBatchSize(100),
		processor.WithWorkers(5),
		processor.WithFlushInterval(2*time.Second),
		processor.WithQueueSize(1000),
	)
	defer batchProcessor.Shutdown(context.Background())

	// 创建追踪器提供者
	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(batchProcessor),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)
	defer provider.Shutdown(context.Background())

	// 获取追踪器
	tr := provider.GetTracer("test-service")

	// 创建Span
	_, span := tr.Start(context.Background(), "test-operation",
		tracerpkg.WithSpanKind(types.SpanKindServer),
		tracerpkg.WithForceRecord(),
	)

	// 设置属性
	span.SetAttributes(
		attribute.String("key1", "value1"),
		attribute.Int("key2", 123),
		attribute.Bool("key3", true),
	)

	// 添加事件
	span.AddEvent("event1", "test", func() map[string]any {
		return map[string]any{
			"data": "test",
		}
	})

	// 添加日志
	span.AddLog(types.SpanLog{
		Timestamp: time.Now().Format(time.RFC3339),
		Severity:  types.SpanLogSeverityInfo,
		Message:   "test log",
	})

	// 结束Span
	span.End()

	// 等待处理完成（增加等待时间，确保批处理器有时间处理）
	time.Sleep(3 * time.Second)

	// 验证导出器统计
	stats := exporter.getStats()
	if stats.exportSpanCount == 0 {
		t.Errorf("期望导出至少1个span，实际导出%d个", stats.exportSpanCount)
	}
}

// TestIntegration_ConcurrentSpans 集成测试：并发Span创建
func TestIntegration_ConcurrentSpans(t *testing.T) {
	// 创建mock导出器
	exporter := newMockExporter()

	// 创建批处理器
	batchProcessor := mustNewBatchSpanProcessor(t, exporter,
		processor.WithBatchSize(100),
		processor.WithWorkers(10),
		processor.WithFlushInterval(1*time.Second),
		processor.WithQueueSize(5000),
	)
	defer batchProcessor.Shutdown(context.Background())

	// 创建追踪器提供者
	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(batchProcessor),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)
	defer provider.Shutdown(context.Background())

	// 获取追踪器
	tr := provider.GetTracer("test-service")

	// 并发创建Span
	concurrency := 100
	spansPerGoroutine := 10
	totalSpans := concurrency * spansPerGoroutine

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < spansPerGoroutine; j++ {
				_, span := tr.Start(context.Background(), "test-operation",
					tracerpkg.WithSpanKind(types.SpanKindServer),
					tracerpkg.WithForceRecord(),
				)
				span.SetAttributes(attribute.String("goroutine_id", fmt.Sprintf("%d", goroutineID)))
				span.End()
			}
		}(i)
	}

	wg.Wait()

	// 等待处理完成
	time.Sleep(2 * time.Second)

	// 验证导出器统计
	stats := exporter.getStats()
	if stats.exportSpanCount < int64(totalSpans*9/10) { // 允许10%的误差
		t.Errorf("期望导出至少%d个span，实际导出%d个", totalSpans*9/10, stats.exportSpanCount)
	}
}

// mockExporter 用于集成测试的mock导出器
type mockExporter struct {
	exportCallCount int64
	exportSpanCount int64
}

func newMockExporter() *mockExporter {
	return &mockExporter{}
}

func (m *mockExporter) ExportSpan(span trace.SpanSnapshot) error {
	return m.ExportSpans([]trace.SpanSnapshot{span})
}

func (m *mockExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	atomic.AddInt64(&m.exportCallCount, 1)
	atomic.AddInt64(&m.exportSpanCount, int64(len(spans)))
	return nil
}

func (m *mockExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockExporter) getStats() struct {
	exportCallCount int64
	exportSpanCount int64
} {
	return struct {
		exportCallCount int64
		exportSpanCount int64
	}{
		exportCallCount: atomic.LoadInt64(&m.exportCallCount),
		exportSpanCount: atomic.LoadInt64(&m.exportSpanCount),
	}
}
