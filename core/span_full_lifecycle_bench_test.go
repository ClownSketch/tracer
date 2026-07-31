package core

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tracerpkg "github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
	"github.com/ClownSketch/tracer/types/operation"
)

// ==================== Mock Exporter ====================

// mockExporter 用于基准测试的简单导出器实现
// 不实际导出数据，只计数，避免 I/O 影响性能测试
type mockExporter struct {
	exportCallCount int64 // 导出调用次数
	exportSpanCount int64 // 导出的 span 总数
	totalExportTime int64 // 总导出耗时（纳秒）
	minExportTime   int64 // 最小导出耗时（纳秒）
	maxExportTime   int64 // 最大导出耗时（纳秒）
}

func newMockExporter() *mockExporter {
	return &mockExporter{
		minExportTime: int64(^uint64(0) >> 1), // 初始化为最大值
	}
}

func (m *mockExporter) ExportSpan(span trace.SpanSnapshot) error {
	return m.ExportSpans([]trace.SpanSnapshot{span})
}

func (m *mockExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	start := time.Now()

	// 模拟极小的处理时间（避免影响测试）
	time.Sleep(1 * time.Microsecond)

	duration := time.Since(start).Nanoseconds()

	atomic.AddInt64(&m.exportCallCount, 1)
	atomic.AddInt64(&m.exportSpanCount, int64(len(spans)))
	atomic.AddInt64(&m.totalExportTime, duration)

	// 更新最小和最大耗时
	for {
		min := atomic.LoadInt64(&m.minExportTime)
		if duration < min {
			if atomic.CompareAndSwapInt64(&m.minExportTime, min, duration) {
				break
			}
		} else {
			break
		}
	}

	for {
		max := atomic.LoadInt64(&m.maxExportTime)
		if duration > max {
			if atomic.CompareAndSwapInt64(&m.maxExportTime, max, duration) {
				break
			}
		} else {
			break
		}
	}

	return nil
}

func (m *mockExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockExporter) getStats() (callCount, spanCount, avgTime, minTime, maxTime int64) {
	callCount = atomic.LoadInt64(&m.exportCallCount)
	spanCount = atomic.LoadInt64(&m.exportSpanCount)
	totalTime := atomic.LoadInt64(&m.totalExportTime)
	minTime = atomic.LoadInt64(&m.minExportTime)
	maxTime = atomic.LoadInt64(&m.maxExportTime)

	if callCount > 0 {
		avgTime = totalTime / callCount
	}

	return
}

// ==================== Helper Functions ====================

// createFullLifecycleSpan 创建一个完整的 Span 生命周期，模拟真实使用场景
func createFullLifecycleSpan(tracer trace.Tracer, spanName string) {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, spanName,
		tracerpkg.WithSpanKind(types.SpanKindInternal),
		tracerpkg.WithForceRecord(),
	)

	// 设置属性
	span.SetAttributes(
		attribute.String("service.name", "test-service"),
		attribute.String("service.version", "1.0.0"),
		attribute.Int("user.id", 12345),
		attribute.Bool("is.active", true),
		attribute.Float64("response.time", 0.123),
	)

	// 设置全局属性
	span.SetGlobalAttributes(
		attribute.String("environment", "production"),
		attribute.String("region", "us-east-1"),
	)

	// 设置继承属性
	span.SetInheritedAttributes(
		attribute.String("request.id", "req-12345"),
		attribute.String("session.id", "session-67890"),
	)

	// 添加事件 - SQL 操作
	sqlInfo := &operation.SQLOperationInfo{
		Table:       "users",
		Operation:   "SELECT",
		Rows:        10,
		SQL:         "SELECT * FROM users WHERE id = ?",
		CostSeconds: 0.005,
		Success:     true,
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	span.AddEvent("sql.operation", "sql", func() map[string]any {
		return map[string]any{
			"table":        sqlInfo.Table,
			"operation":    sqlInfo.Operation,
			"rows":         sqlInfo.Rows,
			"sql":          sqlInfo.SQL,
			"cost_seconds": sqlInfo.CostSeconds,
			"success":      sqlInfo.Success,
			"timestamp":    sqlInfo.Timestamp,
		}
	})

	// 添加事件 - Redis 操作
	redisInfo := &operation.RedisOperationInfo{
		IndexDb:     "0",
		Operation:   "GET",
		Key:         "user:12345",
		CostSeconds: 0.001,
		Success:     true,
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	span.AddEvent("redis.operation", "redis", func() map[string]any {
		return map[string]any{
			"index_db":     redisInfo.IndexDb,
			"operation":    redisInfo.Operation,
			"key":          redisInfo.Key,
			"cost_seconds": redisInfo.CostSeconds,
			"success":      redisInfo.Success,
			"timestamp":    redisInfo.Timestamp,
		}
	})

	// 添加日志
	span.AddLog(types.SpanLog{
		Timestamp: time.Now().Format(time.RFC3339),
		Severity:  types.SpanLogSeverityInfo,
		Message:   "Processing request",
		Fields: map[string]any{
			"step": "validation",
		},
	})

	span.AddLog(types.SpanLog{
		Timestamp: time.Now().Format(time.RFC3339),
		Severity:  types.SpanLogSeverityInfo,
		Message:   "Request completed",
		Fields: map[string]any{
			"step": "completion",
		},
	})

	// 模拟一些处理时间
	time.Sleep(1 * time.Microsecond)

	// 结束 Span
	span.End()

	_ = ctx // 避免未使用变量警告
}

// ==================== Benchmark Tests ====================

// BenchmarkSpan_FullLifecycle 基准测试：完整 Span 生命周期（单个）
func BenchmarkSpan_FullLifecycle(b *testing.B) {
	exporter := newMockExporter()
	batchProcessor := processor.NewBatchSpanProcessor(exporter,
		processor.WithBatchSize(100),
		processor.WithWorkers(5),
		processor.WithFlushInterval(2*time.Second),
		processor.WithQueueSize(1000),
	)
	defer batchProcessor.Shutdown(context.Background())

	tracer := NewTracerImpl("test", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		createFullLifecycleSpan(tracer, fmt.Sprintf("span-%d", i))
	}
}

// BenchmarkSpan_FullLifecycle_Parallel 基准测试：完整 Span 生命周期（高并发）
func BenchmarkSpan_FullLifecycle_Parallel(b *testing.B) {
	exporter := newMockExporter()
	batchProcessor := processor.NewBatchSpanProcessor(exporter,
		processor.WithBatchSize(500),
		processor.WithWorkers(10),
		processor.WithFlushInterval(1*time.Second),
		processor.WithQueueSize(5000),
	)
	defer batchProcessor.Shutdown(context.Background())

	tracer := NewTracerImpl("test", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			createFullLifecycleSpan(tracer, fmt.Sprintf("span-%d", id))
			id++
		}
	})
}

var spanIDCounter int64

// BenchmarkSpan_FullLifecycle_HighConcurrency 基准测试：极端高并发场景
func BenchmarkSpan_FullLifecycle_HighConcurrency(b *testing.B) {
	exporter := newMockExporter()
	batchProcessor := processor.NewBatchSpanProcessor(exporter,
		processor.WithBatchSize(1000),
		processor.WithWorkers(20),
		processor.WithFlushInterval(500*time.Millisecond),
		processor.WithQueueSize(10000),
	)
	defer batchProcessor.Shutdown(context.Background())

	tracer := NewTracerImpl("test", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			createFullLifecycleSpan(tracer, fmt.Sprintf("span-%d", id))
			id++
		}
	})
}

// BenchmarkSpan_CreationOnly 基准测试：仅测试 Span 创建（不包含结束）
func BenchmarkSpan_CreationOnly(b *testing.B) {
	tracer := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx, span := tracer.Start(ctx, fmt.Sprintf("span-%d", i),
			tracerpkg.WithSpanKind(types.SpanKindInternal),
			tracerpkg.WithForceRecord(),
		)
		_ = ctx
		_ = span
	}
}

// BenchmarkSpan_EndOnly 基准测试：仅测试 Span 结束（包含快照创建）
func BenchmarkSpan_EndOnly(b *testing.B) {
	exporter := newMockExporter()
	batchProcessor := processor.NewBatchSpanProcessor(exporter,
		processor.WithBatchSize(100),
		processor.WithWorkers(5),
		processor.WithFlushInterval(2*time.Second),
	)
	defer batchProcessor.Shutdown(context.Background())

	tracer := NewTracerImpl("test", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

	// 预先创建所有 Span
	spans := make([]trace.Span, b.N)
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx, span := tracer.Start(ctx, fmt.Sprintf("span-%d", i),
			tracerpkg.WithSpanKind(types.SpanKindInternal),
			tracerpkg.WithForceRecord(),
		)
		spans[i] = span
		_ = ctx
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		spans[i].End()
	}
}

// BenchmarkSpan_AttributeOperations 基准测试：属性操作性能
func BenchmarkSpan_AttributeOperations(b *testing.B) {
	tracer := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx, span := tracer.Start(ctx, "test-span",
			tracerpkg.WithSpanKind(types.SpanKindInternal),
			tracerpkg.WithForceRecord(),
		)

		// 设置多个属性
		for j := 0; j < 10; j++ {
			span.SetAttributes(
				attribute.String(fmt.Sprintf("key-%d", j), fmt.Sprintf("value-%d", j)),
				attribute.Int(fmt.Sprintf("int-%d", j), j),
			)
		}

		span.End()
		_ = ctx
	}
}

// BenchmarkSpan_EventOperations 基准测试：事件操作性能
func BenchmarkSpan_EventOperations(b *testing.B) {
	tracer := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx, span := tracer.Start(ctx, "test-span",
			tracerpkg.WithSpanKind(types.SpanKindInternal),
			tracerpkg.WithForceRecord(),
		)

		// 添加多个事件
		for j := 0; j < 5; j++ {
			span.AddEvent(fmt.Sprintf("event-%d", j), "test", func() map[string]any {
				return map[string]any{
					"id":    j,
					"value": fmt.Sprintf("event-value-%d", j),
				}
			})
		}

		span.End()
		_ = ctx
	}
}

// BenchmarkSpan_LogOperations 基准测试：日志操作性能
func BenchmarkSpan_LogOperations(b *testing.B) {
	tracer := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx, span := tracer.Start(ctx, "test-span",
			tracerpkg.WithSpanKind(types.SpanKindInternal),
			tracerpkg.WithForceRecord(),
		)

		// 添加多个日志
		for j := 0; j < 5; j++ {
			span.AddLog(types.SpanLog{
				Timestamp: time.Now().Format(time.RFC3339),
				Severity:  types.SpanLogSeverityInfo,
				Message:   fmt.Sprintf("Log message %d", j),
				Fields: map[string]any{
					"log_id": j,
				},
			})
		}

		span.End()
		_ = ctx
	}
}

// BenchmarkSpan_SnapshotCreation 基准测试：快照创建性能
func BenchmarkSpan_SnapshotCreation(b *testing.B) {
	tracer := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	// 预先创建所有 Span 并填充数据
	spans := make([]trace.Span, b.N)
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx, span := tracer.Start(ctx, fmt.Sprintf("span-%d", i),
			tracerpkg.WithSpanKind(types.SpanKindInternal),
			tracerpkg.WithForceRecord(),
		)

		// 填充数据
		span.SetAttributes(
			attribute.String("key1", "value1"),
			attribute.Int("key2", 123),
		)
		span.AddEvent("event1", "test", func() map[string]any {
			return map[string]any{"data": "test"}
		})
		span.AddLog(types.SpanLog{
			Timestamp: time.Now().Format(time.RFC3339),
			Severity:  types.SpanLogSeverityInfo,
			Message:   "test log",
		})

		spans[i] = span
		_ = ctx
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 获取快照（会触发快照创建）
		spans[i].End()
		snapshot := spans[i].GetSnapshot()
		_ = snapshot
	}
}

// ==================== Performance Test ====================

// TestSpan_FullLifecycle_Performance 性能测试：完整 Span 生命周期在不同配置下的性能
func TestSpan_FullLifecycle_Performance(t *testing.T) {
	if os.Getenv("TRACER_STRESS_TESTS") != "1" {
		t.Skip("设置 TRACER_STRESS_TESTS=1 后执行完整 Span 压力测试")
	}

	configs := []struct {
		name          string
		batchSize     int
		workers       int
		queueSize     int
		flushInterval time.Duration
		concurrency   int
		spanCount     int
	}{
		{
			name:          "低并发-小批次",
			batchSize:     50,
			workers:       2,
			queueSize:     500,
			flushInterval: 1 * time.Second,
			concurrency:   10,
			spanCount:     10000,
		},
		{
			name:          "中等并发-中批次",
			batchSize:     100,
			workers:       5,
			queueSize:     1000,
			flushInterval: 2 * time.Second,
			concurrency:   100,
			spanCount:     100000,
		},
		{
			name:          "高并发-大批次",
			batchSize:     500,
			workers:       10,
			queueSize:     5000,
			flushInterval: 1 * time.Second,
			concurrency:   1000,
			spanCount:     1000000,
		},
		{
			name:          "极端高并发",
			batchSize:     1000,
			workers:       20,
			queueSize:     10000,
			flushInterval: 500 * time.Millisecond,
			concurrency:   5000,
			spanCount:     5000000,
		},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			exporter := newMockExporter()
			batchProcessor := processor.NewBatchSpanProcessor(exporter,
				processor.WithBatchSize(cfg.batchSize),
				processor.WithWorkers(cfg.workers),
				processor.WithFlushInterval(cfg.flushInterval),
				processor.WithQueueSize(cfg.queueSize),
			).(*processor.BatchSpanProcessor)
			defer batchProcessor.Shutdown(context.Background())

			tracer := NewTracerImpl("test", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

			// 记录开始时间和内存
			startTime := time.Now()
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			// 队列长度监控
			var maxQueueLength int64
			var totalQueueLength int64
			var queueCheckCount int64

			// 启动监控 goroutine
			ctx, cancel := context.WithCancel(context.Background())
			var monitorWg sync.WaitGroup

			monitorWg.Add(1)
			go func() {
				defer monitorWg.Done()
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						queueLen := int64(batchProcessor.GetQueueLength())
						atomic.AddInt64(&totalQueueLength, queueLen)
						atomic.AddInt64(&queueCheckCount, 1)
						for {
							current := atomic.LoadInt64(&maxQueueLength)
							if queueLen <= current || atomic.CompareAndSwapInt64(&maxQueueLength, current, queueLen) {
								break
							}
						}
					}
				}
			}()

			// 并发创建 span
			var wg sync.WaitGroup
			var processedCount int64
			spansPerGoroutine := cfg.spanCount / cfg.concurrency
			remainingSpans := cfg.spanCount % cfg.concurrency

			for i := 0; i < cfg.concurrency; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					spansToCreate := spansPerGoroutine
					if goroutineID < remainingSpans {
						spansToCreate++
					}

					for j := 0; j < spansToCreate; j++ {
						spanID := goroutineID*spansPerGoroutine + j
						createFullLifecycleSpan(tracer, fmt.Sprintf("span-%d", spanID))
						atomic.AddInt64(&processedCount, 1)
					}
				}(i)
			}

			// 等待所有 span 创建完成
			wg.Wait()

			// 等待处理完成
			maxWaitTime := 30 * time.Second
			checkInterval := 100 * time.Millisecond
			elapsed := time.Duration(0)

			for elapsed < maxWaitTime {
				time.Sleep(checkInterval)
				elapsed += checkInterval

				// 检查队列和 batch 是否都为空
				queueLen := batchProcessor.GetQueueLength()
				if queueLen == 0 {
					// 再等待一个刷新间隔，确保所有异步操作完成
					time.Sleep(cfg.flushInterval + 200*time.Millisecond)
					break
				}
			}

			// 关闭处理器
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()

			if err := batchProcessor.Shutdown(shutdownCtx); err != nil {
				t.Logf("关闭处理器警告: %v", err)
			}
			cancel()
			monitorWg.Wait()

			// 记录结束时间和内存
			endTime := time.Now()
			duration := endTime.Sub(startTime)
			runtime.ReadMemStats(&m2)

			// 获取导出器统计
			callCount, exportSpanCount, avgTime, minTime, maxTime := exporter.getStats()

			// 获取最终状态
			finalQueueLen := batchProcessor.GetQueueLength()

			// 计算平均队列长度
			var avgQueueLength float64
			totalQueueLengthValue := atomic.LoadInt64(&totalQueueLength)
			queueCheckCountValue := atomic.LoadInt64(&queueCheckCount)
			maxQueueLengthValue := atomic.LoadInt64(&maxQueueLength)
			if queueCheckCountValue > 0 {
				avgQueueLength = float64(totalQueueLengthValue) / float64(queueCheckCountValue)
			}

			// 计算 QPS
			qps := float64(processedCount) / duration.Seconds()

			// 计算内存使用
			memoryUsage := int64(m2.Alloc) - int64(m1.Alloc)

			// 输出性能指标
			t.Logf("=== 性能测试结果: %s ===", cfg.name)
			t.Logf("配置: BatchSize=%d, Workers=%d, QueueSize=%d, FlushInterval=%v",
				cfg.batchSize, cfg.workers, cfg.queueSize, cfg.flushInterval)
			t.Logf("并发数: %d", cfg.concurrency)
			t.Logf("总 Span 数: %d (期望: %d)", processedCount, cfg.spanCount)
			t.Logf("测试时长: %v", duration)
			t.Logf("处理 QPS: %.2f", qps)
			t.Logf("导出调用次数: %d", callCount)
			t.Logf("导出 Span 数: %d", exportSpanCount)
			t.Logf("平均导出时间: %v", time.Duration(avgTime))
			t.Logf("最小导出时间: %v", time.Duration(minTime))
			t.Logf("最大导出时间: %v", time.Duration(maxTime))
			t.Logf("最大队列长度: %d", maxQueueLengthValue)
			t.Logf("平均队列长度: %.2f", avgQueueLength)
			t.Logf("最终队列长度: %d", finalQueueLen)
			t.Logf("内存使用: %d KB (%.2f MB)", memoryUsage/1024, float64(memoryUsage)/(1024*1024))
			t.Logf("GC 次数: %d", m2.NumGC-m1.NumGC)
			t.Logf("")

			if exportSpanCount != int64(cfg.spanCount) {
				t.Fatalf("链路导出不完整: 期望 %d，实际 %d", cfg.spanCount, exportSpanCount)
			}
		})
	}
}

// TestSpan_FullLifecycle_StressTest 压力测试：极端高并发场景
func TestSpan_FullLifecycle_StressTest(t *testing.T) {
	if os.Getenv("TRACER_STRESS_TESTS") != "1" {
		t.Skip("设置 TRACER_STRESS_TESTS=1 后执行极端 Span 压力测试")
	}

	exporter := newMockExporter()
	batchProcessor := processor.NewBatchSpanProcessor(exporter,
		processor.WithBatchSize(1000),
		processor.WithWorkers(20),
		processor.WithFlushInterval(500*time.Millisecond),
		processor.WithQueueSize(10000),
	).(*processor.BatchSpanProcessor)
	defer batchProcessor.Shutdown(context.Background())

	tracer := NewTracerImpl("test", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

	concurrency := 10000
	spansPerGoroutine := 100
	totalSpans := int64(concurrency * spansPerGoroutine)

	startTime := time.Now()

	var wg sync.WaitGroup
	var processedCount int64

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < spansPerGoroutine; j++ {
				spanID := goroutineID*spansPerGoroutine + j
				createFullLifecycleSpan(tracer, fmt.Sprintf("span-%d", spanID))
				atomic.AddInt64(&processedCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// 等待处理完成
	maxWaitTime := 60 * time.Second
	checkInterval := 200 * time.Millisecond
	elapsed := time.Duration(0)

	for elapsed < maxWaitTime {
		time.Sleep(checkInterval)
		elapsed += checkInterval

		queueLen := batchProcessor.GetQueueLength()
		if queueLen == 0 {
			time.Sleep(1 * time.Second)
			break
		}
	}

	// 关闭处理器
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := batchProcessor.Shutdown(shutdownCtx); err != nil {
		t.Logf("关闭处理器警告: %v", err)
	}

	duration := time.Since(startTime)
	callCount, exportSpanCount, _, _, _ := exporter.getStats()

	finalQueueLen := batchProcessor.GetQueueLength()

	qps := float64(processedCount) / duration.Seconds()

	t.Logf("=== 压力测试结果 ===")
	t.Logf("并发数: %d", concurrency)
	t.Logf("每个协程 Span 数: %d", spansPerGoroutine)
	t.Logf("总 Span 数: %d (期望: %d)", processedCount, totalSpans)
	t.Logf("测试时长: %v", duration)
	t.Logf("处理 QPS: %.2f", qps)
	t.Logf("导出调用次数: %d", callCount)
	t.Logf("导出 Span 数: %d", exportSpanCount)
	t.Logf("最终队列长度: %d", finalQueueLen)

	// 验证所有 span 都被处理
	if processedCount != totalSpans {
		t.Errorf("期望处理 %d 个 span，实际处理 %d 个", totalSpans, processedCount)
	}
	if exportSpanCount != totalSpans {
		t.Errorf("期望导出 %d 个 span，实际导出 %d 个", totalSpans, exportSpanCount)
	}

	// 验证队列已清空
	if finalQueueLen > 0 {
		t.Logf("警告: 队列中仍有 %d 个未处理的 span", finalQueueLen)
	}
}
