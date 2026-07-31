package processor

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
)

// newMockSpanSnapshot 创建新的 mock span snapshot（使用 mock 包）
func newMockSpanSnapshot(id int) trace.SpanSnapshot {
	return mock.NewSpanSnapshotMock(id)
}

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

// BenchmarkBatchProcessor_OnEnd 基准测试：OnEnd 方法性能
func BenchmarkBatchProcessor_OnEnd(b *testing.B) {
	exporter := newMockExporter()
	processor := NewBatchSpanProcessor(exporter,
		WithBatchSize(100),
		WithWorkers(5),
		WithFlushInterval(2*time.Second),
		WithQueueHighWaterMark(800),
	)
	defer processor.Shutdown(context.Background())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			span := mock.NewSpanSnapshotMock(id)
			processor.OnEnd(span)
			id++
		}
	})
}

var spanIDCounter int64

// BenchmarkBatchProcessor_OnEnd_HighConcurrency 基准测试：高并发场景下的 OnEnd 性能
func BenchmarkBatchProcessor_OnEnd_HighConcurrency(b *testing.B) {
	exporter := newMockExporter()
	processor := NewBatchSpanProcessor(exporter,
		WithBatchSize(500),
		WithWorkers(10),
		WithFlushInterval(1*time.Second),
		WithQueueHighWaterMark(8000),
	)
	defer processor.Shutdown(context.Background())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			span := mock.NewSpanSnapshotMock(id)
			processor.OnEnd(span)
			id++
		}
	})
}

// BenchmarkBatchProcessor_OnEnd_DifferentBatchSizes 基准测试：不同批次大小的影响
func BenchmarkBatchProcessor_OnEnd_DifferentBatchSizes(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			exporter := newMockExporter()
			processor := NewBatchSpanProcessor(exporter,
				WithBatchSize(batchSize),
				WithWorkers(5),
				WithFlushInterval(2*time.Second),
				WithQueueHighWaterMark(800),
			)
			defer processor.Shutdown(context.Background())

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(atomic.AddInt64(&spanIDCounter, 1))
				for pb.Next() {
					span := mock.NewSpanSnapshotMock(id)
					processor.OnEnd(span)
					id++
				}
			})
		})
	}
}

// BenchmarkBatchProcessor_OnEnd_DifferentWorkers 基准测试：不同工作协程数的影响
func BenchmarkBatchProcessor_OnEnd_DifferentWorkers(b *testing.B) {
	workers := []int{1, 2, 4, 8, 16, 32}

	for _, worker := range workers {
		b.Run(fmt.Sprintf("Workers_%d", worker), func(b *testing.B) {
			exporter := newMockExporter()
			processor := NewBatchSpanProcessor(exporter,
				WithBatchSize(100),
				WithWorkers(worker),
				WithFlushInterval(2*time.Second),
				WithQueueHighWaterMark(800),
			)
			defer processor.Shutdown(context.Background())

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(atomic.AddInt64(&spanIDCounter, 1))
				for pb.Next() {
					span := mock.NewSpanSnapshotMock(id)
					processor.OnEnd(span)
					id++
				}
			})
		})
	}
}

// BenchmarkBatchProcessor_OnEnd_DifferentQueueSizes 基准测试：不同队列大小的影响
func BenchmarkBatchProcessor_OnEnd_DifferentQueueSizes(b *testing.B) {
	queueSizes := []int{100, 500, 1000, 5000, 10000}

	for _, queueSize := range queueSizes {
		b.Run(fmt.Sprintf("QueueSize_%d", queueSize), func(b *testing.B) {
			exporter := newMockExporter()
			processor := NewBatchSpanProcessor(exporter,
				WithBatchSize(100),
				WithWorkers(5),
				WithFlushInterval(2*time.Second),
				WithQueueHighWaterMark(int(float64(queueSize)*0.8)),
			)
			defer processor.Shutdown(context.Background())

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(atomic.AddInt64(&spanIDCounter, 1))
				for pb.Next() {
					span := mock.NewSpanSnapshotMock(id)
					processor.OnEnd(span)
					id++
				}
			})
		})
	}
}

// BenchmarkBatchProcessor_OnEnd_StressTest 压力测试：持续高并发写入
func BenchmarkBatchProcessor_OnEnd_StressTest(b *testing.B) {
	exporter := newMockExporter()
	processor := NewBatchSpanProcessor(exporter,
		WithBatchSize(500),
		WithWorkers(10),
		WithFlushInterval(500*time.Millisecond),
		WithQueueHighWaterMark(8000),
	)
	defer processor.Shutdown(context.Background())

	// 预热
	for i := 0; i < 1000; i++ {
		processor.OnEnd(newMockSpanSnapshot(i))
	}
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			span := mock.NewSpanSnapshotMock(id)
			processor.OnEnd(span)
			id++
		}
	})
}

// TestBatchProcessor_PerformanceMetrics 性能指标测试：测试不同配置下的性能指标
func TestBatchProcessor_PerformanceMetrics(t *testing.T) {
	if os.Getenv("TRACER_STRESS_TESTS") != "1" {
		t.Skip("设置 TRACER_STRESS_TESTS=1 后执行 BatchProcessor 性能测试")
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
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			exporter := newMockExporter()
			batchProcessor := NewBatchSpanProcessor(exporter,
				WithBatchSize(cfg.batchSize),
				WithWorkers(cfg.workers),
				WithFlushInterval(cfg.flushInterval),
				WithQueueSize(cfg.queueSize),
				WithQueueHighWaterMark(int(float64(cfg.queueSize)*0.8)),
			).(*BatchSpanProcessor)
			defer batchProcessor.Shutdown(context.Background())

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
						queueLen := int64(len(batchProcessor.queue))
						atomic.AddInt64(&totalQueueLength, queueLen)
						atomic.AddInt64(&queueCheckCount, 1)

						for {
							currentMax := atomic.LoadInt64(&maxQueueLength)
							if queueLen <= currentMax {
								break
							}
							if atomic.CompareAndSwapInt64(&maxQueueLength, currentMax, queueLen) {
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
						span := newMockSpanSnapshot(spanID)
						batchProcessor.OnEnd(span)
						atomic.AddInt64(&processedCount, 1)
					}
				}(i)
			}

			// 等待所有 span 创建完成
			wg.Wait()

			// 等待处理完成 - 等待足够长的时间让所有 span 被处理
			maxWaitTime := 10 * time.Second
			checkInterval := 100 * time.Millisecond
			elapsed := time.Duration(0)

			for elapsed < maxWaitTime {
				time.Sleep(checkInterval)
				elapsed += checkInterval

				// 新模型下 batch 由聚合循环私有持有，这里只检查入口队列是否已经排空。
				queueLen := len(batchProcessor.queue)
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

			// 获取导出器统计（注意：由于 flush 中的导出被注释，这些值可能为 0）
			callCount, exportSpanCount, avgTime, minTime, maxTime := exporter.getStats()

			// 获取最终状态
			finalQueueLen := len(batchProcessor.queue)
			finalBatchLen := 0

			// 计算平均队列长度
			var avgQueueLength float64
			totalQueueLengthValue := atomic.LoadInt64(&totalQueueLength)
			queueCheckCountValue := atomic.LoadInt64(&queueCheckCount)
			maxQueueLengthValue := atomic.LoadInt64(&maxQueueLength)
			if queueCheckCountValue > 0 {
				avgQueueLength = float64(totalQueueLengthValue) / float64(queueCheckCountValue)
			}

			// 计算平均批大小（基于配置的 batchSize，因为导出被注释）
			avgBatchSize := float64(cfg.batchSize)

			// 计算 QPS（基于处理的 span 数）
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
			t.Logf("平均批大小: %.2f (基于配置)", avgBatchSize)
			t.Logf("平均导出时间: %v", time.Duration(avgTime))
			t.Logf("最小导出时间: %v", time.Duration(minTime))
			t.Logf("最大导出时间: %v", time.Duration(maxTime))
			t.Logf("最大队列长度: %d", maxQueueLengthValue)
			t.Logf("平均队列长度: %.2f", avgQueueLength)
			t.Logf("最终队列长度: %d", finalQueueLen)
			t.Logf("最终批次长度: %d", finalBatchLen)
			t.Logf("内存使用: %d KB", memoryUsage/1024)
			t.Logf("")

			if exportSpanCount != int64(cfg.spanCount) {
				t.Fatalf("链路导出不完整: 期望 %d，实际 %d", cfg.spanCount, exportSpanCount)
			}
		})
	}
}

// TestBatchProcessor_ConcurrentStress 并发压力测试：测试极端并发场景
func TestBatchProcessor_ConcurrentStress(t *testing.T) {
	if os.Getenv("TRACER_STRESS_TESTS") != "1" {
		t.Skip("设置 TRACER_STRESS_TESTS=1 后执行 BatchProcessor 极端并发测试")
	}

	exporter := newMockExporter()
	batchProcessor := NewBatchSpanProcessor(exporter,
		WithBatchSize(1000),
		WithWorkers(20),
		WithFlushInterval(500*time.Millisecond),
		WithQueueHighWaterMark(8000),
	).(*BatchSpanProcessor)
	defer batchProcessor.Shutdown(context.Background())

	concurrency := 5000
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
				span := newMockSpanSnapshot(spanID)
				batchProcessor.OnEnd(span)
				atomic.AddInt64(&processedCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// 等待处理完成 - 等待足够长的时间让所有 span 被处理
	// 由于 flush 中的导出被注释了，我们需要等待足够长的时间让 worker 处理完所有 span
	maxWaitTime := 10 * time.Second
	checkInterval := 100 * time.Millisecond
	elapsed := time.Duration(0)

	for elapsed < maxWaitTime {
		time.Sleep(checkInterval)
		elapsed += checkInterval

		// 新模型下 batch 由聚合循环私有持有，这里只检查入口队列是否已经排空。
		queueLen := len(batchProcessor.queue)
		if queueLen == 0 {
			// 再等待一个刷新间隔，确保所有异步操作完成
			time.Sleep(600 * time.Millisecond)
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

	// 获取最终状态
	finalQueueLen := len(batchProcessor.queue)
	finalBatchLen := 0

	// 计算实际处理的 span 数（已从队列中取出并放入 batch 的）
	// 由于导出被注释，我们通过统计已处理的 span 数来验证
	actualProcessed := processedCount - int64(finalQueueLen)

	qps := float64(processedCount) / duration.Seconds()

	t.Logf("=== 并发压力测试结果 ===")
	t.Logf("并发数: %d", concurrency)
	t.Logf("每个协程 Span 数: %d", spansPerGoroutine)
	t.Logf("总 Span 数: %d (期望: %d)", processedCount, totalSpans)
	t.Logf("测试时长: %v", duration)
	t.Logf("处理 QPS: %.2f", qps)
	t.Logf("导出调用次数: %d", callCount)
	t.Logf("导出 Span 数: %d", exportSpanCount)
	t.Logf("最终队列长度: %d", finalQueueLen)
	t.Logf("最终批次长度: %d", finalBatchLen)
	t.Logf("实际处理数: %d (已从队列取出)", actualProcessed)

	// 验证所有 span 都被放入队列（由于导出被注释，我们只验证处理流程）
	if processedCount != totalSpans {
		t.Errorf("期望处理 %d 个 span，实际处理 %d 个", totalSpans, processedCount)
	}
	if exportSpanCount != totalSpans {
		t.Errorf("期望导出 %d 个 span，实际导出 %d 个", totalSpans, exportSpanCount)
	}

	// 验证队列和 batch 都已清空（说明所有 span 都已被 worker 处理）
	if finalQueueLen > 0 || finalBatchLen > 0 {
		t.Logf("警告: 队列或批次中仍有未处理的 span (队列: %d, 批次: %d)", finalQueueLen, finalBatchLen)
	}
}
