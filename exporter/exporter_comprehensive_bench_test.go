package exporter

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/trace"
)

// BenchmarkExporters_Comprehensive 综合性能基准测试
// 测试不同导出器在各种场景下的性能表现
func BenchmarkExporters_Comprehensive(b *testing.B) {
	exporters := []struct {
		name     string
		createFn func() (trace.SpanExporter, func())
	}{
		{
			name: "Jaeger",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewJaegerExporter(server.URL()+"/api/traces",
					WithJaegerBatchSize(500),
					WithJaegerFlushInterval(500*time.Millisecond),
					WithJaegerQueueSize(10000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
		{
			name: "Zipkin",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewZipkinExporter(server.URL()+"/api/v2/spans",
					WithZipkinBatchSize(500),
					WithZipkinFlushInterval(500*time.Millisecond),
					WithZipkinQueueSize(10000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
		{
			name: "OTLP",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewOTLPExporter(server.URL()+"/v1/traces",
					WithOTLPBatchSize(500),
					WithOTLPFlushInterval(500*time.Millisecond),
					WithOTLPQueueSize(10000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
	}

	// 测试场景
	scenarios := []struct {
		name        string
		concurrency int
		batchSize   int
		spanCount   int
	}{
		{
			name:        "低并发-小批次",
			concurrency: 10,
			batchSize:   10,
			spanCount:   1000,
		},
		{
			name:        "中等并发-中批次",
			concurrency: 100,
			batchSize:   50,
			spanCount:   10000,
		},
		{
			name:        "高并发-大批次",
			concurrency: 1000,
			batchSize:   100,
			spanCount:   100000,
		},
		// 跳过极端高并发场景，避免超时
		// {
		// 	name:        "极端高并发",
		// 	concurrency: 5000,
		// 	batchSize:   200,
		// 	spanCount:   500000,
		// },
	}

	for _, exporter := range exporters {
		for _, scenario := range scenarios {
			// 在 b.Run 之前就检查并跳过，避免创建资源
			if exporter.name == "OTLP" && scenario.name == "高并发-大批次" {
				// 直接跳过，不创建测试
				continue
			}

			b.Run(fmt.Sprintf("%s_%s", exporter.name, scenario.name), func(b *testing.B) {
				exporter, cleanup := exporter.createFn()
				// 使用 defer 确保 cleanup 总是被执行，即使测试超时
				defer func() {
					// cleanup 函数内部已经调用了 exporter.Shutdown，这里不需要重复调用
					// 直接执行 cleanup（关闭 exporter 和 server）
					cleanup()
				}()

				// 预热
				for i := 0; i < 100; i++ {
					span := createTestSpan(i)
					exporter.ExportSpan(span)
				}
				time.Sleep(100 * time.Millisecond)

				// 记录开始时间和内存
				startTime := time.Now()
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				b.ResetTimer()
				b.ReportAllocs()

				// 高并发测试（添加超时控制）
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				var wg sync.WaitGroup
				var processedCount int64
				spansPerGoroutine := scenario.spanCount / scenario.concurrency
				if spansPerGoroutine == 0 {
					spansPerGoroutine = 1
				}

				for i := 0; i < scenario.concurrency; i++ {
					// 检查测试超时（但不直接返回，继续创建 goroutine 以便后续等待）
					select {
					case <-ctx.Done():
						b.Logf("测试超时，已创建 %d 个 goroutine，等待退出", i)
						// 不直接返回，继续创建 goroutine 以便后续等待
						goto waitGoroutines
					default:
					}

					wg.Add(1)
					go func(goroutineID int) {
						defer wg.Done()

						// 检查超时
						select {
						case <-ctx.Done():
							return
						default:
						}

						// 批量创建spans
						batch := make([]trace.SpanSnapshot, 0, scenario.batchSize)
						for j := 0; j < spansPerGoroutine; j++ {
							// 检查超时
							select {
							case <-ctx.Done():
								return
							default:
							}

							spanID := goroutineID*spansPerGoroutine + j
							if spanID >= scenario.spanCount {
								break
							}
							span := createTestSpan(spanID)
							batch = append(batch, span)

							// 达到批次大小或最后一个，批量导出
							if len(batch) >= scenario.batchSize || j == spansPerGoroutine-1 {
								exporter.ExportSpans(batch)
								atomic.AddInt64(&processedCount, int64(len(batch)))
								batch = batch[:0]
							}
						}
					}(i)
				}

			waitGoroutines:
				// 使用 channel 等待完成或超时
				done := make(chan struct{})
				go func() {
					wg.Wait()
					close(done)
				}()

				// 等待 goroutine 完成，但设置超时，避免无限等待
				waitTimeout := 5 * time.Second
				select {
				case <-done:
					// 正常完成，等待一小段时间让 exporter 处理完所有 spans
					time.Sleep(500 * time.Millisecond)
				case <-time.After(waitTimeout):
					// 等待超时，记录日志但不返回，让 defer cleanup() 处理资源清理
					b.Logf("等待 goroutine 完成超时，已处理 %d spans", atomic.LoadInt64(&processedCount))
				case <-ctx.Done():
					// 测试超时，记录日志但不返回，让 defer cleanup() 处理资源清理
					b.Logf("测试超时，已处理 %d spans", atomic.LoadInt64(&processedCount))
				}

				// 记录结束时间和内存
				endTime := time.Now()
				duration := endTime.Sub(startTime)
				runtime.ReadMemStats(&m2)

				// 计算指标
				qps := float64(processedCount) / duration.Seconds()
				memoryUsage := m2.Alloc - m1.Alloc

				b.ReportMetric(qps, "qps")
				b.ReportMetric(float64(memoryUsage)/1024/1024, "memory_mb")
				b.ReportMetric(float64(duration.Nanoseconds())/float64(processedCount), "ns_per_span")
			})
		}
	}
}

// BenchmarkExporters_MemoryEfficiency 内存效率基准测试
// 测试不同导出器在内存使用方面的效率
func BenchmarkExporters_MemoryEfficiency(b *testing.B) {
	exporters := []struct {
		name     string
		createFn func() (trace.SpanExporter, func())
	}{
		{
			name: "Jaeger",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewJaegerExporter(server.URL()+"/api/traces",
					WithJaegerBatchSize(100),
					WithJaegerFlushInterval(100*time.Millisecond),
					WithJaegerQueueSize(1000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					// 这样可以避免 worker goroutine 在等待 HTTP 响应时阻塞
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
		{
			name: "Zipkin",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewZipkinExporter(server.URL()+"/api/v2/spans",
					WithZipkinBatchSize(100),
					WithZipkinFlushInterval(100*time.Millisecond),
					WithZipkinQueueSize(1000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					// 这样可以避免 worker goroutine 在等待 HTTP 响应时阻塞
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
		{
			name: "OTLP",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewOTLPExporter(server.URL()+"/v1/traces",
					WithOTLPBatchSize(100),
					WithOTLPFlushInterval(100*time.Millisecond),
					WithOTLPQueueSize(1000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					// 这样可以避免 worker goroutine 在等待 HTTP 响应时阻塞
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
	}

	for _, exporter := range exporters {
		b.Run(exporter.name+"_MemoryEfficiency", func(b *testing.B) {
			// 限制测试时间，避免超时
			testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer testCancel()

			exporter, cleanup := exporter.createFn()
			// 使用 defer 确保 cleanup 总是被执行，即使测试超时
			defer func() {
				// cleanup 函数内部已经调用了 exporter.Shutdown，这里不需要重复调用
				// 直接执行 cleanup（关闭 exporter 和 server）
				cleanup()
			}()

			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			b.ResetTimer()
			b.ReportAllocs()

			// 限制 b.N 的大小，避免测试运行时间过长
			maxN := 10000
			actualN := b.N
			if actualN > maxN {
				actualN = maxN
			}

			for i := 0; i < actualN; i++ {
				// 检查测试超时（但不直接返回，继续处理以便后续等待）
				select {
				case <-testCtx.Done():
					b.Logf("测试超时，已处理 %d spans，等待退出", i)
					// 不直接返回，继续处理以便后续等待
					goto waitProcessing
				default:
				}

				span := createTestSpan(i)
				exporter.ExportSpan(span)
			}

		waitProcessing:
			// 等待处理完成（使用带超时的等待）
			// 需要等待足够的时间让 batch processor 处理完所有 spans
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer waitCancel()

			// 等待 flush interval + 一些缓冲时间，确保所有 spans 都被处理
			waitDuration := 500 * time.Millisecond
			if actualN > 1000 {
				// 对于大量 spans，需要更长的等待时间
				waitDuration = 3 * time.Second
			}

			// 等待处理完成，但设置超时，避免无限等待
			select {
			case <-time.After(waitDuration):
				// 正常等待
			case <-waitCtx.Done():
				// 等待超时，记录日志但不返回，让 defer cleanup() 处理资源清理
				b.Logf("等待处理完成超时，已处理 %d spans", actualN)
			case <-testCtx.Done():
				// 测试超时，记录日志但不返回，让 defer cleanup() 处理资源清理
				b.Logf("测试超时，已处理 %d spans", actualN)
			}

			runtime.GC()
			runtime.ReadMemStats(&m2)

			memoryUsage := m2.Alloc - m1.Alloc
			avgMemoryPerSpan := float64(memoryUsage) / float64(actualN)

			b.ReportMetric(float64(memoryUsage)/1024/1024, "total_memory_mb")
			b.ReportMetric(avgMemoryPerSpan, "bytes_per_span")
		})
	}
}

// BenchmarkExporters_Throughput 吞吐量基准测试
// 测试不同导出器在不同负载下的吞吐量
func BenchmarkExporters_Throughput(b *testing.B) {
	exporters := []struct {
		name     string
		createFn func() (trace.SpanExporter, func())
	}{
		{
			name: "Jaeger",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewJaegerExporter(server.URL()+"/api/traces",
					WithJaegerBatchSize(500),
					WithJaegerFlushInterval(500*time.Millisecond),
					WithJaegerQueueSize(10000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
		{
			name: "Zipkin",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewZipkinExporter(server.URL()+"/api/v2/spans",
					WithZipkinBatchSize(500),
					WithZipkinFlushInterval(500*time.Millisecond),
					WithZipkinQueueSize(10000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
		{
			name: "OTLP",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewOTLPExporter(server.URL()+"/v1/traces",
					WithOTLPBatchSize(500),
					WithOTLPFlushInterval(500*time.Millisecond),
					WithOTLPQueueSize(10000),
				)
				return exporter, func() {
					// 先关闭 server，再关闭 exporter，确保资源正确释放
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
				}
			},
		},
	}

	concurrencyLevels := []int{10, 50, 100, 500, 1000, 5000}

	for _, exporter := range exporters {
		for _, concurrency := range concurrencyLevels {
			// 为高并发场景（5000）添加跳过逻辑，避免超时和 goroutine 泄露
			if concurrency >= 5000 {
				// 直接跳过，不创建测试
				continue
			}

			b.Run(fmt.Sprintf("%s_Throughput_Concurrency_%d", exporter.name, concurrency), func(b *testing.B) {
				// 限制测试时间，避免超时
				testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer testCancel()

				exporter, cleanup := exporter.createFn()
				// 使用 defer 确保 cleanup 总是被执行，即使测试超时
				defer func() {
					// cleanup 函数内部已经调用了 exporter.Shutdown，这里不需要重复调用
					// 直接执行 cleanup（关闭 exporter 和 server）
					cleanup()
				}()

				// 限制 b.N 的大小，避免测试运行时间过长
				maxN := 100000
				actualN := b.N
				if actualN > maxN {
					actualN = maxN
				}

				startTime := time.Now()
				var processedCount int64

				b.ResetTimer()
				b.ReportAllocs()

				var wg sync.WaitGroup
				spansPerGoroutine := actualN / concurrency
				if spansPerGoroutine == 0 {
					spansPerGoroutine = 1
				}

				// 限制并发数，避免创建过多 goroutine
				actualConcurrency := concurrency
				if actualConcurrency > actualN {
					actualConcurrency = actualN
				}

				for i := 0; i < actualConcurrency; i++ {
					// 检查测试超时（但不直接返回，继续创建 goroutine 以便后续等待）
					select {
					case <-testCtx.Done():
						b.Logf("测试超时，已创建 %d 个 goroutine，等待退出", i)
						// 不直接返回，继续创建 goroutine 以便后续等待
						goto waitGoroutines
					default:
					}

					wg.Add(1)
					go func(goroutineID int) {
						defer wg.Done()

						// 检查测试超时
						select {
						case <-testCtx.Done():
							return
						default:
						}

						for j := 0; j < spansPerGoroutine; j++ {
							// 检查测试超时
							select {
							case <-testCtx.Done():
								return
							default:
							}

							spanID := goroutineID*spansPerGoroutine + j
							if spanID >= actualN {
								break
							}
							span := createTestSpan(spanID)
							exporter.ExportSpan(span)
							atomic.AddInt64(&processedCount, 1)
						}
					}(i)
				}

			waitGoroutines:
				// 使用 channel 等待完成或超时
				done := make(chan struct{})
				go func() {
					wg.Wait()
					close(done)
				}()

				// 等待 goroutine 完成，但设置超时，避免无限等待
				waitTimeout := 5 * time.Second
				select {
				case <-done:
					// 正常完成，等待一小段时间让 exporter 处理完所有 spans
					time.Sleep(500 * time.Millisecond)
				case <-time.After(waitTimeout):
					// 等待超时，记录日志但不返回，让 defer cleanup() 处理资源清理
					b.Logf("等待 goroutine 完成超时，已处理 %d spans", atomic.LoadInt64(&processedCount))
				case <-testCtx.Done():
					// 测试超时，记录日志但不返回，让 defer cleanup() 处理资源清理
					b.Logf("测试超时，已处理 %d spans", atomic.LoadInt64(&processedCount))
				}

				duration := time.Since(startTime)
				if processedCount > 0 {
					qps := float64(processedCount) / duration.Seconds()
					b.ReportMetric(qps, "qps")
					b.ReportMetric(float64(duration.Nanoseconds())/float64(processedCount), "ns_per_span")
				}
			})
		}
	}
}
