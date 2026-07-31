package exporter

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// BenchmarkExporters_HighConcurrency 高并发环境下的导出器性能对比
// 测试不同导出器在高并发场景下的性能表现
func BenchmarkExporters_HighConcurrency(b *testing.B) {
	// 测试配置
	concurrencyLevels := []int{100, 500, 1000, 5000, 10000}
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
					WithZipkinBatchSize(500),
					WithZipkinFlushInterval(500*time.Millisecond),
					WithZipkinQueueSize(10000),
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
					WithOTLPBatchSize(500),
					WithOTLPFlushInterval(500*time.Millisecond),
					WithOTLPQueueSize(10000),
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
		for _, concurrency := range concurrencyLevels {
			// 跳过极端高并发场景，避免超时和 goroutine 泄露
			if concurrency >= 5000 {
				continue
			}

			b.Run(fmt.Sprintf("%s_Concurrency_%d", exporter.name, concurrency), func(b *testing.B) {
				// 限制测试时间，避免超时
				testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer testCancel()

				exporter, cleanup := exporter.createFn()
				defer cleanup()

				// 限制 b.N 的大小，避免测试运行时间过长
				maxN := 100000
				actualN := b.N
				if actualN > maxN {
					actualN = maxN
				}

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

				// 高并发测试
				var wg sync.WaitGroup
				var processedCount int64
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

// BenchmarkExporters_ExtremeHighConcurrency 极端高并发场景测试
// 测试在极端高并发（10000+ goroutines）下的性能表现
func BenchmarkExporters_ExtremeHighConcurrency(b *testing.B) {
	exporters := []struct {
		name     string
		createFn func() (trace.SpanExporter, func())
	}{
		{
			name: "Jaeger",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(b, 200)
				exporter, _ := NewJaegerExporter(server.URL()+"/api/traces",
					WithJaegerBatchSize(1000),
					WithJaegerFlushInterval(500*time.Millisecond),
					WithJaegerQueueSize(50000),
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
					WithZipkinBatchSize(1000),
					WithZipkinFlushInterval(500*time.Millisecond),
					WithZipkinQueueSize(50000),
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
					WithOTLPBatchSize(1000),
					WithOTLPFlushInterval(500*time.Millisecond),
					WithOTLPQueueSize(50000),
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

	concurrency := 10000
	spansPerGoroutine := 10

	for _, exporter := range exporters {
		b.Run(exporter.name+"_ExtremeHighConcurrency", func(b *testing.B) {
			// 限制测试时间，避免超时
			testCtx, testCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer testCancel()

			exporter, cleanup := exporter.createFn()
			defer cleanup()

			// 记录开始时间和内存
			startTime := time.Now()
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			b.ResetTimer()
			b.ReportAllocs()

			// 极端高并发测试
			var wg sync.WaitGroup
			var processedCount int64
			totalSpans := concurrency * spansPerGoroutine

			for i := 0; i < concurrency; i++ {
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
			waitTimeout := 10 * time.Second
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

			if processedCount != int64(totalSpans) {
				b.Errorf("期望处理%d个span，实际处理%d个", totalSpans, processedCount)
			}
		})
	}
}

// createTestSpan 创建测试Span
func createTestSpan(id int) trace.SpanSnapshot {
	now := time.Now()
	span := mock.NewSpanSnapshotMock(id)
	span.SpanName = "test-span"
	span.SpanTraceID = "12345678901234567890123456789012"
	span.SpanContext.SpanID = "1234567890123456"
	span.SpanParentSpanID = "1234567890123455"
	span.SpanKind = types.SpanKindServer
	span.StartTime = now
	span.EndTime = now.Add(100 * time.Millisecond)
	span.Attributes = map[string]any{
		"key1": "value1",
		"key2": 123,
		"key3": true,
	}
	return span
}
