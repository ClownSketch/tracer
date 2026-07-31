package exporter

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/trace"
)

// TestGoroutineLeak_MemoryEfficiency 测试 BenchmarkExporters_MemoryEfficiency 的 goroutine 泄露
func TestGoroutineLeak_MemoryEfficiency(t *testing.T) {
	exporters := []struct {
		name     string
		createFn func() (trace.SpanExporter, func())
	}{
		{
			name: "Jaeger",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(t, 200)
				exporter, _ := NewJaegerExporter(server.URL()+"/api/traces",
					WithJaegerBatchSize(100),
					WithJaegerFlushInterval(100*time.Millisecond),
					WithJaegerQueueSize(1000),
				)
				return exporter, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
					server.Close()
				}
			},
		},
		{
			name: "Zipkin",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(t, 200)
				exporter, _ := NewZipkinExporter(server.URL()+"/api/v2/spans",
					WithZipkinBatchSize(100),
					WithZipkinFlushInterval(100*time.Millisecond),
					WithZipkinQueueSize(1000),
				)
				return exporter, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
					server.Close()
				}
			},
		},
		{
			name: "OTLP",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(t, 200)
				exporter, _ := NewOTLPExporter(server.URL()+"/v1/traces",
					WithOTLPBatchSize(100),
					WithOTLPFlushInterval(100*time.Millisecond),
					WithOTLPQueueSize(1000),
				)
				return exporter, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
					server.Close()
				}
			},
		},
	}

	for _, exporterType := range exporters {
		t.Run(exporterType.name, func(t *testing.T) {
			// 记录初始 goroutine 数量
			runtime.GC()
			initialGoroutines := runtime.NumGoroutine()

			// 创建 exporter
			exporter, cleanup := exporterType.createFn()

			// 执行一些操作
			for i := 0; i < 1000; i++ {
				span := createTestSpan(i)
				exporter.ExportSpan(span)
			}

			// 等待处理完成
			time.Sleep(500 * time.Millisecond)

			// 清理资源
			cleanup()

			// 等待 goroutine 退出
			time.Sleep(1 * time.Second)

			// 检查 goroutine 数量
			runtime.GC()
			finalGoroutines := runtime.NumGoroutine()

			// 允许一些误差（系统 goroutine 等）
			leakedGoroutines := finalGoroutines - initialGoroutines
			if leakedGoroutines > 2 {
				t.Errorf("%s: 检测到 goroutine 泄露，初始: %d, 最终: %d, 泄露: %d",
					exporterType.name, initialGoroutines, finalGoroutines, leakedGoroutines)
			}
		})
	}
}

// TestGoroutineLeak_Throughput 测试 BenchmarkExporters_Throughput 的 goroutine 泄露
func TestGoroutineLeak_Throughput(t *testing.T) {
	exporters := []struct {
		name     string
		createFn func() (trace.SpanExporter, func())
	}{
		{
			name: "Jaeger",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(t, 200)
				exporter, _ := NewJaegerExporter(server.URL()+"/api/traces",
					WithJaegerBatchSize(500),
					WithJaegerFlushInterval(500*time.Millisecond),
					WithJaegerQueueSize(10000),
				)
				return exporter, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
					server.Close()
				}
			},
		},
		{
			name: "Zipkin",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(t, 200)
				exporter, _ := NewZipkinExporter(server.URL()+"/api/v2/spans",
					WithZipkinBatchSize(500),
					WithZipkinFlushInterval(500*time.Millisecond),
					WithZipkinQueueSize(10000),
				)
				return exporter, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
					server.Close()
				}
			},
		},
		{
			name: "OTLP",
			createFn: func() (trace.SpanExporter, func()) {
				server := newMockHTTPServer(t, 200)
				exporter, _ := NewOTLPExporter(server.URL()+"/v1/traces",
					WithOTLPBatchSize(500),
					WithOTLPFlushInterval(500*time.Millisecond),
					WithOTLPQueueSize(10000),
				)
				return exporter, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exporter.Shutdown(ctx)
					server.Close()
				}
			},
		},
	}

	concurrencyLevels := []int{10, 50, 100}

	for _, exporterType := range exporters {
		for _, concurrency := range concurrencyLevels {
			t.Run(fmt.Sprintf("%s_Concurrency_%d", exporterType.name, concurrency), func(t *testing.T) {
				// 记录初始 goroutine 数量
				runtime.GC()
				initialGoroutines := runtime.NumGoroutine()

				// 创建 exporter
				exporter, cleanup := exporterType.createFn()

				// 执行并发操作
				var workers sync.WaitGroup
				for i := 0; i < concurrency; i++ {
					workers.Add(1)
					go func(id int) {
						defer workers.Done()
						for j := 0; j < 100; j++ {
							span := createTestSpan(id*100 + j)
							if err := exporter.ExportSpan(span); err != nil {
								t.Errorf("导出 Span 失败: %v", err)
								return
							}
						}
					}(i)
				}
				workers.Wait()

				// 等待处理完成
				time.Sleep(1 * time.Second)

				// 清理资源
				cleanup()

				// 等待 goroutine 退出
				time.Sleep(1 * time.Second)

				// 检查 goroutine 数量
				runtime.GC()
				finalGoroutines := runtime.NumGoroutine()

				// 允许一些误差（系统 goroutine 等）
				leakedGoroutines := finalGoroutines - initialGoroutines
				if leakedGoroutines > 2 {
					t.Errorf("%s_Concurrency_%d: 检测到 goroutine 泄露，初始: %d, 最终: %d, 泄露: %d",
						exporterType.name, concurrency, initialGoroutines, finalGoroutines, leakedGoroutines)
				}
			})
		}
	}
}
