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
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/exporter"
	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// routingAwareMockExporter 模拟按集合路由的导出器，用于无 MongoDB 时的全链路压测。
type routingAwareMockExporter struct {
	exportCalls     int64
	exportSpanCount int64
	collectionHits  sync.Map // collection -> *int64
}

func newRoutingAwareMockExporter() *routingAwareMockExporter {
	return &routingAwareMockExporter{}
}

func (m *routingAwareMockExporter) ExportSpan(span trace.SpanSnapshot) error {
	return m.ExportSpans([]trace.SpanSnapshot{span})
}

func (m *routingAwareMockExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	atomic.AddInt64(&m.exportCalls, 1)
	atomic.AddInt64(&m.exportSpanCount, int64(len(spans)))

	groups := make(map[string]int)
	for _, span := range spans {
		if span == nil {
			continue
		}
		collection := span.GetMongoCollection()
		if collection == "" {
			collection = "default"
		}
		groups[collection]++
	}

	for collection, count := range groups {
		val, _ := m.collectionHits.LoadOrStore(collection, new(int64))
		atomic.AddInt64(val.(*int64), int64(count))
	}

	return nil
}

func (m *routingAwareMockExporter) Shutdown(context.Context) error {
	return nil
}

func benchMongoCollectionNames(count int) []string {
	if count <= 1 {
		return []string{"gp_traces_bench_default"}
	}
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("gp_traces_bench_%d", i)
	}
	return names
}

// simulateRequestWithMongoRouting 模拟一次 HTTP 请求：父 Span + 子 Span + 异步 Span，均带集合路由。
func simulateRequestWithMongoRouting(tr trace.Tracer, collection string, requestID int) {
	ctx := context.Background()
	ctx, parent := tr.Start(ctx, "http.request",
		tracerpkg.WithSpanKind(types.SpanKindServer),
		tracerpkg.WithForceRecord(),
		tracerpkg.WithMongoCollection(collection),
	)
	parent.SetAttributes(
		attribute.String("request.id", fmt.Sprintf("req-%d", requestID)),
		attribute.String("mongo.collection", collection),
	)

	_, dbSpan := tr.Start(ctx, "db.query",
		tracerpkg.WithSpanKind(types.SpanKindInternal),
		tracerpkg.WithForceRecord(),
	)
	dbSpan.SetAttributes(attribute.String("db.table", "orders"))
	dbSpan.End()

	asyncCtx, asyncSpan := baggage.StartAsyncSpan(ctx, tr, "async.notify",
		tracerpkg.WithSpanKind(types.SpanKindAsync),
		tracerpkg.WithForceRecord(),
	)
	_ = asyncCtx
	asyncSpan.SetAttributes(attribute.String("job", "notify"))
	asyncSpan.End()

	parent.End()
}

// BenchmarkMongoRouting_RequestLifecycle_Parallel 全链路并发：不同请求写入不同集合（无 Mongo I/O）。
func BenchmarkMongoRouting_RequestLifecycle_Parallel(b *testing.B) {
	collectionCounts := []int{1, 8, 32}

	for _, count := range collectionCounts {
		collections := benchMongoCollectionNames(count)
		b.Run(fmt.Sprintf("collections=%d", count), func(b *testing.B) {
			mockExp := newRoutingAwareMockExporter()
			batchProcessor := processor.NewBatchSpanProcessor(mockExp,
				processor.WithBatchSize(100),
				processor.WithWorkers(8),
				processor.WithFlushInterval(500*time.Millisecond),
				processor.WithQueueSize(10000),
			)
			defer batchProcessor.Shutdown(context.Background())

			tr := NewTracerImpl("routing-bench", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(atomic.AddInt64(&mongoRoutingBenchSpanID, 1))
				for pb.Next() {
					collection := collections[id%len(collections)]
					simulateRequestWithMongoRouting(tr, collection, id)
					id++
				}
			})
		})
	}
}

var mongoRoutingBenchSpanID int64

// BenchmarkMongoRouting_SnapshotExport_Parallel 对比路由导出器在快照层的并发导出（需 MongoDB）。
func BenchmarkMongoRouting_SnapshotExport_Parallel(b *testing.B) {
	uri := firstNonEmptyEnvValue(os.Getenv("TRACER_BENCH_MONGO_URI"), os.Getenv("MONGO_URI"))
	if uri == "" {
		b.Skip("未设置 TRACER_BENCH_MONGO_URI 或 MONGO_URI，跳过")
	}

	database := firstNonEmptyEnvValue(os.Getenv("TRACER_BENCH_MONGO_DATABASE"), "tracer_routing_bench")
	defaultCollection := firstNonEmptyEnvValue(os.Getenv("TRACER_BENCH_MONGO_COLLECTION"), "traces_default")
	prefix := fmt.Sprintf("gp_traces_core_%d_", time.Now().UnixNano())

	collectionCounts := []int{1, 8}
	for _, count := range collectionCounts {
		collections := benchMongoCollectionNamesWithPrefix(prefix, count)
		allowed := append([]string{defaultCollection}, collections...)

		b.Run(fmt.Sprintf("collections=%d", count), func(b *testing.B) {
			routingExp, err := exporter.NewMongoDBRoutingExporter(
				uri, database, defaultCollection,
				exporter.RoutingWithMongoOption(exporter.WithMongoDBTimeout(15*time.Second)),
				exporter.WithMongoDBRoutingAllowedCollections(allowed...),
			)
			if err != nil {
				b.Fatalf("创建路由导出器失败: %v", err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				_ = routingExp.Shutdown(ctx)
			}()

			batchProcessor := processor.NewBatchSpanProcessor(routingExp,
				processor.WithBatchSize(100),
				processor.WithWorkers(8),
				processor.WithFlushInterval(500*time.Millisecond),
				processor.WithQueueSize(10000),
			)
			defer batchProcessor.Shutdown(context.Background())

			tr := NewTracerImpl("routing-bench", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := int(atomic.AddInt64(&mongoRoutingBenchSpanID, 1))
				for pb.Next() {
					collection := collections[id%len(collections)]
					simulateRequestWithMongoRouting(tr, collection, id)
					id++
				}
			})
		})
	}
}

func benchMongoCollectionNamesWithPrefix(prefix string, count int) []string {
	if count <= 1 {
		return []string{prefix + "single"}
	}
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return names
}

func firstNonEmptyEnvValue(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func waitForMockExportCount(mockExp *routingAwareMockExporter, expected int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exported := atomic.LoadInt64(&mockExp.exportSpanCount)
		if exported >= expected {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("超时: 期望导出 %d spans，实际 %d", expected, atomic.LoadInt64(&mockExp.exportSpanCount))
}

// TestMongoRouting_RequestLifecycle_ConcurrentMetrics 全链路并发指标（默认无需 MongoDB）。
func TestMongoRouting_RequestLifecycle_ConcurrentMetrics(t *testing.T) {
	scenarios := []struct {
		name              string
		collectionCount   int
		concurrency       int
		requestsPerWorker int
	}{
		{name: "16并发-单集合", collectionCount: 1, concurrency: 16, requestsPerWorker: 500},
		{name: "16并发-8集合", collectionCount: 8, concurrency: 16, requestsPerWorker: 500},
		{name: "64并发-单集合", collectionCount: 1, concurrency: 64, requestsPerWorker: 200},
		{name: "64并发-8集合", collectionCount: 8, concurrency: 64, requestsPerWorker: 200},
		{name: "128并发-32集合", collectionCount: 32, concurrency: 128, requestsPerWorker: 100},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			collections := benchMongoCollectionNames(sc.collectionCount)
			mockExp := newRoutingAwareMockExporter()
			batchProcessor := processor.NewBatchSpanProcessor(mockExp,
				processor.WithBatchSize(100),
				processor.WithWorkers(8),
				processor.WithFlushInterval(300*time.Millisecond),
				processor.WithQueueSize(100000),
			).(*processor.BatchSpanProcessor)

			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()
			defer func() {
				_ = batchProcessor.Shutdown(shutdownCtx)
			}()

			tr := NewTracerImpl("routing-bench", nil, batchProcessor, sampler.NewAlwaysSampleSampler())

			totalRequests := int64(sc.concurrency * sc.requestsPerWorker)
			spansPerRequest := int64(3) // parent + db + async
			expectedSpans := totalRequests * spansPerRequest

			var processed int64
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)
			start := time.Now()

			var wg sync.WaitGroup
			wg.Add(sc.concurrency)
			for worker := 0; worker < sc.concurrency; worker++ {
				go func(workerID int) {
					defer wg.Done()
					baseID := workerID * sc.requestsPerWorker
					for i := 0; i < sc.requestsPerWorker; i++ {
						requestID := baseID + i
						collection := collections[requestID%len(collections)]
						simulateRequestWithMongoRouting(tr, collection, requestID)
						atomic.AddInt64(&processed, 1)
					}
				}(worker)
			}
			wg.Wait()

			if err := waitForMockExportCount(mockExp, expectedSpans, 30*time.Second); err != nil {
				t.Fatalf("等待导出完成失败: %v (queue=%d)", err, batchProcessor.GetQueueLength())
			}

			elapsed := time.Since(start)
			runtime.ReadMemStats(&m2)

			exported := atomic.LoadInt64(&mockExp.exportSpanCount)
			requestQPS := float64(processed) / elapsed.Seconds()
			spanQPS := float64(exported) / elapsed.Seconds()

			t.Logf("=== %s ===", sc.name)
			t.Logf("集合数: %d, 并发: %d, 每 worker 请求数: %d", sc.collectionCount, sc.concurrency, sc.requestsPerWorker)
			t.Logf("请求数: %d, 导出 span 数: %d (期望 %d)", processed, exported, expectedSpans)
			t.Logf("请求 QPS: %.2f, Span QPS: %.2f, 耗时: %v", requestQPS, spanQPS, elapsed)
			t.Logf("导出批次: %d", atomic.LoadInt64(&mockExp.exportCalls))
			t.Logf("内存增量: %.2f MB", float64(m2.Alloc-m1.Alloc)/1024/1024)

			mockExp.collectionHits.Range(func(key, value any) bool {
				t.Logf("  集合 %q: %d spans", key.(string), atomic.LoadInt64(value.(*int64)))
				return true
			})

			if processed != totalRequests {
				t.Fatalf("请求数不匹配: 期望 %d, 实际 %d", totalRequests, processed)
			}
			if exported != expectedSpans {
				t.Fatalf("span 数不匹配: 期望 %d, 实际 %d", expectedSpans, exported)
			}
		})
	}
}

// TestMongoRouting_CollectionInheritance_Concurrent 并发下验证集合名继承正确性。
func TestMongoRouting_CollectionInheritance_Concurrent(t *testing.T) {
	tr := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())
	collections := benchMongoCollectionNames(8)

	var wg sync.WaitGroup
	errCh := make(chan string, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(requestID int) {
			defer wg.Done()

			collection := collections[requestID%len(collections)]
			ctx, parent := tr.Start(context.Background(), "http.request",
				tracerpkg.WithMongoCollection(collection),
			)
			if got := parent.GetMongoCollection(); got != collection {
				errCh <- fmt.Sprintf("parent request=%d: got %q want %q", requestID, got, collection)
				return
			}

			_, child := tr.Start(ctx, "internal.step")
			if got := child.GetMongoCollection(); got != collection {
				errCh <- fmt.Sprintf("child request=%d: got %q want %q", requestID, got, collection)
				return
			}

			parent.End()
			child.End()
		}(i)
	}

	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}
}

// TestMongoRouting_SnapshotPreservesCollection 快照层保留集合名。
func TestMongoRouting_SnapshotPreservesCollection(t *testing.T) {
	collection := "gp_traces_webhook"
	span := mock.NewSpanSnapshotMockWithOptions(1, mock.WithMongoCollection(collection))
	if got := span.GetMongoCollection(); got != collection {
		t.Fatalf("mock snapshot collection: got %q want %q", got, collection)
	}
}
