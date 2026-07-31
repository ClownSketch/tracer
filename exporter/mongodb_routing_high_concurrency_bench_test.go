package exporter

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoRoutingBenchConfig 高并发路由基准测试配置。
type mongoRoutingBenchConfig struct {
	URI                string
	Database           string
	DefaultCollection  string
	CollectionPrefix   string
	AllowedCollections []string
}

func loadMongoRoutingBenchConfig(b *testing.B) mongoRoutingBenchConfig {
	b.Helper()

	uri := firstNonEmptyEnv(
		os.Getenv("TRACER_BENCH_MONGO_URI"),
		os.Getenv("MONGO_URI"),
	)
	if uri == "" {
		b.Skip("未设置 TRACER_BENCH_MONGO_URI 或 MONGO_URI，跳过 MongoDB 路由写入基准测试")
	}

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	return mongoRoutingBenchConfig{
		URI:               uri,
		Database:          firstNonEmptyEnv(os.Getenv("TRACER_BENCH_MONGO_DATABASE"), "tracer_routing_bench"),
		DefaultCollection: firstNonEmptyEnv(os.Getenv("TRACER_BENCH_MONGO_COLLECTION"), "traces_default"),
		CollectionPrefix:  "gp_traces_bench_" + runID + "_",
	}
}

func firstNonEmptyEnv(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func benchRoutingCollectionNames(prefix string, count int) []string {
	if count <= 1 {
		return []string{prefix + "single"}
	}
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return names
}

func createRoutingBenchSpan(id int, collection string) trace.SpanSnapshot {
	now := time.Now()
	span := mock.NewSpanSnapshotMock(id)
	span.SpanName = "http.request"
	span.SpanTraceID = fmt.Sprintf("%032x", id)
	span.SpanContext.SpanID = fmt.Sprintf("%016x", id)
	span.SpanParentSpanID = fmt.Sprintf("%016x", id+1)
	span.SpanKind = types.SpanKindServer
	span.StartTime = now
	span.EndTime = now.Add(12 * time.Millisecond)
	span.MongoCollection = collection
	span.Attributes = map[string]any{
		"bench.id":         id,
		"bench.collection": collection,
		"service":          "routing-bench",
	}
	return span
}

func newBenchRoutingExporter(b *testing.B, cfg mongoRoutingBenchConfig, allowed ...string) (*MongoDBRoutingExporter, func()) {
	b.Helper()

	opts := []MongoDBRoutingExporterOption{
		RoutingWithMongoOption(WithMongoDBTimeout(15 * time.Second)),
		RoutingWithMongoOption(WithMongoDBRetries(2, 100*time.Millisecond)),
	}
	if len(allowed) > 0 {
		opts = append(opts, WithMongoDBRoutingAllowedCollections(allowed...))
	}

	exp, err := NewMongoDBRoutingExporter(
		cfg.URI,
		cfg.Database,
		cfg.DefaultCollection,
		opts...,
	)
	if err != nil {
		b.Fatalf("创建 MongoDBRoutingExporter 失败: %v", err)
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = exp.Shutdown(ctx)
	}

	return exp, cleanup
}

func connectBenchMongoClient(b *testing.B, uri string) *mongo.Client {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		b.Fatalf("连接 MongoDB 失败: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		b.Fatalf("Ping MongoDB 失败: %v", err)
	}
	return client
}

func dropBenchRoutingCollections(b *testing.B, client *mongo.Client, cfg mongoRoutingBenchConfig, collections []string) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database(cfg.Database)
	names := append([]string{cfg.DefaultCollection}, collections...)
	for _, name := range names {
		if name == "" {
			continue
		}
		_ = db.Collection(name).Drop(ctx)
	}
}

type routingBenchScenario struct {
	name            string
	collectionCount int
	concurrency     int
	batchSize       int
	spansPerWorker  int
}

func routingBenchScenarios() []routingBenchScenario {
	return []routingBenchScenario{
		{name: "C32_B50_S200", collectionCount: 1, concurrency: 32, batchSize: 50, spansPerWorker: 200},
		{name: "C32_B50_S200", collectionCount: 8, concurrency: 32, batchSize: 50, spansPerWorker: 200},
		{name: "C32_B50_S200", collectionCount: 32, concurrency: 32, batchSize: 50, spansPerWorker: 200},
		{name: "C64_B100_S100", collectionCount: 1, concurrency: 64, batchSize: 100, spansPerWorker: 100},
		{name: "C64_B100_S100", collectionCount: 8, concurrency: 64, batchSize: 100, spansPerWorker: 100},
		{name: "C64_B100_S100", collectionCount: 32, concurrency: 64, batchSize: 100, spansPerWorker: 100},
	}
}

func runRoutingExportBenchmark(b *testing.B, exp *MongoDBRoutingExporter, scenario routingBenchScenario, collections []string, idOffset int) int64 {
	b.Helper()

	totalSpans := int64(scenario.concurrency * scenario.spansPerWorker)
	var processed int64

	var wg sync.WaitGroup
	wg.Add(scenario.concurrency)

	for worker := 0; worker < scenario.concurrency; worker++ {
		go func(workerID int) {
			defer wg.Done()

			batch := make([]trace.SpanSnapshot, 0, scenario.batchSize)
			baseID := idOffset + workerID*scenario.spansPerWorker
			for i := 0; i < scenario.spansPerWorker; i++ {
				spanID := baseID + i
				collection := collections[spanID%len(collections)]
				batch = append(batch, createRoutingBenchSpan(spanID, collection))

				if len(batch) >= scenario.batchSize || i == scenario.spansPerWorker-1 {
					if err := exp.ExportSpans(batch); err != nil {
						b.Errorf("ExportSpans 失败 worker=%d: %v", workerID, err)
					}
					atomic.AddInt64(&processed, int64(len(batch)))
					batch = batch[:0]
				}
			}
		}(worker)
	}

	wg.Wait()

	if processed != totalSpans {
		b.Fatalf("期望导出 %d 条 span，实际 %d 条", totalSpans, processed)
	}

	stats := exp.GetStats()
	if stats["exportErrors"] > 0 {
		b.Fatalf("导出错误数: %d", stats["exportErrors"])
	}

	return processed
}

func warmRoutingCollections(tb testing.TB, exp *MongoDBRoutingExporter, collections []string) {
	tb.Helper()
	for index, collectionName := range collections {
		span := createRoutingBenchSpan(index, collectionName)
		collection, err := exp.resolveCollection(span)
		span.Release()
		if err != nil || collection == nil {
			tb.Fatalf("预热 MongoDB 路由集合 %q 失败: %v", collectionName, err)
		}
	}
}

func reportRoutingBenchMetrics(b *testing.B, processed int64, elapsed time.Duration) {
	b.Helper()
	if processed <= 0 || elapsed <= 0 {
		return
	}
	qps := float64(processed) / elapsed.Seconds()
	b.ReportMetric(qps, "spans_per_sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(processed), "ns_per_span")
}

// BenchmarkMongoDBRouting_HighConcurrency 对比单集合与多集合路由写入性能。
//
// 运行示例:
//
//	MONGO_URI='mongodb://127.0.0.1:27017' \
//	go test ./exporter -run='^$' -bench=BenchmarkMongoDBRouting_ -benchmem -benchtime=3s -count=1
func BenchmarkMongoDBRouting_HighConcurrency(b *testing.B) {
	cfg := loadMongoRoutingBenchConfig(b)
	client := connectBenchMongoClient(b, cfg.URI)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
	}()

	for _, scenario := range routingBenchScenarios() {
		collections := benchRoutingCollectionNames(cfg.CollectionPrefix, scenario.collectionCount)
		allowed := append([]string{cfg.DefaultCollection}, collections...)

		b.Run(fmt.Sprintf("collections=%d/%s", scenario.collectionCount, scenario.name), func(b *testing.B) {
			dropBenchRoutingCollections(b, client, cfg, collections)

			exp, cleanup := newBenchRoutingExporter(b, cfg, allowed...)
			defer cleanup()
			warmRoutingCollections(b, exp, collections)

			b.ReportAllocs()
			b.ResetTimer()
			var totalProcessed int64
			spansPerIteration := scenario.concurrency * scenario.spansPerWorker
			start := time.Now()
			for i := 0; i < b.N; i++ {
				totalProcessed += runRoutingExportBenchmark(b, exp, scenario, collections, i*spansPerIteration)
			}
			reportRoutingBenchMetrics(b, totalProcessed, time.Since(start))
		})
	}
}

// BenchmarkMongoDBRouting_vs_SingleExporter 对比路由导出器与单集合导出器。
func BenchmarkMongoDBRouting_vs_SingleExporter(b *testing.B) {
	cfg := loadMongoRoutingBenchConfig(b)
	client := connectBenchMongoClient(b, cfg.URI)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
	}()

	scenario := routingBenchScenario{
		name:            "C32_B50_S200",
		collectionCount: 8,
		concurrency:     32,
		batchSize:       50,
		spansPerWorker:  200,
	}
	collections := benchRoutingCollectionNames(cfg.CollectionPrefix, scenario.collectionCount)

	b.Run("SingleExporter_OneCollection", func(b *testing.B) {
		dropBenchRoutingCollections(b, client, cfg, nil)

		exp, err := NewMongoDBExporter(
			cfg.URI,
			cfg.Database,
			cfg.DefaultCollection,
			WithMongoDBTimeout(15*time.Second),
			WithMongoDBRetries(2, 100*time.Millisecond),
		)
		if err != nil {
			b.Fatalf("创建 MongoDBExporter 失败: %v", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_ = exp.Shutdown(ctx)
		}()

		b.ReportAllocs()
		b.ResetTimer()
		var totalProcessed int64
		start := time.Now()
		spansPerIteration := scenario.concurrency * scenario.spansPerWorker
		for i := 0; i < b.N; i++ {
			totalProcessed += runSingleExporterBenchmark(b, exp, scenario, i*spansPerIteration)
		}
		reportRoutingBenchMetrics(b, totalProcessed, time.Since(start))
	})

	b.Run("RoutingExporter_EightCollections", func(b *testing.B) {
		dropBenchRoutingCollections(b, client, cfg, collections)
		allowed := append([]string{cfg.DefaultCollection}, collections...)

		exp, cleanup := newBenchRoutingExporter(b, cfg, allowed...)
		defer cleanup()
		warmRoutingCollections(b, exp, collections)

		b.ReportAllocs()
		b.ResetTimer()
		var totalProcessed int64
		spansPerIteration := scenario.concurrency * scenario.spansPerWorker
		start := time.Now()
		for i := 0; i < b.N; i++ {
			totalProcessed += runRoutingExportBenchmark(b, exp, scenario, collections, i*spansPerIteration)
		}
		reportRoutingBenchMetrics(b, totalProcessed, time.Since(start))
	})
}

func runSingleExporterBenchmark(b *testing.B, exp *MongoDBExporter, scenario routingBenchScenario, idOffset int) int64 {
	b.Helper()

	totalSpans := int64(scenario.concurrency * scenario.spansPerWorker)
	var processed int64

	var wg sync.WaitGroup
	wg.Add(scenario.concurrency)
	for worker := 0; worker < scenario.concurrency; worker++ {
		go func(workerID int) {
			defer wg.Done()

			batch := make([]trace.SpanSnapshot, 0, scenario.batchSize)
			baseID := idOffset + workerID*scenario.spansPerWorker
			for i := 0; i < scenario.spansPerWorker; i++ {
				spanID := baseID + i
				batch = append(batch, createRoutingBenchSpan(spanID, ""))

				if len(batch) >= scenario.batchSize || i == scenario.spansPerWorker-1 {
					if err := exp.ExportSpans(batch); err != nil {
						b.Errorf("ExportSpans 失败: %v", err)
					}
					atomic.AddInt64(&processed, int64(len(batch)))
					batch = batch[:0]
				}
			}
		}(worker)
	}
	wg.Wait()

	if processed != totalSpans {
		b.Fatalf("期望导出 %d 条 span，实际 %d 条", totalSpans, processed)
	}

	return processed
}

// BenchmarkMongoDBRouting_RoutingOverhead 纯路由解析开销（不依赖 MongoDB）。
func BenchmarkMongoDBRouting_RoutingOverhead(b *testing.B) {
	base := newMongoDBExporterWithDefaults()
	base.collection = &mongo.Collection{}
	base.database = &mongo.Database{}

	collectionCounts := []int{1, 8, 32, 128}
	for _, count := range collectionCounts {
		prefix := fmt.Sprintf("gp_traces_overhead_%d_", count)
		collections := benchRoutingCollectionNames(prefix, count)

		exp := &MongoDBRoutingExporter{
			base:                  base,
			defaultCollectionName: "traces_default",
			allowedCollections:    nil,
		}
		for _, name := range collections {
			exp.collectionCache.Store(name, base.collection)
			exp.indexedCollections.Store(name, struct{}{})
		}
		exp.collectionCache.Store("traces_default", base.collection)
		exp.indexedCollections.Store("traces_default", struct{}{})

		b.Run(fmt.Sprintf("collections=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				span := createRoutingBenchSpan(i, collections[i%len(collections)])
				coll, err := exp.resolveCollection(span)
				if err != nil || coll == nil {
					b.Fatal(err)
				}
				_ = exp.base.buildDocument(span, false)
			}
		})
	}
}

// TestMongoDBRouting_HighConcurrencyMetrics 输出不同集合数量下的高并发性能指标。
//
// 运行示例:
//
//	MONGO_URI='mongodb://127.0.0.1:27017' \
//	go test ./exporter -run TestMongoDBRouting_HighConcurrencyMetrics -count=1 -v
func TestMongoDBRouting_HighConcurrencyMetrics(t *testing.T) {
	cfg := loadMongoRoutingBenchConfigForTest(t)
	client := connectBenchMongoClientForTest(t, cfg.URI)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
	}()

	scenarios := []struct {
		name            string
		collectionCount int
		concurrency     int
		batchSize       int
		spansPerWorker  int
	}{
		{name: "低并发-单集合", collectionCount: 1, concurrency: 16, batchSize: 50, spansPerWorker: 100},
		{name: "低并发-8集合", collectionCount: 8, concurrency: 16, batchSize: 50, spansPerWorker: 100},
		{name: "中等并发-单集合", collectionCount: 1, concurrency: 64, batchSize: 50, spansPerWorker: 200},
		{name: "中等并发-8集合", collectionCount: 8, concurrency: 64, batchSize: 50, spansPerWorker: 200},
		{name: "高并发-单集合", collectionCount: 1, concurrency: 128, batchSize: 100, spansPerWorker: 100},
		{name: "高并发-8集合", collectionCount: 8, concurrency: 128, batchSize: 100, spansPerWorker: 100},
		{name: "高并发-32集合", collectionCount: 32, concurrency: 128, batchSize: 100, spansPerWorker: 100},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			collections := benchRoutingCollectionNames(cfg.CollectionPrefix, sc.collectionCount)
			allowed := append([]string{cfg.DefaultCollection}, collections...)
			dropBenchRoutingCollectionsForTest(t, client, cfg, collections)

			exp, cleanup := newBenchRoutingExporterForTest(t, cfg, allowed...)
			defer cleanup()
			warmRoutingCollections(t, exp, collections)

			totalSpans := int64(sc.concurrency * sc.spansPerWorker)
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

					batch := make([]trace.SpanSnapshot, 0, sc.batchSize)
					baseID := workerID * sc.spansPerWorker
					for i := 0; i < sc.spansPerWorker; i++ {
						spanID := baseID + i
						collection := collections[spanID%len(collections)]
						batch = append(batch, createRoutingBenchSpan(spanID, collection))

						if len(batch) >= sc.batchSize || i == sc.spansPerWorker-1 {
							if err := exp.ExportSpans(batch); err != nil {
								t.Errorf("ExportSpans 失败 worker=%d: %v", workerID, err)
								return
							}
							atomic.AddInt64(&processed, int64(len(batch)))
							batch = batch[:0]
						}
					}
				}(worker)
			}
			wg.Wait()

			elapsed := time.Since(start)
			runtime.ReadMemStats(&m2)
			stats := exp.GetStats()

			qps := float64(processed) / elapsed.Seconds()
			t.Logf("=== %s ===", sc.name)
			t.Logf("集合数: %d, 并发: %d, 批次: %d, 每 worker span 数: %d", sc.collectionCount, sc.concurrency, sc.batchSize, sc.spansPerWorker)
			t.Logf("总 span: %d, 耗时: %v, QPS: %.2f", processed, elapsed, qps)
			t.Logf("平均延迟: %.2f µs/span", float64(elapsed.Microseconds())/float64(processed))
			t.Logf("exporter processed=%d errors=%d", stats["processed"], stats["exportErrors"])
			t.Logf("内存增量: %.2f MB, GC 次数: %d", float64(m2.Alloc-m1.Alloc)/1024/1024, m2.NumGC-m1.NumGC)

			if processed != totalSpans {
				t.Fatalf("期望 %d spans，实际 %d", totalSpans, processed)
			}
			if stats["exportErrors"] > 0 {
				t.Fatalf("导出错误: %d", stats["exportErrors"])
			}
		})
	}
}

func loadMongoRoutingBenchConfigForTest(t *testing.T) mongoRoutingBenchConfig {
	t.Helper()
	uri := firstNonEmptyEnv(os.Getenv("TRACER_BENCH_MONGO_URI"), os.Getenv("MONGO_URI"))
	if uri == "" {
		t.Skip("未设置 TRACER_BENCH_MONGO_URI 或 MONGO_URI，跳过 MongoDB 路由性能测试")
	}
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	return mongoRoutingBenchConfig{
		URI:               uri,
		Database:          firstNonEmptyEnv(os.Getenv("TRACER_BENCH_MONGO_DATABASE"), "tracer_routing_bench"),
		DefaultCollection: firstNonEmptyEnv(os.Getenv("TRACER_BENCH_MONGO_COLLECTION"), "traces_default"),
		CollectionPrefix:  "gp_traces_bench_" + runID + "_",
	}
}

func connectBenchMongoClientForTest(t *testing.T, uri string) *mongo.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("连接 MongoDB 失败: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		t.Fatalf("Ping MongoDB 失败: %v", err)
	}
	return client
}

func dropBenchRoutingCollectionsForTest(t *testing.T, client *mongo.Client, cfg mongoRoutingBenchConfig, collections []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database(cfg.Database)
	names := append([]string{cfg.DefaultCollection}, collections...)
	for _, name := range names {
		if name == "" {
			continue
		}
		_ = db.Collection(name).Drop(ctx)
	}
}

func newBenchRoutingExporterForTest(t *testing.T, cfg mongoRoutingBenchConfig, allowed ...string) (*MongoDBRoutingExporter, func()) {
	t.Helper()
	opts := []MongoDBRoutingExporterOption{
		RoutingWithMongoOption(WithMongoDBTimeout(15 * time.Second)),
		RoutingWithMongoOption(WithMongoDBRetries(2, 100*time.Millisecond)),
	}
	if len(allowed) > 0 {
		opts = append(opts, WithMongoDBRoutingAllowedCollections(allowed...))
	}

	exp, err := NewMongoDBRoutingExporter(cfg.URI, cfg.Database, cfg.DefaultCollection, opts...)
	if err != nil {
		t.Fatalf("创建 MongoDBRoutingExporter 失败: %v", err)
	}
	return exp, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = exp.Shutdown(ctx)
	}
}
