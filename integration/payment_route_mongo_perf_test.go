package integration

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
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/providers"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	paymentRouteCollection = "gp_traces_payment"
	paymentRoutePath       = "/api/v1/payment"
)

type paymentRoutePerfConfig struct {
	URI               string
	Database          string
	DefaultCollection string
	RunID             string
}

func loadPaymentRoutePerfConfig(t *testing.T) paymentRoutePerfConfig {
	t.Helper()

	uri := firstNonEmpty(os.Getenv("TRACER_BENCH_MONGO_URI"), os.Getenv("MONGO_URI"))
	if uri == "" {
		t.Skip("未设置 MONGO_URI，跳过 payment 路由真实性能测试")
	}

	runID := fmt.Sprintf("pay-perf-%d", time.Now().UnixNano())
	return paymentRoutePerfConfig{
		URI:               uri,
		Database:          firstNonEmpty(os.Getenv("TRACER_BENCH_MONGO_DATABASE"), "tracer_routing_bench"),
		DefaultCollection: firstNonEmpty(os.Getenv("TRACER_BENCH_MONGO_COLLECTION"), "traces_default"),
		RunID:             runID,
	}
}

func setupPaymentRouteTracer(t *testing.T, cfg paymentRoutePerfConfig) (trace.Tracer, *processor.BatchSpanProcessor, *exporter.MongoDBRoutingExporter, func()) {
	t.Helper()

	routingExp, err := exporter.NewMongoDBRoutingExporter(
		cfg.URI,
		cfg.Database,
		cfg.DefaultCollection,
		exporter.RoutingWithMongoOption(exporter.WithMongoDBTimeout(15*time.Second)),
		exporter.RoutingWithMongoOption(exporter.WithMongoDBRetries(3, 150*time.Millisecond)),
		exporter.WithMongoDBRoutingAllowedCollections(cfg.DefaultCollection, paymentRouteCollection),
	)
	if err != nil {
		t.Fatalf("创建路由导出器失败: %v", err)
	}

	batchProcessor := processor.NewBatchSpanProcessor(routingExp,
		processor.WithBatchSize(100),
		processor.WithWorkers(8),
		processor.WithFlushInterval(200*time.Millisecond),
		processor.WithQueueSize(100000),
	).(*processor.BatchSpanProcessor)

	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(batchProcessor),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
		_ = routingExp.Shutdown(ctx)
	}

	return provider.GetTracer("gateway"), batchProcessor, routingExp, cleanup
}

// simulatePaymentRouteRequest 模拟 /api/v1/payment 中间件 + 业务链路。
// 中间件在 Start 时设置 WithMongoCollection(paymentRouteCollection)，子 Span 自动继承。
func simulatePaymentRouteRequest(tr trace.Tracer, requestID int, runID string) time.Duration {
	start := time.Now()

	ctx, parent := tr.Start(context.Background(), "POST "+paymentRoutePath,
		tracerpkg.WithSpanKind(types.SpanKindServer),
		tracerpkg.WithForceRecord(),
		tracerpkg.WithMongoCollection(paymentRouteCollection),
	)
	parent.SetAttributes(
		attribute.String("http.route", paymentRoutePath),
		attribute.String("http.method", "POST"),
		attribute.String("run_id", runID),
		attribute.Int("request.seq", requestID),
	)

	_, dbSpan := tr.Start(ctx, "payment.persist",
		tracerpkg.WithSpanKind(types.SpanKindInternal),
		tracerpkg.WithForceRecord(),
	)
	dbSpan.SetAttributes(attribute.String("db.table", "payments"))
	dbSpan.End()

	asyncCtx, asyncSpan := baggage.StartAsyncSpan(ctx, tr, "payment.notify",
		tracerpkg.WithSpanKind(types.SpanKindAsync),
		tracerpkg.WithForceRecord(),
	)
	_ = asyncCtx
	asyncSpan.SetAttributes(attribute.String("channel", "webhook"))
	asyncSpan.End()

	parent.End()
	return time.Since(start)
}

// simulateDefaultRouteRequest 模拟其他路由（使用默认集合）。
func simulateDefaultRouteRequest(tr trace.Tracer, requestID int, runID string) time.Duration {
	start := time.Now()

	ctx, parent := tr.Start(context.Background(), "GET /api/v1/health",
		tracerpkg.WithSpanKind(types.SpanKindServer),
		tracerpkg.WithForceRecord(),
	)
	parent.SetAttributes(
		attribute.String("http.route", "/api/v1/health"),
		attribute.String("run_id", runID),
		attribute.Int("request.seq", requestID),
	)

	_, child := tr.Start(ctx, "health.check",
		tracerpkg.WithSpanKind(types.SpanKindInternal),
	)
	child.End()
	parent.End()
	return time.Since(start)
}

type paymentRouteScenario struct {
	name           string
	concurrency    int
	requestsTotal  int
	paymentPercent int // 0-100
}

// TestPaymentRoute_MongoRealWorldPerformance 真实 Mongo 写入：payment 固定集合场景。
//
// 运行:
//
//	MONGO_URI='mongodb://<username>:<password>@127.0.0.1:27017/?authSource=admin' \
//	go test ./integration -run TestPaymentRoute_MongoRealWorldPerformance -v -count=1 -timeout=15m
func TestPaymentRoute_MongoRealWorldPerformance(t *testing.T) {
	cfg := loadPaymentRoutePerfConfig(t)

	client := mustConnectMongo(t, cfg.URI)
	defer disconnectMongo(t, client)

	paymentColl := client.Database(cfg.Database).Collection(paymentRouteCollection)
	defaultColl := client.Database(cfg.Database).Collection(cfg.DefaultCollection)
	cleanupPaymentRouteCollections(t, client, cfg)

	scenarios := []paymentRouteScenario{
		{name: "payment独占-32并发-5000请求", concurrency: 32, requestsTotal: 5000, paymentPercent: 100},
		{name: "payment独占-64并发-10000请求", concurrency: 64, requestsTotal: 10000, paymentPercent: 100},
		{name: "payment独占-128并发-10000请求", concurrency: 128, requestsTotal: 10000, paymentPercent: 100},
		{name: "混合80%payment-64并发-10000请求", concurrency: 64, requestsTotal: 10000, paymentPercent: 80},
		{name: "混合50%payment-64并发-10000请求", concurrency: 64, requestsTotal: 10000, paymentPercent: 50},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			cleanupPaymentRouteCollections(t, client, cfg)

			tr, batchProcessor, routingExp, cleanup := setupPaymentRouteTracer(t, cfg)
			defer cleanup()

			paymentRequests := sc.requestsTotal * sc.paymentPercent / 100
			expectedPaymentSpans := int64(paymentRequests * 3)
			expectedDefaultSpans := int64((sc.requestsTotal - paymentRequests) * 2)

			var (
				requestCount  int64
				totalHandleNs int64
				minHandleNs   int64 = int64(^uint64(0) >> 1)
				maxHandleNs   int64
			)

			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)
			benchStart := time.Now()

			var wg sync.WaitGroup
			sem := make(chan struct{}, sc.concurrency)
			requestsPerWorker := sc.requestsTotal / sc.concurrency
			remainder := sc.requestsTotal % sc.concurrency

			var seq int64
			for w := 0; w < sc.concurrency; w++ {
				wg.Add(1)
				count := requestsPerWorker
				if w < remainder {
					count++
				}
				go func(workerID, n int) {
					defer wg.Done()
					for i := 0; i < n; i++ {
						sem <- struct{}{}
						reqID := workerID*100000 + i
						globalSeq := int(atomic.AddInt64(&seq, 1) - 1)
						isPayment := sc.paymentPercent == 100 || (sc.paymentPercent > 0 && globalSeq%100 < sc.paymentPercent)

						var elapsed time.Duration
						if isPayment {
							elapsed = simulatePaymentRouteRequest(tr, reqID, cfg.RunID)
						} else {
							elapsed = simulateDefaultRouteRequest(tr, reqID, cfg.RunID)
						}
						atomic.AddInt64(&requestCount, 1)

						ns := elapsed.Nanoseconds()
						atomic.AddInt64(&totalHandleNs, ns)
						for {
							cur := atomic.LoadInt64(&minHandleNs)
							if ns >= cur || atomic.CompareAndSwapInt64(&minHandleNs, cur, ns) {
								break
							}
						}
						for {
							cur := atomic.LoadInt64(&maxHandleNs)
							if ns <= cur || atomic.CompareAndSwapInt64(&maxHandleNs, cur, ns) {
								break
							}
						}
						<-sem
					}
				}(w, count)
			}
			wg.Wait()

			genElapsed := time.Since(benchStart)
			expectedTotalSpans := expectedPaymentSpans + expectedDefaultSpans

			if err := waitForMongoSpanCount(routingExp, batchProcessor, expectedTotalSpans, 60*time.Second); err != nil {
				stats := routingExp.GetStats()
				t.Fatalf("等待 Mongo 落库失败: %v (processed=%d errors=%d queue=%d)",
					err, stats["processed"], stats["exportErrors"], batchProcessor.GetQueueLength())
			}

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = batchProcessor.Shutdown(shutdownCtx)

			writeElapsed := time.Since(benchStart)
			runtime.ReadMemStats(&m2)

			paymentDocs, err := countCollectionDocs(paymentColl)
			if err != nil {
				t.Fatalf("统计 payment 集合失败: %v", err)
			}
			defaultDocs, err := countCollectionDocs(defaultColl)
			if err != nil {
				t.Fatalf("统计 default 集合失败: %v", err)
			}

			stats := routingExp.GetStats()
			reqQPS := float64(sc.requestsTotal) / genElapsed.Seconds()
			spanQPS := float64(stats["processed"]) / writeElapsed.Seconds()
			avgHandleUs := float64(totalHandleNs) / float64(sc.requestsTotal) / 1000.0

			t.Logf("========== %s ==========", sc.name)
			t.Logf("场景: payment=%d%%, 并发=%d, 总请求=%d", sc.paymentPercent, sc.concurrency, sc.requestsTotal)
			t.Logf("Span 结构: payment 请求 3 spans/req, 其他路由 2 spans/req")
			t.Logf("--- 业务侧（Start→End，含 3 span 创建，不含 Mongo 等待）---")
			t.Logf("请求生成耗时: %v", genElapsed)
			t.Logf("请求 QPS: %.2f req/s", reqQPS)
			t.Logf("单请求处理: avg=%.2f µs, min=%.2f µs, max=%.2f µs",
				avgHandleUs, float64(minHandleNs)/1000, float64(maxHandleNs)/1000)
			t.Logf("--- Mongo 写入（异步 BatchProcessor + RoutingExporter）---")
			t.Logf("端到端耗时(含落库): %v", writeElapsed)
			t.Logf("导出 spans: %d, QPS: %.2f spans/s", stats["processed"], spanQPS)
			t.Logf("导出错误: %d", stats["exportErrors"])
			t.Logf("--- 落库校验 ---")
			t.Logf("gp_traces_payment: %d (期望 %d)", paymentDocs, expectedPaymentSpans)
			t.Logf("%s: %d (期望 %d)", cfg.DefaultCollection, defaultDocs, expectedDefaultSpans)
			t.Logf("内存增量: %.2f MB", float64(m2.Alloc-m1.Alloc)/1024/1024)

			if stats["exportErrors"] > 0 {
				t.Fatalf("存在导出错误: %d", stats["exportErrors"])
			}
			if paymentDocs != expectedPaymentSpans {
				t.Fatalf("payment 集合文档数不匹配: 期望 %d, 实际 %d", expectedPaymentSpans, paymentDocs)
			}
			if defaultDocs != expectedDefaultSpans {
				t.Fatalf("default 集合文档数不匹配: 期望 %d, 实际 %d", expectedDefaultSpans, defaultDocs)
			}
		})
	}
}

func cleanupPaymentRouteCollections(t *testing.T, client *mongo.Client, cfg paymentRoutePerfConfig) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = client.Database(cfg.Database).Collection(paymentRouteCollection).Drop(ctx)
	_ = client.Database(cfg.Database).Collection(cfg.DefaultCollection).Drop(ctx)
}

func waitForMongoSpanCount(routingExp *exporter.MongoDBRoutingExporter, batchProcessor *processor.BatchSpanProcessor, expected int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := routingExp.GetStats()
		if stats["processed"] >= expected && batchProcessor.GetQueueLength() == 0 {
			time.Sleep(300 * time.Millisecond)
			stats = routingExp.GetStats()
			if stats["processed"] >= expected {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	stats := routingExp.GetStats()
	return fmt.Errorf("超时: 期望 %d spans, processed=%d, queue=%d",
		expected, stats["processed"], batchProcessor.GetQueueLength())
}

func countCollectionDocs(collection *mongo.Collection) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return collection.CountDocuments(ctx, map[string]any{})
}
