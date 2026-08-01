package integration

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/exporter"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/providers"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
)

// batchProcessorProfile BatchProcessor 参数组合（启动时一次性设置，运行期不可变）。
type batchProcessorProfile struct {
	name               string
	batchSize          int
	workers            int
	queueSize          int
	flushInterval      time.Duration
	mongoMaxConcurrent int
	mongoTimeout       time.Duration
	expectDrop         bool // 基线对照组，队列不足时仅告警不 fail
}

func setupPaymentRouteTracerWithProfile(t *testing.T, cfg paymentRoutePerfConfig, p batchProcessorProfile) (trace.Tracer, *processor.BatchSpanProcessor, *exporter.MongoDBRoutingExporter, func()) {
	t.Helper()

	mongoOpts := []exporter.MongoDBRoutingExporterOption{
		exporter.RoutingWithMongoOption(exporter.WithMongoDBTimeout(15 * time.Second)),
		exporter.RoutingWithMongoOption(exporter.WithMongoDBRetries(3, 100*time.Millisecond)),
	}
	if p.mongoTimeout > 0 {
		mongoOpts = append(mongoOpts, exporter.RoutingWithMongoOption(exporter.WithMongoDBTimeout(p.mongoTimeout)))
	}
	if p.mongoMaxConcurrent > 0 {
		mongoOpts = append(mongoOpts, exporter.RoutingWithMongoOption(exporter.WithMongoDBMaxConcurrentWrites(p.mongoMaxConcurrent)))
	}

	routingExp, err := exporter.NewMongoDBRoutingExporter(
		cfg.URI,
		cfg.Database,
		cfg.DefaultCollection,
		append(mongoOpts, exporter.WithMongoDBRoutingAllowedCollections(cfg.DefaultCollection, paymentRouteCollection))...,
	)
	if err != nil {
		t.Fatalf("创建路由导出器失败: %v", err)
	}

	batchProcessor := mustNewBatchSpanProcessor(t, routingExp,
		processor.WithBatchSize(p.batchSize),
		processor.WithWorkers(p.workers),
		processor.WithFlushInterval(p.flushInterval),
		processor.WithQueueSize(p.queueSize),
	)

	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(batchProcessor),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
		_ = routingExp.Shutdown(ctx)
	}

	return provider.GetTracer("gateway"), batchProcessor, routingExp, cleanup
}

type batchTuneResult struct {
	profile          batchProcessorProfile
	requests         int64
	expectedSpans    int64
	exportedSpans    int64
	exportErrors     int64
	paymentDocs      int64
	genElapsed       time.Duration
	e2eElapsed       time.Duration
	maxQueueLen      int64
	avgHandleUs      float64
	droppedSpans     int64
	sustainedReqQPS  float64
	sustainedSpanQPS float64
}

func runBatchTuneScenario(
	t *testing.T,
	cfg paymentRoutePerfConfig,
	p batchProcessorProfile,
	requestsTotal int,
	concurrency int,
) batchTuneResult {
	t.Helper()

	cleanupPaymentRouteCollections(t, mustConnectMongo(t, cfg.URI), cfg)

	tr, batchProcessor, routingExp, cleanup := setupPaymentRouteTracerWithProfile(t, cfg, p)
	defer cleanup()

	expectedSpans := int64(requestsTotal * 3)

	var (
		totalHandleNs int64
		maxQueueLen   int64
	)

	stopMonitor := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				ql := int64(batchProcessor.GetQueueLength())
				for {
					cur := atomic.LoadInt64(&maxQueueLen)
					if ql <= cur || atomic.CompareAndSwapInt64(&maxQueueLen, cur, ql) {
						break
					}
				}
			}
		}
	}()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	start := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	perWorker := requestsTotal / concurrency
	remainder := requestsTotal % concurrency

	for w := 0; w < concurrency; w++ {
		n := perWorker
		if w < remainder {
			n++
		}
		wg.Add(1)
		go func(workerID, count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				sem <- struct{}{}
				elapsed := simulatePaymentRouteRequest(tr, workerID*100000+i, cfg.RunID)
				atomic.AddInt64(&totalHandleNs, elapsed.Nanoseconds())
				<-sem
			}
		}(w, n)
	}
	wg.Wait()
	genElapsed := time.Since(start)

	close(stopMonitor)

	waitTimeout := 120 * time.Second
	if err := waitForMongoSpanCount(routingExp, batchProcessor, expectedSpans, waitTimeout); err != nil {
		t.Logf("[%s] 等待落库: %v (processed=%d queue=%d)", p.name, err,
			routingExp.GetStats()["processed"], batchProcessor.GetQueueLength())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_ = batchProcessor.Shutdown(shutdownCtx)

	e2eElapsed := time.Since(start)
	runtime.ReadMemStats(&m2)
	_ = m1
	_ = m2

	stats := routingExp.GetStats()
	exported := stats["processed"]
	errors := stats["exportErrors"]

	client := mustConnectMongo(t, cfg.URI)
	defer disconnectMongo(t, client)
	paymentDocs, _ := countCollectionDocs(client.Database(cfg.Database).Collection(paymentRouteCollection))

	dropped := expectedSpans - exported
	if dropped < 0 {
		dropped = 0
	}

	return batchTuneResult{
		profile:          p,
		requests:         int64(requestsTotal),
		expectedSpans:    expectedSpans,
		exportedSpans:    exported,
		exportErrors:     errors,
		paymentDocs:      paymentDocs,
		genElapsed:       genElapsed,
		e2eElapsed:       e2eElapsed,
		maxQueueLen:      maxQueueLen,
		avgHandleUs:      float64(totalHandleNs) / float64(requestsTotal) / 1000.0,
		droppedSpans:     dropped,
		sustainedReqQPS:  float64(requestsTotal) / genElapsed.Seconds(),
		sustainedSpanQPS: float64(exported) / e2eElapsed.Seconds(),
	}
}

func batchTuneProfiles() []batchProcessorProfile {
	return []batchProcessorProfile{
		{
			name:               "A-initTracer默认(batch50/worker2/queue自动1万)",
			batchSize:          50,
			workers:            2,
			queueSize:          10000,
			flushInterval:      2 * time.Second,
			mongoMaxConcurrent: 4,
			expectDrop:         true,
		},
		{
			name:               "B-旧bench(batch100/worker8/queue10万)",
			batchSize:          100,
			workers:            8,
			queueSize:          100000,
			flushInterval:      200 * time.Millisecond,
			mongoMaxConcurrent: 8,
			expectDrop:         true,
		},
		{
			name:               "C-推荐2万QPS(batch500/worker16/queue30万)",
			batchSize:          500,
			workers:            16,
			queueSize:          300000,
			flushInterval:      100 * time.Millisecond,
			mongoMaxConcurrent: 16,
		},
		{
			name:               "D-推荐4万QPS(batch1000/worker24/queue50万)",
			batchSize:          1000,
			workers:            24,
			queueSize:          500000,
			flushInterval:      50 * time.Millisecond,
			mongoMaxConcurrent: 24,
		},
		{
			name:               "E-激进(batch1000/worker32/queue80万)",
			batchSize:          1000,
			workers:            32,
			queueSize:          800000,
			flushInterval:      50 * time.Millisecond,
			mongoMaxConcurrent: 32,
			mongoTimeout:       20 * time.Second,
		},
	}
}

// TestPaymentRoute_BatchProcessorTuning_20k40kQPS 对比不同 BatchProcessor 参数在 2万/4万 请求突发下的表现。
//
// 运行:
//
//	MONGO_URI='mongodb://<username>:<password>@127.0.0.1:27017/?authSource=admin' \
//	go test ./integration -run TestPaymentRoute_BatchProcessorTuning_20k40kQPS -v -count=1 -timeout=30m
func TestPaymentRoute_BatchProcessorTuning_20k40kQPS(t *testing.T) {
	cfg := loadPaymentRoutePerfConfig(t)
	client := mustConnectMongo(t, cfg.URI)
	defer disconnectMongo(t, client)

	loads := []struct {
		name        string
		requests    int
		concurrency int
		targetQPS   int
	}{
		{name: "2万请求突发(目标~2万QPS)", requests: 20000, concurrency: 512, targetQPS: 20000},
		{name: "4万请求突发(目标~4万QPS)", requests: 40000, concurrency: 512, targetQPS: 40000},
	}

	for _, load := range loads {
		t.Run(load.name, func(t *testing.T) {
			var results []batchTuneResult
			for _, profile := range batchTuneProfiles() {
				t.Run(profile.name, func(t *testing.T) {
					res := runBatchTuneScenario(t, cfg, profile, load.requests, load.concurrency)
					results = append(results, res)

					t.Logf("--- 参数: batchSize=%d workers=%d queueSize=%d flush=%v mongoConcurrent=%d ---",
						profile.batchSize, profile.workers, profile.queueSize, profile.flushInterval, profile.mongoMaxConcurrent)
					t.Logf("请求数=%d 期望spans=%d 导出spans=%d 落库=%d 丢弃=%d 导出错误=%d",
						res.requests, res.expectedSpans, res.exportedSpans, res.paymentDocs, res.droppedSpans, res.exportErrors)
					t.Logf("生成阶段: %v, 请求QPS=%.0f, avgHandle=%.1fµs, 峰值队列=%d",
						res.genElapsed, res.sustainedReqQPS, res.avgHandleUs, res.maxQueueLen)
					t.Logf("端到端(含落库): %v, spanQPS=%.0f", res.e2eElapsed, res.sustainedSpanQPS)

					if res.exportErrors > 0 {
						t.Errorf("导出错误: %d", res.exportErrors)
					}
					if res.droppedSpans > 0 {
						msg := fmt.Sprintf("队列溢出丢弃 spans: %d (queueSize=%d 不足或 Mongo 过慢)", res.droppedSpans, profile.queueSize)
						if profile.expectDrop {
							t.Log("⚠️ [基线预期] " + msg)
						} else {
							t.Error(msg)
						}
					}
					if res.paymentDocs != res.expectedSpans {
						msg := fmt.Sprintf("落库不完整: 期望 %d, 实际 %d", res.expectedSpans, res.paymentDocs)
						if profile.expectDrop {
							t.Log("⚠️ [基线预期] " + msg)
						} else {
							t.Error(msg)
						}
					}
				})
			}

			if len(results) > 0 {
				best := results[0]
				for _, r := range results[1:] {
					if r.droppedSpans > 0 || r.exportErrors > 0 {
						continue
					}
					if r.sustainedSpanQPS > best.sustainedSpanQPS {
						best = r
					}
				}
				t.Logf("")
				t.Logf("========== %s 小结: 最优配置 %s (spanQPS=%.0f, 峰值队列=%d) ==========",
					load.name, best.profile.name, best.sustainedSpanQPS, best.maxQueueLen)
			}
		})
	}
}

// TestPaymentRoute_InitTracerConfigExample 演示主项目 InitTracer 静态配置写法（不连 Mongo，仅编译校验）。
func TestPaymentRoute_InitTracerConfigExample(t *testing.T) {
	cfg := providers.TracerConfig{
		ServiceName:                "gateway",
		ExporterType:               providers.ExporterTypeMongoDBRouting,
		MongoDBURI:                 "mongodb://127.0.0.1:27017",
		MongoDBDatabase:            "tracer",
		MongoDBCollection:          "traces_default",
		BatchSize:                  500,
		BatchInterval:              100 * time.Millisecond,
		Workers:                    16,
		QueueSize:                  300000,
		MongoDBMaxConcurrentWrites: 16,
		MongoDBTimeout:             15 * time.Second,
		FallbackDir:                t.TempDir(),
	}
	if cfg.BatchSize != 500 || cfg.QueueSize != 300000 {
		t.Fatalf("配置示例异常")
	}
	t.Logf("InitTracer 静态配置示例: batchSize=%d workers=%d queueSize=%d flush=%v",
		cfg.BatchSize, cfg.Workers, cfg.QueueSize, cfg.BatchInterval)
	t.Log("providers.InitTracer(cfg)  // 进程启动时调用一次，运行期不可动态修改")
}
