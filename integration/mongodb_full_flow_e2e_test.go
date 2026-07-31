package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tracerpkg "github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/exporter"
	ginmiddleware "github.com/ClownSketch/tracer/middleware/gin"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/providers"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/types"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoE2EConfig struct {
	URI         string
	Database    string
	Collection  string
	Requests    int
	Concurrency int
	RunID       string
}

// TestMongoDBExporter_FullFlowE2E 会完整覆盖:
// HTTP 请求 -> Gin 中间件 -> TracerProvider -> BatchSpanProcessor -> MongoDBExporter -> MongoDB 落库校验
//
// 运行方式:
// MONGO_URI='mongodb://127.0.0.1:27017' \
// TRACER_E2E_MONGO_DATABASE='tracer_e2e' \
// go test ./integration -run TestMongoDBExporter_FullFlowE2E -count=1 -v
func TestMongoDBExporter_FullFlowE2E(t *testing.T) {
	cfg := loadMongoE2EConfig(t)

	verifyClient := mustConnectMongo(t, cfg.URI)
	defer disconnectMongo(t, verifyClient)

	collection := verifyClient.Database(cfg.Database).Collection(cfg.Collection)
	defer cleanupMongoRunDocuments(t, collection, cfg.RunID)

	exp, err := exporter.NewMongoDBExporter(
		cfg.URI,
		cfg.Database,
		cfg.Collection,
		exporter.WithMongoDBBatchSize(20),
		exporter.WithMongoDBFlushInterval(150*time.Millisecond),
		exporter.WithMongoDBQueueSize(4000),
		exporter.WithMongoDBMaxConcurrentWrites(8),
		exporter.WithMongoDBTimeout(10*time.Second),
		exporter.WithMongoDBRetries(3, 200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建 MongoDBExporter 失败: %v", err)
	}

	batchProcessor := processor.NewBatchSpanProcessor(
		exp,
		processor.WithBatchSize(20),
		processor.WithWorkers(8),
		processor.WithFlushInterval(150*time.Millisecond),
		processor.WithQueueSize(4000),
	)

	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(batchProcessor),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)

	previousProvider := tracerpkg.GetTracerProvider()
	tracerpkg.SetTracerProvider(provider, "default")
	defer tracerpkg.SetTracerProvider(previousProvider, "default")

	router := newMongoE2ERouter(cfg)
	server := httptest.NewServer(router)
	defer server.Close()

	expectedDocs := int64(cfg.Requests * 3)
	if err := runMongoE2ERequests(server.URL+"/checkout", cfg); err != nil {
		t.Fatalf("执行 HTTP e2e 请求失败: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("关闭 TracerProvider 失败: %v", err)
	}

	if err := waitForMongoDocumentCount(collection, cfg.RunID, expectedDocs, 15*time.Second); err != nil {
		t.Fatalf("等待 MongoDB 落库失败: %v", err)
	}

	count, err := collection.CountDocuments(context.Background(), bson.M{"attributes.run_id": cfg.RunID})
	if err != nil {
		t.Fatalf("统计 MongoDB 文档失败: %v", err)
	}
	if count != expectedDocs {
		t.Fatalf("期望写入 %d 条文档，实际写入 %d 条", expectedDocs, count)
	}

	var serverSpanDoc bson.M
	if err := collection.FindOne(context.Background(), bson.M{
		"attributes.run_id": cfg.RunID,
		"name":              "POST /checkout",
	}).Decode(&serverSpanDoc); err != nil {
		t.Fatalf("查询服务端 span 文档失败: %v", err)
	}

	assertMongoServerSpanDoc(t, serverSpanDoc, cfg.RunID)
}

// TestMongoDBExporter_WALFullFlowE2E 会完整覆盖:
// HTTP 请求 -> Gin 中间件 -> TracerProvider -> WALSpanProcessor -> MongoDBExporter -> MongoDB 落库校验
func TestMongoDBExporter_WALFullFlowE2E(t *testing.T) {
	cfg := loadMongoE2EConfig(t)

	verifyClient := mustConnectMongo(t, cfg.URI)
	defer disconnectMongo(t, verifyClient)

	collection := verifyClient.Database(cfg.Database).Collection(cfg.Collection)
	defer cleanupMongoRunDocuments(t, collection, cfg.RunID)

	exp, err := exporter.NewMongoDBExporter(
		cfg.URI,
		cfg.Database,
		cfg.Collection,
		exporter.WithMongoDBBatchSize(20),
		exporter.WithMongoDBFlushInterval(150*time.Millisecond),
		exporter.WithMongoDBQueueSize(4000),
		exporter.WithMongoDBMaxConcurrentWrites(8),
		exporter.WithMongoDBTimeout(10*time.Second),
		exporter.WithMongoDBRetries(3, 200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建 MongoDBExporter 失败: %v", err)
	}

	walProcessor, err := processor.NewWALSpanProcessor(
		exp,
		processor.WithWALDir(t.TempDir()),
		processor.WithWALPollInterval(20*time.Millisecond),
		processor.WithWALExportBatchSize(20),
		processor.WithWALSegmentSize(4*1024*1024),
	)
	if err != nil {
		t.Fatalf("创建 WAL 处理器失败: %v", err)
	}

	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(walProcessor),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)

	previousProvider := tracerpkg.GetTracerProvider()
	tracerpkg.SetTracerProvider(provider, "default")
	defer tracerpkg.SetTracerProvider(previousProvider, "default")

	router := newMongoE2ERouter(cfg)
	server := httptest.NewServer(router)
	defer server.Close()

	expectedDocs := int64(cfg.Requests * 3)
	if err := runMongoE2ERequests(server.URL+"/checkout", cfg); err != nil {
		t.Fatalf("执行 HTTP WAL e2e 请求失败: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("关闭 TracerProvider 失败: %v", err)
	}

	if err := waitForMongoDocumentCount(collection, cfg.RunID, expectedDocs, 15*time.Second); err != nil {
		t.Fatalf("等待 MongoDB WAL 落库失败: %v", err)
	}

	count, err := collection.CountDocuments(context.Background(), bson.M{"attributes.run_id": cfg.RunID})
	if err != nil {
		t.Fatalf("统计 MongoDB 文档失败: %v", err)
	}
	if count != expectedDocs {
		t.Fatalf("期望写入 %d 条文档，实际写入 %d 条", expectedDocs, count)
	}
}

// TestMongoDBExporter_EventAttributesSchemaE2E 验证事件属性使用固定字段写入 MongoDB。
// 该测试使用随机集合且不删除任何数据，避免影响已有测试或业务集合。
func TestMongoDBExporter_EventAttributesSchemaE2E(t *testing.T) {
	cfg := loadMongoE2EConfig(t)

	verifyClient := mustConnectMongo(t, cfg.URI)
	defer disconnectMongo(t, verifyClient)

	exp, err := exporter.NewMongoDBExporter(
		cfg.URI,
		cfg.Database,
		cfg.Collection,
		exporter.WithMongoDBBatchSize(1),
		exporter.WithMongoDBFlushInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建 MongoDBExporter 失败: %v", err)
	}

	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(processor.NewSimpleSpanProcessor(exp)),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)

	_, span := provider.GetTracer("mongo-event-schema-e2e").Start(
		context.Background(),
		"event-attributes-schema",
		tracerpkg.WithForceRecord(),
	)
	span.SetAttributes(attribute.String("run_id", cfg.RunID))
	span.AddEvent("request.payload", "json", func() map[string]any {
		return map[string]any{
			"run_id": cfg.RunID,
			"stage":  "checkout",
		}
	})
	span.End()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("关闭 TracerProvider 失败: %v", err)
	}

	collection := verifyClient.Database(cfg.Database).Collection(cfg.Collection)
	var doc bson.M
	if err := collection.FindOne(context.Background(), bson.M{
		"attributes.run_id": cfg.RunID,
		"name":              "event-attributes-schema",
	}).Decode(&doc); err != nil {
		t.Fatalf("查询事件属性验证文档失败: %v", err)
	}

	assertMongoEventAttributes(t, doc, cfg.RunID)
}

// TestMongoDBV2Exporter_EventAttributesSchemaE2E 验证 Mongo Driver v2 的事件属性落库结构。
// 该测试使用随机集合且不删除任何数据，避免影响已有测试或业务集合。
func TestMongoDBV2Exporter_EventAttributesSchemaE2E(t *testing.T) {
	cfg := loadMongoE2EConfig(t)

	verifyClient := mustConnectMongo(t, cfg.URI)
	defer disconnectMongo(t, verifyClient)

	exp, err := exporter.NewMongoDBV2Exporter(
		cfg.URI,
		cfg.Database,
		cfg.Collection,
		exporter.WithMongoDBV2BatchSize(1),
		exporter.WithMongoDBV2FlushInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建 MongoDBV2Exporter 失败: %v", err)
	}

	provider := providers.NewTracerProvider(
		providers.WithSpanProcessor(processor.NewSimpleSpanProcessor(exp)),
		providers.WithSampler(sampler.NewAlwaysSampleSampler()),
	)

	_, span := provider.GetTracer("mongo-v2-event-schema-e2e").Start(
		context.Background(),
		"event-attributes-schema-v2",
		tracerpkg.WithForceRecord(),
	)
	span.SetAttributes(attribute.String("run_id", cfg.RunID))
	span.AddEvent("request.payload", "json", func() map[string]any {
		return map[string]any{
			"run_id": cfg.RunID,
			"stage":  "checkout",
		}
	})
	span.End()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("关闭 TracerProvider 失败: %v", err)
	}

	collection := verifyClient.Database(cfg.Database).Collection(cfg.Collection)
	var doc bson.M
	if err := collection.FindOne(context.Background(), bson.M{
		"attributes.run_id": cfg.RunID,
		"name":              "event-attributes-schema-v2",
	}).Decode(&doc); err != nil {
		t.Fatalf("查询 Mongo Driver v2 事件属性验证文档失败: %v", err)
	}

	assertMongoEventAttributes(t, doc, cfg.RunID)
}

func loadMongoE2EConfig(t *testing.T) mongoE2EConfig {
	t.Helper()

	uri := firstNonEmpty(os.Getenv("TRACER_E2E_MONGO_URI"), os.Getenv("MONGO_URI"))
	if uri == "" {
		t.Skip("未设置 TRACER_E2E_MONGO_URI 或 MONGO_URI，跳过 Mongo e2e 测试")
	}

	runID := fmt.Sprintf("mongo-e2e-%d", time.Now().UnixNano())
	database := firstNonEmpty(os.Getenv("TRACER_E2E_MONGO_DATABASE"), os.Getenv("MONGO_DATABASE"), "tracer_e2e")
	collection := firstNonEmpty(os.Getenv("TRACER_E2E_MONGO_COLLECTION"), fmt.Sprintf("traces_%s", runID))

	return mongoE2EConfig{
		URI:         uri,
		Database:    database,
		Collection:  collection,
		Requests:    parseEnvInt("TRACER_E2E_REQUESTS", 48),
		Concurrency: parseEnvInt("TRACER_E2E_CONCURRENCY", 8),
		RunID:       runID,
	}
}

func newMongoE2ERouter(cfg mongoE2EConfig) http.Handler {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(ginmiddleware.GinMiddleware())

	router.POST("/checkout", func(c *gin.Context) {
		span := baggage.GetSpanContext(c.Request.Context())

		var payload map[string]any
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		requestID := fmt.Sprintf("%v", payload["request_id"])
		lang := fmt.Sprintf("%v", payload["lang"])

		span.SetAttributes(
			attribute.String("run_id", cfg.RunID),
			attribute.String("request_id", requestID),
			attribute.String("route_name", "checkout"),
			attribute.String("lang", lang),
		)
		span.SetResource(&types.ResourceInfo{
			ServiceName: "mongo-e2e-service",
			Host:        "integration-test",
			Attributes: map[string]any{
				"run_id": cfg.RunID,
				"lang":   lang,
				"labels": []any{"mongo", "e2e", "full-flow"},
			},
		})
		span.SetResourceUsage(&types.ResourceMetrics{
			CPUUsage:    0.42,
			MemoryUsage: 64,
			NetworkIO:   8,
		})
		span.AddEvent("request.payload", "json", func() map[string]any {
			return map[string]any{
				"run_id": cfg.RunID,
				"payload": map[string]any{
					"lang":  lang,
					"order": payload,
				},
				"labels": []any{"checkout", "mongo-e2e"},
			}
		})
		span.AddLog(types.SpanLog{
			Timestamp: time.Now().Format(time.RFC3339Nano),
			Severity:  types.SpanLogSeverityInfo,
			Message:   "handler accepted payload",
			Fields: map[string]any{
				"run_id": cfg.RunID,
				"body": map[string]any{
					"lang":  lang,
					"order": payload,
				},
			},
			Attributes: map[string]any{
				"stage": "handler",
				"lang":  lang,
			},
		})

		dbCtx, dbSpan := tracerpkg.GetTracer("").Start(
			c.Request.Context(),
			"mongo-persist",
			tracerpkg.WithForceRecord(),
			tracerpkg.WithSpanKind(types.SpanKindInternal),
		)
		dbSpan.SetAttributes(
			attribute.String("run_id", cfg.RunID),
			attribute.String("request_id", requestID),
			attribute.String("db_action", "persist"),
		)
		dbSpan.AddEvent("db.payload", "json", func() map[string]any {
			return map[string]any{
				"run_id": cfg.RunID,
				"meta": map[string]any{
					"lang": lang,
				},
				"items": []any{
					map[string]any{"sku": "sku-1", "qty": 1},
					map[string]any{"sku": "sku-2", "qty": 2},
				},
			}
		})
		dbSpan.AddLog(types.SpanLog{
			Timestamp: time.Now().Format(time.RFC3339Nano),
			Severity:  types.SpanLogSeverityInfo,
			Message:   "db span completed",
			Fields: map[string]any{
				"run_id": cfg.RunID,
				"meta": map[string]any{
					"lang": lang,
				},
			},
		})
		dbSpan.End()
		_ = dbCtx

		asyncCtx, asyncSpan := baggage.StartAsyncSpan(
			c.Request.Context(),
			tracerpkg.GetTracer(""),
			"async-enrich",
			tracerpkg.WithForceRecord(),
			tracerpkg.WithSpanKind(types.SpanKindAsync),
		)
		asyncSpan.SetAttributes(
			attribute.String("run_id", cfg.RunID),
			attribute.String("request_id", requestID),
			attribute.String("job", "enrich"),
		)
		asyncSpan.AddEvent("async.payload", "json", func() map[string]any {
			return map[string]any{
				"run_id": cfg.RunID,
				"meta": map[string]any{
					"lang":   lang,
					"source": "async-worker",
				},
			}
		})
		asyncSpan.AddLog(types.SpanLog{
			Timestamp: time.Now().Format(time.RFC3339Nano),
			Severity:  types.SpanLogSeverityInfo,
			Message:   "async enrichment complete",
			Fields: map[string]any{
				"run_id": cfg.RunID,
				"body": map[string]any{
					"lang":  lang,
					"extra": []any{"fraud-check", "inventory-check"},
				},
			},
		})
		asyncSpan.End()
		_ = asyncCtx

		c.JSON(http.StatusOK, gin.H{
			"ok":         true,
			"run_id":     cfg.RunID,
			"request_id": requestID,
		})
	})

	return router
}

func runMongoE2ERequests(url string, cfg mongoE2EConfig) error {
	var nextRequestID int64
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.Requests)
	sem := make(chan struct{}, cfg.Concurrency)
	transport := &http.Transport{
		MaxIdleConns:        cfg.Concurrency * 2,
		MaxIdleConnsPerHost: cfg.Concurrency,
		IdleConnTimeout:     30 * time.Second,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	for i := 0; i < cfg.Requests; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			requestID := atomic.AddInt64(&nextRequestID, 1)
			body := map[string]any{
				"request_id": requestID,
				"lang":       "zh-CN",
				"customer": map[string]any{
					"id":   fmt.Sprintf("user-%d", requestID),
					"lang": "zh-CN",
				},
				"items": []map[string]any{
					{"sku": "sku-1", "qty": 1},
					{"sku": "sku-2", "qty": 2},
				},
				"metadata": map[string]any{
					"lang": "zh-CN",
					"tags": []any{"checkout", "mongo", "trace"},
				},
			}

			payload, err := json.Marshal(body)
			if err != nil {
				errCh <- fmt.Errorf("序列化请求体失败: %w", err)
				return
			}

			req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
			if err != nil {
				errCh <- fmt.Errorf("创建请求失败: %w", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				errCh <- fmt.Errorf("发送请求失败: %w", err)
				return
			}

			data, readErr := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if readErr != nil {
				errCh <- fmt.Errorf("读取响应失败: %w", readErr)
				return
			}
			if closeErr != nil {
				errCh <- fmt.Errorf("关闭响应失败: %w", closeErr)
				return
			}
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("响应状态异常: %d body=%s", resp.StatusCode, string(data))
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func waitForMongoDocumentCount(collection *mongo.Collection, runID string, expected int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	filter := bson.M{"attributes.run_id": runID}

	for {
		count, err := collection.CountDocuments(context.Background(), filter)
		if err == nil && count == expected {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("超时未等到预期文档数，期望=%d 实际=%d", expected, count)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func assertMongoServerSpanDoc(t *testing.T, doc bson.M, runID string) {
	t.Helper()

	if doc["name"] != "POST /checkout" {
		t.Fatalf("期望服务端 span 名称为 POST /checkout，实际为 %#v", doc["name"])
	}

	attrs, ok := doc["attributes"].(bson.M)
	if !ok {
		t.Fatalf("期望 attributes 为 bson.M，实际为 %T", doc["attributes"])
	}
	if attrs["run_id"] != runID {
		t.Fatalf("期望 run_id=%s，实际为 %#v", runID, attrs["run_id"])
	}

	resource, ok := doc["resource"].(bson.M)
	if !ok {
		t.Fatalf("期望 resource 为 bson.M，实际为 %T", doc["resource"])
	}
	if resource["service_name"] != "mongo-e2e-service" {
		t.Fatalf("期望 service_name 为 mongo-e2e-service，实际为 %#v", resource["service_name"])
	}

	logs, ok := doc["logs"].(bson.A)
	if !ok || len(logs) == 0 {
		t.Fatalf("期望 logs 非空，实际为 %T %#v", doc["logs"], doc["logs"])
	}

	events, ok := doc["events"].(bson.A)
	if !ok || len(events) == 0 {
		t.Fatalf("期望 events 非空，实际为 %T %#v", doc["events"], doc["events"])
	}

	assertMongoEventAttributes(t, doc, runID)
}

func assertMongoEventAttributes(t *testing.T, doc bson.M, runID string) {
	t.Helper()

	events, ok := doc["events"].(bson.A)
	if !ok || len(events) == 0 {
		t.Fatalf("期望 events 非空，实际为 %T %#v", doc["events"], doc["events"])
	}

	var event bson.M
	for _, item := range events {
		candidate, candidateOK := item.(bson.M)
		if candidateOK && candidate["name"] == "request.payload" {
			event = candidate
			break
		}
	}
	if event == nil {
		t.Fatalf("未找到 request.payload 事件，实际 events=%#v", events)
	}
	eventAttributes, ok := event["attributes"].(bson.M)
	if !ok {
		t.Fatalf("期望 events[0].attributes 为 bson.M，实际事件文档为 %#v", event)
	}
	eventValues, ok := eventAttributes["json"].(bson.A)
	if !ok || len(eventValues) == 0 {
		t.Fatalf("期望事件 attributes.json 非空，实际为 %T %#v", eventAttributes["json"], eventAttributes["json"])
	}
	eventValue, ok := eventValues[0].(bson.M)
	if !ok {
		t.Fatalf("期望事件 attributes.json[0] 为 bson.M，实际为 %T %#v", eventValues[0], eventValues[0])
	}
	if eventValue["run_id"] != runID {
		t.Fatalf("期望事件属性 run_id=%s，实际为 %#v", runID, eventValue["run_id"])
	}
}

func mustConnectMongo(t *testing.T, uri string) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func disconnectMongo(t *testing.T, client *mongo.Client) {
	t.Helper()
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		t.Fatalf("断开 MongoDB 连接失败: %v", err)
	}
}

func cleanupMongoRunDocuments(t *testing.T, collection *mongo.Collection, runID string) {
	t.Helper()
	if collection == nil || runID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := collection.DeleteMany(ctx, bson.M{"attributes.run_id": runID}); err != nil {
		t.Fatalf("清理 MongoDB 测试数据失败: %v", err)
	}
}

func parseEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return defaultValue
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
