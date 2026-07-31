package exporter

import (
	"testing"
	"time"

	"github.com/ClownSketch/tracer/trace"
)

var (
	_ trace.SpanExporter     = (*MongoDBV2Exporter)(nil)
	_ trace.SyncSpanExporter = (*MongoDBV2Exporter)(nil)
	_ trace.SpanExporter     = (*MongoDBRoutingV2Exporter)(nil)
	_ trace.SyncSpanExporter = (*MongoDBRoutingV2Exporter)(nil)
)

func TestMongoDBV2ExporterOptions(t *testing.T) {
	exporter := newMongoDBV2ExporterWithDefaults()

	WithMongoDBV2BatchSize(120)(exporter)
	WithMongoDBV2FlushInterval(3 * time.Second)(exporter)
	WithMongoDBV2Timeout(8 * time.Second)(exporter)
	WithMongoDBV2MaxConcurrentWrites(6)(exporter)
	WithMongoDBV2Retries(5, 400*time.Millisecond)(exporter)

	if exporter.batchSize != 120 {
		t.Fatalf("期望 batchSize=120，实际为 %d", exporter.batchSize)
	}
	if exporter.flushInterval != 3*time.Second {
		t.Fatalf("期望 flushInterval=3s，实际为 %s", exporter.flushInterval)
	}
	if exporter.timeout != 8*time.Second {
		t.Fatalf("期望 timeout=8s，实际为 %s", exporter.timeout)
	}
	if exporter.maxConcurrentWrites != 6 {
		t.Fatalf("期望 maxConcurrentWrites=6，实际为 %d", exporter.maxConcurrentWrites)
	}
	if exporter.maxRetries != 5 || exporter.retryDelay != 400*time.Millisecond {
		t.Fatalf(
			"期望重试配置为 5/400ms，实际为 %d/%s",
			exporter.maxRetries,
			exporter.retryDelay,
		)
	}
}

func TestNewMongoDBV2ExporterRequiresConnectionParameters(t *testing.T) {
	if _, err := NewMongoDBV2Exporter("", "", ""); err == nil {
		t.Fatal("期望空连接参数返回错误")
	}
}

func TestMongoDBRoutingV2ExporterCollectionWhitelist(t *testing.T) {
	exporter := &MongoDBRoutingV2Exporter{
		allowedCollections: map[string]struct{}{
			"trace_gateway": {},
		},
	}

	if !exporter.isCollectionAllowed("trace_gateway") {
		t.Fatal("期望白名单集合可用")
	}
	if exporter.isCollectionAllowed("trace_worker") {
		t.Fatal("期望非白名单集合被拒绝")
	}
}
