package exporter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
)

func TestMongoDBRoutingExporter_isCollectionAllowed(t *testing.T) {
	e := &MongoDBRoutingExporter{
		allowedCollections: map[string]struct{}{
			"gp_traces_webhook": {},
			"gp_traces_default": {},
		},
	}

	if !e.isCollectionAllowed("gp_traces_webhook") {
		t.Fatal("expected webhook collection to be allowed")
	}
	if e.isCollectionAllowed("gp_traces_other") {
		t.Fatal("expected unknown collection to be rejected")
	}

	open := &MongoDBRoutingExporter{}
	if !open.isCollectionAllowed("any_collection") {
		t.Fatal("expected no whitelist to allow all collections")
	}
}

func TestMongoDBRoutingExporters_ReleaseSyncSnapshotsOnRouteError(t *testing.T) {
	tests := []struct {
		name   string
		export func(*mock.SpanSnapshotMock) error
	}{
		{
			name: "mongo-driver-v1",
			export: func(span *mock.SpanSnapshotMock) error {
				routingExporter := &MongoDBRoutingExporter{base: &MongoDBExporter{}}
				return routingExporter.ExportSpansSync(context.Background(), []trace.SpanSnapshot{span})
			},
		},
		{
			name: "mongo-driver-v2",
			export: func(span *mock.SpanSnapshotMock) error {
				routingExporter := &MongoDBRoutingV2Exporter{base: &MongoDBV2Exporter{}}
				return routingExporter.ExportSpansSync(context.Background(), []trace.SpanSnapshot{span})
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var releaseCount atomic.Int64
			span := mock.NewSpanSnapshotMock(1)
			span.ReleaseFunc = func() {
				releaseCount.Add(1)
			}
			if err := testCase.export(span); err == nil {
				t.Fatal("缺少默认 MongoDB 集合时应返回错误")
			}
			if releaseCount.Load() != 1 {
				t.Fatalf("同步路由失败后快照释放次数错误: %d", releaseCount.Load())
			}
		})
	}
}
