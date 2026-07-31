package core

import (
	"context"
	"testing"

	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/types"
)

func TestMongoCollectionInheritance(t *testing.T) {
	tr := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	parentCtx, parent := tr.Start(context.Background(), "http.request",
		func(c *types.SpanConfig) {
			c.SpanKind = types.SpanKindServer
		},
	)
	parent.SetMongoCollection("gp_traces_webhook")

	_, child := tr.Start(parentCtx, "async.task",
		func(c *types.SpanConfig) {
			c.SpanKind = types.SpanKindAsync
		},
	)
	if got := child.GetMongoCollection(); got != "gp_traces_webhook" {
		t.Fatalf("expected inherited collection gp_traces_webhook, got %q", got)
	}

	_, override := tr.Start(parentCtx, "other.task",
		func(c *types.SpanConfig) {
			c.MongoCollection = "gp_traces_payments"
		},
	)
	if got := override.GetMongoCollection(); got != "gp_traces_payments" {
		t.Fatalf("expected override collection gp_traces_payments, got %q", got)
	}
}

func TestMongoCollectionSnapshot(t *testing.T) {
	tr := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	_, span := tr.Start(context.Background(), "test.span", func(c *types.SpanConfig) {
		c.ForceRecord = types.RecordPolicyAlways
	})
	span.SetMongoCollection("gp_traces_default")
	span.End()

	snapshot := span.GetSnapshot()
	if snapshot == nil {
		t.Fatal("expected snapshot")
	}
	defer snapshot.Release()

	if got := snapshot.GetMongoCollection(); got != "gp_traces_default" {
		t.Fatalf("expected snapshot collection gp_traces_default, got %q", got)
	}
}
