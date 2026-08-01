package core

import (
	"context"
	"testing"

	"github.com/ClownSketch/tracer/attribute"
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

func TestPropagationAttributesInheritance(t *testing.T) {
	tr := NewTracerImpl("test", nil, nil, sampler.NewAlwaysSampleSampler())

	parentCtx, parent := tr.Start(context.Background(), "parent")
	parent.SetGlobalAttribute("region", attribute.StringValue("IN"))
	parent.SetInheritedAttribute("request_id", attribute.StringValue("req-1"))

	_, child := tr.Start(parentCtx, "child")
	globalAttributes := child.GetGlobalAttributes()
	if value := globalAttributes["region"].Value.String(); value != "IN" {
		t.Fatalf("子 Span 全局属性=%q，期望=IN", value)
	}
	inheritedAttributes := child.GetInheritedAttributes()
	if value := inheritedAttributes["request_id"].Value.String(); value != "req-1" {
		t.Fatalf("子 Span 继承属性=%q，期望=req-1", value)
	}

	child.End()
	if snapshot := child.GetSnapshot(); snapshot != nil {
		snapshot.Release()
	}
	parent.End()
	if snapshot := parent.GetSnapshot(); snapshot != nil {
		snapshot.Release()
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
