package core

import (
	"context"
	"sync"
	"testing"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

type dropSampler struct{}

func (dropSampler) ShouldSample(types.SamplingParameters) types.SamplingResult {
	return types.SamplingResult{Decision: types.SamplingDecisionDrop}
}

type lifecycleCaptureProcessor struct {
	mu         sync.Mutex
	endCount   int
	attributes []map[string]any
}

func (p *lifecycleCaptureProcessor) OnStart(context.Context, trace.Span) {}

func (p *lifecycleCaptureProcessor) OnEnd(snapshot trace.SpanSnapshot) {
	attributes := snapshot.GetAttributes()
	attributeCopy := make(map[string]any, len(attributes))
	for key, value := range attributes {
		attributeCopy[key] = value
	}
	p.mu.Lock()
	p.endCount++
	p.attributes = append(p.attributes, attributeCopy)
	p.mu.Unlock()
	snapshot.Release()
}

func (p *lifecycleCaptureProcessor) Shutdown(context.Context) error { return nil }

func TestStartInheritsRemoteSamplingDecision(t *testing.T) {
	parent := types.SpanContext{
		TraceID:      "0123456789abcdef0123456789abcdef",
		ParentSpanID: "0123456789abcdef",
		TraceFlags:   types.TraceFlagsSampled,
		Remote:       true,
	}
	ctx := baggage.WithContextSpanContext(context.Background(), parent)
	tracer := NewTracerImpl("test", nil, nil, dropSampler{})

	_, span := tracer.Start(ctx, "child")
	defer span.End()

	spanContext := span.SpanContext()
	if spanContext.TraceID != parent.TraceID {
		t.Fatalf("TraceID = %q, want %q", spanContext.TraceID, parent.TraceID)
	}
	if spanContext.ParentSpanID != parent.ParentSpanID {
		t.Fatalf("ParentSpanID = %q, want %q", spanContext.ParentSpanID, parent.ParentSpanID)
	}
	if spanContext.TraceFlags&types.TraceFlagsSampled == 0 {
		t.Fatal("sampled remote parent must produce a sampled child")
	}
}

func TestStartRejectsUnsampledRemoteParent(t *testing.T) {
	parent := types.SpanContext{
		TraceID:      "0123456789abcdef0123456789abcdef",
		ParentSpanID: "0123456789abcdef",
		Remote:       true,
	}
	ctx := baggage.WithContextSpanContext(context.Background(), parent)
	tracer := NewTracerImpl("test", nil, nil, nil)

	returnedCtx, span := tracer.Start(ctx, "child")
	if span.SpanContext().Validate() {
		t.Fatalf("unsampled remote parent should return a no-op span: %+v", span.SpanContext())
	}
	if returnedCtx != ctx {
		t.Fatal("dropped span should preserve the input context")
	}
}

func TestForceRecordDoesNotChangeRemoteSamplingFlag(t *testing.T) {
	parent := types.SpanContext{
		TraceID:      "0123456789abcdef0123456789abcdef",
		ParentSpanID: "0123456789abcdef",
		Remote:       true,
	}
	ctx := baggage.WithContextSpanContext(context.Background(), parent)
	processor := &lifecycleCaptureProcessor{}
	tracer := NewTracerImpl("test", nil, processor, nil)

	_, span := tracer.Start(ctx, "forced-child", func(config *types.SpanConfig) {
		config.ForceRecord = types.RecordPolicyAlways
	})
	if span.SpanContext().TraceFlags&types.TraceFlagsSampled != 0 {
		t.Fatalf("forced local record changed remote sampling flags: %02x", span.SpanContext().TraceFlags)
	}
	span.End()

	processor.mu.Lock()
	defer processor.mu.Unlock()
	if processor.endCount != 1 {
		t.Fatalf("forced span export count = %d, want 1", processor.endCount)
	}
}

func TestEndedSpanCannotMutateReusedState(t *testing.T) {
	processor := &lifecycleCaptureProcessor{}
	tracer := NewTracerImpl("test", nil, processor, recordOnlySampler{})

	_, ended := tracer.Start(context.Background(), "ended", func(config *types.SpanConfig) {
		config.ForceRecord = types.RecordPolicyAlways
	})
	ended.SetAttribute("owner", attribute.StringValue("ended"))
	ended.End()

	_, active := tracer.Start(context.Background(), "active", func(config *types.SpanConfig) {
		config.ForceRecord = types.RecordPolicyAlways
	})
	active.SetAttribute("owner", attribute.StringValue("active"))

	var waitGroup sync.WaitGroup
	for index := 0; index < 64; index++ {
		waitGroup.Add(1)
		go func(value int) {
			defer waitGroup.Done()
			ended.SetAttribute("stale", attribute.IntValue(value))
			ended.End()
		}(index)
	}
	waitGroup.Wait()
	active.End()

	processor.mu.Lock()
	defer processor.mu.Unlock()
	if processor.endCount != 2 {
		t.Fatalf("OnEnd count = %d, want 2", processor.endCount)
	}
	activeAttributes := processor.attributes[1]
	if _, exists := activeAttributes["stale"]; exists {
		t.Fatalf("ended span mutated a reused state: %#v", activeAttributes)
	}
	if activeAttributes["owner"] != "active" {
		t.Fatalf("active owner = %#v, want active", activeAttributes["owner"])
	}
	if active.GetSnapshot() != nil {
		t.Fatal("processor-managed span must not expose its owned snapshot")
	}
}
