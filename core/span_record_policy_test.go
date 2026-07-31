package core

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// recordOnlySampler 创建 Span 但不触发「采样即始终导出」升格。
type recordOnlySampler struct{}

func (recordOnlySampler) ShouldSample(types.SamplingParameters) types.SamplingResult {
	return types.SamplingResult{Decision: types.SamplingDecisionRecordOnly}
}

type captureProcessor struct {
	mu    sync.Mutex
	ends  int
	names []string
}

func (p *captureProcessor) OnStart(context.Context, trace.Span) {}

func (p *captureProcessor) OnEnd(snap trace.SpanSnapshot) {
	p.mu.Lock()
	p.ends++
	if snap != nil {
		p.names = append(p.names, snap.GetSpanName())
	}
	p.mu.Unlock()
	if snap != nil {
		snap.Release()
	}
}

func (p *captureProcessor) Shutdown(context.Context) error { return nil }

func (p *captureProcessor) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ends
}

func newTestTracer(t *testing.T, proc trace.SpanProcessor) trace.Tracer {
	t.Helper()
	provider := NewTracerImpl("test", nil, proc, sampler.NewAlwaysSampleSampler())
	return provider
}

func TestRecordPolicyNone_NoExportOnSuccess(t *testing.T) {
	proc := &captureProcessor{}
	provider := NewTracerImpl("test", nil, proc, recordOnlySampler{})
	tr := provider

	_, span := tr.Start(context.Background(), "ok")
	span.End()

	if proc.count() != 0 {
		t.Fatalf("expected no export, got %d", proc.count())
	}
}

func TestRecordPolicyAlways_ExportOnSuccess(t *testing.T) {
	proc := &captureProcessor{}
	tr := newTestTracer(t, proc)

	_, span := tr.Start(context.Background(), "ok", func(c *types.SpanConfig) {
		c.ForceRecord = types.RecordPolicyAlways
	})
	span.End()

	if proc.count() != 1 {
		t.Fatalf("expected 1 export, got %d", proc.count())
	}
}

func TestRecordPolicyOnError_ExportOnlyWithError(t *testing.T) {
	proc := &captureProcessor{}
	tr := newTestTracer(t, proc)

	t.Run("success", func(t *testing.T) {
		_, span := tr.Start(context.Background(), "ok", func(c *types.SpanConfig) {
			c.ForceRecord = types.RecordPolicyOnError
		})
		span.End()
	})

	if proc.count() != 0 {
		t.Fatalf("success path: expected no export, got %d", proc.count())
	}

	t.Run("with_error", func(t *testing.T) {
		_, span := tr.Start(context.Background(), "fail", func(c *types.SpanConfig) {
			c.ForceRecord = types.RecordPolicyOnError
		})
		span.RecordError(errors.New("boom"))
		span.End()
	})

	if proc.count() != 1 {
		t.Fatalf("error path: expected 1 export, got %d", proc.count())
	}
}

func TestRecordPolicyOnError_RuntimeWithRecordOnError(t *testing.T) {
	proc := &captureProcessor{}
	tr := newTestTracer(t, proc)

	_, span := tr.Start(context.Background(), "fail")
	span.WithRecordOnError()
	span.WithError(errors.New("boom"), "failed")
	span.End()

	if proc.count() != 1 {
		t.Fatalf("expected 1 export, got %d", proc.count())
	}
}

func TestWithForceNotRecord_SuppressesOnError(t *testing.T) {
	proc := &captureProcessor{}
	tr := newTestTracer(t, proc)

	_, span := tr.Start(context.Background(), "fail", func(c *types.SpanConfig) {
		c.ForceRecord = types.RecordPolicyOnError
	})
	span.WithForceNotRecord()
	span.RecordError(errors.New("boom"))
	span.End()

	if proc.count() != 0 {
		t.Fatalf("expected no export after WithForceNotRecord, got %d", proc.count())
	}
}

func TestResolveRecordPolicy_SamplingRecordAndSample(t *testing.T) {
	proc := &captureProcessor{}
	provider := NewTracerImpl("test", nil, proc, sampler.NewProbabilitySampler(1.0))

	_, span := provider.Start(context.Background(), "sampled")
	span.End()

	if proc.count() != 1 {
		t.Fatalf("RecordAndSample with default policy should export, got %d", proc.count())
	}
}
