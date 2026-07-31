package processor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

type multiProcessorTestSnapshot struct {
	attributes map[string]any
	releases   atomic.Int64
}

func (s *multiProcessorTestSnapshot) GetStartTime() time.Time     { return time.Time{} }
func (s *multiProcessorTestSnapshot) GetEndTime() time.Time       { return time.Time{} }
func (s *multiProcessorTestSnapshot) GetSpanName() string         { return "multi" }
func (s *multiProcessorTestSnapshot) GetSpanKind() types.SpanKind { return types.SpanKindInternal }
func (s *multiProcessorTestSnapshot) GetSpanTraceID() string {
	return "00000000000000000000000000000001"
}
func (s *multiProcessorTestSnapshot) GetSpanID() string                        { return "0000000000000001" }
func (s *multiProcessorTestSnapshot) GetSpanParentSpanID() string              { return "" }
func (s *multiProcessorTestSnapshot) GetLinkedSpans() []types.SpanContext      { return nil }
func (s *multiProcessorTestSnapshot) GetAttributes() map[string]any            { return s.attributes }
func (s *multiProcessorTestSnapshot) GetEvents() []types.SpanEvent             { return nil }
func (s *multiProcessorTestSnapshot) GetLogs() []types.SpanLog                 { return nil }
func (s *multiProcessorTestSnapshot) GetErrorDetail() *types.ErrorDetail       { return nil }
func (s *multiProcessorTestSnapshot) GetStatus() types.SpanStatus              { return types.SpanStatus{} }
func (s *multiProcessorTestSnapshot) GetResource() *types.ResourceInfo         { return nil }
func (s *multiProcessorTestSnapshot) GetResourceUsage() *types.ResourceMetrics { return nil }
func (s *multiProcessorTestSnapshot) GetMongoCollection() string               { return "" }
func (s *multiProcessorTestSnapshot) Release()                                 { s.releases.Add(1) }

type retainingTestProcessor struct {
	received chan trace.SpanSnapshot
}

func (p *retainingTestProcessor) OnStart(context.Context, trace.Span) {}
func (p *retainingTestProcessor) OnEnd(span trace.SpanSnapshot) {
	p.received <- span
}
func (p *retainingTestProcessor) Shutdown(context.Context) error { return nil }

func TestMultiSpanProcessorRetainsSnapshotForEveryProcessor(t *testing.T) {
	first := &retainingTestProcessor{received: make(chan trace.SpanSnapshot, 1)}
	second := &retainingTestProcessor{received: make(chan trace.SpanSnapshot, 1)}
	processor := NewMultiSpanProcessor(first, second)
	snapshot := &multiProcessorTestSnapshot{attributes: map[string]any{"order_no": "PI1"}}

	processor.OnEnd(snapshot)
	firstSnapshot := <-first.received
	secondSnapshot := <-second.received

	firstSnapshot.Release()
	if snapshot.releases.Load() != 0 {
		t.Fatalf("底层快照被提前释放: %d", snapshot.releases.Load())
	}
	if len(secondSnapshot.GetAttributes()) != 1 {
		t.Fatalf("第二个处理器没有读到完整属性")
	}
	secondSnapshot.Release()
	if snapshot.releases.Load() != 1 {
		t.Fatalf("底层快照释放次数错误: %d", snapshot.releases.Load())
	}
}

func TestMultiSpanProcessorWithoutChildrenReleasesSnapshot(t *testing.T) {
	processor := NewMultiSpanProcessor()
	snapshot := &multiProcessorTestSnapshot{}

	processor.OnEnd(snapshot)

	if snapshot.releases.Load() != 1 {
		t.Fatalf("空处理器没有释放快照: %d", snapshot.releases.Load())
	}
}
