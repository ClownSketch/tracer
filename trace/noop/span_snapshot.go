package noop

import (
	"time"

	"github.com/ClownSketch/tracer/types"
)

type NoopSpanSnapshot struct{}

func (n *NoopSpanSnapshot) GetStartTime() time.Time {
	return time.Time{}
}

func (n *NoopSpanSnapshot) GetEndTime() time.Time {
	return time.Time{}
}

func (n *NoopSpanSnapshot) GetSpanName() string {
	return ""
}

func (n *NoopSpanSnapshot) GetSpanKind() types.SpanKind {
	return types.SpanKindInternal
}

func (n *NoopSpanSnapshot) GetSpanTraceID() string {
	return ""
}

func (n *NoopSpanSnapshot) GetSpanID() string {
	return ""
}

func (n *NoopSpanSnapshot) GetSpanParentSpanID() string {
	return ""
}

func (n *NoopSpanSnapshot) GetLinkedSpans() []types.SpanContext {
	return nil
}

func (n *NoopSpanSnapshot) GetAttributes() map[string]any {
	return nil
}

func (n *NoopSpanSnapshot) GetEvents() []types.SpanEvent {
	return nil
}

func (n *NoopSpanSnapshot) GetLogs() []types.SpanLog {
	return nil
}

func (n *NoopSpanSnapshot) GetErrorDetail() *types.ErrorDetail {
	return nil
}

func (n *NoopSpanSnapshot) GetStatus() types.SpanStatus {
	return types.SpanStatus{}
}

func (n *NoopSpanSnapshot) GetResource() *types.ResourceInfo {
	return nil
}

func (n *NoopSpanSnapshot) GetResourceUsage() *types.ResourceMetrics {
	return nil
}

func (n *NoopSpanSnapshot) GetMongoCollection() string {
	return ""
}

// Release 释放快照资源（No-op 实现，无需释放资源）
func (n *NoopSpanSnapshot) Release() {
	// No-op: 无需释放资源
}
