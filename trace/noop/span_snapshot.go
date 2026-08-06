package noop

import (
	"time"

	"github.com/ClownSketch/tracer/types"
)

// NoopSpanSnapshot 实现不保存任何数据的 Span 快照。
type NoopSpanSnapshot struct{}

// GetStartTime 返回零值开始时间。
func (n *NoopSpanSnapshot) GetStartTime() time.Time {
	return time.Time{}
}

// GetEndTime 返回零值结束时间。
func (n *NoopSpanSnapshot) GetEndTime() time.Time {
	return time.Time{}
}

// GetSpanName 返回空 Span 名称。
func (n *NoopSpanSnapshot) GetSpanName() string {
	return ""
}

// GetSpanKind 返回默认内部 Span 类型。
func (n *NoopSpanSnapshot) GetSpanKind() types.SpanKind {
	return types.SpanKindInternal
}

// GetSpanTraceID 返回空 TraceID。
func (n *NoopSpanSnapshot) GetSpanTraceID() string {
	return ""
}

// GetSpanID 返回空 SpanID。
func (n *NoopSpanSnapshot) GetSpanID() string {
	return ""
}

// GetSpanParentSpanID 返回空父级 SpanID。
func (n *NoopSpanSnapshot) GetSpanParentSpanID() string {
	return ""
}

// GetLinkedSpans 返回空关联 Span 集合。
func (n *NoopSpanSnapshot) GetLinkedSpans() []types.SpanContext {
	return nil
}

// GetAttributes 返回空属性集合。
func (n *NoopSpanSnapshot) GetAttributes() map[string]any {
	return nil
}

// GetEvents 返回空事件集合。
func (n *NoopSpanSnapshot) GetEvents() []types.SpanEvent {
	return nil
}

// GetLogs 返回空日志集合。
func (n *NoopSpanSnapshot) GetLogs() []types.SpanLog {
	return nil
}

// GetErrorDetail 返回空错误详情。
func (n *NoopSpanSnapshot) GetErrorDetail() *types.ErrorDetail {
	return nil
}

// GetStatus 返回零值 Span 状态。
func (n *NoopSpanSnapshot) GetStatus() types.SpanStatus {
	return types.SpanStatus{}
}

// GetResource 返回空资源信息。
func (n *NoopSpanSnapshot) GetResource() *types.ResourceInfo {
	return nil
}

// GetResourceUsage 返回空资源指标。
func (n *NoopSpanSnapshot) GetResourceUsage() *types.ResourceMetrics {
	return nil
}

// GetMongoCollection 返回空 MongoDB 集合名。
func (n *NoopSpanSnapshot) GetMongoCollection() string {
	return ""
}

// Release 释放快照资源（No-op 实现，无需释放资源）
func (n *NoopSpanSnapshot) Release() {
}
