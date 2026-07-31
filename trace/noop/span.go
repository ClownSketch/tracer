package noop

import (
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// NoopSpan 是空 Span，用于不需要真实 Span 的场景
type NoopSpan struct{}

// End 结束当前 Span（空实现）
func (s *NoopSpan) End() {}

// WithForceRecord 标记 Span 需要强制记录（空实现）
func (s *NoopSpan) WithForceRecord() trace.Span { return s }

// WithRecordOnError 标记 Span 仅在错误时记录（空实现）
func (s *NoopSpan) WithRecordOnError() trace.Span { return s }

// WithForceNotRecord 标记 Span 不需要强制记录（空实现）
func (s *NoopSpan) WithForceNotRecord() trace.Span { return s }

// SpanContext 返回空 SpanContext
func (s *NoopSpan) SpanContext() types.SpanContext {
	return types.SpanContext{}
}

// GetStartTime 返回空时间
func (s *NoopSpan) GetStartTime() time.Time {
	return time.Time{}
}

// GetEndTime 返回空时间
func (s *NoopSpan) GetEndTime() time.Time {
	return time.Time{}
}

// GetSpanName 返回空字符串
func (s *NoopSpan) GetSpanName() string {
	return ""
}

// GetSpanKind 返回默认 SpanKind
func (s *NoopSpan) GetSpanKind() types.SpanKind {
	return types.SpanKindInternal
}

// GetSpanTraceID 返回空字符串
func (s *NoopSpan) GetSpanTraceID() string {
	return ""
}

// GetSpanParentSpanID 返回空字符串
func (s *NoopSpan) GetSpanParentSpanID() string {
	return ""
}

// AddLinkedSpan 空实现
func (s *NoopSpan) AddLinkedSpan(spanContext types.SpanContext) {}

// GetLinkedSpans 返回空切片
func (s *NoopSpan) GetLinkedSpans() []types.SpanContext {
	return nil
}

// SetAttribute 空实现
func (s *NoopSpan) SetAttributeConfig(key string, value attribute.Value, opts ...attribute.AttributeOption) {
}

// SetAttribute 空实现
func (s *NoopSpan) SetAttribute(key string, value attribute.Value) {}

// SetAttributes 空实现
func (s *NoopSpan) SetAttributes(attrs ...attribute.KeyValue) {}

// SetGlobalAttribute 空实现
func (s *NoopSpan) SetGlobalAttribute(key string, value attribute.Value) {}

// SetGlobalAttributes 空实现
func (s *NoopSpan) SetGlobalAttributes(attrs ...attribute.KeyValue) {}

// GetGlobalAttributes 返回空 map
func (s *NoopSpan) GetGlobalAttributes() map[string]attribute.Attribute {
	return map[string]attribute.Attribute{}
}

// SetInheritedAttribute 空实现
func (s *NoopSpan) SetInheritedAttribute(key string, value attribute.Value) {}

// SetInheritedAttributes 空实现
func (s *NoopSpan) SetInheritedAttributes(attrs ...attribute.KeyValue) {}

// GetInheritedAttributes 返回空 map
func (s *NoopSpan) GetInheritedAttributes() map[string]attribute.Attribute {
	return map[string]attribute.Attribute{}
}

// GetAttributes 返回空 map
func (s *NoopSpan) GetAttributes() map[string]any {
	return map[string]any{}
}

// AddEvent 空实现
func (s *NoopSpan) AddEvent(name, eventType string, eventHandler types.Event) {}

// GetEvents 返回空切片
func (s *NoopSpan) GetEvents() []types.SpanEvent {
	return nil
}

// AddLog 空实现
func (s *NoopSpan) AddLog(log types.SpanLog) trace.Span {
	return s
}

// GetLogs 返回空切片
func (s *NoopSpan) GetLogs() []types.SpanLog {
	return nil
}

// RecordError 空实现
func (s *NoopSpan) RecordError(err error) trace.Span { return s }

// WithError 空实现
func (s *NoopSpan) WithError(err error, message string) trace.Span { return s }

// GetErrorDetail 返回 nil
func (s *NoopSpan) GetErrorDetail() *types.ErrorDetail {
	return nil
}

// SetStatus 空实现
func (s *NoopSpan) SetStatus(status types.SpanStatus) {}

// SetResource 空实现
func (s *NoopSpan) SetResource(resource *types.ResourceInfo) {}

// GetResource 返回 nil
func (s *NoopSpan) GetResource() *types.ResourceInfo {
	return nil
}

// SetResourceUsage 空实现
func (s *NoopSpan) SetResourceUsage(usage *types.ResourceMetrics) {}

// GetResourceUsage 返回 nil
func (s *NoopSpan) GetResourceUsage() *types.ResourceMetrics {
	return nil
}

// SetMongoCollection 空实现
func (s *NoopSpan) SetMongoCollection(name string) trace.Span { return s }

// GetMongoCollection 返回空字符串
func (s *NoopSpan) GetMongoCollection() string { return "" }

// GetSnapshot 返回空快照
func (s *NoopSpan) GetSnapshot() trace.SpanSnapshot {
	return &NoopSpanSnapshot{}
}
