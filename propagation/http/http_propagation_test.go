package http

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// mockContextCarrier 用于测试的模拟载体
type mockContextCarrier struct {
	data map[string]string
}

func newMockContextCarrier() *mockContextCarrier {
	return &mockContextCarrier{data: make(map[string]string)}
}

func (m *mockContextCarrier) Set(key, value string) {
	m.data[key] = value
}

func (m *mockContextCarrier) Get(key string) string {
	return m.data[key]
}

func (m *mockContextCarrier) Keys() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func TestHTTPPropagator_Extract(t *testing.T) {
	propagator := NewHTTPPropagator()

	tests := []struct {
		name         string
		setupCarrier func() trace.ContextCarrier
		expectValid  bool
	}{
		{
			"正常 W3C 格式",
			func() trace.ContextCarrier {
				carrier := newMockContextCarrier()
				carrier.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
				return carrier
			},
			true,
		},
		{
			"空载体",
			func() trace.ContextCarrier {
				return newMockContextCarrier()
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			carrier := tt.setupCarrier()
			ctx := context.Background()

			ctx = propagator.Extract(ctx, carrier)
			// 从 context 中提取 SpanContext
			spanContext := baggage.SpanContextFromContext(ctx)

			if spanContext.Validate() != tt.expectValid {
				t.Fatalf("SpanContext.Validate() = %v, want %v: %+v", spanContext.Validate(), tt.expectValid, spanContext)
			}
		})
	}
}

// mockSpan 用于测试的有效 Span 实现
type mockSpan struct {
	spanContext types.SpanContext
}

func (m *mockSpan) SpanContext() types.SpanContext {
	return m.spanContext
}

func (m *mockSpan) End() {}

func (m *mockSpan) GetStartTime() time.Time {
	return time.Now()
}

func (m *mockSpan) GetEndTime() time.Time {
	return time.Time{}
}

func (m *mockSpan) WithForceRecord() trace.Span { return m }

func (m *mockSpan) WithRecordOnError() trace.Span { return m }

func (m *mockSpan) WithForceNotRecord() trace.Span { return m }

func (m *mockSpan) GetSpanName() string {
	return "test-span"
}

func (m *mockSpan) GetSpanKind() types.SpanKind {
	return types.SpanKindServer
}

func (m *mockSpan) GetSpanTraceID() string {
	return m.spanContext.TraceID
}

func (m *mockSpan) GetSpanParentSpanID() string {
	return m.spanContext.ParentSpanID
}

func (m *mockSpan) AddLinkedSpan(spanContext types.SpanContext) {}

func (m *mockSpan) GetLinkedSpans() []types.SpanContext {
	return nil
}

func (m *mockSpan) SetAttributeConfig(key string, value attribute.Value, opts ...attribute.AttributeOption) {
}

func (m *mockSpan) SetAttribute(key string, value attribute.Value) {}

func (m *mockSpan) SetAttributes(attrs ...attribute.KeyValue) {}

func (m *mockSpan) SetGlobalAttribute(key string, value attribute.Value) {}

func (m *mockSpan) SetGlobalAttributes(attrs ...attribute.KeyValue) {}

func (m *mockSpan) GetGlobalAttributes() map[string]attribute.Attribute {
	return nil
}

func (m *mockSpan) SetInheritedAttribute(key string, value attribute.Value) {}

func (m *mockSpan) SetInheritedAttributes(attrs ...attribute.KeyValue) {}

func (m *mockSpan) GetInheritedAttributes() map[string]attribute.Attribute {
	return nil
}

func (m *mockSpan) GetAttributes() map[string]any {
	return nil
}

func (m *mockSpan) AddEvent(name, eventType string, eventHandler types.Event) {}

func (m *mockSpan) GetEvents() []types.SpanEvent {
	return nil
}

func (m *mockSpan) AddLog(log types.SpanLog) trace.Span {
	return m
}

func (m *mockSpan) GetLogs() []types.SpanLog {
	return nil
}

func (m *mockSpan) RecordError(err error) trace.Span { return m }

func (m *mockSpan) WithError(err error, message string) trace.Span {
	return m
}

func (m *mockSpan) GetErrorDetail() *types.ErrorDetail {
	return nil
}

func (m *mockSpan) SetStatus(status types.SpanStatus) {}

func (m *mockSpan) SetResource(resource *types.ResourceInfo) {}

func (m *mockSpan) GetResource() *types.ResourceInfo {
	return nil
}

func (m *mockSpan) SetResourceUsage(usage *types.ResourceMetrics) {}

func (m *mockSpan) GetResourceUsage() *types.ResourceMetrics {
	return nil
}

func (m *mockSpan) SetMongoCollection(name string) trace.Span { return m }

func (m *mockSpan) GetMongoCollection() string { return "" }

func (m *mockSpan) GetSnapshot() trace.SpanSnapshot {
	return nil
}

func TestHTTPPropagator_Inject(t *testing.T) {
	propagator := NewHTTPPropagator()

	// 创建模拟的 SpanContext
	spanContext := types.SpanContext{
		TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:     "00f067aa0ba902b7",
		TraceFlags: 1,
	}

	// 创建模拟的 Span（使用有效的 SpanContext）
	mockSpan := &mockSpan{spanContext: spanContext}

	tests := []struct {
		name         string
		setupContext func() context.Context
		expectError  bool
	}{
		{
			"正常注入",
			func() context.Context {
				return baggage.WithSpanContext(context.Background(), mockSpan)
			},
			false,
		},
		{
			"无 Span 的上下文",
			func() context.Context {
				return context.Background()
			},
			true, // 应该返回错误
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			carrier := newMockContextCarrier()
			ctx := tt.setupContext()

			err := propagator.Inject(ctx, carrier)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				// 验证注入的头部
				if traceParent := carrier.Get("traceparent"); traceParent == "" {
					t.Error("traceparent header should be set")
				}
				if traceID := carrier.Get("X-Trace-ID"); traceID != spanContext.TraceID {
					t.Errorf("X-Trace-ID = %v, want %v", traceID, spanContext.TraceID)
				}
				if traceFlags := carrier.Get("X-Trace-Flags"); traceFlags != "01" {
					t.Errorf("X-Trace-Flags = %v, want 01", traceFlags)
				}
			}
		})
	}
}

func TestHTTPPropagator_Integration(t *testing.T) {
	propagator := NewHTTPPropagator()

	// 创建完整的 HTTP 请求头
	header := make(http.Header)
	header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	header.Set("tracestate", "key=value")
	header.Set("baggage", "user=john,request=123")

	carrier := NewHTTPHeaderCarrier(header)
	ctx := context.Background()

	// 测试提取
	ctx = propagator.Extract(ctx, carrier)
	// 从 context 中提取 SpanContext
	spanContext := baggage.SpanContextFromContext(ctx)

	if spanContext.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("TraceID mismatch")
	}

	// 测试 baggage 是否被提取
	extractedBaggage := baggage.GetBaggage(ctx)
	if len(extractedBaggage) == 0 {
		t.Error("Baggage should be extracted")
	}
	if extractedBaggage["user"] != "john" {
		t.Errorf("Baggage user = %v, want john", extractedBaggage["user"])
	}
	if extractedBaggage["request"] != "123" {
		t.Errorf("Baggage request = %v, want 123", extractedBaggage["request"])
	}

	// 测试注入
	newHeader := make(http.Header)
	newCarrier := NewHTTPHeaderCarrier(newHeader)

	// 需要创建包含 Span 的上下文来测试注入
	// 创建有效的 SpanContext
	testSpanContext := types.SpanContext{
		TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:     "00f067aa0ba902b7",
		TraceFlags: 1,
	}
	testSpan := &mockSpan{spanContext: testSpanContext}
	injectCtx := baggage.WithSpanContext(context.Background(), testSpan)

	if err := propagator.Inject(injectCtx, newCarrier); err != nil {
		t.Fatalf("Inject failed: %v", err)
	}

	if newHeader.Get("traceparent") == "" {
		t.Error("Inject should set traceparent header")
	}
}
