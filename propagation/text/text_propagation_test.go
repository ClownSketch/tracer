package text

import (
	"context"
	"testing"

	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/types"
)

// mockCarrier 用于测试的模拟载体
type mockCarrier struct {
	data map[string]string
}

func newMockCarrier() *mockCarrier {
	return &mockCarrier{data: make(map[string]string)}
}

func (m *mockCarrier) Set(key, value string) {
	m.data[key] = value
}

func (m *mockCarrier) Get(key string) string {
	return m.data[key]
}

func (m *mockCarrier) Keys() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func TestTextMapPropagator_Extract_W3C(t *testing.T) {
	propagator := NewTextMapPropagator()

	tests := []struct {
		name        string
		traceParent string
		traceState  string
		expectValid bool
		expected    types.SpanContext
	}{
		{
			"标准 W3C 格式",
			"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"key1=value1,key2=value2",
			true,
			types.SpanContext{
				TraceID:      "4bf92f3577b34da6a3ce929d0e0e4736",
				ParentSpanID: "00f067aa0ba902b7", // 父SpanID（发送方的当前 spanID）
				TraceFlags:   1,
				TraceState:   "key1=value1,key2=value2",
				Remote:       true,
			},
		},
		{
			"格式错误",
			"invalid-format",
			"",
			false,
			types.SpanContext{},
		},
		{
			"不支持的版本",
			"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"",
			false,
			types.SpanContext{},
		},
		{
			"长度错误",
			"00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b-01",
			"",
			false,
			types.SpanContext{},
		},
		{
			"全零 TraceID",
			"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
			"",
			false,
			types.SpanContext{},
		},
		{
			"大写十六进制",
			"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
			"",
			false,
			types.SpanContext{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			carrier := newMockCarrier()
			carrier.Set(TraceParentHeaderKey, tt.traceParent)
			if tt.traceState != "" {
				carrier.Set(TraceStateHeaderKey, tt.traceState)
			}

			ctx := context.Background()
			ctx = propagator.Extract(ctx, carrier)
			// 从 context 中提取 SpanContext
			result := baggage.SpanContextFromContext(ctx)

			if result.Validate() != tt.expectValid {
				t.Fatalf("SpanContext.Validate() = %v, want %v: %+v", result.Validate(), tt.expectValid, result)
			}
			if tt.expectValid {
				if result.TraceID != tt.expected.TraceID {
					t.Errorf("TraceID = %v, want %v", result.TraceID, tt.expected.TraceID)
				}
				if result.SpanID != tt.expected.SpanID {
					t.Errorf("SpanID = %v, want %v", result.SpanID, tt.expected.SpanID)
				}
				if result.ParentSpanID != tt.expected.ParentSpanID {
					t.Errorf("ParentSpanID = %v, want %v", result.ParentSpanID, tt.expected.ParentSpanID)
				}
				if result.TraceFlags != tt.expected.TraceFlags {
					t.Errorf("TraceFlags = %v, want %v", result.TraceFlags, tt.expected.TraceFlags)
				}
				if result.TraceState != tt.expected.TraceState {
					t.Errorf("TraceState = %v, want %v", result.TraceState, tt.expected.TraceState)
				}
				if result.Remote != tt.expected.Remote {
					t.Errorf("Remote = %v, want %v", result.Remote, tt.expected.Remote)
				}
			}
		})
	}
}

func TestTextMapPropagator_Extract_CustomHeaders(t *testing.T) {
	propagator := NewTextMapPropagator()

	tests := []struct {
		name     string
		traceID  string
		spanID   string
		flags    string
		expected types.SpanContext
	}{
		{
			"自定义头部",
			"0123456789abcdef0123456789abcdef",
			"0123456789abcdef",
			"",
			types.SpanContext{
				TraceID:      "0123456789abcdef0123456789abcdef",
				ParentSpanID: "0123456789abcdef",
				TraceFlags:   types.TraceFlagsSampled,
				Remote:       true,
			},
		},
		{
			"未采样自定义头部",
			"0123456789abcdef0123456789abcdef",
			"0123456789abcdef",
			"00",
			types.SpanContext{
				TraceID:      "0123456789abcdef0123456789abcdef",
				ParentSpanID: "0123456789abcdef",
				Remote:       true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			carrier := newMockCarrier()
			carrier.Set(CustomTraceIDHeaderKey, tt.traceID)
			carrier.Set(CustomSpanIDHeaderKey, tt.spanID)
			if tt.flags != "" {
				carrier.Set(CustomTraceFlagsHeaderKey, tt.flags)
			}

			ctx := context.Background()
			ctx = propagator.Extract(ctx, carrier)
			// 从 context 中提取 SpanContext
			result := baggage.SpanContextFromContext(ctx)

			if result.TraceID != tt.expected.TraceID {
				t.Errorf("TraceID = %v, want %v", result.TraceID, tt.expected.TraceID)
			}
			if result.SpanID != tt.expected.SpanID {
				t.Errorf("SpanID = %v, want %v", result.SpanID, tt.expected.SpanID)
			}
			if result.ParentSpanID != tt.expected.ParentSpanID {
				t.Errorf("ParentSpanID = %v, want %v", result.ParentSpanID, tt.expected.ParentSpanID)
			}
			if result.TraceFlags != tt.expected.TraceFlags {
				t.Errorf("TraceFlags = %v, want %v", result.TraceFlags, tt.expected.TraceFlags)
			}
			if result.Remote != tt.expected.Remote {
				t.Errorf("Remote = %v, want %v", result.Remote, tt.expected.Remote)
			}
		})
	}
}

func TestTextMapPropagator_Extract_NewRoot(t *testing.T) {
	propagator := NewTextMapPropagator()
	carrier := newMockCarrier() // 空载体

	ctx := context.Background()
	ctx = propagator.Extract(ctx, carrier)
	// 从 context 中提取 SpanContext
	result := baggage.SpanContextFromContext(ctx)

	if result.Validate() {
		t.Fatalf("empty carrier should not create a SpanContext: %+v", result)
	}
}
