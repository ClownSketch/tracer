package types

import "testing"

func TestSpanContextValidate(t *testing.T) {
	tests := []struct {
		name    string
		context SpanContext
		valid   bool
	}{
		{
			name: "local root",
			context: SpanContext{
				TraceID: "0123456789abcdef0123456789abcdef",
				SpanID:  "0123456789abcdef",
			},
			valid: true,
		},
		{
			name: "remote parent",
			context: SpanContext{
				TraceID:      "0123456789abcdef0123456789abcdef",
				ParentSpanID: "0123456789abcdef",
				Remote:       true,
			},
			valid: true,
		},
		{
			name: "uppercase id",
			context: SpanContext{
				TraceID: "0123456789ABCDEF0123456789ABCDEF",
				SpanID:  "0123456789abcdef",
			},
		},
		{
			name: "zero trace id",
			context: SpanContext{
				TraceID: "00000000000000000000000000000000",
				SpanID:  "0123456789abcdef",
			},
		},
		{
			name: "zero span id",
			context: SpanContext{
				TraceID: "0123456789abcdef0123456789abcdef",
				SpanID:  "0000000000000000",
			},
		},
		{
			name: "remote without parent",
			context: SpanContext{
				TraceID: "0123456789abcdef0123456789abcdef",
				Remote:  true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.context.Validate(); got != test.valid {
				t.Fatalf("Validate() = %v, want %v", got, test.valid)
			}
		})
	}
}
