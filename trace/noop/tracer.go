package noop

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

type NoopTracer struct{}

func (n *NoopTracer) Start(ctx context.Context, spanName string, options ...types.SpanOptions) (context.Context, trace.Span) {
	return ctx, &NoopSpan{}
}
