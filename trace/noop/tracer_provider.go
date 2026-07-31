package noop

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
)

type NoopTracerProvider struct{}

func (n *NoopTracerProvider) GetTracer(tracerName string) trace.Tracer {
	return &NoopTracer{}
}

func (n *NoopTracerProvider) Shutdown(ctx context.Context) error {
	return nil
}
