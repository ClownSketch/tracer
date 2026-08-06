package noop

import (
	"context"

	"github.com/ClownSketch/tracer/trace"
)

// NoopTracerProvider 提供无操作 Tracer。
type NoopTracerProvider struct{}

// GetTracer 返回无操作 Tracer。
// @param tracerName string Tracer 名称
// @return result trace.Tracer 无操作 Tracer
func (n *NoopTracerProvider) GetTracer(tracerName string) trace.Tracer {
	return &NoopTracer{}
}

// Shutdown 完成无操作 Provider 的关闭流程。
// @param ctx context.Context 上下文
// @return err error 始终为 nil
func (n *NoopTracerProvider) Shutdown(ctx context.Context) error {
	return nil
}
