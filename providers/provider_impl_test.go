package providers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/trace"
)

type providerCaptureProcessor struct {
	resourceServiceName string
	shutdownErr         error
}

type providerBlockingProcessor struct {
	release       chan struct{}
	shutdownCount atomic.Int64
}

func (p *providerBlockingProcessor) OnStart(context.Context, trace.Span) {}

func (p *providerBlockingProcessor) OnEnd(snapshot trace.SpanSnapshot) {
	snapshot.Release()
}

func (p *providerBlockingProcessor) Shutdown(context.Context) error {
	p.shutdownCount.Add(1)
	<-p.release
	return nil
}

func (p *providerCaptureProcessor) OnStart(context.Context, trace.Span) {}

func (p *providerCaptureProcessor) OnEnd(snapshot trace.SpanSnapshot) {
	if resource := snapshot.GetResource(); resource != nil {
		p.resourceServiceName = resource.ServiceName
	}
	snapshot.Release()
}

func (p *providerCaptureProcessor) Shutdown(context.Context) error {
	return p.shutdownErr
}

func TestTracerProviderAddsServiceResource(t *testing.T) {
	processor := &providerCaptureProcessor{}
	provider := NewTracerProvider(
		WithSpanProcessor(processor),
		WithServiceName("manager"),
	)
	_, span := provider.GetTracer("http").Start(context.Background(), "request")
	span.End()

	if processor.resourceServiceName != "manager" {
		t.Fatalf("service name = %q, want manager", processor.resourceServiceName)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown provider: %v", err)
	}
}

func TestTracerProviderShutdownReturnsAllErrorsOnEveryCall(t *testing.T) {
	firstErr := errors.New("first")
	secondErr := errors.New("second")
	provider := NewTracerProvider(
		WithSpanProcessor(&providerCaptureProcessor{shutdownErr: firstErr}),
		WithSpanProcessor(&providerCaptureProcessor{shutdownErr: secondErr}),
	)

	for attempt := 0; attempt < 2; attempt++ {
		err := provider.Shutdown(context.Background())
		if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
			t.Fatalf("shutdown attempt %d returned incomplete error: %v", attempt+1, err)
		}
	}
}

func TestTracerProviderShutdownContinuesAfterCallerTimeout(t *testing.T) {
	blockingProcessor := &providerBlockingProcessor{release: make(chan struct{})}
	provider := NewTracerProvider(WithSpanProcessor(blockingProcessor))

	firstCtx, firstCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer firstCancel()
	if err := provider.Shutdown(firstCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("首次关闭应返回超时错误，实际: %v", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- provider.Shutdown(context.Background())
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("处理器释放前不应完成关闭，实际: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	close(blockingProcessor.release)
	if err := <-secondDone; err != nil {
		t.Fatalf("再次关闭应等待后台清理完成: %v", err)
	}
	if blockingProcessor.shutdownCount.Load() != 1 {
		t.Fatalf("处理器 Shutdown 调用次数错误: %d", blockingProcessor.shutdownCount.Load())
	}
}
