package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/providers"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/trace/noop"
	"github.com/gin-gonic/gin"
)

type panicCaptureProcessor struct {
	mu         sync.Mutex
	endCount   int
	errorFound bool
}

func (p *panicCaptureProcessor) OnStart(context.Context, trace.Span) {}

func (p *panicCaptureProcessor) OnEnd(snapshot trace.SpanSnapshot) {
	p.mu.Lock()
	p.endCount++
	p.errorFound = snapshot.GetErrorDetail() != nil
	p.mu.Unlock()
	snapshot.Release()
}

func (p *panicCaptureProcessor) Shutdown(context.Context) error { return nil }

func TestGinMiddlewareRecordsAndRepanics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	processor := &panicCaptureProcessor{}
	provider := providers.NewTracerProvider(providers.WithSpanProcessor(processor))
	tracer.SetTracerProvider(provider, "gin-test")
	defer func() {
		_ = provider.Shutdown(context.Background())
		tracer.SetTracerProvider(&noop.NoopTracerProvider{}, "default")
	}()

	router := gin.New()
	router.Use(GinMiddleware())
	router.GET("/panic", func(*gin.Context) {
		panic("boom")
	})

	request := httptest.NewRequest(http.MethodGet, "/panic", nil)
	recorder := httptest.NewRecorder()
	recovered := serveAndRecover(router, recorder, request)
	if recovered != "boom" {
		t.Fatalf("recovered panic = %#v, want boom", recovered)
	}

	processor.mu.Lock()
	defer processor.mu.Unlock()
	if processor.endCount != 1 {
		t.Fatalf("OnEnd count = %d, want 1", processor.endCount)
	}
	if !processor.errorFound {
		t.Fatal("panic was not recorded on the span")
	}
}

func serveAndRecover(handler http.Handler, writer http.ResponseWriter, request *http.Request) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	handler.ServeHTTP(writer, request)
	return nil
}
