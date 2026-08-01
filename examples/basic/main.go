package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/providers"
)

func main() {
	provider, err := providers.InitTracer(providers.TracerConfig{
		ServiceName:   "tracer-basic-example",
		ExporterType:  providers.ExporterTypeFile,
		LogFile:       "./storage/example/basic/traces.log",
		FallbackDir:   "./storage/example/basic/fallback",
		SampleRate:    1,
		BatchSize:     50,
		BatchInterval: time.Second,
		Workers:       2,
		QueueSize:     1000,
	})
	if err != nil {
		log.Fatalf("初始化 Tracer 失败: %v", err)
	}

	tracer.SetTracerProvider(provider, "tracer-basic-example")
	if err := createOrder(context.Background()); err != nil {
		log.Printf("创建订单失败: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 Tracer 失败: %v", err)
	}
}

func createOrder(ctx context.Context) error {
	ctx, span := tracer.GetTracer("order").Start(
		ctx,
		"order.create",
		tracer.WithSpanKind(tracer.SpanKindInternal),
		tracer.WithForceRecord(),
	)
	defer span.End()

	span.SetAttributes(
		attribute.String("order.no", "P202607310001"),
		attribute.String("currency", "INR"),
		attribute.Int64("amount", 10000),
	)
	span.AddEvent("order.validated", "business", func() map[string]any {
		return map[string]any{"valid": true}
	})
	span.AddLog(tracer.SpanLog{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Severity:  tracer.SpanLogSeverityInfo,
		Message:   "订单创建完成",
	})

	if traceID := tracer.GetTraceID(ctx); traceID == "" {
		err := fmt.Errorf("当前上下文缺少 TraceID")
		span.RecordError(err)
		return err
	}
	return nil
}
