package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/providers"
)

func main() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("必须设置 MONGO_URI")
	}

	provider, err := providers.InitTracer(providers.TracerConfig{
		ServiceName:        "tracer-wal-example",
		ExporterType:       providers.ExporterTypeMongoDB,
		SampleRate:         1,
		UseWAL:             true,
		WALDir:             "./storage/example/wal/data",
		WALSegmentSize:     32 * 1024 * 1024,
		WALExportBatchSize: 100,
		WALPollInterval:    200 * time.Millisecond,
		WALFlushInterval:   2 * time.Millisecond,
		WALBufferSize:      256 * 1024,
		WALSyncOnWrite:     false,
		MongoDBURI:         mongoURI,
		MongoDBDatabase:    "tracer_example",
		MongoDBCollection:  "traces_wal",
		MongoDBTimeout:     10 * time.Second,
		MongoDBMaxRetries:  3,
		MongoDBRetryDelay:  200 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("初始化 Tracer 失败: %v", err)
	}
	tracer.SetTracerProvider(provider, "tracer-wal-example")

	ctx, span := tracer.GetTracer("audit").Start(
		context.Background(),
		"manual.adjust.submit",
		tracer.WithForceRecord(),
	)
	span.SetAttributes(
		attribute.String("adjust_no", "MA202607310001"),
		attribute.String("subject_type", "tenant"),
	)
	span.End()

	shutdownCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 Tracer 失败: %v", err)
	}
}
