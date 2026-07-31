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

const (
	defaultCollection = "gp_traces_default"
	gatewayCollection = "gp_traces_gateway"
	workerCollection  = "gp_traces_worker"
)

func main() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("必须设置 MONGO_URI")
	}

	provider, err := providers.InitTracerE(providers.TracerConfig{
		ServiceName:       "tracer-routing-example",
		ExporterType:      providers.ExporterTypeMongoDBRouting,
		SampleRate:        1,
		BatchSize:         100,
		BatchInterval:     time.Second,
		Workers:           4,
		QueueSize:         4000,
		FallbackDir:       "./storage/example/mongodb-routing/fallback",
		MongoDBURI:        mongoURI,
		MongoDBDatabase:   "tracer_example",
		MongoDBCollection: defaultCollection,
		MongoDBAllowedCollections: []string{
			defaultCollection,
			gatewayCollection,
			workerCollection,
		},
		MongoDBTimeout:    10 * time.Second,
		MongoDBMaxRetries: 3,
		MongoDBRetryDelay: 200 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("初始化 Tracer 失败: %v", err)
	}
	tracer.SetTracerProvider(provider, "tracer-routing-example")

	ctx, span := tracer.GetTracer("gateway").Start(
		context.Background(),
		"gateway.payin.create",
		tracer.WithForceRecord(),
		tracer.WithMongoCollection(gatewayCollection),
	)
	span.SetAttributes(attribute.String("out_trade_no", "PI202607310001"))
	span.End()

	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 Tracer 失败: %v", err)
	}
}
