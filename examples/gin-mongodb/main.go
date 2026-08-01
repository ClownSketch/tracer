package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/attribute"
	ginmiddleware "github.com/ClownSketch/tracer/middleware/gin"
	propagationhttp "github.com/ClownSketch/tracer/propagation/http"
	"github.com/ClownSketch/tracer/providers"
	"github.com/gin-gonic/gin"
)

func main() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("必须设置 MONGO_URI")
	}

	provider, err := providers.InitTracer(providers.TracerConfig{
		ServiceName:       "tracer-gin-example",
		ExporterType:      providers.ExporterTypeMongoDB,
		SampleRate:        1,
		BatchSize:         100,
		BatchInterval:     time.Second,
		Workers:           4,
		QueueSize:         4000,
		FallbackDir:       "./storage/example/gin-mongodb/fallback",
		MongoDBURI:        mongoURI,
		MongoDBDatabase:   envOrDefault("TRACER_MONGO_DATABASE", "tracer_example"),
		MongoDBCollection: envOrDefault("TRACER_MONGO_COLLECTION", "traces_gin"),
		MongoDBTimeout:    10 * time.Second,
		MongoDBMaxRetries: 3,
		MongoDBRetryDelay: 200 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("初始化 Tracer 失败: %v", err)
	}
	tracer.SetTracerProvider(provider, "tracer-gin-example")

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(ginmiddleware.GinCrossServiceMiddleware())
	router.POST("/orders", createOrderHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-signalCtx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP 服务异常退出: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 HTTP 服务失败: %v", err)
	}
	if err := provider.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 Tracer 失败: %v", err)
	}
}

func createOrderHandler(c *gin.Context) {
	ctx, span := tracer.GetTracer("order").Start(
		c.Request.Context(),
		"order.validate",
		tracer.WithSpanKind(tracer.SpanKindInternal),
	)
	defer span.End()

	span.SetAttributes(
		attribute.String("order.no", "P202607310002"),
		attribute.String("currency", "BRL"),
	)

	upstreamRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://upstream.example/orders", http.NoBody)
	if err != nil {
		span.RecordError(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	propagator := propagationhttp.NewHTTPPropagator()
	carrier := propagationhttp.NewHTTPHeaderCarrier(upstreamRequest.Header)
	if err := propagator.Inject(ctx, carrier); err != nil {
		span.RecordError(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"trace_id":    tracer.GetTraceID(ctx),
		"traceparent": upstreamRequest.Header.Get("traceparent"),
	})
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
