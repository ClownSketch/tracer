package providers

import (
	"context"
	"testing"

	"github.com/ClownSketch/tracer/trace/noop"
)

func TestInitTracerEReturnsConfigurationError(t *testing.T) {
	provider, err := InitTracerE(TracerConfig{
		ServiceName:  "invalid-exporter",
		ExporterType: ExporterType("missing"),
	})
	if err == nil {
		t.Fatal("expected unsupported exporter error")
	}
	if provider != nil {
		t.Fatal("provider must be nil after initialization failure")
	}
}

func TestInitTracerERejectsUnverifiedProtocolExporters(t *testing.T) {
	for _, exporterType := range []ExporterType{ExporterTypeJaeger, ExporterTypeZipkin} {
		provider, err := InitTracerE(TracerConfig{
			ServiceName:      "protocol-check",
			ExporterType:     exporterType,
			ExporterEndpoint: "http://127.0.0.1:1",
		})
		if err == nil {
			t.Fatalf("%s 未完成协议验证时不应允许生产初始化", exporterType)
		}
		if provider != nil {
			t.Fatalf("%s 初始化失败后不应返回 provider", exporterType)
		}
	}
}

func TestInitTracerFallsBackToNoop(t *testing.T) {
	provider := InitTracer(TracerConfig{
		ServiceName:  "invalid-exporter",
		ExporterType: ExporterType("missing"),
	})
	defer provider.Shutdown(context.Background())

	if _, ok := provider.(*noop.NoopTracerProvider); !ok {
		t.Fatalf("provider type = %T, want *noop.NoopTracerProvider", provider)
	}
}
