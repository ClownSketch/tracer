package providers

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/ClownSketch/tracer/exporter"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

const testCustomExporterType ExporterType = "test_custom"

const testPointerExporterType ExporterType = "test_pointer"

const testDuplicateExporterType ExporterType = "test_duplicate"

type testCustomExporterOption struct {
	writer *bytes.Buffer
}

func (testCustomExporterOption) ExporterType() ExporterType {
	return testCustomExporterType
}

type testPointerExporterOption struct {
	writer *bytes.Buffer
}

func (*testPointerExporterOption) ExporterType() ExporterType {
	return testPointerExporterType
}

type testDuplicateExporterOption struct{}

func (testDuplicateExporterOption) ExporterType() ExporterType {
	return testDuplicateExporterType
}

func TestInitTracerReturnsConfigurationError(t *testing.T) {
	provider, err := InitTracer(TracerConfig{
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

func TestInitTracerRejectsUnverifiedProtocolExporters(t *testing.T) {
	for _, exporterType := range []ExporterType{ExporterTypeJaeger, ExporterTypeZipkin} {
		provider, err := InitTracer(TracerConfig{
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

func TestInitTracerDoesNotSilentlyFallback(t *testing.T) {
	provider, err := InitTracer(TracerConfig{
		ServiceName:  "invalid-exporter",
		ExporterType: ExporterType("missing"),
	})
	if err == nil {
		t.Fatal("初始化失败时必须返回错误")
	}
	if provider != nil {
		t.Fatalf("初始化失败后 provider 应为空，实际为 %T", provider)
	}
}

func TestInitTracerUsesRegisteredCustomExporter(t *testing.T) {
	var factoryCalled atomic.Bool
	output := &bytes.Buffer{}
	err := RegisterExporter(testCustomExporterOption{}, func(config ExporterConfig[testCustomExporterOption]) (trace.SpanExporter, error) {
		factoryCalled.Store(true)
		return exporter.NewConsoleSpanExporter(exporter.WithWriter(config.Options.writer)), nil
	})
	if err != nil {
		t.Fatalf("注册自定义导出器失败: %v", err)
	}

	provider, err := InitTracer(TracerConfig{
		ServiceName:    "custom-exporter",
		SampleRate:     1,
		BatchSize:      1,
		ExporterOption: testCustomExporterOption{writer: output},
	})
	if err != nil {
		t.Fatalf("使用自定义导出器初始化失败: %v", err)
	}
	if !factoryCalled.Load() {
		t.Fatal("自定义导出器工厂未被调用")
	}
	_, span := provider.GetTracer("custom-exporter").Start(context.Background(), "custom-export")
	span.End()
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("关闭 Provider 失败: %v", err)
	}
	if output.Len() == 0 {
		t.Fatal("自定义导出器未收到 Span")
	}
}

func TestRegisterExporterSupportsPointerOption(t *testing.T) {
	err := RegisterExporter(&testPointerExporterOption{}, func(config ExporterConfig[*testPointerExporterOption]) (trace.SpanExporter, error) {
		return exporter.NewConsoleSpanExporter(exporter.WithWriter(config.Options.writer)), nil
	})
	if err != nil {
		t.Fatalf("注册指针配置导出器失败: %v", err)
	}

	output := &bytes.Buffer{}
	spanExporter, err := CreateExporterFromOption(&testPointerExporterOption{writer: output})
	if err != nil {
		t.Fatalf("创建指针配置导出器失败: %v", err)
	}
	if err := spanExporter.Shutdown(context.Background()); err != nil {
		t.Fatalf("关闭指针配置导出器失败: %v", err)
	}

	var nilOption *testPointerExporterOption
	if _, err := CreateExporterFromOption(nilOption); err == nil {
		t.Fatal("空指针配置应返回错误")
	}
}

func TestRegisterExporterRejectsDuplicateType(t *testing.T) {
	factory := func(config ExporterConfig[testDuplicateExporterOption]) (trace.SpanExporter, error) {
		return exporter.NewConsoleSpanExporter(), nil
	}
	if err := RegisterExporter(testDuplicateExporterOption{}, factory); err != nil {
		t.Fatalf("首次注册导出器失败: %v", err)
	}
	if err := RegisterExporter(testDuplicateExporterOption{}, factory); err == nil {
		t.Fatal("重复注册导出器类型应返回错误")
	}
}

func TestCreateExporterRejectsMismatchedConfigType(t *testing.T) {
	_, err := CreateExporter(ExporterConfig[testCustomExporterOption]{
		Type:    ExporterTypeFile,
		Options: testCustomExporterOption{writer: &bytes.Buffer{}},
	})
	if err == nil {
		t.Fatal("配置类型与选项类型不一致时应返回错误")
	}
}

func TestInitTracerRejectsMismatchedExporterType(t *testing.T) {
	provider, err := InitTracer(TracerConfig{
		ServiceName:    "mismatched-exporter",
		SampleRate:     1,
		ExporterType:   ExporterTypeFile,
		ExporterOption: testCustomExporterOption{writer: &bytes.Buffer{}},
	})
	if err == nil {
		t.Fatal("导出器类型与选项不一致时应返回错误")
	}
	if provider != nil {
		t.Fatalf("初始化失败后 provider 应为空，实际为 %T", provider)
	}
}

func TestInitTracerRejectsNilPointerExporterOption(t *testing.T) {
	var option *testPointerExporterOption
	provider, err := InitTracer(TracerConfig{
		ServiceName:    "nil-pointer-exporter",
		SampleRate:     1,
		ExporterOption: option,
	})
	if err == nil {
		t.Fatal("空指针导出器配置应返回错误")
	}
	if provider != nil {
		t.Fatalf("初始化失败后 provider 应为空，实际为 %T", provider)
	}
}

func TestCreateConfiguredSamplerBoundary(t *testing.T) {
	testCases := []struct {
		name       string
		sampleRate float64
		decision   types.SamplingDecision
	}{
		{name: "不采样", sampleRate: 0, decision: types.SamplingDecisionDrop},
		{name: "全采样", sampleRate: 1, decision: types.SamplingDecisionRecordAndSample},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			samplerInstance, err := createConfiguredSampler(testCase.sampleRate)
			if err != nil {
				t.Fatalf("创建采样器失败: %v", err)
			}
			result := samplerInstance.ShouldSample(types.SamplingParameters{TraceID: "fixed-trace-id"})
			if result.Decision != testCase.decision {
				t.Fatalf("采样决策 = %v，期望 %v", result.Decision, testCase.decision)
			}
		})
	}

	for _, sampleRate := range []float64{-0.01, 1.01} {
		if _, err := createConfiguredSampler(sampleRate); err == nil {
			t.Fatalf("非法采样率 %v 应返回错误", sampleRate)
		}
	}
}
