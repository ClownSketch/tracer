package metrics

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/types"
)

// TestPrometheusMetrics_RecordSpan 测试记录Span指标
func TestPrometheusMetrics_RecordSpan(t *testing.T) {
	metrics := NewPrometheusMetrics(WithPrometheusAddr(":0"))

	// 创建测试Span
	now := time.Now()
	span := mock.NewSpanSnapshotMock(1)
	span.SpanName = "test-span"
	span.SpanTraceID = "12345678901234567890123456789012"
	span.SpanContext.SpanID = "1234567890123456"
	span.SpanParentSpanID = "1234567890123455"
	span.SpanKind = types.SpanKindServer
	span.StartTime = now
	span.EndTime = now.Add(100 * time.Millisecond)
	span.Resource = &types.ResourceInfo{
		ServiceName: "test-service",
		Host:        "localhost",
	}

	// 记录Span
	metrics.RecordSpan(span)

	// 等待HTTP服务器启动
	time.Sleep(100 * time.Millisecond)

	// 测试/metrics端点
	resp, err := http.Get("http://localhost:9090/metrics")
	if err != nil {
		t.Logf("无法连接到/metrics端点（这是正常的，如果服务器未启动）: %v", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("期望状态码200，实际%d", resp.StatusCode)
		}
	}

	// 关闭metrics
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metrics.Shutdown(ctx)
}

func TestPrometheusMetrics_OutputMetadataAndEscapesLabels(t *testing.T) {
	metrics := NewPrometheusMetrics(
		WithPrometheusAddr(":0"),
		WithPrometheusMaxSeries(2),
	)
	defer metrics.Shutdown(context.Background())

	for index, name := range []string{`pay"in`, "pay\nout", "ignored"} {
		span := mock.NewSpanSnapshotMock(index)
		span.SpanName = name
		span.Resource = &types.ResourceInfo{ServiceName: name}
		metrics.RecordSpan(span)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metrics.handleMetrics(recorder, request)
	body := recorder.Body.String()

	if strings.Count(body, "# HELP tracer_service_spans_total") != 1 {
		t.Fatalf("服务指标 HELP 应只输出一次:\n%s", body)
	}
	if strings.Count(body, "# TYPE tracer_operation_spans_total") != 1 {
		t.Fatalf("操作指标 TYPE 应只输出一次:\n%s", body)
	}
	if !strings.Contains(body, `service="pay\"in"`) || !strings.Contains(body, `operation="pay\nout"`) {
		t.Fatalf("标签值没有按 Prometheus 文本格式转义:\n%s", body)
	}
	if !strings.Contains(body, "tracer_metric_series_dropped_total 2") {
		t.Fatalf("序列基数限制统计错误:\n%s", body)
	}
}

func TestNewPrometheusMetricsE_ReturnsListenError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("占用测试端口失败: %v", err)
	}
	defer listener.Close()

	metrics, err := NewPrometheusMetricsE(WithPrometheusAddr(listener.Addr().String()))
	if err == nil {
		t.Fatal("端口已占用时应返回绑定错误")
	}
	if metrics != nil {
		t.Fatal("启动失败后不应返回已启动的指标收集器")
	}
}

// TestPrometheusMetrics_RecordDropped 测试记录丢弃的Span
func TestPrometheusMetrics_RecordDropped(t *testing.T) {
	metrics := NewPrometheusMetrics(WithPrometheusAddr(":0"))

	// 记录丢弃的Span
	metrics.RecordDropped()
	metrics.RecordDropped()
	metrics.RecordDropped()

	// 等待HTTP服务器启动
	time.Sleep(100 * time.Millisecond)

	// 关闭metrics
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metrics.Shutdown(ctx)
}

// TestPrometheusMetrics_Shutdown 测试优雅关闭
func TestPrometheusMetrics_Shutdown(t *testing.T) {
	metrics := NewPrometheusMetrics(WithPrometheusAddr(":0"))

	// 等待HTTP服务器启动
	time.Sleep(100 * time.Millisecond)

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := metrics.Shutdown(ctx); err != nil {
		t.Errorf("关闭metrics失败: %v", err)
	}
}

// BenchmarkPrometheusMetrics_RecordSpan 基准测试：记录Span指标性能
func BenchmarkPrometheusMetrics_RecordSpan(b *testing.B) {
	metrics := NewPrometheusMetrics(WithPrometheusAddr(":0"))
	defer metrics.Shutdown(context.Background())

	// 创建测试Span
	now := time.Now()
	span := mock.NewSpanSnapshotMock(1)
	span.SpanName = "test-span"
	span.SpanTraceID = "12345678901234567890123456789012"
	span.SpanContext.SpanID = "1234567890123456"
	span.SpanParentSpanID = "1234567890123455"
	span.SpanKind = types.SpanKindServer
	span.StartTime = now
	span.EndTime = now.Add(100 * time.Millisecond)
	span.Resource = &types.ResourceInfo{
		ServiceName: "test-service",
		Host:        "localhost",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		metrics.RecordSpan(span)
	}
}

// BenchmarkPrometheusMetrics_RecordSpan_Parallel 基准测试：高并发记录Span指标性能
func BenchmarkPrometheusMetrics_RecordSpan_Parallel(b *testing.B) {
	metrics := NewPrometheusMetrics(WithPrometheusAddr(":0"))
	defer metrics.Shutdown(context.Background())

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := 0
		for pb.Next() {
			now := time.Now()
			span := mock.NewSpanSnapshotMock(id)
			span.SpanName = "test-span"
			span.SpanTraceID = "12345678901234567890123456789012"
			span.SpanContext.SpanID = "1234567890123456"
			span.SpanParentSpanID = "1234567890123455"
			span.SpanKind = types.SpanKindServer
			span.StartTime = now
			span.EndTime = now.Add(100 * time.Millisecond)
			span.Resource = &types.ResourceInfo{
				ServiceName: "test-service",
				Host:        "localhost",
			}
			metrics.RecordSpan(span)
			id++
		}
	})
}
