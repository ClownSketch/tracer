package exporter

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

var spanIDCounter int64

// TestOTLPExporter_ExportSpan 测试单个Span导出
func TestOTLPExporter_ExportSpan(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(server.URL() + "/v1/traces")
	if err != nil {
		t.Fatalf("创建OTLP导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	now := time.Now()
	span := mock.NewSpanSnapshotMock(1)
	span.SpanName = "test-span"
	span.SpanTraceID = "12345678901234567890123456789012"
	span.SpanContext.SpanID = "1234567890123456"
	span.SpanParentSpanID = "1234567890123455"
	span.SpanKind = types.SpanKindServer
	span.StartTime = now
	span.EndTime = now.Add(100 * time.Millisecond)

	exporter.ExportSpan(span)
	time.Sleep(100 * time.Millisecond)

	stats := exporter.GetStats()
	if stats["processed"] != 1 {
		t.Errorf("期望处理1个span，实际处理%d个", stats["processed"])
	}
}

// TestOTLPExporter_ExportSpans 测试批量Span导出
func TestOTLPExporter_ExportSpans(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(server.URL()+"/v1/traces",
		WithOTLPBatchSize(10),
		WithOTLPFlushInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建OTLP导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	spans := make([]trace.SpanSnapshot, 20)
	now := time.Now()
	for i := 0; i < 20; i++ {
		span := mock.NewSpanSnapshotMock(i)
		span.SpanName = "test-span"
		span.SpanTraceID = "12345678901234567890123456789012"
		span.SpanContext.SpanID = "1234567890123456"
		span.SpanParentSpanID = "1234567890123455"
		span.SpanKind = types.SpanKindServer
		span.StartTime = now
		span.EndTime = now.Add(100 * time.Millisecond)
		spans[i] = span
	}

	exporter.ExportSpans(spans)
	time.Sleep(500 * time.Millisecond)

	stats := exporter.GetStats()
	if stats["processed"] != 20 {
		t.Errorf("期望处理20个span，实际处理%d个", stats["processed"])
	}
}

// TestOTLPExporter_GetStats 测试获取统计信息
func TestOTLPExporter_GetStats(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(server.URL() + "/v1/traces")
	if err != nil {
		t.Fatalf("创建OTLP导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	now := time.Now()
	span := mock.NewSpanSnapshotMock(1)
	span.SpanName = "test-span"
	span.SpanTraceID = "12345678901234567890123456789012"
	span.SpanContext.SpanID = "1234567890123456"
	span.SpanParentSpanID = "1234567890123455"
	span.SpanKind = types.SpanKindServer
	span.StartTime = now
	span.EndTime = now.Add(100 * time.Millisecond)

	exporter.ExportSpan(span)
	time.Sleep(100 * time.Millisecond)

	stats := exporter.GetStats()
	if stats["processed"] < 1 {
		t.Errorf("期望处理数量 >= 1，实际: %d", stats["processed"])
	}
	if stats["queue_len"] < 0 {
		t.Errorf("期望队列长度 >= 0，实际: %d", stats["queue_len"])
	}
	if stats["queue_cap"] < 0 {
		t.Errorf("期望队列容量 >= 0，实际: %d", stats["queue_cap"])
	}
}

// TestOTLPExporter_WithOptions 测试各种选项
func TestOTLPExporter_WithOptions(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(
		server.URL()+"/v1/traces",
		WithOTLPBatchSize(100),
		WithOTLPFlushInterval(200*time.Millisecond),
		WithOTLPQueueSize(1000),
		WithOTLPTimeout(5*time.Second),
		WithOTLPEnableGzip(true),
	)
	if err != nil {
		t.Fatalf("创建OTLP导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	if exporter == nil {
		t.Error("期望导出器不为 nil")
	}
}

// TestOTLPExporter_GzipResourceAndStatus 验证 gzip、服务名和状态映射完整写入请求。
func TestOTLPExporter_GzipResourceAndStatus(t *testing.T) {
	var received otlpExportRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("缺少 gzip 请求头")
		}
		gzipReader, err := gzip.NewReader(request.Body)
		if err != nil {
			t.Errorf("读取 gzip 请求失败: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		defer gzipReader.Close()
		data, err := io.ReadAll(gzipReader)
		if err != nil {
			t.Errorf("读取请求体失败: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(data, &received); err != nil {
			t.Errorf("解析请求体失败: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	otlpExporter, err := NewOTLPExporter(server.URL, WithOTLPEnableGzip(true))
	if err != nil {
		t.Fatalf("创建 OTLP 导出器失败: %v", err)
	}
	span := mock.NewSpanSnapshotMock(1)
	span.Resource = &types.ResourceInfo{ServiceName: "gateway"}
	span.Status = types.SpanStatus{Code: types.StatusCodeOk}

	if err := otlpExporter.ExportSpan(span); err != nil {
		t.Fatalf("导出 Span 失败: %v", err)
	}

	resourceSpans := received.ResourceSpans
	if len(resourceSpans) != 1 || resourceSpans[0].Resource.Attributes["service.name"] != "gateway" {
		t.Fatalf("服务名称写入错误: %+v", resourceSpans)
	}
	exportedSpans := resourceSpans[0].ScopeSpans[0].Spans
	if len(exportedSpans) != 1 || exportedSpans[0].Status == nil || exportedSpans[0].Status.Code != "STATUS_CODE_OK" {
		t.Fatalf("状态映射错误: %+v", exportedSpans)
	}
}

// TestOTLPExporter_ExportSpans_Empty 测试空Span列表
func TestOTLPExporter_ExportSpans_Empty(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(server.URL() + "/v1/traces")
	if err != nil {
		t.Fatalf("创建OTLP导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	exporter.ExportSpans([]trace.SpanSnapshot{})

	stats := exporter.GetStats()
	if stats["processed"] != 0 {
		t.Errorf("期望处理数量为 0，实际: %d", stats["processed"])
	}
}

// TestOTLPExporter_Shutdown_Timeout 测试关闭超时
func TestOTLPExporter_Shutdown_Timeout(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(server.URL() + "/v1/traces")
	if err != nil {
		t.Fatalf("创建OTLP导出器失败: %v", err)
	}

	// 使用很短的超时时间
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	defer cancel()

	// 应该返回超时错误
	err = exporter.Shutdown(ctx)
	if err == nil {
		t.Error("期望返回超时错误")
	}
}

// BenchmarkOTLPExporter_ExportSpan_Parallel 基准测试：高并发单个Span导出
func BenchmarkOTLPExporter_ExportSpan_Parallel(b *testing.B) {
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(server.URL()+"/v1/traces",
		WithOTLPBatchSize(500),
		WithOTLPFlushInterval(500*time.Millisecond),
		WithOTLPQueueSize(10000),
	)
	if err != nil {
		b.Fatalf("创建OTLP导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
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
			exporter.ExportSpan(span)
			id++
		}
	})
}

// BenchmarkOTLPExporter_ExportSpans_Parallel 基准测试：高并发批量Span导出
func BenchmarkOTLPExporter_ExportSpans_Parallel(b *testing.B) {
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewOTLPExporter(server.URL()+"/v1/traces",
		WithOTLPBatchSize(500),
		WithOTLPFlushInterval(500*time.Millisecond),
		WithOTLPQueueSize(10000),
	)
	if err != nil {
		b.Fatalf("创建OTLP导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			spans := make([]trace.SpanSnapshot, 10)
			now := time.Now()
			for j := 0; j < 10; j++ {
				span := mock.NewSpanSnapshotMock(id*10 + j)
				span.SpanName = "test-span"
				span.SpanTraceID = "12345678901234567890123456789012"
				span.SpanContext.SpanID = "1234567890123456"
				span.SpanParentSpanID = "1234567890123455"
				span.SpanKind = types.SpanKindServer
				span.StartTime = now
				span.EndTime = now.Add(100 * time.Millisecond)
				spans[j] = span
			}
			exporter.ExportSpans(spans)
			id++
		}
	})
}
