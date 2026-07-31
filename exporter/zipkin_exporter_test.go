package exporter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// TestZipkinExporter_ExportSpan 测试单个Span导出
func TestZipkinExporter_ExportSpan(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(server.URL() + "/api/v2/spans")
	if err != nil {
		t.Fatalf("创建Zipkin导出器失败: %v", err)
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

// TestZipkinExporter_ExportSpans 测试批量Span导出
func TestZipkinExporter_ExportSpans(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(server.URL()+"/api/v2/spans",
		WithZipkinBatchSize(10),
		WithZipkinFlushInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建Zipkin导出器失败: %v", err)
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

// TestZipkinExporter_GetStats 测试获取统计信息
func TestZipkinExporter_GetStats(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(server.URL() + "/api/v2/spans")
	if err != nil {
		t.Fatalf("创建Zipkin导出器失败: %v", err)
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

// TestZipkinExporter_WithOptions 测试各种选项
func TestZipkinExporter_WithOptions(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(
		server.URL()+"/api/v2/spans",
		WithZipkinBatchSize(100),
		WithZipkinFlushInterval(200*time.Millisecond),
		WithZipkinQueueSize(1000),
		WithZipkinTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("创建Zipkin导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	if exporter == nil {
		t.Error("期望导出器不为 nil")
	}
}

// TestZipkinExporter_ExportSpans_Empty 测试空Span列表
func TestZipkinExporter_ExportSpans_Empty(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(server.URL() + "/api/v2/spans")
	if err != nil {
		t.Fatalf("创建Zipkin导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	exporter.ExportSpans([]trace.SpanSnapshot{})

	stats := exporter.GetStats()
	if stats["processed"] != 0 {
		t.Errorf("期望处理数量为 0，实际: %d", stats["processed"])
	}
}

// TestZipkinExporter_Shutdown_Timeout 测试关闭超时
func TestZipkinExporter_Shutdown_Timeout(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(server.URL() + "/api/v2/spans")
	if err != nil {
		t.Fatalf("创建Zipkin导出器失败: %v", err)
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

// BenchmarkZipkinExporter_ExportSpan_Parallel 基准测试：高并发单个Span导出
func BenchmarkZipkinExporter_ExportSpan_Parallel(b *testing.B) {
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(server.URL()+"/api/v2/spans",
		WithZipkinBatchSize(500),
		WithZipkinFlushInterval(500*time.Millisecond),
		WithZipkinQueueSize(10000),
	)
	if err != nil {
		b.Fatalf("创建Zipkin导出器失败: %v", err)
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

// BenchmarkZipkinExporter_ExportSpans_Parallel 基准测试：高并发批量Span导出
func BenchmarkZipkinExporter_ExportSpans_Parallel(b *testing.B) {
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewZipkinExporter(server.URL()+"/api/v2/spans",
		WithZipkinBatchSize(500),
		WithZipkinFlushInterval(500*time.Millisecond),
		WithZipkinQueueSize(10000),
	)
	if err != nil {
		b.Fatalf("创建Zipkin导出器失败: %v", err)
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
