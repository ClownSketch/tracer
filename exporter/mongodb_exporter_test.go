package exporter

import (
	"context"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// TestMongoDBExporter_ExportSpan 测试单个Span导出
func TestMongoDBExporter_ExportSpan(t *testing.T) {
	// 注意：这个测试需要真实的MongoDB连接
	// 如果没有MongoDB，可以跳过测试
	t.Skip("需要MongoDB连接，跳过测试")

	exporter, err := NewMongoDBExporter(
		"mongodb://localhost:27017",
		"test_db",
		"test_collection",
		WithMongoDBBatchSize(10),
		WithMongoDBFlushInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建MongoDB导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

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

	// 导出Span
	exporter.ExportSpan(span)

	// 等待处理完成
	time.Sleep(500 * time.Millisecond)

	// 验证统计信息
	stats := exporter.GetStats()
	if stats["processed"] != 1 {
		t.Errorf("期望处理1个span，实际处理%d个", stats["processed"])
	}
}

// TestMongoDBExporter_ExportSpans 测试批量Span导出
func TestMongoDBExporter_ExportSpans(t *testing.T) {
	t.Skip("需要MongoDB连接，跳过测试")

	exporter, err := NewMongoDBExporter(
		"mongodb://localhost:27017",
		"test_db",
		"test_collection",
		WithMongoDBBatchSize(10),
		WithMongoDBFlushInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建MongoDB导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	// 创建测试Spans
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

	// 导出Spans
	exporter.ExportSpans(spans)

	// 等待处理完成
	time.Sleep(500 * time.Millisecond)

	// 验证统计信息
	stats := exporter.GetStats()
	if stats["processed"] != 20 {
		t.Errorf("期望处理20个span，实际处理%d个", stats["processed"])
	}
}

// TestMongoDBExporter_Shutdown 测试优雅关闭
func TestMongoDBExporter_Shutdown(t *testing.T) {
	t.Skip("需要MongoDB连接，跳过测试")

	exporter, err := NewMongoDBExporter(
		"mongodb://localhost:27017",
		"test_db",
		"test_collection",
	)
	if err != nil {
		t.Fatalf("创建MongoDB导出器失败: %v", err)
	}

	// 创建并导出一些Spans
	for i := 0; i < 10; i++ {
		now := time.Now()
		span := mock.NewSpanSnapshotMock(i)
		span.SpanName = "test-span"
		span.SpanTraceID = "12345678901234567890123456789012"
		span.SpanContext.SpanID = "1234567890123456"
		span.SpanParentSpanID = "1234567890123455"
		span.SpanKind = types.SpanKindServer
		span.StartTime = now
		span.EndTime = now.Add(100 * time.Millisecond)
		exporter.ExportSpan(span)
	}

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := exporter.Shutdown(ctx); err != nil {
		t.Errorf("关闭导出器失败: %v", err)
	}
}
