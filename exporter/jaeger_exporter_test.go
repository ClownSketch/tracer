package exporter

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// TestJaegerExporter_ExportSpan 测试单个Span导出
func TestJaegerExporter_ExportSpan(t *testing.T) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL() + "/api/traces")
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
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
	time.Sleep(100 * time.Millisecond)

	// 验证统计信息
	stats := exporter.GetStats()
	if stats["processed"] != 1 {
		t.Errorf("期望处理1个span，实际处理%d个", stats["processed"])
	}
}

// TestJaegerExporter_ExportSpans 测试批量Span导出
func TestJaegerExporter_ExportSpans(t *testing.T) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL()+"/api/traces",
		WithJaegerBatchSize(10),
		WithJaegerFlushInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	// 创建测试Spans
	spans := make([]trace.SpanSnapshot, 20)
	for i := 0; i < 20; i++ {
		now := time.Now()
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

// TestJaegerExporter_DirectExport 测试直接批量写路径
func TestJaegerExporter_DirectExport(t *testing.T) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL()+"/api/traces",
		WithJaegerBatchSize(10),
		WithJaegerFlushInterval(1*time.Second),
	)
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	// 直接创建大量 spans，验证同步导出不会依赖内部队列
	for i := 0; i < 100; i++ {
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

	// 等待处理完成
	time.Sleep(2 * time.Second)

	// 验证统计信息
	stats := exporter.GetStats()
	if stats["processed"] != 100 {
		t.Errorf("期望处理100个span，实际处理%d个", stats["processed"])
	}
}

// TestJaegerExporter_Shutdown 测试优雅关闭
func TestJaegerExporter_Shutdown(t *testing.T) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL() + "/api/traces")
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
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

// TestJaegerExporter_GetStats 测试获取统计信息
func TestJaegerExporter_GetStats(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL() + "/api/traces")
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
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

// TestJaegerExporter_WithOptions 测试各种选项
func TestJaegerExporter_WithOptions(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(
		server.URL()+"/api/traces",
		WithJaegerBatchSize(100),
		WithJaegerFlushInterval(200*time.Millisecond),
		WithJaegerQueueSize(1000),
		WithJaegerTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	if exporter == nil {
		t.Error("期望导出器不为 nil")
	}
}

// TestJaegerExporter_ExportSpans_Empty 测试空Span列表
func TestJaegerExporter_ExportSpans_Empty(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL() + "/api/traces")
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	exporter.ExportSpans([]trace.SpanSnapshot{})

	stats := exporter.GetStats()
	if stats["processed"] != 0 {
		t.Errorf("期望处理数量为 0，实际: %d", stats["processed"])
	}
}

// TestJaegerExporter_Shutdown_Timeout 测试关闭超时
func TestJaegerExporter_Shutdown_Timeout(t *testing.T) {
	server := newMockHTTPServer(t, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL() + "/api/traces")
	if err != nil {
		t.Fatalf("创建Jaeger导出器失败: %v", err)
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

// BenchmarkJaegerExporter_ExportSpan 基准测试：单个Span导出
func BenchmarkJaegerExporter_ExportSpan(b *testing.B) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL()+"/api/traces",
		WithJaegerBatchSize(100),
		WithJaegerFlushInterval(1*time.Second),
	)
	if err != nil {
		b.Fatalf("创建Jaeger导出器失败: %v", err)
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 创建新的span（避免重复使用）
		now := time.Now()
		newSpan := mock.NewSpanSnapshotMock(i)
		newSpan.SpanName = "test-span"
		newSpan.SpanTraceID = "12345678901234567890123456789012"
		newSpan.SpanContext.SpanID = "1234567890123456"
		newSpan.SpanParentSpanID = "1234567890123455"
		newSpan.SpanKind = types.SpanKindServer
		newSpan.StartTime = now
		newSpan.EndTime = now.Add(100 * time.Millisecond)
		exporter.ExportSpan(newSpan)
	}
}

// BenchmarkJaegerExporter_ExportSpan_Parallel 基准测试：高并发单个Span导出
func BenchmarkJaegerExporter_ExportSpan_Parallel(b *testing.B) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL()+"/api/traces",
		WithJaegerBatchSize(500),
		WithJaegerFlushInterval(500*time.Millisecond),
		WithJaegerQueueSize(10000),
	)
	if err != nil {
		b.Fatalf("创建Jaeger导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

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
			exporter.ExportSpan(span)
			id++
		}
	})
}

// BenchmarkJaegerExporter_ExportSpans 基准测试：批量Span导出
func BenchmarkJaegerExporter_ExportSpans(b *testing.B) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL()+"/api/traces",
		WithJaegerBatchSize(100),
		WithJaegerFlushInterval(1*time.Second),
	)
	if err != nil {
		b.Fatalf("创建Jaeger导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		spans := make([]trace.SpanSnapshot, 10)
		now := time.Now()
		for j := 0; j < 10; j++ {
			span := mock.NewSpanSnapshotMock(i*10 + j)
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
	}
}

// BenchmarkJaegerExporter_ExportSpans_Parallel 基准测试：高并发批量Span导出
func BenchmarkJaegerExporter_ExportSpans_Parallel(b *testing.B) {
	// 创建mock HTTP服务器
	server := newMockHTTPServer(b, 200)
	defer server.Close()

	exporter, err := NewJaegerExporter(server.URL()+"/api/traces",
		WithJaegerBatchSize(500),
		WithJaegerFlushInterval(500*time.Millisecond),
		WithJaegerQueueSize(10000),
	)
	if err != nil {
		b.Fatalf("创建Jaeger导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := 0
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

// BenchmarkJaegerExporter_ConvertToJaegerSpan 基准测试：Span转换性能
func BenchmarkJaegerExporter_ConvertToJaegerSpan(b *testing.B) {
	exporter := &JaegerExporter{}

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

	// 设置属性
	span.Attributes = map[string]any{
		"key1": "value1",
		"key2": 123,
		"key3": true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = exporter.convertToJaegerSpan(span)
	}
}

// newMockHTTPServer 创建mock HTTP服务器
func newMockHTTPServer(t testing.TB, statusCode int) *mockHTTPServer {
	server := &mockHTTPServer{
		t:          t,
		statusCode: statusCode,
		requests:   make(chan []byte, 100),
	}
	server.start()
	return server
}

// mockHTTPServer mock HTTP服务器
type mockHTTPServer struct {
	t          testing.TB
	statusCode int
	requests   chan []byte
	server     *http.Server
	listener   net.Listener
	url        string
}

func (m *mockHTTPServer) start() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case m.requests <- body:
		default:
		}

		// 返回响应
		w.WriteHeader(m.statusCode)
	})

	m.server = &http.Server{
		Handler: mux,
	}

	// 启动服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		m.t.Fatalf("启动mock服务器失败: %v", err)
	}

	m.url = "http://" + listener.Addr().String()
	m.listener = listener

	go func() {
		defer func() {
			if err := recover(); err != nil {
				// 恢复panic，不输出日志
				_ = err
			}
		}()
		m.server.Serve(listener)
	}()
}

func (m *mockHTTPServer) Close() {
	if m.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// 关闭服务器，等待所有连接关闭
		if err := m.server.Shutdown(ctx); err != nil {
			// 如果优雅关闭失败，强制关闭 listener
			if m.listener != nil {
				m.listener.Close()
			}
		}
	}
	// 确保 listener 被关闭
	if m.listener != nil {
		m.listener.Close()
	}
}

func (m *mockHTTPServer) URL() string {
	if m.url == "" {
		return "http://localhost:0"
	}
	return m.url
}
