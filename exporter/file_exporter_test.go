package exporter

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// TestFileExporter_ExportSpan 测试单个Span导出
func TestFileExporter_ExportSpan(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	exporter, err := NewFileSpanExporter(WithFilePath(filePath))
	if err != nil {
		t.Fatalf("创建文件导出器失败: %v", err)
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

	// 等待写入完成
	time.Sleep(200 * time.Millisecond)

	// 验证文件内容
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}

	if !strings.Contains(string(content), "test-span") {
		t.Errorf("期望文件内容包含 'test-span'，实际内容: %s", string(content))
	}
}

// TestFileExporter_ExportSpans 测试批量Span导出
func TestFileExporter_ExportSpans(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	exporter, err := NewFileSpanExporter(WithFilePath(filePath))
	if err != nil {
		t.Fatalf("创建文件导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	now := time.Now()
	spans := make([]trace.SpanSnapshot, 5)
	for i := 0; i < 5; i++ {
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

	// 等待写入完成
	time.Sleep(200 * time.Millisecond)

	// 验证文件内容
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}

	if !strings.Contains(string(content), "test-span") {
		t.Errorf("期望文件内容包含 'test-span'，实际内容: %s", string(content))
	}
}

// TestFileExporter_ExportSpans_Empty 测试空Span列表
func TestFileExporter_ExportSpans_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	exporter, err := NewFileSpanExporter(WithFilePath(filePath))
	if err != nil {
		t.Fatalf("创建文件导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	exporter.ExportSpans([]trace.SpanSnapshot{})

	// 等待写入完成
	time.Sleep(200 * time.Millisecond)

	// 验证文件内容为空
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}

	if len(content) > 0 {
		t.Errorf("期望文件内容为空，实际内容: %s", string(content))
	}
}

// TestFileExporter_WithOptions 测试各种选项
func TestFileExporter_WithOptions(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	exporter, err := NewFileSpanExporter(
		WithFilePath(filePath),
		WithMaxFileSize(1024*1024), // 1MB
		WithRotateInterval(1*time.Hour),
		WithMaxBackups(10),
		WithAsyncBufferSize(500),
		WithEnqueueTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("创建文件导出器失败: %v", err)
	}
	defer exporter.Shutdown(context.Background())

	if exporter == nil {
		t.Error("期望导出器不为 nil")
	}
}

// TestFileExporter_Shutdown 测试关闭导出器
func TestFileExporter_Shutdown(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	exporter, err := NewFileSpanExporter(WithFilePath(filePath))
	if err != nil {
		t.Fatalf("创建文件导出器失败: %v", err)
	}

	ctx := context.Background()
	if err := exporter.Shutdown(ctx); err != nil {
		t.Errorf("期望关闭成功，实际错误: %v", err)
	}
	if err := exporter.ExportSpan(mock.NewSpanSnapshotMock(1)); err == nil {
		t.Error("关闭后导出应返回错误")
	}
}

// TestFileExporter_GetStats 测试获取统计信息
func TestFileExporter_GetStats(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	exporter, err := NewFileSpanExporter(WithFilePath(filePath))
	if err != nil {
		t.Fatalf("创建文件导出器失败: %v", err)
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

	// 等待写入完成
	time.Sleep(200 * time.Millisecond)

	writeCount, errorCount, currentSize := exporter.GetStats()
	if writeCount < 1 {
		t.Errorf("期望写入计数 >= 1，实际: %d", writeCount)
	}
	if errorCount < 0 {
		t.Errorf("期望错误计数 >= 0，实际: %d", errorCount)
	}
	if currentSize <= 0 {
		t.Errorf("期望当前文件大小 > 0，实际: %d", currentSize)
	}
}

// TestFileExporter_InvalidPath 测试无效路径
func TestFileExporter_InvalidPath(t *testing.T) {
	// 使用无效路径（根目录，通常没有写权限）
	_, err := NewFileSpanExporter(WithFilePath("/invalid/path/test.log"))
	if err == nil {
		t.Error("期望创建文件导出器失败，但成功了")
	}
}

// TestFileExporter_RotateBySize 验证文件达到阈值后能够完成轮转且不会死锁。
func TestFileExporter_RotateBySize(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "rotate.log")
	fileExporter, err := NewFileSpanExporter(
		WithFilePath(filePath),
		WithMaxFileSize(1),
		WithMaxBackups(2),
	)
	if err != nil {
		t.Fatalf("创建文件导出器失败: %v", err)
	}
	defer fileExporter.Shutdown(context.Background())

	if err := fileExporter.ExportSpan(mock.NewSpanSnapshotMock(1)); err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- fileExporter.ExportSpan(mock.NewSpanSnapshotMock(2))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("轮转写入失败: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("文件轮转发生死锁")
	}

	backups, err := filepath.Glob(filePath + ".*")
	if err != nil {
		t.Fatalf("查询备份文件失败: %v", err)
	}
	if len(backups) == 0 {
		t.Fatal("文件达到阈值后没有生成备份")
	}
}
