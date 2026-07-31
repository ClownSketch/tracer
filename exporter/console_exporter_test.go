package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// TestConsoleExporter_ExportSpan 测试单个Span导出
func TestConsoleExporter_ExportSpan(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewConsoleSpanExporter(WithWriter(&buf))

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

	output := buf.String()
	if !strings.Contains(output, "test-span") {
		t.Errorf("期望输出包含 'test-span'，实际输出: %s", output)
	}
}

// TestConsoleExporter_ExportSpans 测试批量Span导出
func TestConsoleExporter_ExportSpans(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewConsoleSpanExporter(WithWriter(&buf))

	now := time.Now()
	spans := make([]trace.SpanSnapshot, 3)
	for i := 0; i < 3; i++ {
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

	output := buf.String()
	if !strings.Contains(output, "test-span") {
		t.Errorf("期望输出包含 'test-span'，实际输出: %s", output)
	}
}

// TestConsoleExporter_ExportSpans_Empty 测试空Span列表
func TestConsoleExporter_ExportSpans_Empty(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewConsoleSpanExporter(WithWriter(&buf))

	exporter.ExportSpans([]trace.SpanSnapshot{})

	output := buf.String()
	if output != "" {
		t.Errorf("期望输出为空，实际输出: %s", output)
	}
}

// TestConsoleExporter_ExportSpans_Nil 测试nil Span
func TestConsoleExporter_ExportSpans_Nil(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewConsoleSpanExporter(WithWriter(&buf))

	spans := []trace.SpanSnapshot{nil, nil}
	exporter.ExportSpans(spans)

	output := buf.String()
	if output != "" {
		t.Errorf("期望输出为空，实际输出: %s", output)
	}
}

// TestConsoleExporter_WithJSON 测试JSON格式输出
func TestConsoleExporter_WithJSON(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewConsoleSpanExporter(
		WithWriter(&buf),
		WithJSON(true),
		WithPrettyPrint(false),
	)

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

	output := buf.String()
	var data map[string]any
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		t.Errorf("期望输出为有效的JSON，实际输出: %s, 错误: %v", output, err)
	}
}

// TestConsoleExporter_WithPrettyPrint 测试美化输出
func TestConsoleExporter_WithPrettyPrint(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewConsoleSpanExporter(
		WithWriter(&buf),
		WithJSON(true),
		WithPrettyPrint(true),
	)

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

	output := buf.String()
	var data map[string]any
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		t.Errorf("期望输出为有效的JSON，实际输出: %s, 错误: %v", output, err)
	}

	// 美化输出应该包含换行和缩进
	if !strings.Contains(output, "\n") {
		t.Errorf("期望美化输出包含换行，实际输出: %s", output)
	}
}

// TestConsoleExporter_Shutdown 测试关闭导出器
func TestConsoleExporter_Shutdown(t *testing.T) {
	exporter := NewConsoleSpanExporter()

	ctx := context.Background()
	if err := exporter.Shutdown(ctx); err != nil {
		t.Errorf("期望关闭成功，实际错误: %v", err)
	}
}
