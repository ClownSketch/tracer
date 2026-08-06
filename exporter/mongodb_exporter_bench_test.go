package exporter

import (
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/types"
)

func BenchmarkMongoDBExporter_BuildDocument(b *testing.B) {
	exporter := newMongoDBExporterWithDefaults()
	now := time.Now()
	span := mock.NewSpanSnapshotMock(1)
	span.SpanName = "mongo-bench-span"
	span.SpanTraceID = "12345678901234567890123456789012"
	span.SpanContext.SpanID = "1234567890123456"
	span.SpanParentSpanID = "1234567890123455"
	span.SpanKind = types.SpanKindServer
	span.StartTime = now
	span.EndTime = now.Add(15 * time.Millisecond)
	span.Attributes = map[string]any{
		"service": "gateway",
		"method":  "POST",
		"path":    "/resources",
		"retry":   false,
		"amount":  128.5,
	}
	span.Events = []types.SpanEvent{
		{
			Name:      "validated",
			Timestamp: now.Format(time.RFC3339Nano),
			Attributes: map[string]any{
				"stage": "request",
				"ok":    true,
			},
		},
	}
	span.Logs = []types.SpanLog{
		{
			Timestamp: now.Format(time.RFC3339Nano),
			Message:   "request accepted",
			Severity:  types.SpanLogSeverityInfo,
			Attributes: map[string]any{
				"channel": "api",
			},
			Fields: map[string]any{
				"provider": "mock",
				"cost":     32,
			},
		},
	}
	span.Resource = &types.ResourceInfo{
		ServiceName: "tracer-bench",
		Host:        "127.0.0.1",
		Attributes: map[string]any{
			"env": "test",
		},
	}
	span.ResourceUsage = &types.ResourceMetrics{
		CPUUsage:    0.32,
		MemoryUsage: 1024,
		NetworkIO:   2048,
	}
	span.ErrorDetail = &types.ErrorDetail{
		Code:         "E_BENCH",
		Message:      "bench error",
		BusinessCode: "REQUEST_BENCH",
		MetaData: map[string]any{
			"retryable": false,
		},
		StackTrace: []types.StackFrame{
			{
				File:         "/tmp/file.go",
				FileName:     "file.go",
				FunctionName: "BenchmarkMongoDBExporter_BuildDocument",
				LineNumber:   42,
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = exporter.buildDocument(span, false)
	}
}

func BenchmarkMongoDBExporter_PrepareInsertDocs(b *testing.B) {
	exporter := newMongoDBExporterWithDefaults()
	items := make([]mongoQueueItem, 500)
	for i := range items {
		items[i] = mongoQueueItem{
			doc: mongoSpanDocument{
				Name:         "bench",
				TraceID:      "12345678901234567890123456789012",
				SpanID:       "1234567890123456",
				ParentSpanID: "1234567890123455",
				Kind:         types.SpanKindServer,
				StartTime:    time.Now(),
				EndTime:      time.Now().Add(5 * time.Millisecond),
				Duration:     int64(5 * time.Millisecond),
				CreatedAt:    time.Now().Unix(),
			},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docs := exporter.borrowDocsBuffer(len(items))
		for j, item := range items {
			docs[j] = item.doc
		}
		exporter.releaseDocsBuffer(docs)
	}
}
