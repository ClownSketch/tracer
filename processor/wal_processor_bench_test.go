package processor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
)

type syncBenchExporter struct {
	exportCallCount int64
	exportSpanCount int64
}

func (m *syncBenchExporter) ExportSpan(span trace.SpanSnapshot) error {
	return m.ExportSpanSync(context.Background(), span)
}

func (m *syncBenchExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	return m.ExportSpansSync(context.Background(), spans)
}

func (m *syncBenchExporter) Shutdown(context.Context) error {
	return nil
}

func (m *syncBenchExporter) ExportSpanSync(ctx context.Context, span trace.SpanSnapshot) error {
	return m.ExportSpansSync(ctx, []trace.SpanSnapshot{span})
}

func (m *syncBenchExporter) ExportSpansSync(ctx context.Context, spans []trace.SpanSnapshot) error {
	atomic.AddInt64(&m.exportCallCount, 1)
	atomic.AddInt64(&m.exportSpanCount, int64(len(spans)))

	for _, span := range spans {
		if span != nil {
			span.Release()
		}
	}
	return nil
}

// BenchmarkWALSpanProcessor_OnEnd 基准测试：WAL 主路径 OnEnd 性能。
func BenchmarkWALSpanProcessor_OnEnd(b *testing.B) {
	exporter := &syncBenchExporter{}
	processor, err := NewWALSpanProcessor(exporter,
		WithWALDir(b.TempDir()),
		WithWALExportBatchSize(100),
		WithWALPollInterval(20*time.Millisecond),
		WithWALSegmentSize(32*1024*1024),
	)
	if err != nil {
		b.Fatalf("创建 WAL 处理器失败: %v", err)
	}
	defer processor.Shutdown(context.Background())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			span := mock.NewSpanSnapshotMock(id)
			processor.OnEnd(span)
			id++
		}
	})
}

// BenchmarkWALSpanProcessor_OnEnd_HighConcurrency 基准测试：高并发场景下的 WAL OnEnd 性能。
func BenchmarkWALSpanProcessor_OnEnd_HighConcurrency(b *testing.B) {
	exporter := &syncBenchExporter{}
	processor, err := NewWALSpanProcessor(exporter,
		WithWALDir(b.TempDir()),
		WithWALExportBatchSize(500),
		WithWALPollInterval(10*time.Millisecond),
		WithWALSegmentSize(64*1024*1024),
	)
	if err != nil {
		b.Fatalf("创建 WAL 处理器失败: %v", err)
	}
	defer processor.Shutdown(context.Background())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddInt64(&spanIDCounter, 1))
		for pb.Next() {
			span := mock.NewSpanSnapshotMock(id)
			processor.OnEnd(span)
			id++
		}
	})
}
