package processor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
)

type syncMockExporter struct {
	fail          atomic.Bool
	exportCount   atomic.Int64
	shutdownCount atomic.Int64

	mu        sync.Mutex
	spanNames []string
}

func (m *syncMockExporter) ExportSpan(span trace.SpanSnapshot) error {
	return m.ExportSpanSync(context.Background(), span)
}

func (m *syncMockExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	return m.ExportSpansSync(context.Background(), spans)
}

func (m *syncMockExporter) Shutdown(context.Context) error {
	m.shutdownCount.Add(1)
	return nil
}

func newSyncMockExporter(shouldFail bool) *syncMockExporter {
	exporter := &syncMockExporter{}
	exporter.fail.Store(shouldFail)
	return exporter
}

func (m *syncMockExporter) ExportSpanSync(ctx context.Context, span trace.SpanSnapshot) error {
	return m.ExportSpansSync(ctx, []trace.SpanSnapshot{span})
}

func (m *syncMockExporter) ExportSpansSync(ctx context.Context, spans []trace.SpanSnapshot) error {
	m.exportCount.Add(int64(len(spans)))
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		if span != nil {
			names = append(names, span.GetSpanName())
			span.Release()
		}
	}

	if m.fail.Load() {
		return errors.New("forced export failure")
	}

	m.mu.Lock()
	m.spanNames = append(m.spanNames, names...)
	m.mu.Unlock()
	return nil
}

func (m *syncMockExporter) exportedNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.spanNames))
	copy(out, m.spanNames)
	return out
}

func TestWALSpanProcessor_ReplaysFromDiskAcrossRestart(t *testing.T) {
	walDir := t.TempDir()

	firstExporter := newSyncMockExporter(true)
	firstProcessor, err := NewWALSpanProcessor(
		firstExporter,
		WithWALDir(walDir),
		WithWALPollInterval(20*time.Millisecond),
		WithWALExportBatchSize(2),
		WithWALSegmentSize(1024),
	)
	if err != nil {
		t.Fatalf("创建首个 WAL 处理器失败: %v", err)
	}

	for i := 0; i < 3; i++ {
		firstProcessor.OnEnd(mock.NewSpanSnapshotMock(i + 1))
	}

	time.Sleep(150 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := firstProcessor.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("第一次关闭 WAL 处理器失败: %v", err)
	}

	secondExporter := newSyncMockExporter(false)
	secondProcessor, err := NewWALSpanProcessor(
		secondExporter,
		WithWALDir(walDir),
		WithWALPollInterval(20*time.Millisecond),
		WithWALExportBatchSize(2),
		WithWALSegmentSize(1024),
	)
	if err != nil {
		t.Fatalf("创建重启 WAL 处理器失败: %v", err)
	}

	if err := waitForCondition(5*time.Second, func() bool {
		return len(secondExporter.exportedNames()) == 3
	}); err != nil {
		t.Fatalf("等待 WAL 重放完成失败: %v", err)
	}

	if err := secondProcessor.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("第二次关闭 WAL 处理器失败: %v", err)
	}

	names := secondExporter.exportedNames()
	if len(names) != 3 {
		t.Fatalf("期望重放 3 个 span，实际重放 %d 个", len(names))
	}
}

func TestWALSpanProcessor_ShutdownContinuesAfterCallerTimeout(t *testing.T) {
	exporter := newSyncMockExporter(false)
	spanProcessor, err := NewWALSpanProcessor(
		exporter,
		WithWALDir(t.TempDir()),
		WithWALPollInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("创建 WAL 处理器失败: %v", err)
	}
	processor := spanProcessor.(*WALSpanProcessor)

	processor.flushWg.Add(1)
	firstCtx, firstCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer firstCancel()
	if err := processor.Shutdown(firstCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("首次关闭应返回超时错误，实际: %v", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- processor.Shutdown(context.Background())
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("后台清理完成前不应结束关闭，实际: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	processor.flushWg.Done()
	if err := <-secondDone; err != nil {
		t.Fatalf("再次关闭应等待后台清理完成: %v", err)
	}
	if exporter.shutdownCount.Load() != 1 {
		t.Fatalf("exporter Shutdown 调用次数错误: %d", exporter.shutdownCount.Load())
	}
}

func waitForCondition(timeout time.Duration, fn func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("condition not met before timeout")
}
