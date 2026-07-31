package processor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ClownSketch/tracer/mock"
)

func TestNewWALSpanProcessorRejectsNilExporter(t *testing.T) {
	processor, err := NewWALSpanProcessor(nil, WithWALDir(t.TempDir()))
	if err == nil {
		t.Fatal("expected nil exporter error")
	}
	if processor != nil {
		t.Fatal("processor must be nil after initialization failure")
	}
}

func TestNewWALSpanProcessorRejectsInvalidDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal-file")
	if err := os.WriteFile(path, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create test file: %v", err)
	}

	processor, err := NewWALSpanProcessor(newSyncMockExporter(false), WithWALDir(path))
	if err == nil {
		t.Fatal("expected invalid WAL directory error")
	}
	if processor != nil {
		t.Fatal("processor must be nil after initialization failure")
	}
}

func TestWALSpanProcessorUsesDirectExporterWhenWriterFails(t *testing.T) {
	exporter := newSyncMockExporter(false)
	spanProcessor, err := NewWALSpanProcessor(exporter, WithWALDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create WAL processor: %v", err)
	}
	processor := spanProcessor.(*WALSpanProcessor)
	if err := processor.closeWriter(); err != nil {
		t.Fatalf("close WAL writer: %v", err)
	}

	processor.OnEnd(mock.NewSpanSnapshotMock(1))
	stats := processor.GetStats()
	if stats["accepted"] != 1 || stats["direct_exported"] != 1 || stats["dropped"] != 0 {
		t.Fatalf("unexpected WAL stats: %+v", stats)
	}
	if processor.GetLastError() == nil {
		t.Fatal("WAL write failure must remain observable")
	}

	if err := processor.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown WAL processor: %v", err)
	}
}
