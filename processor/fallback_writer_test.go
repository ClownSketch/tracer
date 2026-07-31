package processor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ClownSketch/tracer/fallback"
	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
)

type fallbackTestExporter struct {
	mu          sync.Mutex
	count       int
	collections []string
	err         error
}

func (e *fallbackTestExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

func (e *fallbackTestExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if e.err != nil {
		return e.err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, span := range spans {
		if span != nil {
			e.count++
			e.collections = append(e.collections, span.GetMongoCollection())
		}
	}
	return nil
}

func (e *fallbackTestExporter) Shutdown(context.Context) error {
	return nil
}

func TestFallbackWriter_RecoversCompleteFileAndRouting(t *testing.T) {
	writer := newFallbackWriter(t.TempDir())
	dataList := make([][]byte, 0, 250)
	for index := 0; index < 250; index++ {
		span := mock.NewSpanSnapshotMock(index)
		span.MongoCollection = "gateway_manual"
		data, err := fallback.ConvertSpanSnapshotToJSON(span)
		if err != nil {
			t.Fatalf("序列化 Span 失败: %v", err)
		}
		dataList = append(dataList, data)
	}
	if err := writer.FallbackBatch(dataList); err != nil {
		t.Fatalf("写入 fallback 失败: %v", err)
	}

	activeFiles, _ := filepath.Glob(filepath.Join(writer.dir, "*"+fallbackActiveSuffix))
	if len(activeFiles) != 1 {
		t.Fatalf("活动文件数量错误: %d", len(activeFiles))
	}

	exporter := &fallbackTestExporter{}
	if err := writer.Recover(exporter); err != nil {
		t.Fatalf("恢复 fallback 失败: %v", err)
	}
	if exporter.count != 250 {
		t.Fatalf("恢复数量错误: %d", exporter.count)
	}
	for _, collection := range exporter.collections {
		if collection != "gateway_manual" {
			t.Fatalf("MongoDB 路由信息丢失: %q", collection)
		}
	}
	readyFiles, _ := writer.readyFiles()
	if len(readyFiles) != 0 {
		t.Fatalf("成功恢复后仍残留文件: %v", readyFiles)
	}
}

func TestFallbackWriter_KeepsFileWhenExporterFails(t *testing.T) {
	writer := newFallbackWriter(t.TempDir())
	span := mock.NewSpanSnapshotMock(1)
	data, err := fallback.ConvertSpanSnapshotToJSON(span)
	if err != nil {
		t.Fatalf("序列化 Span 失败: %v", err)
	}
	if err := writer.Fallback(data); err != nil {
		t.Fatalf("写入 fallback 失败: %v", err)
	}

	exporter := &fallbackTestExporter{err: errors.New("mongo unavailable")}
	if err := writer.Recover(exporter); err == nil {
		t.Fatal("导出失败时 Recover 应返回错误")
	}
	readyFiles, _ := writer.readyFiles()
	if len(readyFiles) != 1 {
		t.Fatalf("导出失败后 fallback 文件不应删除: %v", readyFiles)
	}
}

func TestFallbackWriter_DoesNotFinalizeAnotherWriterActiveFile(t *testing.T) {
	directory := t.TempDir()
	firstWriter := newFallbackWriter(directory)
	secondWriter := newFallbackWriter(directory)

	span := mock.NewSpanSnapshotMock(1)
	data, err := fallback.ConvertSpanSnapshotToJSON(span)
	if err != nil {
		t.Fatalf("序列化 Span 失败: %v", err)
	}
	if err := firstWriter.Fallback(data); err != nil {
		t.Fatalf("写入第一个 fallback 失败: %v", err)
	}

	secondExporter := &fallbackTestExporter{}
	if err := secondWriter.Recover(secondExporter); err != nil {
		t.Fatalf("第二个 writer 恢复失败: %v", err)
	}
	if secondExporter.count != 0 {
		t.Fatalf("第二个 writer 不应恢复仍由第一个 writer 持有的活动文件: %d", secondExporter.count)
	}

	firstExporter := &fallbackTestExporter{}
	if err := firstWriter.Recover(firstExporter); err != nil {
		t.Fatalf("第一个 writer 恢复失败: %v", err)
	}
	if firstExporter.count != 1 {
		t.Fatalf("第一个 writer 应恢复自己的活动文件: %d", firstExporter.count)
	}
}

func TestFallbackWriter_RecoversStaleActiveFile(t *testing.T) {
	directory := t.TempDir()
	span := mock.NewSpanSnapshotMock(1)
	data, err := fallback.ConvertSpanSnapshotToJSON(span)
	if err != nil {
		t.Fatalf("序列化 Span 失败: %v", err)
	}

	filePath := filepath.Join(directory, fallbackFilePrefix+"stale_pid999999999"+fallbackActiveSuffix)
	if err := os.WriteFile(filePath, data, 0o640); err != nil {
		t.Fatalf("创建遗留活动文件失败: %v", err)
	}

	exporter := &fallbackTestExporter{}
	if err := newFallbackWriter(directory).Recover(exporter); err != nil {
		t.Fatalf("恢复遗留活动文件失败: %v", err)
	}
	if exporter.count != 1 {
		t.Fatalf("遗留活动文件恢复数量错误: %d", exporter.count)
	}
	if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("遗留活动文件没有被清理: %v", err)
	}
}

func TestFallbackWriter_RecoversLockedFormatFileAfterPIDReuse(t *testing.T) {
	directory := t.TempDir()
	span := mock.NewSpanSnapshotMock(1)
	data, err := fallback.ConvertSpanSnapshotToJSON(span)
	if err != nil {
		t.Fatalf("序列化 Span 失败: %v", err)
	}

	fileName := fmt.Sprintf(
		"%spid-reuse%s_pid%d%s",
		fallbackFilePrefix,
		fallbackLockMarker,
		os.Getpid(),
		fallbackActiveSuffix,
	)
	filePath := filepath.Join(directory, fileName)
	if err := os.WriteFile(filePath, append(data, '\n'), 0o640); err != nil {
		t.Fatalf("创建 PID 复用活动文件失败: %v", err)
	}

	exporter := &fallbackTestExporter{}
	if err := newFallbackWriter(directory).Recover(exporter); err != nil {
		t.Fatalf("恢复 PID 复用活动文件失败: %v", err)
	}
	if exporter.count != 1 {
		t.Fatalf("PID 复用活动文件恢复数量错误: %d", exporter.count)
	}
}

func TestFallbackWriter_RecoversCompleteRecordsBeforePartialTail(t *testing.T) {
	directory := t.TempDir()
	span := mock.NewSpanSnapshotMock(1)
	data, err := fallback.ConvertSpanSnapshotToJSON(span)
	if err != nil {
		t.Fatalf("序列化 Span 失败: %v", err)
	}

	filePath := filepath.Join(directory, fallbackFilePrefix+"partial_pid999999999"+fallbackActiveSuffix)
	fileData := append(append(append([]byte{}, data...), '\n'), []byte(`{"traceID":"partial`)...)
	if err := os.WriteFile(filePath, fileData, 0o640); err != nil {
		t.Fatalf("创建尾部中断文件失败: %v", err)
	}

	exporter := &fallbackTestExporter{}
	if err := newFallbackWriter(directory).Recover(exporter); err != nil {
		t.Fatalf("恢复尾部中断文件失败: %v", err)
	}
	if exporter.count != 1 {
		t.Fatalf("完整记录恢复数量错误: %d", exporter.count)
	}
}

func TestFallbackWriter_QuarantinesCorruptFile(t *testing.T) {
	directory := t.TempDir()
	filePath := filepath.Join(directory, fallbackFilePrefix+"broken"+fallbackReadySuffix)
	if err := os.WriteFile(filePath, []byte("{invalid-json}\n"), 0o640); err != nil {
		t.Fatalf("创建损坏文件失败: %v", err)
	}

	writer := newFallbackWriter(directory)
	if err := writer.Recover(&fallbackTestExporter{}); err == nil {
		t.Fatal("损坏文件恢复应返回错误")
	}
	if _, err := os.Stat(filePath + ".corrupt"); err != nil {
		t.Fatalf("损坏文件没有被保留隔离: %v", err)
	}
}

func TestFallbackWriter_RejectsRecordLargerThanRecoveryLimit(t *testing.T) {
	writer := newFallbackWriter(t.TempDir())
	record := make([]byte, writer.maxRecordSize+1)

	if err := writer.Fallback(record); err == nil {
		t.Fatal("超过恢复上限的单条记录应拒绝写入")
	}
}

func TestFallbackWriter_RecordAtLimitCanRecover(t *testing.T) {
	writer := newFallbackWriter(t.TempDir())
	span := mock.NewSpanSnapshotMock(1)
	span.Attributes["payload"] = string(make([]byte, 128*1024))
	data, err := fallback.ConvertSpanSnapshotToJSON(span)
	if err != nil {
		t.Fatalf("序列化边界 Span 失败: %v", err)
	}
	writer.maxRecordSize = len(data)
	writer.maxSize = int64(len(data) + 1)

	if err := writer.Fallback(data); err != nil {
		t.Fatalf("写入边界记录失败: %v", err)
	}
	exporter := &fallbackTestExporter{}
	if err := writer.Recover(exporter); err != nil {
		t.Fatalf("恢复边界记录失败: %v", err)
	}
	if exporter.count != 1 {
		t.Fatalf("边界记录恢复数量错误: %d", exporter.count)
	}
}
