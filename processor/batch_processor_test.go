package processor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
)

type batchTestExporter struct {
	count atomic.Int64
	delay time.Duration
}

type shutdownDrainExporter struct {
	count         atomic.Int64
	shutdownCount atomic.Int64
	started       chan struct{}
	release       chan struct{}
	once          atomic.Bool
}

type failingFallbackWriter struct{}

func (f *failingFallbackWriter) Fallback([]byte) error {
	return errors.New("fallback unavailable")
}

func (f *failingFallbackWriter) FallbackBatch([][]byte) error {
	return errors.New("fallback unavailable")
}

func (f *failingFallbackWriter) Recover(trace.SpanExporter) error {
	return nil
}

func (f *failingFallbackWriter) Shutdown(context.Context) error {
	return nil
}

func (e *batchTestExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

func (e *batchTestExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if e.delay > 0 {
		time.Sleep(e.delay)
	}
	for _, span := range spans {
		if span != nil {
			e.count.Add(1)
		}
	}
	return nil
}

func (e *batchTestExporter) Shutdown(context.Context) error {
	return nil
}

func (e *shutdownDrainExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

func (e *shutdownDrainExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if e.once.CompareAndSwap(false, true) {
		close(e.started)
		<-e.release
	}
	e.count.Add(int64(len(spans)))
	return nil
}

func (e *shutdownDrainExporter) Shutdown(context.Context) error {
	e.shutdownCount.Add(1)
	return nil
}

func mustNewBatchSpanProcessor(tb testing.TB, exporter trace.SpanExporter, opts ...BatchSpanProcessorOption) *BatchSpanProcessor {
	tb.Helper()
	spanProcessor, err := NewBatchSpanProcessor(exporter, opts...)
	if err != nil {
		tb.Fatalf("创建批处理器失败: %v", err)
	}
	batchProcessor, ok := spanProcessor.(*BatchSpanProcessor)
	if !ok {
		tb.Fatalf("批处理器类型错误: %T", spanProcessor)
	}
	return batchProcessor
}

func TestBatchSpanProcessor_OverloadDoesNotLoseSpans(t *testing.T) {
	exporter := &batchTestExporter{delay: time.Millisecond}
	batchProcessor := mustNewBatchSpanProcessor(t,
		exporter,
		WithQueueSize(8),
		WithQueueHighWaterMark(6),
		WithBatchSize(4),
		WithWorkers(1),
		WithFlushInterval(10*time.Millisecond),
		WithFallbackDir(t.TempDir()),
	)

	const total = 2000
	for index := 0; index < total; index++ {
		batchProcessor.OnEnd(mock.NewSpanSnapshotMock(index))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := batchProcessor.Shutdown(ctx); err != nil {
		t.Fatalf("关闭批处理器失败: %v", err)
	}
	if got := exporter.count.Load(); got != total {
		t.Fatalf("过载后链路数量不完整: 期望 %d，实际 %d", total, got)
	}
	stats := batchProcessor.GetStats()
	if stats["accepted"] != total {
		t.Fatalf("接收统计错误: %+v", stats)
	}
	if stats["failures"] != 0 {
		t.Fatalf("处理器出现异常: %+v, last=%v", stats, batchProcessor.GetLastError())
	}
}

func TestBatchSpanProcessor_HighWaterMarkDoesNotResizeQueue(t *testing.T) {
	batchProcessor := mustNewBatchSpanProcessor(t,
		&batchTestExporter{},
		WithQueueSize(100),
		WithQueueHighWaterMark(80),
	)
	defer batchProcessor.Shutdown(context.Background())

	if batchProcessor.GetMaxQueueSize() != 100 {
		t.Fatalf("高水位配置错误地修改了队列容量: %d", batchProcessor.GetMaxQueueSize())
	}
}

func TestBatchSpanProcessor_ReleasesSpanAfterShutdown(t *testing.T) {
	batchProcessor := mustNewBatchSpanProcessor(t, &batchTestExporter{})
	if err := batchProcessor.Shutdown(context.Background()); err != nil {
		t.Fatalf("关闭批处理器失败: %v", err)
	}

	released := atomic.Int64{}
	span := mock.NewSpanSnapshotMock(1)
	span.ReleaseFunc = func() {
		released.Add(1)
	}
	batchProcessor.OnEnd(span)
	if released.Load() != 1 {
		t.Fatalf("关闭后收到的快照没有释放: %d", released.Load())
	}
	if batchProcessor.GetQueueLength() != 0 {
		t.Fatalf("关闭后仍接受了新快照: %d", batchProcessor.GetQueueLength())
	}
}

func TestBatchSpanProcessor_DropsWithoutBlockingWhenAllOutputsFail(t *testing.T) {
	batchProcessor := mustNewBatchSpanProcessor(t, nil)
	batchProcessor.fallback = &failingFallbackWriter{}

	released := atomic.Int64{}
	span := mock.NewSpanSnapshotMock(1)
	span.ReleaseFunc = func() {
		released.Add(1)
	}

	done := make(chan struct{})
	go func() {
		batchProcessor.handleExportFailure([]trace.SpanSnapshot{span})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("远端与 fallback 同时失败时阻塞了调用方")
	}

	if released.Load() != 1 {
		t.Fatalf("丢弃的快照没有释放: %d", released.Load())
	}
	stats := batchProcessor.GetStats()
	if stats["dropped"] != 1 || stats["failures"] != 1 {
		t.Fatalf("失败与丢弃统计错误: %+v", stats)
	}

	if err := batchProcessor.Shutdown(context.Background()); err != nil {
		t.Fatalf("关闭批处理器失败: %v", err)
	}
}

func TestBatchSpanProcessor_DropsOnlyInvalidFallbackSpan(t *testing.T) {
	primaryExporter := &batchTestExporter{}
	batchProcessor := mustNewBatchSpanProcessor(t,
		primaryExporter,
		WithFallbackDir(t.TempDir()),
	)

	goodReleased := atomic.Int64{}
	goodSpan := mock.NewSpanSnapshotMock(1)
	goodSpan.ReleaseFunc = func() {
		goodReleased.Add(1)
	}

	badReleased := atomic.Int64{}
	badSpan := mock.NewSpanSnapshotMock(2)
	badSpan.Attributes["invalid"] = func() {}
	badSpan.ReleaseFunc = func() {
		badReleased.Add(1)
	}

	batchProcessor.handleExportFailure([]trace.SpanSnapshot{goodSpan, badSpan})

	if goodReleased.Load() != 1 || badReleased.Load() != 1 {
		t.Fatalf("快照释放次数错误: good=%d bad=%d", goodReleased.Load(), badReleased.Load())
	}
	stats := batchProcessor.GetStats()
	if stats["fallback"] != 1 || stats["dropped"] != 1 {
		t.Fatalf("非法 Span 隔离统计错误: %+v", stats)
	}

	exporter := &fallbackTestExporter{}
	if err := batchProcessor.fallback.Recover(exporter); err != nil {
		t.Fatalf("恢复合法 Span 失败: %v", err)
	}
	recoveredCount := primaryExporter.count.Load() + int64(exporter.count)
	if recoveredCount != 1 {
		t.Fatalf("合法 Span 的 fallback 恢复数量错误: %d", recoveredCount)
	}

	if err := batchProcessor.Shutdown(context.Background()); err != nil {
		t.Fatalf("关闭批处理器失败: %v", err)
	}
}

func TestBatchSpanProcessor_DropsOnlyOversizedFallbackSpan(t *testing.T) {
	primaryExporter := &batchTestExporter{}
	batchProcessor := mustNewBatchSpanProcessor(t,
		primaryExporter,
		WithFallbackDir(t.TempDir()),
	)
	batchProcessor.fallback.(*fallbackWriter).maxRecordSize = 1024

	goodSpan := mock.NewSpanSnapshotMock(1)
	oversizedSpan := mock.NewSpanSnapshotMock(2)
	oversizedSpan.Attributes["payload"] = string(make([]byte, 2048))

	batchProcessor.handleExportFailure([]trace.SpanSnapshot{goodSpan, oversizedSpan})

	stats := batchProcessor.GetStats()
	if stats["fallback"] != 1 || stats["dropped"] != 1 {
		t.Fatalf("超限 Span 隔离统计错误: %+v", stats)
	}
	exporter := &fallbackTestExporter{}
	if err := batchProcessor.fallback.Recover(exporter); err != nil {
		t.Fatalf("恢复合法 Span 失败: %v", err)
	}
	recoveredCount := primaryExporter.count.Load() + int64(exporter.count)
	if recoveredCount != 1 {
		t.Fatalf("合法 Span 的 fallback 恢复数量错误: %d", recoveredCount)
	}

	if err := batchProcessor.Shutdown(context.Background()); err != nil {
		t.Fatalf("关闭批处理器失败: %v", err)
	}
}

func TestBatchSpanProcessor_ShutdownDrainsAcceptedQueue(t *testing.T) {
	exporter := &shutdownDrainExporter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	batchProcessor := mustNewBatchSpanProcessor(t,
		exporter,
		WithQueueSize(128),
		WithBatchSize(1),
		WithWorkers(1),
		WithFlushInterval(time.Hour),
	)

	const total = 64
	for index := 0; index < total; index++ {
		batchProcessor.OnEnd(mock.NewSpanSnapshotMock(index))
	}

	select {
	case <-exporter.started:
	case <-time.After(time.Second):
		t.Fatal("first export did not start")
	}

	shutdownDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		shutdownDone <- batchProcessor.Shutdown(ctx)
	}()

	select {
	case <-batchProcessor.shutdownCh:
	case <-time.After(time.Second):
		t.Fatal("shutdown signal was not closed")
	}
	close(exporter.release)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown processor: %v", err)
	}
	if got := exporter.count.Load(); got != total {
		t.Fatalf("shutdown exported %d spans, want %d", got, total)
	}
	if stats := batchProcessor.GetStats(); stats["dropped"] != 0 {
		t.Fatalf("graceful shutdown dropped spans: %+v", stats)
	}
}

func TestBatchSpanProcessor_ShutdownContinuesAfterCallerTimeout(t *testing.T) {
	exporter := &shutdownDrainExporter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	batchProcessor := mustNewBatchSpanProcessor(t,
		exporter,
		WithQueueSize(8),
		WithBatchSize(1),
		WithWorkers(1),
		WithFlushInterval(time.Hour),
	)
	batchProcessor.OnEnd(mock.NewSpanSnapshotMock(1))

	select {
	case <-exporter.started:
	case <-time.After(time.Second):
		t.Fatal("导出任务没有启动")
	}

	firstCtx, firstCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer firstCancel()
	if err := batchProcessor.Shutdown(firstCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("首次关闭应返回超时错误，实际: %v", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		secondCtx, secondCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer secondCancel()
		secondDone <- batchProcessor.Shutdown(secondCtx)
	}()

	select {
	case err := <-secondDone:
		t.Fatalf("导出释放前不应完成关闭，实际: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	close(exporter.release)
	if err := <-secondDone; err != nil {
		t.Fatalf("再次关闭应等待后台清理完成: %v", err)
	}
	if exporter.count.Load() != 1 {
		t.Fatalf("关闭超时后已接收 Span 未完整导出: %d", exporter.count.Load())
	}
	if exporter.shutdownCount.Load() != 1 {
		t.Fatalf("exporter Shutdown 调用次数错误: %d", exporter.shutdownCount.Load())
	}
}
