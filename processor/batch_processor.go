package processor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClownSketch/tracer/fallback"
	"github.com/ClownSketch/tracer/trace"
)

// BatchSpanProcessor 批量处理和导出 Span。
//
// 当前设计里，BatchSpanProcessor 是唯一的异步调度层：
//   - OnEnd 只负责快速入队
//   - 单个聚合循环负责 batch 聚合与 flush 时机
//   - 导出并发由 processor 自己控制
//   - fallback 和 snapshot 释放统一由 processor 负责
type BatchSpanProcessor struct {
	exporter           trace.SpanExporter
	queue              chan trace.SpanSnapshot
	batchSize          int
	workers            int
	ticker             *time.Ticker
	shutdownCh         chan struct{}
	shutdownOnce       sync.Once
	shutdownDone       chan struct{}
	shutdownErr        error
	queueHighWaterMark int
	emergencyFlushCh   chan struct{}
	fallback           trace.FallbackWriter

	admissionMu sync.RWMutex
	closed      atomic.Bool

	exportSem chan struct{}
	exportWg  sync.WaitGroup
	loopWg    sync.WaitGroup
	recoverWg sync.WaitGroup

	acceptedCount atomic.Int64
	exportedCount atomic.Int64
	fallbackCount atomic.Int64
	droppedCount  atomic.Int64
	failureCount  atomic.Int64
	lastErrorMu   sync.RWMutex
	lastError     error
	initErr       error
}

// BatchSpanProcessorOption 配置批处理器的选项
type BatchSpanProcessorOption func(*BatchSpanProcessor)

// WithQueueSize 设置队列大小
func WithQueueSize(size int) BatchSpanProcessorOption {
	return func(processor *BatchSpanProcessor) {
		if size <= 0 {
			return
		}
		processor.queue = make(chan trace.SpanSnapshot, size)
		if processor.queueHighWaterMark == 0 {
			processor.queueHighWaterMark = int(float64(size) * 0.8)
		}
	}
}

// WithBatchSize 设置批次大小
func WithBatchSize(batchSize int) BatchSpanProcessorOption {
	return func(processor *BatchSpanProcessor) {
		if batchSize > 0 {
			processor.batchSize = batchSize
		}
	}
}

// WithWorkers 设置导出并发数
func WithWorkers(workers int) BatchSpanProcessorOption {
	return func(processor *BatchSpanProcessor) {
		if workers > 0 {
			processor.workers = workers
		}
	}
}

// WithFlushInterval 设置刷新间隔
func WithFlushInterval(interval time.Duration) BatchSpanProcessorOption {
	return func(processor *BatchSpanProcessor) {
		if interval > 0 {
			if processor.ticker != nil {
				processor.ticker.Stop()
			}
			processor.ticker = time.NewTicker(interval)
		}
	}
}

// WithQueueHighWaterMark 设置队列高水位线
func WithQueueHighWaterMark(highWaterMark int) BatchSpanProcessorOption {
	return func(processor *BatchSpanProcessor) {
		if highWaterMark <= 0 {
			return
		}
		processor.queueHighWaterMark = highWaterMark
	}
}

// WithFallbackDir 设置回退目录
func WithFallbackDir(dir string) BatchSpanProcessorOption {
	return func(p *BatchSpanProcessor) {
		if dir != "" {
			p.fallback = newFallbackWriter(dir)
			if writer, ok := p.fallback.(*fallbackWriter); ok {
				p.initErr = writer.initErr
			}
		}
	}
}

// NewBatchSpanProcessor 创建批处理器并返回初始化错误。
// fallback 目录不可用时不会启动后台协程。
func NewBatchSpanProcessor(exporter trace.SpanExporter, opts ...BatchSpanProcessorOption) (trace.SpanProcessor, error) {
	processor := &BatchSpanProcessor{
		exporter:           exporter,
		queue:              make(chan trace.SpanSnapshot, 5000),
		batchSize:          100,
		workers:            5,
		ticker:             time.NewTicker(2 * time.Second),
		shutdownCh:         make(chan struct{}),
		shutdownDone:       make(chan struct{}),
		queueHighWaterMark: 800,
		emergencyFlushCh:   make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(processor)
	}
	if processor.initErr != nil {
		processor.ticker.Stop()
		return nil, processor.initErr
	}

	if processor.workers <= 0 {
		processor.workers = 1
	}
	if processor.batchSize <= 0 {
		processor.batchSize = 1
	}
	if processor.queueHighWaterMark <= 0 || processor.queueHighWaterMark > cap(processor.queue) {
		processor.queueHighWaterMark = int(float64(cap(processor.queue)) * 0.8)
		if processor.queueHighWaterMark <= 0 {
			processor.queueHighWaterMark = cap(processor.queue)
		}
	}
	processor.exportSem = make(chan struct{}, processor.workers)

	if processor.fallback != nil && processor.exporter != nil {
		processor.recoverWg.Add(1)
		go func() {
			defer processor.recoverWg.Done()

			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()

			if err := processor.fallback.Recover(processor.exporter); err != nil {
				processor.recordError(err)
			}

			for {
				select {
				case <-ticker.C:
					if err := processor.fallback.Recover(processor.exporter); err != nil {
						processor.recordError(err)
					}
				case <-processor.shutdownCh:
					return
				}
			}
		}()
	}

	processor.loopWg.Add(1)
	go processor.loop()

	return processor, nil
}

// OnStart 在开始时调用处理器
func (b *BatchSpanProcessor) OnStart(ctx context.Context, span trace.Span) {
}

// OnEnd 在结束时调用处理器
func (b *BatchSpanProcessor) OnEnd(span trace.SpanSnapshot) {
	if span == nil {
		return
	}

	b.admissionMu.RLock()
	defer b.admissionMu.RUnlock()

	if b.closed.Load() {
		span.Release()
		return
	}
	b.acceptedCount.Add(1)

	if len(b.queue) >= b.queueHighWaterMark {
		select {
		case b.emergencyFlushCh <- struct{}{}:
		default:
		}
	}

	select {
	case b.queue <- span:
		return
	default:
	}

	select {
	case b.emergencyFlushCh <- struct{}{}:
	default:
	}

	select {
	case b.queue <- span:
		return
	default:
		remaining := b.persistFallback([]trace.SpanSnapshot{span})
		if len(remaining) == 0 {
			return
		}
		if b.fallback != nil {
			b.dropSpans(remaining)
			return
		}

		// 未配置 fallback 的直接调用保留旧有背压语义。
		b.queue <- span
	}
}

func (b *BatchSpanProcessor) loop() {
	defer b.loopWg.Done()

	batch := make([]trace.SpanSnapshot, 0, b.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toExport := make([]trace.SpanSnapshot, len(batch))
		copy(toExport, batch)
		batch = batch[:0]
		b.dispatchBatch(toExport)
	}

	for {
		select {
		case <-b.shutdownCh:
			for {
				select {
				case span := <-b.queue:
					if span != nil {
						batch = append(batch, span)
						if len(batch) >= b.batchSize {
							flush()
						}
					}
				default:
					flush()
					return
				}
			}
		case span := <-b.queue:
			if span == nil {
				continue
			}
			batch = append(batch, span)
			if len(batch) >= b.batchSize {
				flush()
			}
		case <-b.ticker.C:
			flush()
		case <-b.emergencyFlushCh:
			flush()
		}
	}
}

func (b *BatchSpanProcessor) dispatchBatch(spans []trace.SpanSnapshot) {
	if len(spans) == 0 {
		return
	}

	b.exportSem <- struct{}{}
	b.exportWg.Add(1)

	go func(batch []trace.SpanSnapshot) {
		defer func() {
			<-b.exportSem
			b.exportWg.Done()
		}()

		if b.exporter == nil {
			b.recordError(errors.New("batch processor exporter is nil"))
			b.handleExportFailure(batch)
			return
		}

		if err := b.exporter.ExportSpans(batch); err != nil {
			b.recordError(err)
			b.handleExportFailure(batch)
			return
		}

		b.exportedCount.Add(int64(len(batch)))
		releaseSpans(batch)
	}(spans)
}

func (b *BatchSpanProcessor) handleExportFailure(spans []trace.SpanSnapshot) {
	if len(spans) == 0 {
		return
	}

	remaining := b.persistFallback(spans)
	if len(remaining) == 0 {
		return
	}

	// 远端导出和 fallback 同时失败时释放快照并记录丢弃数量。
	// 链路日志不得反向阻塞宿主程序。
	b.dropSpans(remaining)
}

// persistFallback 将可序列化快照写入 fallback，并只淘汰当前非法快照。
// 返回值只包含仍需由调用方释放的快照。
func (b *BatchSpanProcessor) persistFallback(spans []trace.SpanSnapshot) []trace.SpanSnapshot {
	if b.fallback == nil {
		return spans
	}

	dataList := make([][]byte, 0, len(spans))
	validSpans := make([]trace.SpanSnapshot, 0, len(spans))
	for _, span := range spans {
		if span == nil {
			continue
		}
		data, err := fallback.ConvertSpanSnapshotToJSON(span)
		if err != nil {
			b.recordError(err)
			b.dropSpans([]trace.SpanSnapshot{span})
			continue
		}
		if writer, ok := b.fallback.(*fallbackWriter); ok {
			if err := writer.validateRecord(data); err != nil {
				b.recordError(err)
				b.dropSpans([]trace.SpanSnapshot{span})
				continue
			}
		}
		dataList = append(dataList, data)
		validSpans = append(validSpans, span)
	}
	if len(dataList) == 0 {
		return nil
	}
	if err := b.fallback.FallbackBatch(dataList); err != nil {
		b.recordError(err)
		return validSpans
	}

	b.fallbackCount.Add(int64(len(dataList)))
	releaseSpans(validSpans)
	return nil
}

// recordError 保存处理器最近一次可观察错误。
func (b *BatchSpanProcessor) recordError(err error) {
	if err == nil {
		return
	}
	b.failureCount.Add(1)
	b.lastErrorMu.Lock()
	b.lastError = err
	b.lastErrorMu.Unlock()
}

// dropSpans 记录无法继续处理的快照并释放其资源。
func (b *BatchSpanProcessor) dropSpans(spans []trace.SpanSnapshot) {
	var dropped int64
	for _, span := range spans {
		if span != nil {
			dropped++
			span.Release()
		}
	}
	b.droppedCount.Add(dropped)
}

func releaseSpans(spans []trace.SpanSnapshot) {
	for _, span := range spans {
		if span != nil {
			span.Release()
		}
	}
}

// Shutdown 关闭处理器
func (b *BatchSpanProcessor) Shutdown(ctx context.Context) error {
	b.shutdownOnce.Do(func() {
		b.admissionMu.Lock()
		b.closed.Store(true)
		close(b.shutdownCh)
		b.admissionMu.Unlock()

		if b.ticker != nil {
			b.ticker.Stop()
		}

		go b.finishShutdown()
	})

	select {
	case <-b.shutdownDone:
		return b.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishShutdown 在后台排空已接收数据并关闭输出组件。
func (b *BatchSpanProcessor) finishShutdown() {
	defer close(b.shutdownDone)

	b.loopWg.Wait()
	b.exportWg.Wait()
	b.recoverWg.Wait()

	cleanupCtx := context.Background()

	if b.fallback != nil {
		if err := b.fallback.Recover(b.exporter); err != nil {
			b.recordError(err)
			b.shutdownErr = errors.Join(b.shutdownErr, err)
		}
		if err := b.fallback.Shutdown(cleanupCtx); err != nil {
			b.recordError(err)
			b.shutdownErr = errors.Join(b.shutdownErr, err)
		}
	}

	if b.exporter != nil {
		if err := b.exporter.Shutdown(cleanupCtx); err != nil {
			b.recordError(err)
			b.shutdownErr = errors.Join(b.shutdownErr, err)
		}
	}
}

// GetQueueLength 获取当前队列长度
func (b *BatchSpanProcessor) GetQueueLength() int {
	return len(b.queue)
}

// GetMaxQueueSize 获取队列最大容量
func (b *BatchSpanProcessor) GetMaxQueueSize() int {
	return cap(b.queue)
}

// GetStats 返回批处理器接收、导出、fallback 和错误统计。
func (b *BatchSpanProcessor) GetStats() map[string]int64 {
	return map[string]int64{
		"accepted": b.acceptedCount.Load(),
		"exported": b.exportedCount.Load(),
		"fallback": b.fallbackCount.Load(),
		"dropped":  b.droppedCount.Load(),
		"failures": b.failureCount.Load(),
	}
}

// GetLastError 返回批处理器最近一次错误。
func (b *BatchSpanProcessor) GetLastError() error {
	b.lastErrorMu.RLock()
	defer b.lastErrorMu.RUnlock()
	return b.lastError
}
