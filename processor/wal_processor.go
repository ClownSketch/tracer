package processor

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClownSketch/tracer/fallback"
	"github.com/ClownSketch/tracer/trace"
)

const (
	defaultWALDir            = "./storage/wal"
	defaultWALSegmentSize    = 32 * 1024 * 1024
	defaultWALExportBatch    = 100
	defaultWALPollInterval   = 200 * time.Millisecond
	defaultWALFlushInterval  = 2 * time.Millisecond
	defaultWALBufferSize     = 256 * 1024
	walRecordHeaderSize      = 8
	walCheckpointFileName    = "checkpoint.json"
	walSegmentFilePrefix     = "segment-"
	walSegmentFileSuffix     = ".wal"
	walRecordMaxPayloadBytes = 16 * 1024 * 1024
)

// walCheckpoint 记录下一次读取的 WAL 分段和字节偏移。
type walCheckpoint struct {
	SegmentID int64 `json:"segment_id"` // 分段编号
	Offset    int64 `json:"offset"`     // 分段内字节偏移
}

// walSegmentFile 描述磁盘上的一个 WAL 分段文件。
type walSegmentFile struct {
	ID   int64  // 分段编号
	Path string // 文件路径
	Size int64  // 文件大小
}

// WALSpanProcessor 将 span 先写入本地 WAL，再由后台协程同步投递到远端 exporter。
// 这让请求主路径只依赖本地顺序写，远端失败时也能通过 WAL 重放恢复。
type WALSpanProcessor struct {
	exporter        trace.SyncSpanExporter
	baseExporter    trace.SpanExporter
	dir             string
	segmentSize     int64
	exportBatchSize int
	pollInterval    time.Duration
	syncOnWrite     bool
	flushInterval   time.Duration
	bufferSize      int

	writeMu          sync.Mutex
	currentFile      *os.File
	bufferedWriter   *bufio.Writer
	currentSegmentID int64
	currentSize      int64
	writerClosed     bool
	writeDirty       bool

	checkpointMu sync.Mutex
	checkpoint   walCheckpoint

	notifyCh     chan struct{}
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
	dispatchWg   sync.WaitGroup
	flushWg      sync.WaitGroup
	closed       atomic.Bool

	acceptedCount     atomic.Int64
	appendedCount     atomic.Int64
	directExportCount atomic.Int64
	droppedCount      atomic.Int64
	failureCount      atomic.Int64
	lastErrorMu       sync.RWMutex
	lastError         error
}

// WALSpanProcessorOption WAL 处理器配置选项。
type WALSpanProcessorOption func(*WALSpanProcessor)

// WithWALDir 设置 WAL 目录。
func WithWALDir(dir string) WALSpanProcessorOption {
	return func(p *WALSpanProcessor) {
		if dir != "" {
			p.dir = dir
		}
	}
}

// WithWALSegmentSize 设置单个 segment 的最大大小。
func WithWALSegmentSize(size int64) WALSpanProcessorOption {
	return func(p *WALSpanProcessor) {
		if size > 0 {
			p.segmentSize = size
		}
	}
}

// WithWALExportBatchSize 设置后台批量投递大小。
func WithWALExportBatchSize(size int) WALSpanProcessorOption {
	return func(p *WALSpanProcessor) {
		if size > 0 {
			p.exportBatchSize = size
		}
	}
}

// WithWALPollInterval 设置后台扫描 WAL 的轮询间隔。
func WithWALPollInterval(interval time.Duration) WALSpanProcessorOption {
	return func(p *WALSpanProcessor) {
		if interval > 0 {
			p.pollInterval = interval
		}
	}
}

// WithWALSyncOnWrite 设置是否每条写入后立即 fsync。
// 默认关闭以获得更高吞吐；开启后更偏向强一致，但会增加请求延迟。
func WithWALSyncOnWrite(syncOnWrite bool) WALSpanProcessorOption {
	return func(p *WALSpanProcessor) {
		p.syncOnWrite = syncOnWrite
	}
}

// WithWALFlushInterval 设置 WAL 用户态缓冲的刷新间隔。
// 较小的间隔会降低日志停留在用户态缓冲区的时间，较大的间隔则更偏向吞吐。
func WithWALFlushInterval(interval time.Duration) WALSpanProcessorOption {
	return func(p *WALSpanProcessor) {
		if interval > 0 {
			p.flushInterval = interval
		}
	}
}

// WithWALBufferSize 设置 WAL 缓冲写入大小。
func WithWALBufferSize(size int) WALSpanProcessorOption {
	return func(p *WALSpanProcessor) {
		if size > 0 {
			p.bufferSize = size
		}
	}
}

// NewWALSpanProcessor 创建 WAL 处理器。
// 初始化失败时直接返回错误，不启动任何后台协程。
func NewWALSpanProcessor(exporter trace.SyncSpanExporter, opts ...WALSpanProcessorOption) (trace.SpanProcessor, error) {
	processor := &WALSpanProcessor{
		exporter:        exporter,
		dir:             defaultWALDir,
		segmentSize:     defaultWALSegmentSize,
		exportBatchSize: defaultWALExportBatch,
		pollInterval:    defaultWALPollInterval,
		flushInterval:   defaultWALFlushInterval,
		bufferSize:      defaultWALBufferSize,
		notifyCh:        make(chan struct{}, 1),
		shutdownCh:      make(chan struct{}),
		shutdownDone:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(processor)
	}

	baseExporter, ok := exporter.(trace.SpanExporter)
	if exporter != nil && !ok {
		return nil, errors.New("wal processor requires exporter to implement SpanExporter")
	}
	processor.baseExporter = baseExporter

	if err := processor.init(); err != nil {
		return nil, err
	}
	if !processor.syncOnWrite && processor.flushInterval > 0 {
		processor.flushWg.Add(1)
		go processor.flushLoop()
	}
	processor.dispatchWg.Add(1)
	go processor.dispatchLoop()

	return processor, nil
}

// init 创建 WAL 目录、恢复 checkpoint 并打开新的写入分段。
// @return err error 初始化错误
func (p *WALSpanProcessor) init() error {
	if p.exporter == nil {
		return errors.New("wal processor requires a sync span exporter")
	}
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return err
	}

	segments, err := p.listSegments()
	if err != nil {
		return err
	}

	checkpoint, err := p.loadCheckpoint(segments)
	if err != nil {
		return err
	}
	p.checkpoint = checkpoint

	nextID := int64(1)
	if len(segments) > 0 {
		nextID = segments[len(segments)-1].ID + 1
	}

	if err := p.openSegmentLocked(nextID); err != nil {
		return err
	}
	return nil
}

// OnStart 在 span 开始时不做额外处理。
func (p *WALSpanProcessor) OnStart(ctx context.Context, span trace.Span) {
}

// OnEnd 将 span 先落到本地 WAL，随后异步投递。
func (p *WALSpanProcessor) OnEnd(span trace.SpanSnapshot) {
	if span == nil {
		return
	}
	p.acceptedCount.Add(1)

	if p.closed.Load() {
		p.droppedCount.Add(1)
		span.Release()
		return
	}

	payload, err := fallback.ConvertSpanSnapshotToWALJSON(span)
	if err != nil {
		p.recordError(err)
		p.exportDirect(span)
		return
	}

	if err := p.appendPayload(payload); err != nil {
		p.recordError(err)
		p.exportDirect(span)
		return
	}

	p.appendedCount.Add(1)
	span.Release()
	p.notifyDispatcher()
}

// exportDirect 在 WAL 不可写时尝试直接写入远端。
func (p *WALSpanProcessor) exportDirect(span trace.SpanSnapshot) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.exporter.ExportSpanSync(ctx, span); err != nil {
		p.recordError(err)
		p.droppedCount.Add(1)
		return
	}
	p.directExportCount.Add(1)
}

// recordError 保存 WAL 处理器最近一次错误。
func (p *WALSpanProcessor) recordError(err error) {
	if err == nil {
		return
	}
	p.failureCount.Add(1)
	p.lastErrorMu.Lock()
	p.lastError = err
	p.lastErrorMu.Unlock()
}

// Shutdown 停止接收新 span，并尽力把已经写入 WAL 的数据全部投递完成。
func (p *WALSpanProcessor) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		p.closed.Store(true)
		close(p.shutdownCh)
		go p.finishShutdown()
	})

	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-p.shutdownDone:
		return p.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishShutdown 在后台完成 WAL 刷盘、投递和导出器关闭。
func (p *WALSpanProcessor) finishShutdown() {
	defer close(p.shutdownDone)

	p.flushWg.Wait()
	if err := p.closeWriter(); err != nil {
		p.recordError(err)
		p.shutdownErr = errors.Join(p.shutdownErr, err)
	}
	p.dispatchWg.Wait()

	if p.baseExporter != nil {
		if err := p.baseExporter.Shutdown(context.Background()); err != nil {
			p.recordError(err)
			p.shutdownErr = errors.Join(p.shutdownErr, err)
		}
	}
}

// appendPayload 把序列化记录追加到当前 WAL 分段。
// @param payload []byte 序列化 Span
// @return err error 写入或刷盘错误
func (p *WALSpanProcessor) appendPayload(payload []byte) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	if p.writerClosed || p.currentFile == nil || p.bufferedWriter == nil {
		return errors.New("wal writer is closed")
	}

	n, err := writeRecordToWriter(p.bufferedWriter, payload)
	if err != nil {
		return err
	}
	p.currentSize += int64(n)
	p.writeDirty = true

	if p.syncOnWrite {
		if err := p.flushWriterLocked(true); err != nil {
			return err
		}
	}

	if p.currentSize >= p.segmentSize {
		nextID := p.currentSegmentID + 1
		if err := p.openSegmentLocked(nextID); err != nil {
			return err
		}
	}

	return nil
}

// closeWriter 刷新并关闭当前 WAL 写入器。
// @return err error 刷盘或关闭错误
func (p *WALSpanProcessor) closeWriter() error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	if p.writerClosed {
		return nil
	}
	p.writerClosed = true

	if p.currentFile == nil {
		return nil
	}
	if err := p.flushWriterLocked(true); err != nil {
		_ = p.currentFile.Close()
		p.currentFile = nil
		p.bufferedWriter = nil
		return err
	}
	if err := p.currentFile.Close(); err != nil {
		p.currentFile = nil
		p.bufferedWriter = nil
		return err
	}
	p.currentFile = nil
	p.bufferedWriter = nil
	return nil
}

// openSegmentLocked 切换到指定 WAL 分段；调用方必须持有 writeMu。
// @param segmentID int64 新分段编号
// @return err error 文件打开错误
func (p *WALSpanProcessor) openSegmentLocked(segmentID int64) error {
	if p.currentFile != nil {
		if err := p.flushWriterLocked(true); err != nil {
			_ = p.currentFile.Close()
			p.currentFile = nil
			p.bufferedWriter = nil
			return err
		}
		if err := p.currentFile.Close(); err != nil {
			p.currentFile = nil
			p.bufferedWriter = nil
			return err
		}
	}

	path := p.segmentPath(segmentID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}

	p.currentFile = file
	p.bufferedWriter = bufio.NewWriterSize(file, p.bufferSize)
	p.currentSegmentID = segmentID
	p.currentSize = info.Size()
	p.writeDirty = false
	return nil
}

// flushLoop 周期刷新缓冲写入器，并唤醒后台投递协程。
func (p *WALSpanProcessor) flushLoop() {
	defer p.flushWg.Done()

	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.flushWriter(false); err != nil {
				p.recordError(err)
				continue
			}
			p.notifyDispatcher()
		case <-p.shutdownCh:
			if err := p.flushWriter(true); err != nil {
				p.recordError(err)
			}
			return
		}
	}
}

// flushWriter 在加锁后刷新 WAL 写入缓冲区。
// @param sync bool 是否同步到磁盘
// @return err error 刷盘错误
func (p *WALSpanProcessor) flushWriter(sync bool) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.flushWriterLocked(sync)
}

// flushWriterLocked 刷新 WAL 写入缓冲区；调用方必须持有 writeMu。
// @param sync bool 是否同步到磁盘
// @return err error 刷盘错误
func (p *WALSpanProcessor) flushWriterLocked(sync bool) error {
	if p.currentFile == nil || p.bufferedWriter == nil || !p.writeDirty {
		if sync && p.currentFile != nil {
			return p.currentFile.Sync()
		}
		return nil
	}

	if err := p.bufferedWriter.Flush(); err != nil {
		return err
	}
	p.writeDirty = false
	if sync {
		return p.currentFile.Sync()
	}
	return nil
}

// dispatchLoop 按通知或轮询周期读取 WAL 并投递远端。
func (p *WALSpanProcessor) dispatchLoop() {
	defer p.dispatchWg.Done()

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	draining := false
	for {
		progressed, err := p.dispatchOnce()
		if err != nil {
			p.recordError(err)
			if draining {
				return
			}
			select {
			case <-p.shutdownCh:
				draining = true
			case <-ticker.C:
			}
			if draining && p.isFullyDrained() {
				return
			}
			continue
		}

		if draining && p.isFullyDrained() {
			return
		}
		if progressed {
			continue
		}

		select {
		case <-p.shutdownCh:
			draining = true
		case <-p.notifyCh:
		case <-ticker.C:
		}
	}
}

// GetStats 返回 WAL 接收、落盘、直连兜底、丢弃和错误统计。
func (p *WALSpanProcessor) GetStats() map[string]int64 {
	return map[string]int64{
		"accepted":        p.acceptedCount.Load(),
		"appended":        p.appendedCount.Load(),
		"direct_exported": p.directExportCount.Load(),
		"dropped":         p.droppedCount.Load(),
		"failures":        p.failureCount.Load(),
	}
}

// GetLastError 返回 WAL 处理器最近一次错误。
func (p *WALSpanProcessor) GetLastError() error {
	p.lastErrorMu.RLock()
	defer p.lastErrorMu.RUnlock()
	return p.lastError
}

// dispatchOnce 投递一个 WAL 批次并推进 checkpoint。
// @return progressed bool 本轮是否推进了 WAL
// @return err error 读取、导出或 checkpoint 错误
func (p *WALSpanProcessor) dispatchOnce() (bool, error) {
	if advanced, err := p.advanceCheckpointIfSegmentConsumed(); err != nil || advanced {
		return advanced, err
	}

	checkpoint := p.getCheckpoint()
	payloads, nextOffset, err := p.readBatch(checkpoint.SegmentID, checkpoint.Offset)
	if err != nil {
		return false, err
	}
	if len(payloads) == 0 {
		return false, nil
	}

	spans := make([]trace.SpanSnapshot, 0, len(payloads))
	for _, payload := range payloads {
		span, err := fallback.ConvertJSONToSpanSnapshot(payload)
		if err != nil {
			releaseWALSpanSnapshots(spans)
			return false, err
		}
		spans = append(spans, span)
	}

	if err := p.exporter.ExportSpansSync(context.Background(), spans); err != nil {
		return false, err
	}

	if err := p.setCheckpoint(walCheckpoint{SegmentID: checkpoint.SegmentID, Offset: nextOffset}); err != nil {
		return false, err
	}

	return true, nil
}

// advanceCheckpointIfSegmentConsumed 删除已消费分段并切换到下一分段。
// @return advanced bool checkpoint 是否发生变化
// @return err error 文件或 checkpoint 错误
func (p *WALSpanProcessor) advanceCheckpointIfSegmentConsumed() (bool, error) {
	checkpoint := p.getCheckpoint()
	segments, err := p.listSegments()
	if err != nil {
		return false, err
	}
	if len(segments) == 0 {
		return false, nil
	}

	current, index := findSegmentForID(segments, checkpoint.SegmentID)
	if current == nil {
		if segments[0].ID > checkpoint.SegmentID {
			return true, p.setCheckpoint(walCheckpoint{SegmentID: segments[0].ID, Offset: 0})
		}
		return false, nil
	}

	activeSegmentID := p.getActiveSegmentID()
	if current.ID < activeSegmentID && checkpoint.Offset >= current.Size {
		if err := os.Remove(current.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}

		nextCheckpoint := walCheckpoint{SegmentID: current.ID + 1, Offset: 0}
		if index+1 < len(segments) {
			nextCheckpoint.SegmentID = segments[index+1].ID
		}
		return true, p.setCheckpoint(nextCheckpoint)
	}

	return false, nil
}

// readBatch 从指定分段和偏移读取一个完整记录批次。
// @param segmentID int64 分段编号
// @param offset int64 起始字节偏移
// @return payloads [][]byte 读取到的序列化记录
// @return nextOffset int64 下一次读取偏移
// @return err error 文件或校验错误
func (p *WALSpanProcessor) readBatch(segmentID, offset int64) ([][]byte, int64, error) {
	segmentPath := p.segmentPath(segmentID)
	file, err := os.Open(segmentPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, offset, nil
		}
		return nil, offset, err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}

	payloads := make([][]byte, 0, p.exportBatchSize)
	nextOffset := offset
	activeSegmentID := p.getActiveSegmentID()
	isActive := segmentID == activeSegmentID && !p.isWriterClosed()

	for len(payloads) < p.exportBatchSize {
		header := make([]byte, walRecordHeaderSize)
		n, err := io.ReadFull(file, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if isActive {
					return payloads, nextOffset, nil
				}
				if err := truncateFile(segmentPath, nextOffset); err != nil {
					return nil, offset, err
				}
				return payloads, nextOffset, nil
			}
			return nil, offset, err
		}
		nextOffset += int64(n)

		payloadLength := int64(binary.BigEndian.Uint32(header[:4]))
		expectedCRC := binary.BigEndian.Uint32(header[4:])
		if payloadLength <= 0 || payloadLength > walRecordMaxPayloadBytes {
			return nil, offset, errors.New("invalid wal record length")
		}

		payload := make([]byte, payloadLength)
		n, err = io.ReadFull(file, payload)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if isActive {
					return payloads, nextOffset - int64(walRecordHeaderSize), nil
				}
				if err := truncateFile(segmentPath, nextOffset-int64(walRecordHeaderSize)); err != nil {
					return nil, offset, err
				}
				return payloads, nextOffset - int64(walRecordHeaderSize), nil
			}
			return nil, offset, err
		}
		nextOffset += int64(n)

		if crc32.ChecksumIEEE(payload) != expectedCRC {
			return nil, offset, errors.New("wal record crc mismatch")
		}

		payloads = append(payloads, payload)
	}

	return payloads, nextOffset, nil
}

// isFullyDrained 判断全部 WAL 分段是否已经消费完成。
// @return result bool 是否消费完成
func (p *WALSpanProcessor) isFullyDrained() bool {
	checkpoint := p.getCheckpoint()
	segments, err := p.listSegments()
	if err != nil || len(segments) == 0 {
		return err == nil
	}

	last := segments[len(segments)-1]
	if checkpoint.SegmentID < last.ID {
		return false
	}
	return checkpoint.SegmentID == last.ID && checkpoint.Offset >= last.Size
}

// notifyDispatcher 非阻塞唤醒后台投递协程。
func (p *WALSpanProcessor) notifyDispatcher() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}

// getCheckpoint 返回当前 checkpoint 快照。
// @return result walCheckpoint 当前 checkpoint
func (p *WALSpanProcessor) getCheckpoint() walCheckpoint {
	p.checkpointMu.Lock()
	defer p.checkpointMu.Unlock()
	return p.checkpoint
}

// setCheckpoint 持久化并更新内存中的 checkpoint。
// @param checkpoint walCheckpoint 新 checkpoint
// @return err error 持久化错误
func (p *WALSpanProcessor) setCheckpoint(checkpoint walCheckpoint) error {
	p.checkpointMu.Lock()
	defer p.checkpointMu.Unlock()
	if err := p.persistCheckpoint(checkpoint); err != nil {
		return err
	}
	p.checkpoint = checkpoint
	return nil
}

// loadCheckpoint 从磁盘恢复 checkpoint，并在缺失时选择首个分段。
// @param segments []walSegmentFile 已发现的 WAL 分段
// @return checkpoint walCheckpoint 恢复后的 checkpoint
// @return err error 读取或解析错误
func (p *WALSpanProcessor) loadCheckpoint(segments []walSegmentFile) (walCheckpoint, error) {
	path := filepath.Join(p.dir, walCheckpointFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if len(segments) > 0 {
				return walCheckpoint{SegmentID: segments[0].ID, Offset: 0}, nil
			}
			return walCheckpoint{SegmentID: 1, Offset: 0}, nil
		}
		return walCheckpoint{}, err
	}

	var checkpoint walCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return walCheckpoint{}, err
	}
	if checkpoint.SegmentID <= 0 {
		checkpoint.SegmentID = 1
	}
	if checkpoint.Offset < 0 {
		checkpoint.Offset = 0
	}
	return checkpoint, nil
}

// persistCheckpoint 通过临时文件原子替换 checkpoint。
// @param checkpoint walCheckpoint 待保存的 checkpoint
// @return err error 写入错误
func (p *WALSpanProcessor) persistCheckpoint(checkpoint walCheckpoint) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	path := filepath.Join(p.dir, walCheckpointFileName)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// listSegments 扫描并按编号排序 WAL 分段。
// @return segments []walSegmentFile WAL 分段集合
// @return err error 目录读取错误
func (p *WALSpanProcessor) listSegments() ([]walSegmentFile, error) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, err
	}

	segments := make([]walSegmentFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		id, ok := parseSegmentID(entry.Name())
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		segments = append(segments, walSegmentFile{
			ID:   id,
			Path: filepath.Join(p.dir, entry.Name()),
			Size: info.Size(),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID < segments[j].ID
	})
	return segments, nil
}

// segmentPath 返回指定 WAL 分段的完整路径。
// @param segmentID int64 分段编号
// @return result string 文件路径
func (p *WALSpanProcessor) segmentPath(segmentID int64) string {
	return filepath.Join(p.dir, walSegmentFileName(segmentID))
}

// getActiveSegmentID 返回当前写入分段编号。
// @return result int64 分段编号
func (p *WALSpanProcessor) getActiveSegmentID() int64 {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.currentSegmentID
}

// isWriterClosed 返回 WAL 写入器是否已经关闭。
// @return result bool 是否关闭
func (p *WALSpanProcessor) isWriterClosed() bool {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.writerClosed
}

// walSegmentFileName 生成 WAL 分段文件名。
// @param segmentID int64 分段编号
// @return result string 文件名
func walSegmentFileName(segmentID int64) string {
	return walSegmentFilePrefix + strconv.FormatInt(segmentID, 10) + walSegmentFileSuffix
}

// parseSegmentID 从合法 WAL 文件名解析分段编号。
// @param name string 文件名
// @return segmentID int64 分段编号
// @return ok bool 文件名是否有效
func parseSegmentID(name string) (int64, bool) {
	if !strings.HasPrefix(name, walSegmentFilePrefix) || !strings.HasSuffix(name, walSegmentFileSuffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, walSegmentFilePrefix), walSegmentFileSuffix)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// findSegmentForID 查找指定编号的 WAL 分段。
// @param segments []walSegmentFile WAL 分段集合
// @param segmentID int64 目标编号
// @return segment *walSegmentFile 匹配分段
// @return index int 分段下标；未找到时为 -1
func findSegmentForID(segments []walSegmentFile, segmentID int64) (*walSegmentFile, int) {
	for i := range segments {
		if segments[i].ID == segmentID {
			return &segments[i], i
		}
	}
	return nil, -1
}

// writeRecordToWriter 写入长度、校验码和记录载荷。
// @param writer io.Writer 目标写入器
// @param payload []byte 序列化 Span
// @return written int 写入字节数
// @return err error 写入错误
func writeRecordToWriter(writer io.Writer, payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, errors.New("empty wal payload")
	}
	if len(payload) > walRecordMaxPayloadBytes {
		return 0, errors.New("wal payload is too large")
	}

	var header [walRecordHeaderSize]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	headerWritten, err := writer.Write(header[:])
	if err != nil {
		return headerWritten, err
	}
	payloadWritten, err := writer.Write(payload)
	if err != nil {
		return headerWritten + payloadWritten, err
	}
	return headerWritten + payloadWritten, nil
}

// truncateFile 截断不完整 WAL 尾部。
// @param path string 文件路径
// @param size int64 保留长度
// @return err error 截断错误
func truncateFile(path string, size int64) error {
	return os.Truncate(path, size)
}

// releaseWALSpanSnapshots 释放 WAL 投递阶段已经消费的快照。
// @param spans []trace.SpanSnapshot Span 快照集合
func releaseWALSpanSnapshots(spans []trace.SpanSnapshot) {
	for _, span := range spans {
		if span != nil {
			span.Release()
		}
	}
}
