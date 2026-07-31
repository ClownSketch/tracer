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

type walCheckpoint struct {
	SegmentID int64 `json:"segment_id"`
	Offset    int64 `json:"offset"`
}

type walSegmentFile struct {
	ID   int64
	Path string
	Size int64
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

func (p *WALSpanProcessor) flushWriter(sync bool) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.flushWriterLocked(sync)
}

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

func (p *WALSpanProcessor) notifyDispatcher() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}

func (p *WALSpanProcessor) getCheckpoint() walCheckpoint {
	p.checkpointMu.Lock()
	defer p.checkpointMu.Unlock()
	return p.checkpoint
}

func (p *WALSpanProcessor) setCheckpoint(checkpoint walCheckpoint) error {
	p.checkpointMu.Lock()
	defer p.checkpointMu.Unlock()
	if err := p.persistCheckpoint(checkpoint); err != nil {
		return err
	}
	p.checkpoint = checkpoint
	return nil
}

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

func (p *WALSpanProcessor) segmentPath(segmentID int64) string {
	return filepath.Join(p.dir, walSegmentFileName(segmentID))
}

func (p *WALSpanProcessor) getActiveSegmentID() int64 {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.currentSegmentID
}

func (p *WALSpanProcessor) isWriterClosed() bool {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.writerClosed
}

func walSegmentFileName(segmentID int64) string {
	return walSegmentFilePrefix + strconv.FormatInt(segmentID, 10) + walSegmentFileSuffix
}

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

func findSegmentForID(segments []walSegmentFile, segmentID int64) (*walSegmentFile, int) {
	for i := range segments {
		if segments[i].ID == segmentID {
			return &segments[i], i
		}
	}
	return nil, -1
}

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

func truncateFile(path string, size int64) error {
	return os.Truncate(path, size)
}

func releaseWALSpanSnapshots(spans []trace.SpanSnapshot) {
	for _, span := range spans {
		if span != nil {
			span.Release()
		}
	}
}
