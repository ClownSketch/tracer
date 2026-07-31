package processor

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ClownSketch/tracer/fallback"
	"github.com/ClownSketch/tracer/trace"
)

const (
	fallbackFilePrefix           = "trace_"
	fallbackActiveSuffix         = ".ndjson.active"
	fallbackReadySuffix          = ".ndjson"
	fallbackLegacySuffix         = ".ndjson.gz"
	fallbackLockMarker           = "_flock1_"
	defaultFallbackMaxFileSize   = 10 * 1024 * 1024
	defaultFallbackRecordMaxSize = 10 * 1024 * 1024
)

// fallbackWriter 将导出失败的 Span 可靠保存到本地文件。
//
// fallback 只在队列过载或远端导出失败时进入，因此这里优先保证完整性：
// 数据先写入 active 文件，文件完成或恢复前统一同步到磁盘；恢复线程只读取
// 已经原子完成的 ready 文件，不会读取仍在写入的 active 文件。
type fallbackWriter struct {
	dir           string
	maxSize       int64
	maxRecordSize int
	initErr       error

	mu          sync.Mutex
	current     *os.File
	currentPath string
	currentSize int64
	closed      bool

	recoverMu sync.Mutex
}

// newFallbackWriter 创建 fallback 写入器。
func newFallbackWriter(dir string) *fallbackWriter {
	writer := &fallbackWriter{
		dir:           dir,
		maxSize:       defaultFallbackMaxFileSize,
		maxRecordSize: defaultFallbackRecordMaxSize,
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		writer.initErr = fmt.Errorf("创建 fallback 目录失败: %w", err)
	}
	return writer
}

// Fallback 可靠写入单条 Span 数据。
func (w *fallbackWriter) Fallback(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return w.FallbackBatch([][]byte{data})
}

// FallbackBatch 可靠写入一批 Span 数据。
func (w *fallbackWriter) FallbackBatch(dataList [][]byte) error {
	if len(dataList) == 0 {
		return nil
	}
	for _, data := range dataList {
		if err := w.validateRecord(data); err != nil {
			return err
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initErr != nil {
		return w.initErr
	}
	if w.closed {
		return errors.New("fallback writer 已关闭")
	}
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return fmt.Errorf("创建 fallback 目录失败: %w", err)
	}
	if err := w.openActiveFileLocked(); err != nil {
		return err
	}

	for _, data := range dataList {
		if len(data) == 0 {
			continue
		}
		if w.currentSize > 0 && w.currentSize+int64(len(data))+1 > w.maxSize {
			if err := w.finalizeCurrentLocked(); err != nil {
				return err
			}
			if err := w.openActiveFileLocked(); err != nil {
				return err
			}
		}
		if _, err := w.current.Write(data); err != nil {
			return fmt.Errorf("写入 fallback 数据失败: %w", err)
		}
		if _, err := w.current.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("写入 fallback 换行符失败: %w", err)
		}
		w.currentSize += int64(len(data)) + 1
	}

	return nil
}

// validateRecord 校验单条记录是否可以被写入和完整恢复。
func (w *fallbackWriter) validateRecord(data []byte) error {
	if len(data) > w.maxRecordSize {
		return fmt.Errorf("fallback 单条记录超过上限: %d > %d", len(data), w.maxRecordSize)
	}
	return nil
}

// Recover 恢复已经完整落盘的 fallback 文件。
func (w *fallbackWriter) Recover(exporter trace.SpanExporter) error {
	if exporter == nil {
		return errors.New("fallback 恢复缺少 exporter")
	}

	w.recoverMu.Lock()
	defer w.recoverMu.Unlock()

	if w.initErr != nil {
		return w.initErr
	}
	w.mu.Lock()
	if err := w.finalizeCurrentLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	// 扫描遗留文件期间阻止当前 writer 创建尚未加锁的新 active 文件。
	promoteErr := w.promoteStaleActiveFiles()
	w.mu.Unlock()

	files, err := w.readyFiles()
	if err != nil {
		return errors.Join(promoteErr, err)
	}

	recoverErr := promoteErr
	for _, filePath := range files {
		if err := w.recoverFile(filePath, exporter); err != nil {
			recoverErr = errors.Join(recoverErr, err)
		}
	}
	return recoverErr
}

// Shutdown 完成当前文件并关闭 fallback writer。
func (w *fallbackWriter) Shutdown(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initErr != nil {
		return w.initErr
	}
	if w.closed {
		return nil
	}
	w.closed = true
	return w.finalizeCurrentLocked()
}

// openActiveFileLocked 创建一个只允许当前进程写入的活动文件。
func (w *fallbackWriter) openActiveFileLocked() error {
	if w.current != nil {
		return nil
	}

	fileName := fmt.Sprintf(
		"%s%s%s_pid%d%s",
		fallbackFilePrefix,
		time.Now().Format("20060102_150405.000000000"),
		fallbackLockMarker,
		os.Getpid(),
		fallbackActiveSuffix,
	)
	filePath := filepath.Join(w.dir, fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("创建 fallback 活动文件失败: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		_ = os.Remove(filePath)
		return fmt.Errorf("锁定 fallback 活动文件失败: %w", err)
	}

	w.current = file
	w.currentPath = filePath
	w.currentSize = 0
	return nil
}

// finalizeCurrentLocked 将活动文件原子转换为可恢复文件。
func (w *fallbackWriter) finalizeCurrentLocked() error {
	if w.current == nil {
		return nil
	}

	current := w.current
	currentPath := w.currentPath
	currentSize := w.currentSize
	w.current = nil
	w.currentPath = ""
	w.currentSize = 0

	if err := current.Sync(); err != nil {
		_ = current.Close()
		return fmt.Errorf("同步 fallback 活动文件失败: %w", err)
	}
	if err := current.Close(); err != nil {
		return fmt.Errorf("关闭 fallback 活动文件失败: %w", err)
	}
	if currentSize == 0 {
		if err := os.Remove(currentPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("删除空 fallback 文件失败: %w", err)
		}
		return nil
	}

	readyPath := strings.TrimSuffix(currentPath, fallbackActiveSuffix) + fallbackReadySuffix
	if err := os.Rename(currentPath, readyPath); err != nil {
		return fmt.Errorf("完成 fallback 文件失败: %w", err)
	}
	return nil
}

// readyFiles 返回当前可安全恢复的文件。
func (w *fallbackWriter) readyFiles() ([]string, error) {
	readyFiles, err := filepath.Glob(filepath.Join(w.dir, fallbackFilePrefix+"*"+fallbackReadySuffix))
	if err != nil {
		return nil, fmt.Errorf("查询 fallback 文件失败: %w", err)
	}
	legacyFiles, err := filepath.Glob(filepath.Join(w.dir, fallbackFilePrefix+"*"+fallbackLegacySuffix))
	if err != nil {
		return nil, fmt.Errorf("查询旧版 fallback 文件失败: %w", err)
	}
	files := append(readyFiles, legacyFiles...)
	sort.Strings(files)
	return files, nil
}

// promoteStaleActiveFiles 将异常退出进程遗留的活动文件转为可恢复文件。
func (w *fallbackWriter) promoteStaleActiveFiles() error {
	activeFiles, err := filepath.Glob(filepath.Join(w.dir, fallbackFilePrefix+"*"+fallbackActiveSuffix))
	if err != nil {
		return fmt.Errorf("查询 fallback 活动文件失败: %w", err)
	}
	sort.Strings(activeFiles)

	var promoteErr error
	for _, filePath := range activeFiles {
		locked, err := isFallbackActiveFileLocked(filePath)
		if err != nil {
			promoteErr = errors.Join(promoteErr, err)
			continue
		}
		if locked {
			continue
		}
		if !strings.Contains(filepath.Base(filePath), fallbackLockMarker) {
			pid, ok := fallbackActiveFilePID(filePath)
			if !ok || isProcessRunning(pid) {
				continue
			}
		}
		if err := w.promoteStaleActiveFile(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			promoteErr = errors.Join(promoteErr, err)
		}
	}
	return promoteErr
}

// isFallbackActiveFileLocked 判断活动文件是否仍由存活 writer 持有。
func isFallbackActiveFileLocked(filePath string) (bool, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0)
	if err != nil {
		return false, fmt.Errorf("打开 fallback 活动文件失败: %w", err)
	}
	defer file.Close()

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("检查 fallback 活动文件锁失败: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return false, fmt.Errorf("释放 fallback 活动文件检查锁失败: %w", err)
	}
	return false, nil
}

// fallbackActiveFilePID 从活动文件名中解析所属进程 ID。
func fallbackActiveFilePID(filePath string) (int, bool) {
	fileName := filepath.Base(filePath)
	pidIndex := strings.LastIndex(fileName, "_pid")
	if pidIndex < 0 || !strings.HasSuffix(fileName, fallbackActiveSuffix) {
		return 0, false
	}

	pidText := strings.TrimSuffix(fileName[pidIndex+4:], fallbackActiveSuffix)
	pid, err := strconv.Atoi(pidText)
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// isProcessRunning 判断活动文件所属进程是否仍然存活。
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return true
	}

	err = process.Signal(syscall.Signal(0))
	switch {
	case err == nil:
		return true
	case errors.Is(err, syscall.EPERM):
		return true
	case errors.Is(err, os.ErrProcessDone), errors.Is(err, syscall.ESRCH):
		return false
	default:
		return true
	}
}

// promoteStaleActiveFile 修复异常中断的尾部并完成活动文件。
func (w *fallbackWriter) promoteStaleActiveFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("读取遗留 fallback 活动文件失败: %w", err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("删除空 fallback 活动文件失败: %w", err)
		}
		return nil
	}

	if data[len(data)-1] != '\n' {
		lastNewline := bytes.LastIndexByte(data, '\n')
		tail := bytes.TrimSpace(data[lastNewline+1:])
		switch {
		case len(tail) > 0 && json.Valid(tail):
			file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return fmt.Errorf("打开遗留 fallback 活动文件失败: %w", err)
			}
			if _, err := file.Write([]byte{'\n'}); err != nil {
				_ = file.Close()
				return fmt.Errorf("完成遗留 fallback 记录失败: %w", err)
			}
			if err := file.Sync(); err != nil {
				_ = file.Close()
				return fmt.Errorf("同步遗留 fallback 活动文件失败: %w", err)
			}
			if err := file.Close(); err != nil {
				return fmt.Errorf("关闭遗留 fallback 活动文件失败: %w", err)
			}
		case lastNewline >= 0:
			if err := os.Truncate(filePath, int64(lastNewline+1)); err != nil {
				return fmt.Errorf("修复遗留 fallback 活动文件失败: %w", err)
			}
			file, err := os.OpenFile(filePath, os.O_WRONLY, 0)
			if err != nil {
				return fmt.Errorf("打开已修复 fallback 活动文件失败: %w", err)
			}
			if err := file.Sync(); err != nil {
				_ = file.Close()
				return fmt.Errorf("同步已修复 fallback 活动文件失败: %w", err)
			}
			if err := file.Close(); err != nil {
				return fmt.Errorf("关闭已修复 fallback 活动文件失败: %w", err)
			}
		default:
			return w.quarantineCorruptFile(filePath, errors.New("遗留 fallback 活动文件没有完整记录"))
		}
	}

	readyPath := strings.TrimSuffix(filePath, fallbackActiveSuffix) + fallbackReadySuffix
	if err := os.Rename(filePath, readyPath); err != nil {
		return fmt.Errorf("完成遗留 fallback 活动文件失败: %w", err)
	}
	return nil
}

// recoverFile 恢复一个完整文件；只有全部导出成功后才删除源文件。
func (w *fallbackWriter) recoverFile(filePath string, exporter trace.SpanExporter) error {
	reader, closeReader, err := openFallbackReader(filePath)
	if err != nil {
		return w.quarantineCorruptFile(filePath, err)
	}
	defer closeReader()

	scanner := bufio.NewScanner(reader)
	// Scanner 需要额外容纳一字节换行符，确保写入上限内的记录都能恢复。
	scanner.Buffer(make([]byte, 64*1024), w.maxRecordSize+1)
	batch := make([]trace.SpanSnapshot, 0, 100)

	exportBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := exporter.ExportSpans(batch); err != nil {
			releaseSpans(batch)
			batch = batch[:0]
			return err
		}
		releaseSpans(batch)
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		span, err := fallback.ConvertJSONToSpanSnapshot(line)
		if err != nil {
			releaseSpans(batch)
			return w.quarantineCorruptFile(filePath, fmt.Errorf("解析 fallback 数据失败: %w", err))
		}
		batch = append(batch, span)
		if len(batch) == cap(batch) {
			if err := exportBatch(); err != nil {
				return fmt.Errorf("恢复 fallback 文件 %s 失败: %w", filepath.Base(filePath), err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		releaseSpans(batch)
		return w.quarantineCorruptFile(filePath, fmt.Errorf("读取 fallback 数据失败: %w", err))
	}
	if err := exportBatch(); err != nil {
		return fmt.Errorf("恢复 fallback 文件 %s 失败: %w", filepath.Base(filePath), err)
	}
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("删除已恢复 fallback 文件失败: %w", err)
	}
	return nil
}

// quarantineCorruptFile 保留损坏文件并阻止恢复线程反复读取。
func (w *fallbackWriter) quarantineCorruptFile(filePath string, cause error) error {
	corruptPath := filePath + ".corrupt"
	if err := os.Rename(filePath, corruptPath); err != nil {
		return errors.Join(cause, fmt.Errorf("隔离损坏 fallback 文件失败: %w", err))
	}
	return fmt.Errorf("fallback 文件已隔离为 %s: %w", filepath.Base(corruptPath), cause)
}

// openFallbackReader 打开新版 NDJSON 或旧版 gzip fallback 文件。
func openFallbackReader(filePath string) (io.Reader, func(), error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, func() {}, fmt.Errorf("打开 fallback 文件失败: %w", err)
	}
	if !strings.HasSuffix(filePath, fallbackLegacySuffix) {
		return file, func() { _ = file.Close() }, nil
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, func() {}, fmt.Errorf("打开旧版 fallback 压缩流失败: %w", err)
	}
	return gzipReader, func() {
		_ = gzipReader.Close()
		_ = file.Close()
	}, nil
}
