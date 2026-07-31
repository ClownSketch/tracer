package exporter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// FileSpanExporter 文件导出器配置选项
type FileExporterOption func(*FileSpanExporter)

// FileSpanExporter 文件导出器。
// BatchProcessor 负责异步调度；导出器收到 batch 后直接同步写入文件。
type FileSpanExporter struct {
	filePath        string        // 文件路径
	file            *os.File      // 文件句柄
	writer          *bufio.Writer // 缓冲写入器
	mu              sync.Mutex    // 保护文件写入
	stopOnce        sync.Once     // 确保只关闭一次
	shutdownDone    chan struct{} // 后台关闭完成信号
	shutdownErr     error         // 后台关闭错误
	writeCount      int64         // 写入计数
	errorCount      int64         // 错误计数
	maxFileSize     int64         // 最大文件大小（字节）
	currentSize     int64         // 当前文件大小
	rotateBySize    bool          // 是否按大小轮转
	rotateByTime    bool          // 是否按时间轮转
	rotateInterval  time.Duration // 轮转间隔
	lastRotateTime  time.Time     // 上次轮转时间
	maxBackups      int           // 最大备份文件数
	asyncBufferSize int           // 保留配置项，控制 writer buffer 大小
	enqueueTimeout  time.Duration // 保留配置项，避免破坏现有配置入口
}

// WithFilePath 设置文件路径
func WithFilePath(filePath string) FileExporterOption {
	return func(e *FileSpanExporter) {
		e.filePath = filePath
	}
}

// WithMaxFileSize 设置最大文件大小（字节），超过此大小会轮转
func WithMaxFileSize(size int64) FileExporterOption {
	return func(e *FileSpanExporter) {
		e.maxFileSize = size
		e.rotateBySize = size > 0
	}
}

// WithRotateInterval 设置轮转间隔（按时间轮转）
func WithRotateInterval(interval time.Duration) FileExporterOption {
	return func(e *FileSpanExporter) {
		e.rotateInterval = interval
		e.rotateByTime = interval > 0
		if e.rotateByTime {
			e.lastRotateTime = time.Now()
		}
	}
}

// WithMaxBackups 设置最大备份文件数
func WithMaxBackups(maxBackups int) FileExporterOption {
	return func(e *FileSpanExporter) {
		e.maxBackups = maxBackups
	}
}

// WithAsyncBufferSize 设置文件 writer 的缓冲区大小。
func WithAsyncBufferSize(size int) FileExporterOption {
	return func(e *FileSpanExporter) {
		e.asyncBufferSize = size
	}
}

// WithEnqueueTimeout 保留配置入口。
// 现在入队只发生在 BatchProcessor，这里不再维护 exporter 内部队列。
func WithEnqueueTimeout(timeout time.Duration) FileExporterOption {
	return func(e *FileSpanExporter) {
		e.enqueueTimeout = timeout
	}
}

// NewFileSpanExporter 创建文件导出器
func NewFileSpanExporter(opts ...FileExporterOption) (*FileSpanExporter, error) {
	e := &FileSpanExporter{
		filePath:        "tracer.log",
		maxFileSize:     100 * 1024 * 1024, // 默认 100MB
		rotateBySize:    true,
		rotateByTime:    false,
		rotateInterval:  0,
		maxBackups:      5,
		asyncBufferSize: 4096,            // 直接作为 bufio buffer 大小
		enqueueTimeout:  5 * time.Second, // 保留配置入口
		lastRotateTime:  time.Now(),
	}

	// 应用选项
	for _, opt := range opts {
		opt(e)
	}

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(e.filePath), 0755); err != nil {
		return nil, fmt.Errorf("创建目录失败: %w", err)
	}

	// 打开文件
	if err := e.openFile(); err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}

	return e, nil
}

// openFile 打开文件
func (e *FileSpanExporter) openFile() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.openFileLocked()
}

// openFileLocked 在持有文件锁时打开当前日志文件。
func (e *FileSpanExporter) openFileLocked() error {
	// 如果文件已打开，先关闭
	if e.file != nil {
		if e.writer != nil {
			if err := e.writer.Flush(); err != nil {
				return err
			}
		}
		if err := e.file.Close(); err != nil {
			return err
		}
	}

	// 打开文件（追加模式）
	file, err := os.OpenFile(e.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	e.file = file
	bufferSize := e.asyncBufferSize
	if bufferSize <= 0 {
		bufferSize = 4096
	}
	e.writer = bufio.NewWriterSize(file, bufferSize)

	// 获取当前文件大小
	info, err := file.Stat()
	if err == nil {
		e.currentSize = info.Size()
	} else {
		e.currentSize = 0
	}

	return nil
}

// writeData 写入数据
func (e *FileSpanExporter) writeData(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.writer == nil || e.file == nil {
		return errors.New("file exporter 已关闭")
	}

	if e.rotateByTime && time.Since(e.lastRotateTime) >= e.rotateInterval {
		if err := e.rotateFile(); err != nil {
			return err
		}
	}

	// 检查是否需要轮转（按大小）
	if e.rotateBySize && e.currentSize >= e.maxFileSize {
		if err := e.rotateFile(); err != nil {
			return err
		}
	}

	// 写入数据（每条记录一行）
	n, err := e.writer.Write(data)
	if err != nil {
		return err
	}

	if err := e.writer.WriteByte('\n'); err != nil {
		return err
	}
	e.currentSize += int64(n + 1) // 加上换行符

	return nil
}

// flushBuffer 刷新缓冲区
func (e *FileSpanExporter) flushBuffer() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.writer != nil {
		return e.writer.Flush()
	}
	return nil
}

// rotateFile 轮转文件
func (e *FileSpanExporter) rotateFile() error {
	// 刷新缓冲区
	if e.writer != nil {
		if err := e.writer.Flush(); err != nil {
			return err
		}
	}

	// 关闭当前文件
	if e.file != nil {
		if err := e.file.Close(); err != nil {
			return err
		}
		e.file = nil
		e.writer = nil
	}

	// 重命名当前文件
	if _, err := os.Stat(e.filePath); err == nil {
		// 生成备份文件名（带时间戳，使用 strings.Builder 优化性能）
		timestamp := time.Now().Format("20060102-150405.000000000")
		var builder strings.Builder
		builder.Grow(len(e.filePath) + len(timestamp) + 1) // 预分配容量
		builder.WriteString(e.filePath)
		builder.WriteString(".")
		builder.WriteString(timestamp)
		backupPath := builder.String()

		// 重命名文件
		if err := os.Rename(e.filePath, backupPath); err != nil {
			return err
		}

		// 清理旧备份文件
		e.cleanupOldBackups()
	}

	// 重置文件大小
	e.currentSize = 0
	e.lastRotateTime = time.Now()

	// 重新打开文件
	return e.openFileLocked()
}

// cleanupOldBackups 清理旧备份文件
func (e *FileSpanExporter) cleanupOldBackups() {
	if e.maxBackups <= 0 {
		return
	}

	// 获取目录
	dir := filepath.Dir(e.filePath)
	baseName := filepath.Base(e.filePath)

	// 读取目录
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	// 查找所有备份文件
	var backups []os.FileInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			// 检查是否是备份文件（以 baseName. 开头）
			if len(entry.Name()) > len(baseName)+1 && entry.Name()[:len(baseName)+1] == baseName+"." {
				backups = append(backups, info)
			}
		}
	}

	// 如果备份文件数量超过限制，删除最旧的
	if len(backups) > e.maxBackups {
		// 按修改时间排序（最旧的在前面）
		for i := 0; i < len(backups)-1; i++ {
			for j := i + 1; j < len(backups); j++ {
				if backups[i].ModTime().After(backups[j].ModTime()) {
					backups[i], backups[j] = backups[j], backups[i]
				}
			}
		}

		// 删除最旧的文件
		for i := 0; i < len(backups)-e.maxBackups; i++ {
			oldPath := filepath.Join(dir, backups[i].Name())
			os.Remove(oldPath)
		}
	}
}

// ExportSpan 同步导出单个 Span
func (e *FileSpanExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

// ExportSpans 同步导出多个 Span。
// processor 负责 fallback 和快照释放，这里只负责直接写文件。
func (e *FileSpanExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	for _, span := range spans {
		if span == nil {
			continue
		}

		// 立即构建可序列化的数据结构
		spanData := e.buildSpanData(span)

		// 立即序列化为 JSON
		data, err := json.Marshal(spanData)
		if err != nil {
			atomic.AddInt64(&e.errorCount, 1)
			return err
		}

		if err := e.writeData(data); err != nil {
			atomic.AddInt64(&e.errorCount, 1)
			return err
		}

		atomic.AddInt64(&e.writeCount, 1)
	}

	if err := e.flushBuffer(); err != nil {
		atomic.AddInt64(&e.errorCount, 1)
		return err
	}
	return nil
}

// buildSpanData 构建span数据用于JSON序列化
// 参考 console_exporter.go 的实现
func (e *FileSpanExporter) buildSpanData(span trace.SpanSnapshot) map[string]any {
	data := make(map[string]any)

	// 基本信息
	data["name"] = span.GetSpanName()
	data["traceID"] = span.GetSpanTraceID()
	data["spanID"] = span.GetSpanID()
	data["parentSpanID"] = span.GetSpanParentSpanID()
	data["kind"] = span.GetSpanKind()
	data["startTime"] = span.GetStartTime().Format(time.RFC3339Nano)
	data["endTime"] = span.GetEndTime().Format(time.RFC3339Nano)
	data["duration"] = span.GetEndTime().Sub(span.GetStartTime()).String()

	// 状态
	status := span.GetStatus()
	if status.Code != "" || status.Description != "" {
		statusData := make(map[string]any)
		if status.Code != "" {
			statusData["code"] = status.Code
		}
		if status.Description != "" {
			statusData["description"] = status.Description
		}
		data["status"] = statusData
	}

	// 属性 - 深拷贝
	if attrs := span.GetAttributes(); len(attrs) > 0 {
		attrsCopy := make(map[string]any, len(attrs))
		for k, v := range attrs {
			attrsCopy[k] = v
		}
		data["attributes"] = attrsCopy
	}

	// 事件 - 深拷贝嵌套的 Attributes
	if events := span.GetEvents(); len(events) > 0 {
		eventData := make([]map[string]any, len(events))
		for i, event := range events {
			ev := map[string]any{
				"name":      event.Name,
				"timestamp": event.Timestamp,
			}
			if len(event.Attributes) > 0 {
				// 深拷贝 event.Attributes
				eventAttrsCopy := make(map[string]any, len(event.Attributes))
				for k, v := range event.Attributes {
					eventAttrsCopy[k] = v
				}
				ev["attributes"] = eventAttrsCopy
			}

			eventData[i] = ev
		}

		data["events"] = eventData
	}

	// 日志 - 深拷贝嵌套的 Attributes 和 Fields
	if logs := span.GetLogs(); len(logs) > 0 {
		logData := make([]map[string]any, len(logs))
		for i, log := range logs {
			lg := map[string]any{
				"timestamp": log.Timestamp,
				"message":   log.Message,
				"severity":  log.Severity,
			}
			if len(log.Attributes) > 0 {
				// 深拷贝 log.Attributes
				logAttrsCopy := make(map[string]any, len(log.Attributes))
				for k, v := range log.Attributes {
					logAttrsCopy[k] = v
				}
				lg["attributes"] = logAttrsCopy
			}
			if log.Fields != nil {
				// 深拷贝 log.Fields - 需要先进行类型断言
				if fieldsMap, ok := log.Fields.(map[string]any); ok {
					logFieldsCopy := make(map[string]any, len(fieldsMap))
					for k, v := range fieldsMap {
						logFieldsCopy[k] = v
					}
					lg["fields"] = logFieldsCopy
				} else {
					// 如果不是 map 类型，直接使用原值
					lg["fields"] = log.Fields
				}
			}
			if log.EventType != "" {
				lg["eventType"] = log.EventType
			}
			logData[i] = lg
		}
		data["logs"] = logData
	}

	// 错误详情
	if errDetail := span.GetErrorDetail(); errDetail != nil {
		errorData := make(map[string]any)
		if errDetail.Code != "" {
			errorData["code"] = errDetail.Code
		}
		errorData["message"] = errDetail.Message
		if errDetail.BusinessCode != "" {
			errorData["businessCode"] = errDetail.BusinessCode
		}
		if len(errDetail.BusinessMessage) > 0 {
			errorData["businessMessage"] = errDetail.BusinessMessage
		}
		if errDetail.HttpCode > 0 {
			errorData["httpCode"] = errDetail.HttpCode
		}
		if errDetail.Timestamp != "" {
			errorData["timestamp"] = errDetail.Timestamp
		}
		if len(errDetail.MetaData) > 0 {
			// 深拷贝 MetaData
			metaDataCopy := make(map[string]any, len(errDetail.MetaData))
			for k, v := range errDetail.MetaData {
				metaDataCopy[k] = v
			}
			errorData["metaData"] = metaDataCopy
		}
		if len(errDetail.StackTrace) > 0 {
			stackTraceDocs := make([]types.StackFrame, len(errDetail.StackTrace))
			for i, v := range errDetail.StackTrace {
				stackTraceDocs[i] = types.StackFrame{
					File:         v.File,
					FileName:     v.FileName,
					LineNumber:   v.LineNumber,
					FunctionName: v.FunctionName,
				}
			}
			errorData["stackTrace"] = stackTraceDocs
		}
		data["error"] = errorData
	}

	// 资源信息 - 深拷贝 Attributes
	if resource := span.GetResource(); resource != nil {
		resourceData := make(map[string]any)
		if resource.ServiceName != "" {
			resourceData["serviceName"] = resource.ServiceName
		}
		if resource.Host != "" {
			resourceData["host"] = resource.Host
		}
		if len(resource.Attributes) > 0 {
			// 深拷贝 resource.Attributes
			resourceAttrsCopy := make(map[string]any, len(resource.Attributes))
			for k, v := range resource.Attributes {
				resourceAttrsCopy[k] = v
			}
			resourceData["attributes"] = resourceAttrsCopy
		}
		if len(resourceData) > 0 {
			data["resource"] = resourceData
		}
	}

	// 资源使用情况
	if usage := span.GetResourceUsage(); usage != nil {
		usageData := make(map[string]any)
		if usage.CPUUsage > 0 {
			usageData["cpuUsage"] = usage.CPUUsage
		}
		if usage.MemoryUsage > 0 {
			usageData["memoryUsage"] = usage.MemoryUsage
		}
		if usage.DiskUsage > 0 {
			usageData["diskUsage"] = usage.DiskUsage
		}
		if usage.NetworkIO > 0 {
			usageData["networkIO"] = usage.NetworkIO
		}
		if len(usageData) > 0 {
			data["resourceUsage"] = usageData
		}
	}

	// 关联 Span
	if links := span.GetLinkedSpans(); len(links) > 0 {
		linkData := make([]map[string]any, len(links))
		for i, link := range links {
			linkItem := map[string]any{
				"traceID": link.TraceID,
				"spanID":  link.SpanID,
			}
			if link.ParentSpanID != "" {
				linkItem["parentSpanID"] = link.ParentSpanID
			}
			linkData[i] = linkItem
		}
		data["links"] = linkData
	}

	return data
}

// Shutdown 关闭导出器
func (e *FileSpanExporter) Shutdown(ctx context.Context) error {
	e.stopOnce.Do(func() {
		e.shutdownDone = make(chan struct{})
		go e.finishShutdown()
	})

	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-e.shutdownDone:
		return e.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishShutdown 在后台刷新缓冲并关闭文件。
func (e *FileSpanExporter) finishShutdown() {
	defer close(e.shutdownDone)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.writer != nil {
		e.shutdownErr = errors.Join(e.shutdownErr, e.writer.Flush())
	}
	if e.file != nil {
		e.shutdownErr = errors.Join(e.shutdownErr, e.file.Close())
	}
	e.file = nil
	e.writer = nil
}

// GetStats 获取统计信息
func (e *FileSpanExporter) GetStats() (writeCount, errorCount int64, currentSize int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return atomic.LoadInt64(&e.writeCount), atomic.LoadInt64(&e.errorCount), e.currentSize
}
