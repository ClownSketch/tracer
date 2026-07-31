package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClownSketch/tracer/trace"
)

// ZipkinExporter Zipkin 导出器。
// BatchProcessor 负责异步调度；导出器收到 batch 后直接同步发送到 Zipkin。
// 性能优化：
//   - 使用对象池减少内存分配
//   - 批量发送减少网络开销
//   - 超时控制防止阻塞
type ZipkinExporter struct {
	// Zipkin配置
	endpoint      string        // Zipkin Collector端点，如 "http://localhost:9411/api/v2/spans"
	timeout       time.Duration // HTTP请求超时时间
	batchSize     int           // 批量大小
	flushInterval time.Duration // 刷新间隔
	headers       map[string]string

	// 统计信息
	processedCount int64 // 处理数量
	droppedCount   int64 // 丢弃数量
	exportErrors   int64 // 导出错误数量

	// HTTP客户端（复用连接，提升性能）
	httpClient *http.Client

	// 对象池（减少内存分配）
	spanPool sync.Pool
}

// ZipkinExporterOption Zipkin导出器选项
type ZipkinExporterOption func(*ZipkinExporter)

// WithZipkinEndpoint 设置Zipkin端点
func WithZipkinEndpoint(endpoint string) ZipkinExporterOption {
	return func(e *ZipkinExporter) {
		e.endpoint = endpoint
	}
}

// WithZipkinTimeout 设置HTTP请求超时时间
func WithZipkinTimeout(timeout time.Duration) ZipkinExporterOption {
	return func(e *ZipkinExporter) {
		e.timeout = timeout
	}
}

// WithZipkinHeaders 设置发送到 Zipkin Collector 的固定请求头。
func WithZipkinHeaders(headers map[string]string) ZipkinExporterOption {
	return func(e *ZipkinExporter) {
		if len(headers) == 0 {
			return
		}
		e.headers = make(map[string]string, len(headers))
		for key, value := range headers {
			e.headers[key] = value
		}
	}
}

// WithZipkinBatchSize 设置批量大小
func WithZipkinBatchSize(size int) ZipkinExporterOption {
	return func(e *ZipkinExporter) {
		if size > 0 {
			e.batchSize = size
		}
	}
}

// WithZipkinFlushInterval 设置刷新间隔
func WithZipkinFlushInterval(interval time.Duration) ZipkinExporterOption {
	return func(e *ZipkinExporter) {
		if interval > 0 {
			e.flushInterval = interval
		}
	}
}

// WithZipkinQueueSize 保留配置入口。
// 现在异步队列只存在于 BatchProcessor，这里不再维护 exporter 内部队列。
func WithZipkinQueueSize(size int) ZipkinExporterOption {
	return func(e *ZipkinExporter) {}
}

// zipkinSpan Zipkin Span格式（V2 API）
type zipkinSpan struct {
	TraceID        string             `json:"traceId"`                  // 追踪ID（16进制字符串，32位）
	ID             string             `json:"id"`                       // Span ID（16进制字符串，16位）
	ParentID       string             `json:"parentId"`                 // 父Span ID（16进制字符串，16位）
	Name           string             `json:"name"`                     // 操作名称
	Kind           string             `json:"kind,omitempty"`           // Span类型：CLIENT, SERVER, PRODUCER, CONSUMER
	Timestamp      int64              `json:"timestamp"`                // 开始时间（微秒）
	Duration       int64              `json:"duration"`                 // 持续时间（微秒）
	LocalEndpoint  *zipkinEndpoint    `json:"localEndpoint,omitempty"`  // 本地端点
	RemoteEndpoint *zipkinEndpoint    `json:"remoteEndpoint,omitempty"` // 远程端点
	Tags           map[string]string  `json:"tags,omitempty"`           // 标签
	Annotations    []zipkinAnnotation `json:"annotations,omitempty"`    // 注解（日志）
	Shared         bool               `json:"shared,omitempty"`         // 是否共享
}

// zipkinEndpoint Zipkin端点
type zipkinEndpoint struct {
	ServiceName string `json:"serviceName"`    // 服务名称
	IPv4        string `json:"ipv4,omitempty"` // IPv4地址
	IPv6        string `json:"ipv6,omitempty"` // IPv6地址
	Port        int    `json:"port,omitempty"` // 端口
}

// zipkinAnnotation Zipkin注解（用于日志）
type zipkinAnnotation struct {
	Timestamp int64  `json:"timestamp"` // 时间戳（微秒）
	Value     string `json:"value"`     // 值
}

// NewZipkinExporter 创建Zipkin导出器
func NewZipkinExporter(endpoint string, opts ...ZipkinExporterOption) (*ZipkinExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("Zipkin端点不能为空")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100

	e := &ZipkinExporter{
		endpoint:      endpoint,
		timeout:       10 * time.Second,
		batchSize:     50,
		flushInterval: 2 * time.Second,
		httpClient: &http.Client{
			Timeout:   10 * time.Second,
			Transport: transport,
		},
	}

	// 初始化对象池
	e.spanPool = sync.Pool{
		New: func() any {
			return &zipkinSpan{
				Tags:        make(map[string]string, 8),
				Annotations: make([]zipkinAnnotation, 0, 4),
			}
		},
	}

	// 应用选项
	for _, opt := range opts {
		opt(e)
	}

	// 更新HTTP客户端超时
	e.httpClient.Timeout = e.timeout

	return e, nil
}

// ExportSpan 同步导出单个 span
func (e *ZipkinExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

// ExportSpans 同步批量导出多个 span
func (e *ZipkinExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	atomic.AddInt64(&e.processedCount, int64(len(spans)))

	batch := make([]*zipkinSpan, 0, len(spans))
	for _, span := range spans {
		if span == nil {
			continue
		}
		batch = append(batch, e.convertToZipkinSpan(span))
	}
	return e.flush(batch)
}

// convertToZipkinSpan 将SpanSnapshot转换为Zipkin格式
func (e *ZipkinExporter) convertToZipkinSpan(span trace.SpanSnapshot) *zipkinSpan {
	// 从对象池获取span（减少内存分配）
	zSpan := e.spanPool.Get().(*zipkinSpan)

	// 重置span（清理旧数据）
	if zSpan.Tags != nil {
		clear(zSpan.Tags)
	} else {
		zSpan.Tags = make(map[string]string, 8)
	}
	if zSpan.Annotations != nil {
		zSpan.Annotations = zSpan.Annotations[:0]
	} else {
		zSpan.Annotations = make([]zipkinAnnotation, 0, 4)
	}

	// 转换TraceID和SpanID
	zSpan.TraceID = span.GetSpanTraceID()
	zSpan.ID = span.GetSpanID()
	zSpan.ParentID = span.GetSpanParentSpanID()
	zSpan.Name = span.GetSpanName()

	// 转换Span类型
	zSpan.Kind = e.convertSpanKind(span.GetSpanKind().String())

	// 计算时间（微秒）
	startTime := span.GetStartTime()
	zSpan.Timestamp = startTime.UnixMicro()
	zSpan.Duration = span.GetEndTime().Sub(startTime).Microseconds()

	// 转换标签
	e.convertTags(span, zSpan)

	// 转换注解（日志和事件）
	e.convertAnnotations(span, zSpan)

	// 转换端点信息
	e.convertEndpoints(span, zSpan)

	return zSpan
}

// convertSpanKind 转换Span类型
func (e *ZipkinExporter) convertSpanKind(kind string) string {
	switch kind {
	case "server":
		return "SERVER"
	case "client":
		return "CLIENT"
	case "producer":
		return "PRODUCER"
	case "consumer":
		return "CONSUMER"
	default:
		return ""
	}
}

// convertTags 转换标签
func (e *ZipkinExporter) convertTags(span trace.SpanSnapshot, zSpan *zipkinSpan) {
	// 添加状态
	status := span.GetStatus()
	if status.Code != "" {
		zSpan.Tags["status.code"] = status.Code.String()
	}
	if status.Description != "" {
		zSpan.Tags["status.description"] = status.Description
	}

	// 添加属性
	if attrs := span.GetAttributes(); len(attrs) > 0 {
		for k, v := range attrs {
			// Zipkin只支持string类型的标签
			zSpan.Tags[k] = fmt.Sprintf("%v", v)
		}
	}

	// 添加错误信息
	if errDetail := span.GetErrorDetail(); errDetail != nil {
		zSpan.Tags["error"] = "true"
		if errDetail.Message != "" {
			zSpan.Tags["error.message"] = errDetail.Message
		}
		if errDetail.Code != "" {
			zSpan.Tags["error.code"] = errDetail.Code
		}
	}
}

// convertAnnotations 转换注解（日志和事件）
func (e *ZipkinExporter) convertAnnotations(span trace.SpanSnapshot, zSpan *zipkinSpan) {
	// 转换Span日志
	if spanLogs := span.GetLogs(); len(spanLogs) > 0 {
		for _, log := range spanLogs {
			// 解析时间戳
			timestamp, err := time.Parse(time.RFC3339Nano, log.Timestamp)
			if err != nil {
				timestamp = time.Now()
			}

			// 构建日志消息
			value := log.Message
			if log.Severity != "" {
				value = fmt.Sprintf("[%s] %s", log.Severity, value)
			}

			zSpan.Annotations = append(zSpan.Annotations, zipkinAnnotation{
				Timestamp: timestamp.UnixMicro(),
				Value:     value,
			})
		}
	}

	// 转换Span事件
	if events := span.GetEvents(); len(events) > 0 {
		for _, event := range events {
			// 解析时间戳
			timestamp, err := time.Parse(time.RFC3339Nano, event.Timestamp)
			if err != nil {
				timestamp = time.Now()
			}

			zSpan.Annotations = append(zSpan.Annotations, zipkinAnnotation{
				Timestamp: timestamp.UnixMicro(),
				Value:     event.Name,
			})
		}
	}
}

// convertEndpoints 转换端点信息
func (e *ZipkinExporter) convertEndpoints(span trace.SpanSnapshot, zSpan *zipkinSpan) {
	if resource := span.GetResource(); resource != nil {
		if resource.ServiceName != "" {
			zSpan.LocalEndpoint = &zipkinEndpoint{
				ServiceName: resource.ServiceName,
			}
			if resource.Host != "" {
				zSpan.LocalEndpoint.IPv4 = resource.Host
			}
		}
	}
}

// flush 批量发送到Zipkin
func (e *ZipkinExporter) flush(spans []*zipkinSpan) error {
	if len(spans) == 0 {
		return nil
	}

	// 序列化为JSON
	jsonData, err := json.Marshal(spans)
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		// 归还spans到对象池
		for _, span := range spans {
			e.returnSpanToPool(span)
		}
		return err
	}

	// 发送HTTP请求（带超时控制）
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", e.endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		for _, span := range spans {
			e.returnSpanToPool(span)
		}
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	for key, value := range e.headers {
		req.Header.Set(key, value)
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		for _, span := range spans {
			e.returnSpanToPool(span)
		}
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		for _, span := range spans {
			e.returnSpanToPool(span)
		}
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		for _, span := range spans {
			e.returnSpanToPool(span)
		}
		return fmt.Errorf("zipkin export failed with status %d", resp.StatusCode)
	}

	// 成功发送后，归还spans到对象池
	for _, span := range spans {
		e.returnSpanToPool(span)
	}
	return nil
}

// returnSpanToPool 归还span到对象池
func (e *ZipkinExporter) returnSpanToPool(span *zipkinSpan) {
	if span == nil {
		return
	}
	// 清理数据
	if span.Tags != nil {
		clear(span.Tags)
	}
	if span.Annotations != nil {
		span.Annotations = span.Annotations[:0]
	}
	span.LocalEndpoint = nil
	span.RemoteEndpoint = nil
	// 归还到对象池
	e.spanPool.Put(span)
}

// Shutdown 关闭导出器并清理资源
func (e *ZipkinExporter) Shutdown(ctx context.Context) error {
	if ctx == nil {
		e.httpClient.CloseIdleConnections()
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		e.httpClient.CloseIdleConnections()
		return nil
	}
}

// GetStats 获取统计信息
func (e *ZipkinExporter) GetStats() map[string]int64 {
	return map[string]int64{
		"processed": atomic.LoadInt64(&e.processedCount),
		"dropped":   atomic.LoadInt64(&e.droppedCount),
		"errors":    atomic.LoadInt64(&e.exportErrors),
		"queue_len": 0,
		"queue_cap": 0,
	}
}
