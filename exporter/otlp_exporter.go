package exporter

import (
	"bytes"
	"compress/gzip"
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

// OTLPExporter OTLP 导出器（OpenTelemetry Protocol）。
// BatchProcessor 负责异步调度；导出器收到 batch 后直接同步发送到 OTLP Collector。
// 性能优化：
//   - 使用对象池减少内存分配
//   - 批量发送减少网络开销
//   - 超时控制防止阻塞
//   - 支持gzip压缩（可选）
type OTLPExporter struct {
	// OTLP配置
	endpoint      string        // OTLP Collector端点，如 "http://localhost:4318/v1/traces"
	timeout       time.Duration // HTTP请求超时时间
	batchSize     int           // 批量大小
	flushInterval time.Duration // 刷新间隔
	enableGzip    bool          // 是否启用gzip压缩

	// 统计信息
	processedCount int64 // 处理数量
	droppedCount   int64 // 丢弃数量
	exportErrors   int64 // 导出错误数量

	// HTTP客户端（复用连接，提升性能）
	httpClient *http.Client

	// 对象池（减少内存分配）
	spanPool sync.Pool
}

// OTLPExporterOption OTLP导出器选项
type OTLPExporterOption func(*OTLPExporter)

// WithOTLPEndpoint 设置OTLP端点
func WithOTLPEndpoint(endpoint string) OTLPExporterOption {
	return func(e *OTLPExporter) {
		e.endpoint = endpoint
	}
}

// WithOTLPTimeout 设置HTTP请求超时时间
func WithOTLPTimeout(timeout time.Duration) OTLPExporterOption {
	return func(e *OTLPExporter) {
		e.timeout = timeout
	}
}

// WithOTLPBatchSize 设置批量大小
func WithOTLPBatchSize(size int) OTLPExporterOption {
	return func(e *OTLPExporter) {
		if size > 0 {
			e.batchSize = size
		}
	}
}

// WithOTLPFlushInterval 设置刷新间隔
func WithOTLPFlushInterval(interval time.Duration) OTLPExporterOption {
	return func(e *OTLPExporter) {
		if interval > 0 {
			e.flushInterval = interval
		}
	}
}

// WithOTLPQueueSize 保留配置入口。
// 现在异步队列只存在于 BatchProcessor，这里不再维护 exporter 内部队列。
func WithOTLPQueueSize(size int) OTLPExporterOption {
	return func(e *OTLPExporter) {}
}

// WithOTLPEnableGzip 设置是否启用gzip压缩
func WithOTLPEnableGzip(enable bool) OTLPExporterOption {
	return func(e *OTLPExporter) {
		e.enableGzip = enable
	}
}

// otlpSpan OTLP Span格式（简化版，符合OTLP规范）
type otlpSpan struct {
	ServiceName       string         `json:"-"`                    // Span 所属服务名称
	TraceID           string         `json:"trace_id"`             // 追踪ID（16进制字符串）
	SpanID            string         `json:"span_id"`              // Span ID（16进制字符串）
	ParentSpanID      string         `json:"parent_span_id"`       // 父Span ID（16进制字符串）
	Name              string         `json:"name"`                 // 操作名称
	Kind              string         `json:"kind"`                 // Span类型
	StartTimeUnixNano int64          `json:"start_time_unix_nano"` // 开始时间（纳秒）
	EndTimeUnixNano   int64          `json:"end_time_unix_nano"`   // 结束时间（纳秒）
	Attributes        map[string]any `json:"attributes,omitempty"` // 属性
	Events            []otlpEvent    `json:"events,omitempty"`     // 事件
	Links             []otlpLink     `json:"links,omitempty"`      // 关联Span
	Status            *otlpStatus    `json:"status,omitempty"`     // 状态
}

// otlpEvent OTLP事件
type otlpEvent struct {
	Name                   string         `json:"name"`                               // 事件名称
	TimeUnixNano           int64          `json:"time_unix_nano"`                     // 时间戳（纳秒）
	Attributes             map[string]any `json:"attributes,omitempty"`               // 属性
	DroppedAttributesCount int32          `json:"dropped_attributes_count,omitempty"` // 丢弃的属性数量
}

// otlpLink OTLP关联Span
type otlpLink struct {
	TraceID    string         `json:"trace_id"`             // 追踪ID
	SpanID     string         `json:"span_id"`              // Span ID
	Attributes map[string]any `json:"attributes,omitempty"` // 属性
}

// otlpStatus OTLP状态
type otlpStatus struct {
	Code    string `json:"code"`              // 状态码：OK, ERROR, UNSET
	Message string `json:"message,omitempty"` // 状态消息
}

// otlpResourceSpans OTLP资源Spans（批量格式）
type otlpResourceSpans struct {
	Resource   *otlpResource    `json:"resource"`    // 资源信息
	ScopeSpans []otlpScopeSpans `json:"scope_spans"` // 作用域Spans
}

// otlpResource OTLP资源
type otlpResource struct {
	Attributes map[string]any `json:"attributes,omitempty"` // 资源属性
}

// otlpScopeSpans OTLP作用域Spans
type otlpScopeSpans struct {
	Spans []otlpSpan `json:"spans"` // Span列表
}

// otlpExportRequest OTLP导出请求
type otlpExportRequest struct {
	ResourceSpans []otlpResourceSpans `json:"resource_spans"` // 资源Spans列表
}

// NewOTLPExporter 创建OTLP导出器
func NewOTLPExporter(endpoint string, opts ...OTLPExporterOption) (*OTLPExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("OTLP端点不能为空")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100

	e := &OTLPExporter{
		endpoint:      endpoint,
		timeout:       10 * time.Second,
		batchSize:     50,
		flushInterval: 2 * time.Second,
		enableGzip:    false, // 默认不启用压缩，减少CPU开销
		httpClient: &http.Client{
			Timeout:   10 * time.Second,
			Transport: transport,
		},
	}

	// 初始化对象池
	e.spanPool = sync.Pool{
		New: func() any {
			return &otlpSpan{
				Attributes: make(map[string]any, 8),
				Events:     make([]otlpEvent, 0, 4),
				Links:      make([]otlpLink, 0, 2),
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
func (e *OTLPExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

// ExportSpans 同步批量导出多个 span
func (e *OTLPExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	atomic.AddInt64(&e.processedCount, int64(len(spans)))

	batch := make([]*otlpSpan, 0, len(spans))
	for _, span := range spans {
		if span == nil {
			continue
		}
		batch = append(batch, e.convertToOTLPSpan(span))
	}
	return e.flush(batch)
}

// convertToOTLPSpan 将SpanSnapshot转换为OTLP格式
func (e *OTLPExporter) convertToOTLPSpan(span trace.SpanSnapshot) *otlpSpan {
	// 从对象池获取span（减少内存分配）
	oSpan := e.spanPool.Get().(*otlpSpan)

	// 重置span（清理旧数据）
	if oSpan.Attributes != nil {
		clear(oSpan.Attributes)
	} else {
		oSpan.Attributes = make(map[string]any, 8)
	}
	if oSpan.Events != nil {
		oSpan.Events = oSpan.Events[:0]
	} else {
		oSpan.Events = make([]otlpEvent, 0, 4)
	}
	if oSpan.Links != nil {
		oSpan.Links = oSpan.Links[:0]
	} else {
		oSpan.Links = make([]otlpLink, 0, 2)
	}

	// 转换基础信息
	oSpan.TraceID = span.GetSpanTraceID()
	oSpan.SpanID = span.GetSpanID()
	oSpan.ParentSpanID = span.GetSpanParentSpanID()
	oSpan.Name = span.GetSpanName()
	oSpan.ServiceName = ""
	if resource := span.GetResource(); resource != nil {
		oSpan.ServiceName = resource.ServiceName
	}

	// 转换Span类型
	oSpan.Kind = e.convertSpanKind(span.GetSpanKind().String())

	// 转换时间（纳秒）
	startTime := span.GetStartTime()
	endTime := span.GetEndTime()
	oSpan.StartTimeUnixNano = startTime.UnixNano()
	oSpan.EndTimeUnixNano = endTime.UnixNano()

	// 转换属性
	e.convertAttributes(span, oSpan)

	// 转换事件
	e.convertEvents(span, oSpan)

	// 转换关联Span
	e.convertLinks(span, oSpan)

	// 转换状态
	e.convertStatus(span, oSpan)

	return oSpan
}

// convertSpanKind 转换Span类型
func (e *OTLPExporter) convertSpanKind(kind string) string {
	switch kind {
	case "server":
		return "SPAN_KIND_SERVER"
	case "client":
		return "SPAN_KIND_CLIENT"
	case "producer":
		return "SPAN_KIND_PRODUCER"
	case "consumer":
		return "SPAN_KIND_CONSUMER"
	default:
		return "SPAN_KIND_INTERNAL"
	}
}

// convertAttributes 转换属性
func (e *OTLPExporter) convertAttributes(span trace.SpanSnapshot, oSpan *otlpSpan) {
	if attrs := span.GetAttributes(); len(attrs) > 0 {
		for k, v := range attrs {
			oSpan.Attributes[k] = v
		}
	}
}

// convertEvents 转换事件
func (e *OTLPExporter) convertEvents(span trace.SpanSnapshot, oSpan *otlpSpan) {
	// 转换Span事件
	if events := span.GetEvents(); len(events) > 0 {
		for _, event := range events {
			// 解析时间戳
			timestamp, err := time.Parse(time.RFC3339Nano, event.Timestamp)
			if err != nil {
				timestamp = time.Now()
			}

			otlpEvent := otlpEvent{
				Name:         event.Name,
				TimeUnixNano: timestamp.UnixNano(),
				Attributes:   make(map[string]any),
			}

			// 添加事件属性
			if len(event.Attributes) > 0 {
				for k, v := range event.Attributes {
					otlpEvent.Attributes[k] = v
				}
			}

			oSpan.Events = append(oSpan.Events, otlpEvent)
		}
	}

	// 转换Span日志为事件
	if logs := span.GetLogs(); len(logs) > 0 {
		for _, log := range logs {
			// 解析时间戳
			timestamp, err := time.Parse(time.RFC3339Nano, log.Timestamp)
			if err != nil {
				timestamp = time.Now()
			}

			otlpEvent := otlpEvent{
				Name:         "log",
				TimeUnixNano: timestamp.UnixNano(),
				Attributes:   make(map[string]any),
			}

			// 添加日志属性
			otlpEvent.Attributes["message"] = log.Message
			otlpEvent.Attributes["severity"] = string(log.Severity)
			if len(log.Attributes) > 0 {
				for k, v := range log.Attributes {
					otlpEvent.Attributes[k] = v
				}
			}

			oSpan.Events = append(oSpan.Events, otlpEvent)
		}
	}
}

// convertLinks 转换关联Span
func (e *OTLPExporter) convertLinks(span trace.SpanSnapshot, oSpan *otlpSpan) {
	if linkedSpans := span.GetLinkedSpans(); len(linkedSpans) > 0 {
		for _, linkedSpan := range linkedSpans {
			oSpan.Links = append(oSpan.Links, otlpLink{
				TraceID:    linkedSpan.TraceID,
				SpanID:     linkedSpan.SpanID,
				Attributes: make(map[string]any),
			})
		}
	}
}

// convertStatus 转换状态
func (e *OTLPExporter) convertStatus(span trace.SpanSnapshot, oSpan *otlpSpan) {
	status := span.GetStatus()
	if status.Code != "" || status.Description != "" {
		otlpStatus := &otlpStatus{
			Code:    e.convertStatusCode(string(status.Code)),
			Message: status.Description,
		}

		// 如果有错误，设置为ERROR状态
		if errDetail := span.GetErrorDetail(); errDetail != nil {
			otlpStatus.Code = "STATUS_CODE_ERROR"
			if errDetail.Message != "" {
				otlpStatus.Message = errDetail.Message
			}
		}

		oSpan.Status = otlpStatus
	}
}

// convertStatusCode 转换状态码
func (e *OTLPExporter) convertStatusCode(code string) string {
	switch code {
	case "ok", "OK", "200":
		return "STATUS_CODE_OK"
	case "error", "ERROR", "50000":
		return "STATUS_CODE_ERROR"
	default:
		return "STATUS_CODE_UNSET"
	}
}

// flush 批量发送到OTLP Collector
func (e *OTLPExporter) flush(spans []*otlpSpan) error {
	if len(spans) == 0 {
		return nil
	}

	// 获取服务名称（从第一个span的资源信息中获取）
	serviceName := "unknown"
	for _, span := range spans {
		if span != nil && span.ServiceName != "" {
			serviceName = span.ServiceName
			break
		}
	}
	defer func() {
		for _, span := range spans {
			e.returnSpanToPool(span)
		}
	}()

	// 转换spans为值类型
	spanValues := make([]otlpSpan, len(spans))
	for i, s := range spans {
		spanValues[i] = *s
	}

	// 构建OTLP批量数据
	request := otlpExportRequest{
		ResourceSpans: []otlpResourceSpans{
			{
				Resource: &otlpResource{
					Attributes: map[string]any{
						"service.name": serviceName,
					},
				},
				ScopeSpans: []otlpScopeSpans{
					{
						Spans: spanValues,
					},
				},
			},
		},
	}

	// 序列化为JSON
	jsonData, err := json.Marshal(request)
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		return err
	}

	requestData := jsonData
	if e.enableGzip {
		var compressed bytes.Buffer
		gzipWriter := gzip.NewWriter(&compressed)
		if _, err := gzipWriter.Write(jsonData); err != nil {
			_ = gzipWriter.Close()
			atomic.AddInt64(&e.exportErrors, int64(len(spans)))
			return err
		}
		if err := gzipWriter.Close(); err != nil {
			atomic.AddInt64(&e.exportErrors, int64(len(spans)))
			return err
		}
		requestData = compressed.Bytes()
	}

	// 发送HTTP请求（带超时控制）
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", e.endpoint, bytes.NewReader(requestData))
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if e.enableGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		return fmt.Errorf("otlp export failed with status %d", resp.StatusCode)
	}

	return nil
}

// returnSpanToPool 归还span到对象池
func (e *OTLPExporter) returnSpanToPool(span *otlpSpan) {
	if span == nil {
		return
	}
	// 清理数据
	if span.Attributes != nil {
		clear(span.Attributes)
	}
	if span.Events != nil {
		span.Events = span.Events[:0]
	}
	if span.Links != nil {
		span.Links = span.Links[:0]
	}
	span.Status = nil
	span.ServiceName = ""
	// 归还到对象池
	e.spanPool.Put(span)
}

// Shutdown 关闭导出器并清理资源
func (e *OTLPExporter) Shutdown(ctx context.Context) error {
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
func (e *OTLPExporter) GetStats() map[string]int64 {
	return map[string]int64{
		"processed": atomic.LoadInt64(&e.processedCount),
		"dropped":   atomic.LoadInt64(&e.droppedCount),
		"errors":    atomic.LoadInt64(&e.exportErrors),
		"queue_len": 0,
		"queue_cap": 0,
	}
}
