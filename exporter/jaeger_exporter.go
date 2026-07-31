package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/ClownSketch/tracer/trace"
)

// JaegerExporter Jaeger 导出器。
// BatchProcessor 负责异步调度；导出器收到 batch 后直接同步发送到 Jaeger。
type JaegerExporter struct {
	// Jaeger配置
	endpoint      string        // Jaeger Agent/Collector端点，如 "http://localhost:14268/api/traces"
	timeout       time.Duration // HTTP请求超时时间
	batchSize     int           // 批量大小
	flushInterval time.Duration // 刷新间隔
	headers       map[string]string

	// 统计信息
	processedCount int64 // 处理数量
	droppedCount   int64 // 丢弃数量
	exportErrors   int64 // 导出错误数量

	// HTTP客户端
	httpClient *http.Client
}

// JaegerExporterOption Jaeger导出器选项
type JaegerExporterOption func(*JaegerExporter)

// WithJaegerEndpoint 设置Jaeger端点
func WithJaegerEndpoint(endpoint string) JaegerExporterOption {
	return func(e *JaegerExporter) {
		e.endpoint = endpoint
	}
}

// WithJaegerTimeout 设置HTTP请求超时时间
func WithJaegerTimeout(timeout time.Duration) JaegerExporterOption {
	return func(e *JaegerExporter) {
		e.timeout = timeout
	}
}

// WithJaegerHeaders 设置发送到 Jaeger Collector 的固定请求头。
func WithJaegerHeaders(headers map[string]string) JaegerExporterOption {
	return func(e *JaegerExporter) {
		if len(headers) == 0 {
			return
		}
		e.headers = make(map[string]string, len(headers))
		for key, value := range headers {
			e.headers[key] = value
		}
	}
}

// WithJaegerBatchSize 设置批量大小
func WithJaegerBatchSize(size int) JaegerExporterOption {
	return func(e *JaegerExporter) {
		if size > 0 {
			e.batchSize = size
		}
	}
}

// WithJaegerFlushInterval 设置刷新间隔
func WithJaegerFlushInterval(interval time.Duration) JaegerExporterOption {
	return func(e *JaegerExporter) {
		if interval > 0 {
			e.flushInterval = interval
		}
	}
}

// WithJaegerQueueSize 保留配置入口。
// 现在异步队列只存在于 BatchProcessor，这里不再维护 exporter 内部队列。
func WithJaegerQueueSize(size int) JaegerExporterOption {
	return func(e *JaegerExporter) {}
}

// jaegerSpan Jaeger Span格式
type jaegerSpan struct {
	TraceID       string            `json:"traceID"`       // 追踪ID（16进制字符串）
	SpanID        string            `json:"spanID"`        // Span ID（16进制字符串）
	ParentSpanID  string            `json:"parentSpanID"`  // 父Span ID（16进制字符串）
	OperationName string            `json:"operationName"` // 操作名称
	StartTime     int64             `json:"startTime"`     // 开始时间（微秒）
	Duration      int64             `json:"duration"`      // 持续时间（微秒）
	Tags          []jaegerTag       `json:"tags"`          // 标签
	Logs          []jaegerLog       `json:"logs"`          // 日志
	References    []jaegerReference `json:"references"`    // 引用（关联Span）
	ServiceName   string            `json:"-"`             // 服务名称（不序列化，用于批量处理）
}

// jaegerTag Jaeger标签
type jaegerTag struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
	Type  string `json:"type"` // string, bool, int64, float64
}

// jaegerLog Jaeger日志
type jaegerLog struct {
	Timestamp int64       `json:"timestamp"` // 时间戳（微秒）
	Fields    []jaegerTag `json:"fields"`    // 字段
}

// jaegerReference Jaeger引用（关联Span）
type jaegerReference struct {
	TraceID string `json:"traceID"` // 追踪ID
	SpanID  string `json:"spanID"`  // Span ID
	RefType string `json:"refType"` // CHILD_OF, FOLLOWS_FROM
}

// jaegerBatch Jaeger批量数据格式
type jaegerBatch struct {
	Process jaegerProcess `json:"process"` // 进程信息
	Spans   []jaegerSpan  `json:"spans"`   // Span列表
}

// jaegerProcess Jaeger进程信息
type jaegerProcess struct {
	ServiceName string      `json:"serviceName"` // 服务名称
	Tags        []jaegerTag `json:"tags"`        // 标签
}

// NewJaegerExporter 创建Jaeger导出器
func NewJaegerExporter(endpoint string, opts ...JaegerExporterOption) (*JaegerExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("Jaeger端点不能为空")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100

	e := &JaegerExporter{
		endpoint:      endpoint,
		timeout:       10 * time.Second,
		batchSize:     50,
		flushInterval: 2 * time.Second,
		httpClient: &http.Client{
			Timeout:   10 * time.Second,
			Transport: transport,
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
func (e *JaegerExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

// ExportSpans 同步批量导出多个 span
func (e *JaegerExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	atomic.AddInt64(&e.processedCount, int64(len(spans)))

	batch := make([]*jaegerSpan, 0, len(spans))
	for _, span := range spans {
		if span == nil {
			continue
		}
		batch = append(batch, e.convertToJaegerSpan(span))
	}
	return e.flush(batch)
}

// convertToJaegerSpan 将SpanSnapshot转换为Jaeger格式
func (e *JaegerExporter) convertToJaegerSpan(span trace.SpanSnapshot) *jaegerSpan {
	// 转换TraceID和SpanID（从16进制字符串转换为Jaeger格式）
	traceID := span.GetSpanTraceID()
	spanID := span.GetSpanID()
	parentSpanID := span.GetSpanParentSpanID()

	// 计算时间（微秒）
	startTime := span.GetStartTime().UnixMicro()
	duration := span.GetEndTime().Sub(span.GetStartTime()).Microseconds()

	// 转换标签
	tags := e.convertTags(span)

	// 转换日志
	logs := e.convertLogs(span)

	// 转换引用（关联Span）
	references := e.convertReferences(span)

	return &jaegerSpan{
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parentSpanID,
		OperationName: span.GetSpanName(),
		StartTime:     startTime,
		Duration:      duration,
		Tags:          tags,
		Logs:          logs,
		References:    references,
		ServiceName:   e.getServiceName(span),
	}
}

// convertTags 转换标签
func (e *JaegerExporter) convertTags(span trace.SpanSnapshot) []jaegerTag {
	tags := make([]jaegerTag, 0)

	// 添加Span类型
	tags = append(tags, jaegerTag{
		Key:   "span.kind",
		Value: string(span.GetSpanKind()),
		Type:  "string",
	})

	// 添加状态
	status := span.GetStatus()
	if status.Code != "" {
		tags = append(tags, jaegerTag{
			Key:   "status.code",
			Value: status.Code,
			Type:  "string",
		})
	}
	if status.Description != "" {
		tags = append(tags, jaegerTag{
			Key:   "status.description",
			Value: status.Description,
			Type:  "string",
		})
	}

	// 添加属性
	if attrs := span.GetAttributes(); len(attrs) > 0 {
		for k, v := range attrs {
			tag := jaegerTag{
				Key:  k,
				Type: "string",
			}
			// 根据值类型设置Type
			switch val := v.(type) {
			case string:
				tag.Value = val
				tag.Type = "string"
			case bool:
				tag.Value = val
				tag.Type = "bool"
			case int, int8, int16, int32, int64:
				tag.Value = val
				tag.Type = "int64"
			case uint, uint8, uint16, uint32, uint64:
				tag.Value = val
				tag.Type = "int64"
			case float32, float64:
				tag.Value = val
				tag.Type = "float64"
			default:
				tag.Value = fmt.Sprintf("%v", val)
				tag.Type = "string"
			}
			tags = append(tags, tag)
		}
	}

	// 添加错误信息
	if errDetail := span.GetErrorDetail(); errDetail != nil {
		tags = append(tags, jaegerTag{
			Key:   "error",
			Value: true,
			Type:  "bool",
		})
		if errDetail.Message != "" {
			tags = append(tags, jaegerTag{
				Key:   "error.message",
				Value: errDetail.Message,
				Type:  "string",
			})
		}
		if errDetail.Code != "" {
			tags = append(tags, jaegerTag{
				Key:   "error.code",
				Value: errDetail.Code,
				Type:  "string",
			})
		}
	}

	// 添加资源信息
	if resource := span.GetResource(); resource != nil {
		if resource.ServiceName != "" {
			tags = append(tags, jaegerTag{
				Key:   "service.name",
				Value: resource.ServiceName,
				Type:  "string",
			})
		}
		if resource.Host != "" {
			tags = append(tags, jaegerTag{
				Key:   "host.name",
				Value: resource.Host,
				Type:  "string",
			})
		}
	}

	return tags
}

// getServiceName 从span中获取服务名称
func (e *JaegerExporter) getServiceName(span trace.SpanSnapshot) string {
	if resource := span.GetResource(); resource != nil && resource.ServiceName != "" {
		return resource.ServiceName
	}
	return "unknown"
}

// convertLogs 转换日志
func (e *JaegerExporter) convertLogs(span trace.SpanSnapshot) []jaegerLog {
	logs := make([]jaegerLog, 0)

	// 转换Span日志
	if spanLogs := span.GetLogs(); len(spanLogs) > 0 {
		for _, log := range spanLogs {
			// 解析时间戳
			timestamp, err := time.Parse(time.RFC3339Nano, log.Timestamp)
			if err != nil {
				// 如果解析失败，使用当前时间
				timestamp = time.Now()
			}

			fields := []jaegerTag{
				{
					Key:   "message",
					Value: log.Message,
					Type:  "string",
				},
				{
					Key:   "severity",
					Value: string(log.Severity),
					Type:  "string",
				},
			}

			// 添加日志属性
			if len(log.Attributes) > 0 {
				for k, v := range log.Attributes {
					fields = append(fields, jaegerTag{
						Key:   k,
						Value: fmt.Sprintf("%v", v),
						Type:  "string",
					})
				}
			}

			logs = append(logs, jaegerLog{
				Timestamp: timestamp.UnixMicro(),
				Fields:    fields,
			})
		}
	}

	// 转换Span事件
	if events := span.GetEvents(); len(events) > 0 {
		for _, event := range events {
			// 解析时间戳
			timestamp, err := time.Parse(time.RFC3339Nano, event.Timestamp)
			if err != nil {
				// 如果解析失败，使用当前时间
				timestamp = time.Now()
			}

			fields := []jaegerTag{
				{
					Key:   "event.name",
					Value: event.Name,
					Type:  "string",
				},
			}

			// 添加事件属性
			if len(event.Attributes) > 0 {
				for k, v := range event.Attributes {
					fields = append(fields, jaegerTag{
						Key:   k,
						Value: fmt.Sprintf("%v", v),
						Type:  "string",
					})
				}
			}

			logs = append(logs, jaegerLog{
				Timestamp: timestamp.UnixMicro(),
				Fields:    fields,
			})
		}
	}

	return logs
}

// convertReferences 转换引用（关联Span）
func (e *JaegerExporter) convertReferences(span trace.SpanSnapshot) []jaegerReference {
	references := make([]jaegerReference, 0)

	// 转换关联Span
	if linkedSpans := span.GetLinkedSpans(); len(linkedSpans) > 0 {
		for _, linkedSpan := range linkedSpans {
			references = append(references, jaegerReference{
				TraceID: linkedSpan.TraceID,
				SpanID:  linkedSpan.SpanID,
				RefType: "FOLLOWS_FROM", // 默认使用FOLLOWS_FROM
			})
		}
	}

	// 如果有父Span，添加CHILD_OF引用
	if parentSpanID := span.GetSpanParentSpanID(); parentSpanID != "" {
		references = append(references, jaegerReference{
			TraceID: span.GetSpanTraceID(),
			SpanID:  parentSpanID,
			RefType: "CHILD_OF",
		})
	}

	return references
}

// flush 批量发送到Jaeger
func (e *JaegerExporter) flush(spans []*jaegerSpan) error {
	if len(spans) == 0 {
		return nil
	}

	// 获取服务名称（从第一个span中获取）
	serviceName := "unknown"
	if len(spans) > 0 && spans[0].ServiceName != "" {
		serviceName = spans[0].ServiceName
	}

	// 转换spans为值类型
	spanValues := make([]jaegerSpan, len(spans))
	for i, s := range spans {
		spanValues[i] = *s
	}

	// 构建Jaeger批量数据
	batch := jaegerBatch{
		Process: jaegerProcess{
			ServiceName: serviceName,
			Tags:        []jaegerTag{},
		},
		Spans: spanValues,
	}

	// 序列化为JSON
	jsonData, err := json.Marshal(batch)
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		return err
	}

	// 发送HTTP请求
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", e.endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(spans)))
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	for key, value := range e.headers {
		req.Header.Set(key, value)
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
		return fmt.Errorf("jaeger export failed with status %d", resp.StatusCode)
	}
	return nil
}

// Shutdown 关闭导出器并清理资源
func (e *JaegerExporter) Shutdown(ctx context.Context) error {
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
func (e *JaegerExporter) GetStats() map[string]int64 {
	return map[string]int64{
		"processed": atomic.LoadInt64(&e.processedCount),
		"dropped":   atomic.LoadInt64(&e.droppedCount),
		"errors":    atomic.LoadInt64(&e.exportErrors),
		"queue_len": 0,
		"queue_cap": 0,
	}
}
