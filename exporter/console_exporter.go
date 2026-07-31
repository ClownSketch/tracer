package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// ConsoleSpanExporter 是一个控制台Span导出器实现
// 将span数据输出到控制台，用于调试和开发
type ConsoleSpanExporter struct {
	writer      io.Writer // 输出目标，默认为os.Stdout
	prettyPrint bool      // 是否美化输出
	useJSON     bool      // 是否使用JSON格式
}

// ConsoleExporterOption 控制台导出器选项
type ConsoleExporterOption func(*ConsoleSpanExporter)

// WithWriter 设置输出目标
func WithWriter(w io.Writer) ConsoleExporterOption {
	return func(e *ConsoleSpanExporter) {
		e.writer = w
	}
}

// WithPrettyPrint 设置是否美化输出
func WithPrettyPrint(pretty bool) ConsoleExporterOption {
	return func(e *ConsoleSpanExporter) {
		e.prettyPrint = pretty
	}
}

// WithJSON 设置是否使用JSON格式输出
func WithJSON(useJSON bool) ConsoleExporterOption {
	return func(e *ConsoleSpanExporter) {
		e.useJSON = useJSON
	}
}

// NewConsoleSpanExporter 创建一个新的ConsoleSpanExporter
func NewConsoleSpanExporter(opts ...ConsoleExporterOption) *ConsoleSpanExporter {
	e := &ConsoleSpanExporter{
		writer:      os.Stdout,
		prettyPrint: true,
		useJSON:     false,
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// ExportSpan 同步导出单个 span
func (e *ConsoleSpanExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

// ExportSpans 同步导出多个 span。
// processor 负责 fallback 和快照释放，这里只负责直接输出。
func (e *ConsoleSpanExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	for _, span := range spans {
		if span == nil {
			continue
		}

		var output string
		var err error

		if e.useJSON {
			// 构建数据并序列化
			spanData := e.buildSpanData(span)

			var data []byte
			if e.prettyPrint {
				data, err = json.MarshalIndent(spanData, "", "  ")
			} else {
				data, err = json.Marshal(spanData)
			}

			if err != nil {
				return err
			}

			output = string(data) + "\n"
		} else {
			output = e.buildTextOutput(span)
		}

		_, err = fmt.Fprint(e.writer, output)
		if err != nil {
			return err
		}
	}
	return nil
}

// buildTextOutput 构建文本格式输出（用于在释放快照前构建）
func (e *ConsoleSpanExporter) buildTextOutput(span trace.SpanSnapshot) string {
	return e.exportText(span)
}

// exportText 以文本格式导出span（更易读，返回字符串）
func (e *ConsoleSpanExporter) exportText(span trace.SpanSnapshot) string {
	// 使用 strings.Builder 优化性能（避免多次内存分配和 fmt.Sprintf 开销）
	var builder strings.Builder
	builder.Grow(1024) // 预分配容量

	// 基本信息
	builder.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	builder.WriteString("Span: ")
	builder.WriteString(span.GetSpanName())
	builder.WriteString("\n")
	builder.WriteString("TraceID: ")
	builder.WriteString(span.GetSpanTraceID())
	builder.WriteString("\n")
	builder.WriteString("SpanID: ")
	builder.WriteString(span.GetSpanID())
	builder.WriteString("\n")
	builder.WriteString("ParentSpanID: ")
	builder.WriteString(span.GetSpanParentSpanID())
	builder.WriteString("\n")
	builder.WriteString("Kind: ")
	builder.WriteString(span.GetSpanKind().String())
	builder.WriteString("\n")
	builder.WriteString("Duration: ")
	builder.WriteString(span.GetEndTime().Sub(span.GetStartTime()).String())
	builder.WriteString("\n")

	// 时间信息
	builder.WriteString("StartTime: ")
	builder.WriteString(span.GetStartTime().Format(time.RFC3339Nano))
	builder.WriteString("\n")
	builder.WriteString("EndTime: ")
	builder.WriteString(span.GetEndTime().Format(time.RFC3339Nano))
	builder.WriteString("\n")

	// 属性
	if attrs := span.GetAttributes(); len(attrs) > 0 {
		builder.WriteString("\nAttributes:\n")
		for k, v := range attrs {
			builder.WriteString("  ")
			builder.WriteString(k)
			builder.WriteString(": ")
			// 对于复杂类型，使用 fmt.Sprintf 作为最后手段（但这种情况应该很少）
			builder.WriteString(fmt.Sprintf("%v", v))
			builder.WriteString("\n")
		}
	}

	// 事件
	if events := span.GetEvents(); len(events) > 0 {
		builder.WriteString("\nEvents (")
		builder.WriteString(strconv.Itoa(len(events)))
		builder.WriteString("):\n")
		for i, event := range events {
			builder.WriteString("  [")
			builder.WriteString(strconv.Itoa(i + 1))
			builder.WriteString("] ")
			builder.WriteString(event.Name)
			builder.WriteString(" @ ")
			builder.WriteString(event.Timestamp)
			builder.WriteString("\n")
			if len(event.Attributes) > 0 {
				for k, v := range event.Attributes {
					builder.WriteString("      ")
					builder.WriteString(k)
					builder.WriteString(": ")
					builder.WriteString(fmt.Sprintf("%v", v))
					builder.WriteString("\n")
				}
			}
		}
	}

	// 日志
	if logs := span.GetLogs(); len(logs) > 0 {
		builder.WriteString("\nLogs (")
		builder.WriteString(strconv.Itoa(len(logs)))
		builder.WriteString("):\n")
		for i, log := range logs {
			builder.WriteString("  [")
			builder.WriteString(strconv.Itoa(i + 1))
			builder.WriteString("] [")
			builder.WriteString(log.Severity.String())
			builder.WriteString("] ")
			builder.WriteString(log.Message)
			builder.WriteString(" @ ")
			builder.WriteString(log.Timestamp)
			builder.WriteString("\n")
			if len(log.Attributes) > 0 {
				for k, v := range log.Attributes {
					builder.WriteString("      ")
					builder.WriteString(k)
					builder.WriteString(": ")
					builder.WriteString(fmt.Sprintf("%v", v))
					builder.WriteString("\n")
				}
			}
			if log.Fields != nil {
				fieldsJSON, _ := json.MarshalIndent(log.Fields, "      ", "  ")
				builder.WriteString("      Fields: ")
				builder.WriteString(string(fieldsJSON))
				builder.WriteString("\n")
			}
		}
	}

	// 错误详情
	if errDetail := span.GetErrorDetail(); errDetail != nil {
		builder.WriteString("\nError:\n")
		if errDetail.Code != "" {
			builder.WriteString("  Code: ")
			builder.WriteString(errDetail.Code)
			builder.WriteString("\n")
		}
		builder.WriteString("  Message: ")
		builder.WriteString(errDetail.Message)
		builder.WriteString("\n")
		if errDetail.BusinessCode != "" {
			builder.WriteString("  BusinessCode: ")
			builder.WriteString(errDetail.BusinessCode)
			builder.WriteString("\n")
		}
		if len(errDetail.BusinessMessage) > 0 {
			builder.WriteString("  BusinessMessage: ")
			builder.WriteString(fmt.Sprintf("%v", errDetail.BusinessMessage))
			builder.WriteString("\n")
		}
		if errDetail.HttpCode > 0 {
			builder.WriteString("  HttpCode: ")
			builder.WriteString(strconv.Itoa(errDetail.HttpCode))
			builder.WriteString("\n")
		}
		if errDetail.Timestamp != "" {
			builder.WriteString("  Timestamp: ")
			builder.WriteString(errDetail.Timestamp)
			builder.WriteString("\n")
		}
		if len(errDetail.MetaData) > 0 {
			builder.WriteString("  MetaData:\n")
			for k, v := range errDetail.MetaData {
				builder.WriteString("    ")
				builder.WriteString(k)
				builder.WriteString(": ")
				builder.WriteString(fmt.Sprintf("%v", v))
				builder.WriteString("\n")
			}
		}
		if len(errDetail.StackTrace) > 0 {
			builder.WriteString("  StackTrace:\n")
			for i, frame := range errDetail.StackTrace {
				builder.WriteString("    [")
				builder.WriteString(strconv.Itoa(i + 1))
				builder.WriteString("] ")
				builder.WriteString(frame.File)
				builder.WriteString(":")
				builder.WriteString(strconv.Itoa(frame.LineNumber))
				builder.WriteString(" in ")
				builder.WriteString(frame.FunctionName)
				builder.WriteString("\n")
			}
		}
	}

	// 资源信息
	if resource := span.GetResource(); resource != nil {
		builder.WriteString("\nResource:\n")
		if resource.ServiceName != "" {
			builder.WriteString("  Service: ")
			builder.WriteString(resource.ServiceName)
			builder.WriteString("\n")
		}
		if resource.Host != "" {
			builder.WriteString("  Host: ")
			builder.WriteString(resource.Host)
			builder.WriteString("\n")
		}
		if len(resource.Attributes) > 0 {
			builder.WriteString("  Attributes:\n")
			for k, v := range resource.Attributes {
				builder.WriteString("    ")
				builder.WriteString(k)
				builder.WriteString(": ")
				builder.WriteString(fmt.Sprintf("%v", v))
				builder.WriteString("\n")
			}
		}
	}

	// 资源使用情况
	if usage := span.GetResourceUsage(); usage != nil {
		builder.WriteString("\nResource Usage:\n")
		if usage.CPUUsage > 0 {
			builder.WriteString("  CPU: ")
			builder.WriteString(strconv.FormatFloat(usage.CPUUsage, 'f', 2, 64))
			builder.WriteString("%\n")
		}
		if usage.MemoryUsage > 0 {
			builder.WriteString("  Memory: ")
			builder.WriteString(strconv.FormatFloat(usage.MemoryUsage, 'f', 2, 64))
			builder.WriteString(" MB\n")
		}
		if usage.DiskUsage > 0 {
			builder.WriteString("  Disk: ")
			builder.WriteString(strconv.FormatFloat(usage.DiskUsage, 'f', 2, 64))
			builder.WriteString(" MB\n")
		}
		if usage.NetworkIO > 0 {
			builder.WriteString("  NetworkIO: ")
			builder.WriteString(strconv.FormatFloat(usage.NetworkIO, 'f', 2, 64))
			builder.WriteString(" MB\n")
		}
	}

	builder.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

	return builder.String()
}

// buildSpanData 构建span数据用于JSON输出
func (e *ConsoleSpanExporter) buildSpanData(span trace.SpanSnapshot) map[string]any {
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

	// 属性
	if attrs := span.GetAttributes(); len(attrs) > 0 {
		data["attributes"] = attrs
	}

	// 事件
	if events := span.GetEvents(); len(events) > 0 {
		eventData := make([]map[string]any, len(events))
		for i, event := range events {
			ev := map[string]any{
				"name":      event.Name,
				"timestamp": event.Timestamp,
			}
			if len(event.Attributes) > 0 {
				ev["attributes"] = event.Attributes
			}
			eventData[i] = ev
		}
		data["events"] = eventData
	}

	// 日志
	if logs := span.GetLogs(); len(logs) > 0 {
		logData := make([]map[string]any, len(logs))
		for i, log := range logs {
			lg := map[string]any{
				"timestamp": log.Timestamp,
				"message":   log.Message,
				"severity":  log.Severity,
			}
			if len(log.Attributes) > 0 {
				lg["attributes"] = log.Attributes
			}
			if log.Fields != nil {
				lg["fields"] = log.Fields
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
			errorData["metaData"] = errDetail.MetaData
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

	// 资源信息
	if resource := span.GetResource(); resource != nil {
		resourceData := make(map[string]any)
		if resource.ServiceName != "" {
			resourceData["serviceName"] = resource.ServiceName
		}
		if resource.Host != "" {
			resourceData["host"] = resource.Host
		}
		if len(resource.Attributes) > 0 {
			resourceData["attributes"] = resource.Attributes
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

	return data
}

// Shutdown 关闭导出器并清理资源
func (e *ConsoleSpanExporter) Shutdown(ctx context.Context) error {
	// 控制台导出器不需要特殊清理
	return nil
}
