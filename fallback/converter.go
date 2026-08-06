package fallback

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/ClownSketch/tracer/mock"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

var (
	// SpanSnapshotMock 对象池（性能优化）
	snapshotMockPool = sync.Pool{
		New: func() any {
			return &mock.SpanSnapshotMock{
				Attributes:  make(map[string]any, 8),
				Events:      make([]types.SpanEvent, 0, 5),
				Logs:        make([]types.SpanLog, 0, 2),
				LinkedSpans: make([]types.SpanContext, 0, 1),
			}
		},
	}

	// 属性 map 对象池
	attrMapPool = sync.Pool{
		New: func() any {
			return make(map[string]any, 8)
		},
	}

	// 事件 slice 对象池
	eventSlicePool = sync.Pool{
		New: func() any {
			events := make([]types.SpanEvent, 0, 5)
			return &events
		},
	}

	// 日志 slice 对象池
	logSlicePool = sync.Pool{
		New: func() any {
			logs := make([]types.SpanLog, 0, 2)
			return &logs
		},
	}

	// 关联 Span slice 对象池
	linkSlicePool = sync.Pool{
		New: func() any {
			links := make([]types.SpanContext, 0, 1)
			return &links
		},
	}
)

// ConvertSpanSnapshotToData 将 SpanSnapshot 转换为统一的 SpanData 格式
// 所有导出器都使用这个函数将 span 快照转换为统一的 fallback 格式
func ConvertSpanSnapshotToData(span trace.SpanSnapshot) *SpanData {
	data := &SpanData{
		MongoCollection: span.GetMongoCollection(),
		Name:            span.GetSpanName(),
		TraceID:         span.GetSpanTraceID(),
		SpanID:          span.GetSpanID(),
		ParentSpanID:    span.GetSpanParentSpanID(),
		Kind:            string(span.GetSpanKind()),
		StartTime:       span.GetStartTime().Format(time.RFC3339Nano),
		EndTime:         span.GetEndTime().Format(time.RFC3339Nano),
		Duration:        span.GetEndTime().Sub(span.GetStartTime()).String(),
	}

	// 状态
	status := span.GetStatus()
	if status.Code != "" || status.Description != "" {
		data.Status = &Status{
			Code:        string(status.Code),
			Description: status.Description,
		}
	}

	// 属性
	if attrs := span.GetAttributes(); len(attrs) > 0 {
		// 深拷贝属性，避免并发修改
		data.Attributes = make(map[string]any, len(attrs))
		for k, v := range attrs {
			data.Attributes[k] = v
		}
	}

	// 事件
	if events := span.GetEvents(); len(events) > 0 {
		data.Events = make([]Event, 0, len(events))
		for _, event := range events {
			ev := Event{
				Name:      event.Name,
				Timestamp: event.Timestamp,
			}
			if len(event.Attributes) > 0 {
				// 深拷贝事件属性
				ev.Attributes = make(map[string]any, len(event.Attributes))
				for k, v := range event.Attributes {
					ev.Attributes[k] = v
				}
			}
			data.Events = append(data.Events, ev)
		}
	}

	// 日志
	if logs := span.GetLogs(); len(logs) > 0 {
		data.Logs = make([]Log, 0, len(logs))
		for _, log := range logs {
			lg := Log{
				Timestamp: log.Timestamp,
				Message:   log.Message,
				Severity:  string(log.Severity),
			}
			if len(log.Attributes) > 0 {
				// 深拷贝日志属性
				lg.Attributes = make(map[string]any, len(log.Attributes))
				for k, v := range log.Attributes {
					lg.Attributes[k] = v
				}
			}
			if log.Fields != nil {
				if fieldsMap, ok := log.Fields.(map[string]any); ok {
					fields := make(map[string]any, len(fieldsMap))
					for key, value := range fieldsMap {
						fields[key] = value
					}
					lg.Fields = fields
				} else {
					lg.Fields = log.Fields
				}
			}
			if log.EventType != "" {
				lg.EventType = log.EventType
			}
			data.Logs = append(data.Logs, lg)
		}
	}

	// 错误详情
	if errDetail := span.GetErrorDetail(); errDetail != nil {
		errorData := &Error{
			Message: errDetail.Message,
		}
		if errDetail.Code != "" {
			errorData.Code = errDetail.Code
		}
		if errDetail.BusinessCode != "" {
			errorData.BusinessCode = errDetail.BusinessCode
		}
		if len(errDetail.BusinessMessage) > 0 {
			errorData.BusinessMessage = errDetail.BusinessMessage
		}
		if errDetail.HttpCode > 0 {
			errorData.HttpCode = errDetail.HttpCode
		}
		if errDetail.Timestamp != "" {
			errorData.Timestamp = errDetail.Timestamp
		}
		if len(errDetail.MetaData) > 0 {
			// 深拷贝元数据
			errorData.MetaData = make(map[string]any, len(errDetail.MetaData))
			for k, v := range errDetail.MetaData {
				errorData.MetaData[k] = v
			}
		}
		if len(errDetail.StackTrace) > 0 {
			errorData.StackTrace = make([]StackFrame, 0, len(errDetail.StackTrace))
			for _, frame := range errDetail.StackTrace {
				errorData.StackTrace = append(errorData.StackTrace, StackFrame{
					File:         frame.File,
					FileName:     frame.FileName,
					FunctionName: frame.FunctionName,
					LineNumber:   frame.LineNumber,
				})
			}
		}
		data.Error = errorData
	}

	// 资源信息
	if resource := span.GetResource(); resource != nil {
		resourceData := &Resource{}
		if resource.ServiceName != "" {
			resourceData.ServiceName = resource.ServiceName
		}
		if resource.Host != "" {
			resourceData.Host = resource.Host
		}
		if len(resource.Attributes) > 0 {
			// 深拷贝资源属性
			resourceData.Attributes = make(map[string]any, len(resource.Attributes))
			for k, v := range resource.Attributes {
				resourceData.Attributes[k] = v
			}
		}
		if resourceData.ServiceName != "" || resourceData.Host != "" || len(resourceData.Attributes) > 0 {
			data.Resource = resourceData
		}
	}

	// 资源使用情况
	if usage := span.GetResourceUsage(); usage != nil {
		usageData := &ResourceUsage{}
		if usage.CPUUsage > 0 {
			usageData.CPUUsage = usage.CPUUsage
		}
		if usage.MemoryUsage > 0 {
			usageData.MemoryUsage = usage.MemoryUsage
		}
		if usage.DiskUsage > 0 {
			usageData.DiskUsage = usage.DiskUsage
		}
		if usage.NetworkIO > 0 {
			usageData.NetworkIO = usage.NetworkIO
		}
		if usageData.CPUUsage > 0 || usageData.MemoryUsage > 0 || usageData.DiskUsage > 0 || usageData.NetworkIO > 0 {
			data.ResourceUsage = usageData
		}
	}

	// 关联 Span
	if links := span.GetLinkedSpans(); len(links) > 0 {
		data.Links = make([]Link, 0, len(links))
		for _, link := range links {
			linkItem := Link{
				TraceID: link.TraceID,
				SpanID:  link.SpanID,
			}
			if link.ParentSpanID != "" {
				linkItem.ParentSpanID = link.ParentSpanID
			}
			data.Links = append(data.Links, linkItem)
		}
	}

	return data
}

// ConvertSpanSnapshotToJSON 将 SpanSnapshot 转换为统一的 SpanData JSON 格式
// 返回 JSON 字节数组，用于写入 fallback 存储
// 这里优先使用标准库，避免 unsafe 编码器在高并发下直接崩溃
func ConvertSpanSnapshotToJSON(span trace.SpanSnapshot) ([]byte, error) {
	data := ConvertSpanSnapshotToData(span)
	return json.Marshal(data)
}

// ConvertSpanSnapshotToWALJSON 将 SpanSnapshot 转换为 WAL 使用的 JSON。
// 与 fallback 恢复链路不同，这里默认 snapshot 已经被冻结，因此避免再次深拷贝，
// 以降低高 QPS 场景下的编码分配和 CPU 开销。
func ConvertSpanSnapshotToWALJSON(span trace.SpanSnapshot) ([]byte, error) {
	data := spanDataFromSnapshotNoClone(span)
	return json.Marshal(data)
}

// spanDataFromSnapshotNoClone 构建 WAL 数据，并复用已经冻结的快照字段。
// @param span trace.SpanSnapshot Span 快照
// @return result *SpanData WAL 序列化数据
func spanDataFromSnapshotNoClone(span trace.SpanSnapshot) *SpanData {
	data := &SpanData{
		MongoCollection: span.GetMongoCollection(),
		Name:            span.GetSpanName(),
		TraceID:         span.GetSpanTraceID(),
		SpanID:          span.GetSpanID(),
		ParentSpanID:    span.GetSpanParentSpanID(),
		Kind:            string(span.GetSpanKind()),
		StartTime:       span.GetStartTime().Format(time.RFC3339Nano),
		EndTime:         span.GetEndTime().Format(time.RFC3339Nano),
		Duration:        span.GetEndTime().Sub(span.GetStartTime()).String(),
		Attributes:      span.GetAttributes(),
	}

	status := span.GetStatus()
	if status.Code != "" || status.Description != "" {
		data.Status = &Status{
			Code:        string(status.Code),
			Description: status.Description,
		}
	}

	if events := span.GetEvents(); len(events) > 0 {
		data.Events = make([]Event, len(events))
		for i, event := range events {
			data.Events[i] = Event{
				Name:       event.Name,
				Timestamp:  event.Timestamp,
				Attributes: event.Attributes,
			}
		}
	}

	if logs := span.GetLogs(); len(logs) > 0 {
		data.Logs = make([]Log, len(logs))
		for i, log := range logs {
			data.Logs[i] = Log{
				Timestamp:  log.Timestamp,
				Message:    log.Message,
				Severity:   string(log.Severity),
				Attributes: log.Attributes,
				Fields:     log.Fields,
				EventType:  log.EventType,
			}
		}
	}

	if errDetail := span.GetErrorDetail(); errDetail != nil {
		errorData := &Error{
			Code:            errDetail.Code,
			Message:         errDetail.Message,
			BusinessCode:    errDetail.BusinessCode,
			BusinessMessage: errDetail.BusinessMessage,
			HttpCode:        errDetail.HttpCode,
			Timestamp:       errDetail.Timestamp,
			MetaData:        errDetail.MetaData,
		}
		if len(errDetail.StackTrace) > 0 {
			errorData.StackTrace = make([]StackFrame, len(errDetail.StackTrace))
			for i, frame := range errDetail.StackTrace {
				errorData.StackTrace[i] = StackFrame{
					File:         frame.File,
					FileName:     frame.FileName,
					FunctionName: frame.FunctionName,
					LineNumber:   frame.LineNumber,
				}
			}
		}
		data.Error = errorData
	}

	if resource := span.GetResource(); resource != nil {
		data.Resource = &Resource{
			ServiceName: resource.ServiceName,
			Host:        resource.Host,
			Attributes:  resource.Attributes,
		}
	}

	if usage := span.GetResourceUsage(); usage != nil {
		data.ResourceUsage = &ResourceUsage{
			CPUUsage:    usage.CPUUsage,
			MemoryUsage: usage.MemoryUsage,
			DiskUsage:   usage.DiskUsage,
			NetworkIO:   usage.NetworkIO,
		}
	}

	if linkedSpans := span.GetLinkedSpans(); len(linkedSpans) > 0 {
		data.Links = make([]Link, len(linkedSpans))
		for i, link := range linkedSpans {
			data.Links[i] = Link{
				TraceID:      link.TraceID,
				SpanID:       link.SpanID,
				ParentSpanID: link.ParentSpanID,
			}
		}
	}

	return data
}

// ConvertDataToSpanSnapshot 将统一的 SpanData 格式转换为 SpanSnapshot
// 用于从 fallback 文件恢复数据
// 使用 mock.SpanSnapshotMock 创建快照（避免循环导入）
// 性能优化：
// 1. 使用 time.ParseInLocation 替代 time.Parse，避免时区查找（约快 20-30%）
// 2. 使用对象池减少内存分配
func ConvertDataToSpanSnapshot(data *SpanData) (trace.SpanSnapshot, error) {
	// 解析时间（使用 time.ParseInLocation 避免时区查找，性能更好）
	// 注意：RFC3339Nano 格式通常包含时区信息，但 ParseInLocation 仍然更快
	startTime, err := time.ParseInLocation(time.RFC3339Nano, data.StartTime, time.UTC)
	if err != nil {
		// 如果 ParseInLocation 失败，回退到 Parse
		startTime, err = time.Parse(time.RFC3339Nano, data.StartTime)
		if err != nil {
			return nil, err
		}
	}
	endTime, err := time.ParseInLocation(time.RFC3339Nano, data.EndTime, time.UTC)
	if err != nil {
		// 如果 ParseInLocation 失败，回退到 Parse
		endTime, err = time.Parse(time.RFC3339Nano, data.EndTime)
		if err != nil {
			return nil, err
		}
	}

	// 解析 SpanKind
	var spanKind types.SpanKind
	switch data.Kind {
	case "internal":
		spanKind = types.SpanKindInternal
	case "client":
		spanKind = types.SpanKindClient
	case "server":
		spanKind = types.SpanKindServer
	case "producer":
		spanKind = types.SpanKindProducer
	case "consumer":
		spanKind = types.SpanKindConsumer
	case "corn":
		spanKind = types.SpanKindCron
	case "async":
		spanKind = types.SpanKindAsync
	default:
		spanKind = types.SpanKindInternal
	}

	// 创建 SpanContext
	spanContext := types.SpanContext{
		TraceID:      data.TraceID,
		SpanID:       data.SpanID,
		ParentSpanID: data.ParentSpanID,
	}

	// 从对象池获取 SpanSnapshotMock（性能优化）
	mock := snapshotMockPool.Get().(*mock.SpanSnapshotMock)
	// 重置对象状态
	mock.StartTime = startTime
	mock.EndTime = endTime
	mock.SpanContext = spanContext
	mock.SpanName = data.Name
	mock.SpanKind = spanKind
	mock.SpanTraceID = data.TraceID
	mock.SpanParentSpanID = data.ParentSpanID
	mock.MongoCollection = data.MongoCollection
	// 清空可能残留的数据
	if mock.Attributes != nil {
		clear(mock.Attributes)
	}
	if mock.Events != nil {
		mock.Events = mock.Events[:0]
	}
	if mock.Logs != nil {
		mock.Logs = mock.Logs[:0]
	}
	if mock.LinkedSpans != nil {
		mock.LinkedSpans = mock.LinkedSpans[:0]
	}
	// 设置 Release 函数，用于在释放时归还到对象池
	mock.ReleaseFunc = func() {
		releaseSnapshotMock(mock)
	}

	// 状态
	if data.Status != nil {
		var statusCode types.StatusCode
		switch data.Status.Code {
		case "0":
			statusCode = types.StatusCodeUnset
		case "200":
			statusCode = types.StatusCodeOk
		case "50000":
			statusCode = types.StatusCodeError
		case "50001":
			statusCode = types.StatusCodeWarning
		case "50002":
			statusCode = types.StatusCodeInfo
		case "50003":
			statusCode = types.StatusCodeDebug
		case "50004":
			statusCode = types.StatusCodeTrace
		case "50005":
			statusCode = types.StatusCodeMetric
		case "50006":
			statusCode = types.StatusCodeUnknown
		default:
			statusCode = types.StatusCodeUnset
		}
		mock.Status = types.SpanStatus{
			Code:        statusCode,
			Description: data.Status.Description,
		}
	}

	// 属性（使用对象池）
	if len(data.Attributes) > 0 {
		// 如果 mock.Attributes 为空，从池中获取
		if mock.Attributes == nil {
			mock.Attributes = attrMapPool.Get().(map[string]any)
			clear(mock.Attributes)
		}
		for k, v := range data.Attributes {
			mock.Attributes[k] = v
		}
	}

	// 事件（使用对象池）
	if len(data.Events) > 0 {
		// 如果 mock.Events 为空或容量不足，从池中获取
		if mock.Events == nil || cap(mock.Events) < len(data.Events) {
			if mock.Events != nil {
				events := mock.Events[:0]
				eventSlicePool.Put(&events)
			}
			mock.Events = *eventSlicePool.Get().(*[]types.SpanEvent)
			mock.Events = mock.Events[:0]
		}
		for _, ev := range data.Events {
			event := types.SpanEvent{
				Name:      ev.Name,
				Timestamp: ev.Timestamp,
			}
			if len(ev.Attributes) > 0 {
				// 事件属性也使用对象池
				eventAttrs := attrMapPool.Get().(map[string]any)
				clear(eventAttrs)
				for k, v := range ev.Attributes {
					eventAttrs[k] = v
				}
				event.Attributes = eventAttrs
			}
			mock.Events = append(mock.Events, event)
		}
	}

	// 日志（使用对象池）
	if len(data.Logs) > 0 {
		// 如果 mock.Logs 为空或容量不足，从池中获取
		if mock.Logs == nil || cap(mock.Logs) < len(data.Logs) {
			if mock.Logs != nil {
				logs := mock.Logs[:0]
				logSlicePool.Put(&logs)
			}
			mock.Logs = *logSlicePool.Get().(*[]types.SpanLog)
			mock.Logs = mock.Logs[:0]
		}
		for _, lg := range data.Logs {
			var severity types.SpanLogSeverity
			switch lg.Severity {
			case "debug":
				severity = types.SpanLogSeverityDebug
			case "info":
				severity = types.SpanLogSeverityInfo
			case "warn":
				severity = types.SpanLogSeverityWarn
			case "error":
				severity = types.SpanLogSeverityError
			case "fatal":
				severity = types.SpanLogSeverityFatal
			case "panic":
				severity = types.SpanLogSeverityPanic
			case "trace":
				severity = types.SpanLogSeverityTrace
			case "metric":
				severity = types.SpanLogSeverityMetric
			default:
				severity = types.SpanLogSeverityInfo
			}
			log := types.SpanLog{
				Timestamp: lg.Timestamp,
				Message:   lg.Message,
				Severity:  severity,
			}
			if len(lg.Attributes) > 0 {
				// 日志属性也使用对象池
				logAttrs := attrMapPool.Get().(map[string]any)
				clear(logAttrs)
				for k, v := range lg.Attributes {
					logAttrs[k] = v
				}
				log.Attributes = logAttrs
			}
			if lg.Fields != nil {
				log.Fields = lg.Fields
			}
			if lg.EventType != "" {
				log.EventType = lg.EventType
			}
			mock.Logs = append(mock.Logs, log)
		}
	}

	// 错误详情
	if data.Error != nil {
		errorDetail := &types.ErrorDetail{
			Message: data.Error.Message,
		}
		if data.Error.Code != "" {
			errorDetail.Code = data.Error.Code
		}
		if data.Error.BusinessCode != "" {
			errorDetail.BusinessCode = data.Error.BusinessCode
		}
		if len(data.Error.BusinessMessage) > 0 {
			errorDetail.BusinessMessage = data.Error.BusinessMessage
		}
		if data.Error.HttpCode > 0 {
			errorDetail.HttpCode = data.Error.HttpCode
		}
		if data.Error.Timestamp != "" {
			errorDetail.Timestamp = data.Error.Timestamp
		}
		if len(data.Error.MetaData) > 0 {
			errorDetail.MetaData = make(map[string]any, len(data.Error.MetaData))
			for k, v := range data.Error.MetaData {
				errorDetail.MetaData[k] = v
			}
		}
		if len(data.Error.StackTrace) > 0 {
			errorDetail.StackTrace = make([]types.StackFrame, 0, len(data.Error.StackTrace))
			for _, frame := range data.Error.StackTrace {
				errorDetail.StackTrace = append(errorDetail.StackTrace, types.StackFrame{
					File:         frame.File,
					FileName:     frame.FileName,
					FunctionName: frame.FunctionName,
					LineNumber:   frame.LineNumber,
				})
			}
		}
		mock.ErrorDetail = errorDetail
	}

	// 资源信息
	if data.Resource != nil {
		resource := &types.ResourceInfo{}
		if data.Resource.ServiceName != "" {
			resource.ServiceName = data.Resource.ServiceName
		}
		if data.Resource.Host != "" {
			resource.Host = data.Resource.Host
		}
		if len(data.Resource.Attributes) > 0 {
			resource.Attributes = make(map[string]any, len(data.Resource.Attributes))
			for k, v := range data.Resource.Attributes {
				resource.Attributes[k] = v
			}
		}
		mock.Resource = resource
	}

	// 资源使用情况
	if data.ResourceUsage != nil {
		usage := &types.ResourceMetrics{}
		if data.ResourceUsage.CPUUsage > 0 {
			usage.CPUUsage = data.ResourceUsage.CPUUsage
		}
		if data.ResourceUsage.MemoryUsage > 0 {
			usage.MemoryUsage = data.ResourceUsage.MemoryUsage
		}
		if data.ResourceUsage.DiskUsage > 0 {
			usage.DiskUsage = data.ResourceUsage.DiskUsage
		}
		if data.ResourceUsage.NetworkIO > 0 {
			usage.NetworkIO = data.ResourceUsage.NetworkIO
		}
		mock.ResourceUsage = usage
	}

	// 关联 Span（使用对象池）
	if len(data.Links) > 0 {
		// 如果 mock.LinkedSpans 为空或容量不足，从池中获取
		if mock.LinkedSpans == nil || cap(mock.LinkedSpans) < len(data.Links) {
			if mock.LinkedSpans != nil {
				links := mock.LinkedSpans[:0]
				linkSlicePool.Put(&links)
			}
			mock.LinkedSpans = *linkSlicePool.Get().(*[]types.SpanContext)
			mock.LinkedSpans = mock.LinkedSpans[:0]
		}
		for _, link := range data.Links {
			linkContext := types.SpanContext{
				TraceID: link.TraceID,
				SpanID:  link.SpanID,
			}
			if link.ParentSpanID != "" {
				linkContext.ParentSpanID = link.ParentSpanID
			}
			mock.LinkedSpans = append(mock.LinkedSpans, linkContext)
		}
	}

	return mock, nil
}

// releaseSnapshotMock 释放 SpanSnapshotMock 并归还到对象池
// 清理所有引用，避免内存泄漏
func releaseSnapshotMock(mock *mock.SpanSnapshotMock) {
	if mock == nil {
		return
	}

	// 清理属性 map
	if mock.Attributes != nil {
		clear(mock.Attributes)
		attrMapPool.Put(mock.Attributes)
		mock.Attributes = nil
	}

	// 清理事件 slice 和事件属性
	if mock.Events != nil {
		for i := range mock.Events {
			if mock.Events[i].Attributes != nil {
				clear(mock.Events[i].Attributes)
				attrMapPool.Put(mock.Events[i].Attributes)
			}
		}
		events := mock.Events[:0]
		eventSlicePool.Put(&events)
		mock.Events = nil
	}

	// 清理日志 slice 和日志属性
	if mock.Logs != nil {
		for i := range mock.Logs {
			if mock.Logs[i].Attributes != nil {
				clear(mock.Logs[i].Attributes)
				attrMapPool.Put(mock.Logs[i].Attributes)
			}
			mock.Logs[i] = types.SpanLog{}
		}
		logs := mock.Logs[:0]
		logSlicePool.Put(&logs)
		mock.Logs = nil
	}

	// 清理关联 Span
	if mock.LinkedSpans != nil {
		links := mock.LinkedSpans[:0]
		linkSlicePool.Put(&links)
		mock.LinkedSpans = nil
	}

	// 清理其他字段
	mock.StartTime = time.Time{}
	mock.EndTime = time.Time{}
	mock.SpanContext = types.SpanContext{}
	mock.SpanName = ""
	mock.SpanKind = ""
	mock.SpanTraceID = ""
	mock.SpanParentSpanID = ""
	mock.MongoCollection = ""
	mock.Status = types.SpanStatus{}
	mock.ErrorDetail = nil
	mock.Resource = nil
	mock.ResourceUsage = nil
	mock.ReleaseFunc = nil

	// 归还到对象池
	snapshotMockPool.Put(mock)
}

// ConvertJSONToSpanSnapshot 将 JSON 字节数组转换为 SpanSnapshot
// 用于从 fallback 文件恢复数据
// 这里优先使用标准库，保证恢复链路稳定
func ConvertJSONToSpanSnapshot(jsonData []byte) (trace.SpanSnapshot, error) {
	var data SpanData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, err
	}
	return ConvertDataToSpanSnapshot(&data)
}
