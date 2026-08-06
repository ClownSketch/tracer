package core

import (
	"reflect"
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/types"
)

// snapshotImpl 是 SpanSnapshot 的实现
type snapshotImpl struct {
	startTime        time.Time                  // 开始时间
	endTime          time.Time                  // 结束时间
	spanContext      types.SpanContext          // 当前 Span 上下文
	spanName         string                     // 当前 Span 名称
	spanKind         types.SpanKind             // 当前 Span 类型
	spanTraceID      string                     // 当前 Span 的 TraceID
	spanParentSpanID string                     // 当前 Span 的 ParentSpanID
	linkedSpans      []types.SpanContext        // 关联 Span
	attributes       map[string]any             // 属性
	events           []types.SpanEvent          // 事件
	logs             []types.SpanLog            // 日志
	status           types.SpanStatus           // 当前 Span 状态
	errorDetail      *types.ErrorDetail         // 错误详情
	resource         *types.ResourceInfo        // 系统资源信息
	resourceUsage    *types.ResourceMetrics     // 资源使用情况
	attributeManager attribute.AttributeManager // 属性管理器
	mongoCollection  string                     // MongoDB 导出目标集合名
	buffers          *snapshotBuffers           // 快照内部容器，Release 后归还对象池
}

// 创建快照信息
func createSnapshotInfo(span *spanImpl, state *spanState) *snapshotImpl {
	// snap := &snapshotImpl{
	// 	startTime:        s.startTime,
	// 	endTime:          s.endTime,
	// 	spanContext:      s.spanContext,
	// 	spanName:         s.spanName,
	// 	spanKind:         s.spanKind,
	// 	spanTraceID:      s.spanTraceID,
	// 	spanParentSpanID: s.spanParentSpanID,
	// }

	// // 复制关联 Span
	// snap.linkedSpans = s.GetLinkedSpans()

	// // 复制属性
	// snap.attributes = s.GetAttributes()

	// // 复制事件
	// snap.events = s.GetEvents()

	// // 复制日志
	// snap.logs = s.GetLogs()

	// // 复制错误详情、资源信息、资源使用情况
	// snap.errorDetail = s.GetErrorDetail()

	// // 复制资源信息
	// snap.resource = s.GetResource()

	// // 复制资源使用情况
	// snap.resourceUsage = s.GetResourceUsage()

	// // 属性管理器可以直接引用或深拷贝，取决于业务需求
	// snap.attributeManager = s.attributeManager

	// return snap

	// 获取快照内部容器。
	buffers := snapshotBufferPool.Get().(*snapshotBuffers)
	attr := buffers.attributes
	// 清空残留（以防上次复用留下旧键）
	if len(attr) > 0 {
		clear(attr)
	}

	// 重置事件缓冲区。
	events := buffers.events
	// 清空残留（以防上次复用留下旧键）
	if len(events) > 0 {
		clear(events)
		events = events[:0]
	}

	// 重置日志缓冲区。
	logs := buffers.logs
	// 清空残留（以防上次复用留下旧键）
	if len(logs) > 0 {
		clear(logs)
		logs = logs[:0]
	}

	// 重置关联 Span 缓冲区。
	links := buffers.linkedSpans
	// 清空残留（以防上次复用留下旧键）
	if len(links) > 0 {
		clear(links)
		links = links[:0]
	}

	// 现在在 span 的读锁范围内复制数据（避免竞争）
	state.attrMu.RLock()
	// 如果属性映射不为空，则复制属性映射
	if state.attributes != nil {
		// 遍历属性映射，复制属性映射到目标容器
		for k, v := range state.attributes {
			attr[k] = cloneSnapshotValue(v)
		}
	}
	state.attrMu.RUnlock()

	// 复制事件副本
	state.eventMu.Lock()
	if len(state.events) > 0 {
		for _, event := range state.events {
			events = append(events, cloneSpanEvent(event))
		}
	}
	state.eventMu.Unlock()

	// 复制日志副本
	state.logMu.RLock()
	if len(state.logs) > 0 {
		for _, log := range state.logs {
			logs = append(logs, cloneSpanLog(log))
		}
	}
	state.logMu.RUnlock()

	// 复制关联 Span 副本
	state.linkMu.RLock() // 加读锁
	if len(state.linkedSpans) > 0 {
		// 复制关联 Span 列表
		links = append(links, state.linkedSpans...)
	}
	state.linkMu.RUnlock() // 解锁

	// 获取错误详情，并复制到目标容器
	var errDetail *types.ErrorDetail
	if v := state.errorDetail.Load(); v != nil {
		if ed, ok := v.(*types.ErrorDetail); ok {
			errDetail = cloneErrorDetail(ed)
		}
	}

	// 获取资源信息，并复制到目标容器
	var resInfo *types.ResourceInfo
	if v := state.resource.Load(); v != nil {
		if r, ok := v.(*types.ResourceInfo); ok {
			resInfo = cloneResourceInfo(r)
		}
	}

	// 获取资源使用情况，并复制到目标容器
	var resUsage *types.ResourceMetrics
	if v := state.resourceUsage.Load(); v != nil {
		if ru, ok := v.(*types.ResourceMetrics); ok {
			resUsage = cloneResourceMetrics(ru)
		}
	}

	// 复制状态
	var st types.SpanStatus
	if v := state.status.Load(); v != nil {
		st = *v
	}

	state.mongoMu.RLock()
	mongoCollection := state.mongoCollection
	state.mongoMu.RUnlock()

	// 构建 snapshot（注意：这里把池化容器直接赋值过去，导出后必须 release）
	snap := &snapshotImpl{
		startTime:        span.startTime,
		endTime:          span.endTime,
		spanContext:      span.spanContext,
		spanName:         span.spanName,
		spanKind:         span.spanKind,
		spanTraceID:      span.spanTraceID,
		spanParentSpanID: span.spanParentSpanID,
		attributes:       attr,
		events:           events,
		logs:             logs,
		linkedSpans:      links,
		status:           st,
		errorDetail:      errDetail,
		resource:         resInfo,
		resourceUsage:    resUsage,
		mongoCollection:  mongoCollection,
		buffers:          buffers,
	}

	return snap
}

// cloneSpanEvent 复制事件并隔离其中的可变属性。
// @param event spanEvent 原始事件
// @return result types.SpanEvent 可安全导出的事件副本
func cloneSpanEvent(event spanEvent) types.SpanEvent {
	if event.ownsAttributes {
		return types.SpanEvent{
			Name:       event.event.Name,
			Timestamp:  event.event.Timestamp,
			Attributes: cloneOwnedEventAttributes(event.event.Attributes),
		}
	}

	return types.SpanEvent{
		Name:       event.event.Name,
		Timestamp:  event.event.Timestamp,
		Attributes: cloneMapStringAny(event.event.Attributes),
	}
}

// cloneOwnedEventAttributes 移交 Tracer 创建的外层容器，仅复制调用方提供的可变载荷。
func cloneOwnedEventAttributes(attributes map[string]any) map[string]any {
	for key, value := range attributes {
		if items, ok := value.([]any); ok {
			for index, item := range items {
				items[index] = cloneSnapshotValue(item)
			}
			attributes[key] = items
			continue
		}
		attributes[key] = cloneSnapshotValue(value)
	}
	return attributes
}

// cloneSpanLog 复制日志及其可变字段。
// @param log types.SpanLog 原始日志
// @return result types.SpanLog 日志副本
func cloneSpanLog(log types.SpanLog) types.SpanLog {
	return types.SpanLog{
		Timestamp:  log.Timestamp,
		Message:    log.Message,
		Fields:     cloneSnapshotValue(log.Fields),
		Severity:   log.Severity,
		EventType:  log.EventType,
		Attributes: cloneMapStringAny(log.Attributes),
	}
}

// cloneErrorDetail 复制错误详情，避免导出期间被调用方修改。
// @param detail *types.ErrorDetail 原始错误详情
// @return result *types.ErrorDetail 错误详情副本
func cloneErrorDetail(detail *types.ErrorDetail) *types.ErrorDetail {
	if detail == nil {
		return nil
	}

	return &types.ErrorDetail{
		Code:            detail.Code,
		Message:         detail.Message,
		BusinessCode:    detail.BusinessCode,
		BusinessMessage: append([]string(nil), detail.BusinessMessage...),
		MetaData:        cloneMapStringAny(detail.MetaData),
		StackTrace:      append([]types.StackFrame(nil), detail.StackTrace...),
		HttpCode:        detail.HttpCode,
		Timestamp:       detail.Timestamp,
	}
}

// cloneResourceInfo 复制资源信息及其属性。
// @param resource *types.ResourceInfo 原始资源信息
// @return result *types.ResourceInfo 资源信息副本
func cloneResourceInfo(resource *types.ResourceInfo) *types.ResourceInfo {
	if resource == nil {
		return nil
	}

	return &types.ResourceInfo{
		ServiceName: resource.ServiceName,
		Host:        resource.Host,
		Attributes:  cloneMapStringAny(resource.Attributes),
	}
}

// cloneResourceMetrics 复制资源指标。
// @param metrics *types.ResourceMetrics 原始资源指标
// @return result *types.ResourceMetrics 资源指标副本
func cloneResourceMetrics(metrics *types.ResourceMetrics) *types.ResourceMetrics {
	if metrics == nil {
		return nil
	}

	cloned := *metrics
	return &cloned
}

// cloneMapStringAny 深拷贝字符串键属性集合。
// @param src map[string]any 原始属性集合
// @return result map[string]any 属性副本
func cloneMapStringAny(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = cloneSnapshotValue(v)
	}
	return dst
}

// cloneSnapshotValue 复制快照支持的标量、集合和结构化值。
// @param value any 原始值
// @return result any 可安全导出的值副本
func cloneSnapshotValue(value any) any {
	switch v := value.(type) {
	case nil,
		string,
		bool,
		int,
		int8,
		int16,
		int32,
		int64,
		uint,
		uint8,
		uint16,
		uint32,
		uint64,
		uintptr,
		float32,
		float64,
		time.Time,
		time.Duration,
		types.SpanKind,
		types.StatusCode,
		types.SpanLogSeverity,
		types.SpanContext,
		types.SpanStatus,
		types.StackFrame:
		return v
	case attribute.StringValue:
		return string(v)
	case attribute.IntValue:
		return int(v)
	case attribute.Int64Value:
		return int64(v)
	case attribute.Float32Value:
		return float32(v)
	case attribute.Float64Value:
		return float64(v)
	case attribute.BoolValue:
		return bool(v)
	case attribute.ArrayValue:
		cloned := make([]any, len(v))
		for i, item := range v {
			cloned[i] = cloneSnapshotValue(item)
		}
		return cloned
	case attribute.Value:
		return v.String()
	case map[string]any:
		return cloneMapStringAny(v)
	case []any:
		cloned := make([]any, len(v))
		for i, item := range v {
			cloned[i] = cloneSnapshotValue(item)
		}
		return cloned
	case []string:
		return append([]string(nil), v...)
	case []int:
		return append([]int(nil), v...)
	case []int64:
		return append([]int64(nil), v...)
	case []float64:
		return append([]float64(nil), v...)
	case []bool:
		return append([]bool(nil), v...)
	case []byte:
		return append([]byte(nil), v...)
	case *types.ErrorDetail:
		return cloneErrorDetail(v)
	case types.ErrorDetail:
		cloned := cloneErrorDetail(&v)
		if cloned == nil {
			return types.ErrorDetail{}
		}
		return *cloned
	case *types.ResourceInfo:
		return cloneResourceInfo(v)
	case types.ResourceInfo:
		cloned := cloneResourceInfo(&v)
		if cloned == nil {
			return types.ResourceInfo{}
		}
		return *cloned
	case *types.ResourceMetrics:
		return cloneResourceMetrics(v)
	case types.ResourceMetrics:
		return v
	default:
		return cloneCollectionValue(v)
	}
}

// cloneCollectionValue 通过反射复制未显式覆盖的 Map、Slice 和数组。
// @param value any 原始集合
// @return result any 集合副本；非集合值保持原值
func cloneCollectionValue(value any) any {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil
	}

	switch rv.Kind() {
	case reflect.Map:
		if rv.IsNil() {
			return value
		}
		cloned := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			next := cloneSnapshotValue(iter.Value().Interface())
			cloned.SetMapIndex(iter.Key(), adaptClonedValue(next, rv.Type().Elem(), iter.Value()))
		}
		return cloned.Interface()
	case reflect.Slice:
		if rv.IsNil() {
			return value
		}
		cloned := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := 0; i < rv.Len(); i++ {
			next := cloneSnapshotValue(rv.Index(i).Interface())
			cloned.Index(i).Set(adaptClonedValue(next, rv.Type().Elem(), rv.Index(i)))
		}
		return cloned.Interface()
	case reflect.Array:
		cloned := reflect.New(rv.Type()).Elem()
		for i := 0; i < rv.Len(); i++ {
			next := cloneSnapshotValue(rv.Index(i).Interface())
			cloned.Index(i).Set(adaptClonedValue(next, rv.Type().Elem(), rv.Index(i)))
		}
		return cloned.Interface()
	default:
		return value
	}
}

// adaptClonedValue 将复制结果转换为目标集合元素类型。
// @param value any 复制后的值
// @param targetType reflect.Type 目标元素类型
// @param fallback reflect.Value 无法转换时使用的原值
// @return result reflect.Value 可写入目标集合的值
func adaptClonedValue(value any, targetType reflect.Type, fallback reflect.Value) reflect.Value {
	if value == nil {
		return reflect.Zero(targetType)
	}

	rv := reflect.ValueOf(value)
	if rv.Type().AssignableTo(targetType) {
		return rv
	}
	if rv.Type().ConvertibleTo(targetType) {
		return rv.Convert(targetType)
	}
	return fallback
}

// -----------------------------
// releaseSnapshotResources: 导出器处理完 snapshot 后必须调用（或提供回调给导出器）
// 把池化的 containers 放回池中，避免内存泄露 / 污染
// -----------------------------
func releaseSnapshotResources(snap *snapshotImpl) {
	if snap == nil {
		return
	}

	// 清空快照持有的数据，并把内部容器作为一个整体归还对象池。
	if snap.buffers != nil {
		clear(snap.attributes)
		clear(snap.events)
		clear(snap.logs)
		clear(snap.linkedSpans)

		snap.buffers.attributes = snap.attributes
		snap.buffers.events = snap.events[:0]
		snap.buffers.logs = snap.logs[:0]
		snap.buffers.linkedSpans = snap.linkedSpans[:0]
		snapshotBufferPool.Put(snap.buffers)
		snap.buffers = nil
	}
	snap.attributes = nil
	snap.events = nil
	snap.logs = nil
	snap.linkedSpans = nil

	// 清空错误详情
	snap.errorDetail = nil
	// 清空资源信息
	snap.resource = nil
	// 清空资源使用情况
	snap.resourceUsage = nil
	// 清空属性管理器
	snap.attributeManager = nil
	snap.mongoCollection = ""
}

// GetStartTime 返回开始时间
func (s *snapshotImpl) GetStartTime() time.Time {
	return s.startTime
}

// GetEndTime 返回结束时间
func (s *snapshotImpl) GetEndTime() time.Time {
	return s.endTime
}

// GetSpanName 返回 Span 名称
func (s *snapshotImpl) GetSpanName() string {
	return s.spanName
}

// GetSpanKind 返回 Span 类型
func (s *snapshotImpl) GetSpanKind() types.SpanKind {
	return s.spanKind
}

// GetSpanTraceID 返回 TraceID
func (s *snapshotImpl) GetSpanTraceID() string {
	return s.spanTraceID
}

// GetSpanID 返回 SpanID
func (s *snapshotImpl) GetSpanID() string {
	return s.spanContext.SpanID
}

// GetSpanParentSpanID 返回 ParentSpanID
func (s *snapshotImpl) GetSpanParentSpanID() string {
	return s.spanParentSpanID
}

// GetLinkedSpans 返回关联 Span 副本
func (s *snapshotImpl) GetLinkedSpans() []types.SpanContext {
	return s.linkedSpans
}

// GetAttributes 返回属性副本
func (s *snapshotImpl) GetAttributes() map[string]any {
	return s.attributes
}

// GetEvents 返回事件副本
func (s *snapshotImpl) GetEvents() []types.SpanEvent {
	return s.events
}

// GetLogs 返回日志副本
func (s *snapshotImpl) GetLogs() []types.SpanLog {
	return s.logs
}

// GetErrorDetail 返回错误详情
func (s *snapshotImpl) GetErrorDetail() *types.ErrorDetail {
	return s.errorDetail
}

// GetStatus 返回 Span 状态
func (s *snapshotImpl) GetStatus() types.SpanStatus {
	return s.status
}

// GetResource 返回资源信息
func (s *snapshotImpl) GetResource() *types.ResourceInfo {
	return s.resource
}

// GetResourceUsage 返回资源使用情况
func (s *snapshotImpl) GetResourceUsage() *types.ResourceMetrics {
	return s.resourceUsage
}

// GetMongoCollection 返回 MongoDB 导出目标集合名。
func (s *snapshotImpl) GetMongoCollection() string {
	return s.mongoCollection
}

// Release 释放快照资源，将池化的容器归还到对象池中
// 实现 SpanSnapshot 接口
func (s *snapshotImpl) Release() {
	releaseSnapshotResources(s)
}
