package core

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/trace/noop"
	"github.com/ClownSketch/tracer/types"
	"github.com/ClownSketch/tracer/utils"
)

var (
	// defaultSuccessStatus 表示未显式设置错误时的默认成功状态。
	defaultSuccessStatus = &types.SpanStatus{
		Code:        types.StatusCodeOk,
		Description: "SuccessFully",
	}
	// defaultErrorStatus 表示 Span 未完整结束时的默认错误状态。
	defaultErrorStatus = &types.SpanStatus{
		Code:        types.StatusCodeError,
		Description: "服务发生异常，程序未执行完成",
	}
)

// spanImpl 是对外暴露的 Span 句柄。
// 句柄本身不再复用，避免 End 后旧引用误伤新 Span。
type spanImpl struct {
	startTime        time.Time
	endTime          time.Time
	spanContext      types.SpanContext
	spanName         string
	spanKind         types.SpanKind
	spanTraceID      string
	spanParentSpanID string
	snapshot         atomic.Pointer[snapshotImpl]
	state            atomic.Pointer[spanState]
}

// spanEvent 保存事件数据及其外层属性容器的所有权。
// ownsAttributes 为 true 时，外层 map 和分组切片由 Tracer 创建，可直接移交给快照。
type spanEvent struct {
	event          types.SpanEvent
	ownsAttributes bool
}

// spanState 是可复用的 Span 内部状态。
// 只有这部分会进入对象池，避免对外句柄身份被复用。
type spanState struct {
	lifecycleMu      sync.RWMutex                     // 状态生命周期锁
	tracerImpl       *tracerImpl                      // 所属追踪器
	forceRecord      atomic.Uint32                    // 导出策略，见 types.RecordPolicy*
	linkedSpans      []types.SpanContext              // 关联 Span
	linkMu           sync.RWMutex                     // 关联 Span 读写锁
	attributes       map[string]any                   // 属性
	attrMu           sync.RWMutex                     // 属性读写锁
	events           []spanEvent                      // 事件
	eventIndex       map[string]int                   // 事件索引
	eventMu          sync.RWMutex                     // 事件读写锁
	logs             []types.SpanLog                  // 日志
	logMu            sync.RWMutex                     // 日志读写锁
	status           atomic.Pointer[types.SpanStatus] // 当前 Span 状态
	errorDetail      atomic.Value                     // 错误详情
	resource         atomic.Value                     // 资源信息
	resourceUsage    atomic.Value                     // 资源使用情况
	attributeManager attribute.AttributeManager       // 属性管理器
	onceAttrManager  sync.Once                        // 属性管理器一次性初始化锁
	mongoCollection  string                           // MongoDB 导出目标集合名，空表示使用导出器默认集合
	mongoMu          sync.RWMutex                     // MongoDB 集合名读写锁
}

// createSpan 创建一个新的 Span 句柄，并附着一个可复用的状态对象。
func createSpan() *spanImpl {
	span := &spanImpl{}
	span.state.Store(acquireSpanState())
	return span
}

// Reset 重置当前 Span 状态，供对象池复用。
func (s *spanState) Reset() {
	s.tracerImpl = nil
	s.forceRecord.Store(0)

	s.linkMu.Lock()
	if s.linkedSpans != nil {
		clear(s.linkedSpans)
		s.linkedSpans = s.linkedSpans[:0]
	}
	s.linkMu.Unlock()

	s.attrMu.Lock()
	if s.attributes == nil {
		s.attributes = make(map[string]any, 8)
	} else {
		clear(s.attributes)
	}
	s.attrMu.Unlock()

	s.eventMu.Lock()
	if s.events != nil {
		clear(s.events)
		s.events = s.events[:0]
	}
	if s.eventIndex == nil {
		s.eventIndex = make(map[string]int, 5)
	} else {
		clear(s.eventIndex)
	}
	s.eventMu.Unlock()

	s.logMu.Lock()
	if s.logs != nil {
		clear(s.logs)
		s.logs = s.logs[:0]
	}
	s.logMu.Unlock()

	s.status.Store((*types.SpanStatus)(nil))
	s.errorDetail.Store((*types.ErrorDetail)(nil))
	s.resource.Store((*types.ResourceInfo)(nil))
	s.resourceUsage.Store((*types.ResourceMetrics)(nil))
	s.attributeManager = nil
	s.onceAttrManager = sync.Once{}
	s.mongoMu.Lock()
	s.mongoCollection = ""
	s.mongoMu.Unlock()
}

// loadState 返回当前 Span 持有的内部状态。
// @return state *spanState 当前状态；Span 已结束时返回 nil
func (s *spanImpl) loadState() *spanState {
	return s.state.Load()
}

// lockState 锁定当前仍属于该 Span 的状态。
func (s *spanImpl) lockState() *spanState {
	state := s.state.Load()
	if state == nil {
		return nil
	}
	state.lifecycleMu.RLock()
	if s.state.Load() != state {
		state.lifecycleMu.RUnlock()
		return nil
	}
	return state
}

// ensureAttributeManager 延迟创建属性管理器。
func (s *spanState) ensureAttributeManager() {
	s.onceAttrManager.Do(func() {
		if s.attributeManager == nil {
			s.attributeManager = baggage.NewAttributeManager()
		}
	})
}

// End 结束当前 Span。
// End 只回收内部状态，不再回收对外暴露的 Span 句柄。
func (s *spanImpl) End() {
	// 交换状态，将当前状态设置为 nil，并返回旧状态
	state := s.state.Swap(nil)
	if state == nil {
		return
	}
	state.lifecycleMu.Lock()

	if !shouldExportRecord(state) {
		state.lifecycleMu.Unlock()
		releaseSpanState(state)
		return
	}

	// 设置结束时间
	s.endTime = time.Now()
	// 检查状态
	s.checkSpanEndStatus(state)

	// 创建快照
	snap := createSnapshotInfo(s, state)
	if snap == nil {
		snap = &snapshotImpl{}
	}
	// 获取 Span 处理器
	var spanProcessor trace.SpanProcessor
	// 如果追踪器不为空，则获取 Span 处理器
	if state.tracerImpl != nil {
		spanProcessor = state.tracerImpl.spanProcessor
	}
	state.lifecycleMu.Unlock()
	releaseSpanState(state)

	// 如果 Span 处理器不为空，则调用 OnEnd 方法
	if spanProcessor != nil {
		spanProcessor.OnEnd(snap)
		return
	}
	// 未配置处理器时，把快照所有权留给显式调用 GetSnapshot 的调用方。
	s.snapshot.Store(snap)
}

// GetStartTime 返回开始时间
func (s *spanImpl) GetStartTime() time.Time {
	return s.startTime
}

// GetEndTime 返回结束时间
func (s *spanImpl) GetEndTime() time.Time {
	return s.endTime
}

// WithForceRecord 标记 Span 需要强制记录（始终导出）。
func (s *spanImpl) WithForceRecord() trace.Span {
	state := s.lockState()
	if state == nil {
		return s
	}
	defer state.lifecycleMu.RUnlock()
	state.forceRecord.Store(types.RecordPolicyAlways)
	return s
}

// WithRecordOnError 标记 Span 仅在发生错误时导出。
func (s *spanImpl) WithRecordOnError() trace.Span {
	state := s.lockState()
	if state == nil {
		return s
	}
	defer state.lifecycleMu.RUnlock()
	state.forceRecord.Store(types.RecordPolicyOnError)
	return s
}

// WithForceNotRecord 标记 Span 不导出（覆盖先前的导出策略）。
func (s *spanImpl) WithForceNotRecord() trace.Span {
	state := s.lockState()
	if state == nil {
		return s
	}
	defer state.lifecycleMu.RUnlock()
	state.forceRecord.Store(types.RecordPolicyNone)
	return s
}

// SpanContext 返回 SpanContext
func (s *spanImpl) SpanContext() types.SpanContext {
	return s.spanContext
}

// GetSpanName 返回当前 Span 名称
func (s *spanImpl) GetSpanName() string {
	return s.spanName
}

// GetSpanKind 返回默认 SpanKind
func (s *spanImpl) GetSpanKind() types.SpanKind {
	return s.spanKind
}

// GetSpanTraceID 返回当前 Span 的 TraceID
func (s *spanImpl) GetSpanTraceID() string {
	return s.spanTraceID
}

// GetSpanParentSpanID 返回当前 Span 的 ParentSpanID
func (s *spanImpl) GetSpanParentSpanID() string {
	return s.spanParentSpanID
}

// AddLinkedSpan 添加一个关联的 Span
func (s *spanImpl) AddLinkedSpan(spanContext types.SpanContext) {
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()

	state.linkMu.Lock()
	defer state.linkMu.Unlock()
	state.linkedSpans = append(state.linkedSpans, spanContext)
}

// GetLinkedSpans 返回所有关联的 Span
func (s *spanImpl) GetLinkedSpans() []types.SpanContext {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	state.linkMu.RLock()
	defer state.linkMu.RUnlock()
	if len(state.linkedSpans) == 0 {
		return nil
	}

	result := make([]types.SpanContext, len(state.linkedSpans))
	copy(result, state.linkedSpans)
	return result
}

// SetAttributeConfig 设置属性，带选项的配置属性
func (s *spanImpl) SetAttributeConfig(key string, value attribute.Value, opts ...attribute.AttributeOption) {
	if key == "" || value == nil {
		return
	}
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()

	state.attrMu.Lock()
	defer state.attrMu.Unlock()

	if state.attributes == nil {
		state.attributes = attrMapPool.Get().(map[string]any)
	}

	config := &attribute.Config{Type: attribute.AttributeTypePrivate}
	for _, opt := range opts {
		opt(config)
	}

	if config.Type != attribute.AttributeTypePrivate {
		state.ensureAttributeManager()
		state.attributeManager.AddAttribute(key, value, *config)
	}

	state.attributes[key] = value
}

// SetAttribute 设置单个私有属性
func (s *spanImpl) SetAttribute(key string, value attribute.Value) {
	s.SetAttributes(attribute.KeyValue{Key: key, Value: value})
}

// SetAttributes 批量设置私有属性，键值对形式
func (s *spanImpl) SetAttributes(attrs ...attribute.KeyValue) {
	if len(attrs) == 0 {
		return
	}
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()

	state.attrMu.Lock()
	defer state.attrMu.Unlock()

	if state.attributes == nil {
		state.attributes = attrMapPool.Get().(map[string]any)
	}

	for _, attr := range attrs {
		if attr.Key == "" || attr.Value == nil {
			continue
		}
		state.attributes[attr.Key] = attr.Value
	}
}

// SetGlobalAttribute 设置单个全局属性
func (s *spanImpl) SetGlobalAttribute(key string, value attribute.Value) {
	s.SetGlobalAttributes(attribute.KeyValue{Key: key, Value: value})
}

// SetGlobalAttributes 批量设置全局属性，键值对形式
func (s *spanImpl) SetGlobalAttributes(attrs ...attribute.KeyValue) {
	if len(attrs) == 0 {
		return
	}
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()

	state.attrMu.Lock()
	defer state.attrMu.Unlock()

	if state.attributes == nil {
		state.attributes = attrMapPool.Get().(map[string]any)
	}
	state.ensureAttributeManager()

	for _, attr := range attrs {
		if attr.Key == "" || attr.Value == nil {
			continue
		}

		state.attributeManager.AddAttribute(attr.Key, attr.Value, attribute.Config{
			Type: attribute.AttributeTypeGlobal,
		})
		state.attributes[attr.Key] = attr.Value
	}
}

// GetGlobalAttributes 返回全局属性
func (s *spanImpl) GetGlobalAttributes() map[string]attribute.Attribute {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	state.attrMu.RLock()
	defer state.attrMu.RUnlock()
	if state.attributeManager == nil {
		return nil
	}
	return state.attributeManager.GetGlobalAttributes()
}

// SetInheritedAttribute 设置单个继承属性
func (s *spanImpl) SetInheritedAttribute(key string, value attribute.Value) {
	s.SetInheritedAttributes(attribute.KeyValue{Key: key, Value: value})
}

// SetInheritedAttributes 批量设置继承属性
func (s *spanImpl) SetInheritedAttributes(attrs ...attribute.KeyValue) {
	if len(attrs) == 0 {
		return
	}
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()

	state.attrMu.Lock()
	defer state.attrMu.Unlock()

	if state.attributes == nil {
		state.attributes = attrMapPool.Get().(map[string]any)
	}
	state.ensureAttributeManager()

	for _, attr := range attrs {
		if attr.Key == "" || attr.Value == nil {
			continue
		}

		state.attributeManager.AddAttribute(attr.Key, attr.Value, attribute.Config{
			Type: attribute.AttributeTypeInherited,
		})
		state.attributes[attr.Key] = attr.Value
	}
}

// GetInheritedAttributes 返回继承属性
func (s *spanImpl) GetInheritedAttributes() map[string]attribute.Attribute {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	state.attrMu.RLock()
	defer state.attrMu.RUnlock()
	if state.attributeManager == nil {
		return nil
	}
	return state.attributeManager.GetInheritableAttributes()
}

// GetAttributes 返回所有属性
func (s *spanImpl) GetAttributes() map[string]any {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	state.attrMu.RLock()
	defer state.attrMu.RUnlock()
	if state.attributes == nil {
		return nil
	}

	newMap := make(map[string]any, len(state.attributes))
	for k, v := range state.attributes {
		newMap[k] = v
	}
	return newMap
}

// AddEvent 添加事件信息
func (s *spanImpl) AddEvent(name, eventType string, eventHandler types.Event) {
	if name == "" || eventHandler == nil {
		return
	}

	eventData := eventHandler()
	if eventData == nil {
		return
	}

	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()

	state.eventMu.Lock()
	defer state.eventMu.Unlock()

	if state.eventIndex == nil {
		idx, ok := eventIndexMapPool.Get().(map[string]int)
		if !ok || idx == nil {
			idx = make(map[string]int)
		}
		state.eventIndex = idx
	}

	if state.events == nil {
		evs, ok := eventSlicePool.Get().([]spanEvent)
		if !ok || evs == nil {
			evs = make([]spanEvent, 0, 5)
		}
		state.events = evs
	}

	if index, exists := state.eventIndex[name]; exists && index < len(state.events) {
		event := &state.events[index]
		if eventType == "" {
			event.event.Attributes = eventData
			event.ownsAttributes = false
		} else {
			ops := event.event.Attributes
			if ops == nil {
				ops = make(map[string]any)
				event.ownsAttributes = true
			}
			if attr, exists := ops[eventType]; exists {
				if attrArray, ok := attr.([]any); ok {
					ops[eventType] = append(attrArray, eventData)
				} else {
					ops[eventType] = []any{attr, eventData}
				}
			} else {
				ops[eventType] = []any{eventData}
			}
			event.event.Attributes = ops
		}
		return
	}

	var event spanEvent
	if eventType == "" {
		event.event = types.SpanEvent{
			Name:       name,
			Timestamp:  time.Now().Format(time.RFC3339),
			Attributes: eventData,
		}
	} else {
		event = spanEvent{
			event: types.SpanEvent{
				Name:       name,
				Timestamp:  time.Now().Format(time.RFC3339),
				Attributes: map[string]any{eventType: []any{eventData}},
			},
			ownsAttributes: true,
		}
	}

	state.events = append(state.events, event)
	state.eventIndex[name] = len(state.events) - 1
}

// GetEvents 获取所有事件信息
func (s *spanImpl) GetEvents() (result []types.SpanEvent) {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	state.eventMu.Lock()
	defer state.eventMu.Unlock()
	if len(state.events) == 0 {
		return nil
	}

	result = make([]types.SpanEvent, len(state.events))
	for index := range state.events {
		result[index] = state.events[index].event
		// 返回值暴露了属性 map，结束时必须重新深拷贝，不能再直接移交。
		state.events[index].ownsAttributes = false
	}
	return result
}

// AddLog 添加日志信息
func (s *spanImpl) AddLog(log types.SpanLog) trace.Span {
	state := s.lockState()
	if state == nil {
		return &noop.NoopSpan{}
	}
	defer state.lifecycleMu.RUnlock()

	state.logMu.Lock()
	defer state.logMu.Unlock()

	if state.logs == nil {
		logs, ok := logSlicePool.Get().([]types.SpanLog)
		if !ok || logs == nil {
			logs = make([]types.SpanLog, 0, 2)
		}
		state.logs = logs
	}

	if log.Timestamp == "" {
		log.Timestamp = time.Now().Format(time.RFC3339)
	}

	if log.Fields != nil {
		if err, ok := log.Fields.(error); ok {
			detail := utils.CreateErrorDetail(err, "")
			if detail != nil {
				log.Fields = detail
			}
		}
	}

	state.logs = append(state.logs, log)
	return s
}

// GetLogs 获取所有日志信息
func (s *spanImpl) GetLogs() (result []types.SpanLog) {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	state.logMu.RLock()
	defer state.logMu.RUnlock()
	if len(state.logs) == 0 {
		return nil
	}

	result = make([]types.SpanLog, len(state.logs))
	copy(result, state.logs)
	return result
}

// RecordError 记录错误信息
func (s *spanImpl) RecordError(err error) trace.Span {
	if err == nil {
		return &noop.NoopSpan{}
	}
	state := s.lockState()
	if state == nil {
		return &noop.NoopSpan{}
	}
	defer state.lifecycleMu.RUnlock()

	detail := utils.CreateErrorDetail(err, "")
	if detail != nil {
		state.errorDetail.Store(detail)
	}

	return s
}

// WithError 记录带有描述的错误信息
func (s *spanImpl) WithError(err error, message string) trace.Span {
	if err == nil {
		return &noop.NoopSpan{}
	}
	state := s.lockState()
	if state == nil {
		return &noop.NoopSpan{}
	}
	defer state.lifecycleMu.RUnlock()

	detail := utils.CreateErrorDetail(err, message)
	if detail != nil {
		state.errorDetail.Store(detail)
	}

	return s
}

// GetErrorDetail 返回错误详情
func (s *spanImpl) GetErrorDetail() *types.ErrorDetail {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	v := state.errorDetail.Load()
	if errDetail, ok := v.(*types.ErrorDetail); ok && errDetail != nil {
		return errDetail
	}
	return nil
}

// SetStatus 设置状态
func (s *spanImpl) SetStatus(status types.SpanStatus) {
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()
	state.status.Store(&status)
}

// SetResource 设置资源
func (s *spanImpl) SetResource(resource *types.ResourceInfo) {
	if resource == nil {
		return
	}
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()
	state.resource.Store(cloneResourceInfo(resource))
}

// GetResource 返回资源
func (s *spanImpl) GetResource() *types.ResourceInfo {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	v := state.resource.Load()
	if resInfo, ok := v.(*types.ResourceInfo); ok && resInfo != nil {
		return cloneResourceInfo(resInfo)
	}
	return nil
}

// SetResourceUsage 设置资源使用情况
func (s *spanImpl) SetResourceUsage(usage *types.ResourceMetrics) {
	if usage == nil {
		return
	}
	state := s.lockState()
	if state == nil {
		return
	}
	defer state.lifecycleMu.RUnlock()
	copyUsage := *usage
	state.resourceUsage.Store(&copyUsage)
}

// GetResourceUsage 返回资源使用情况
func (s *spanImpl) GetResourceUsage() *types.ResourceMetrics {
	state := s.lockState()
	if state == nil {
		return nil
	}
	defer state.lifecycleMu.RUnlock()

	v := state.resourceUsage.Load()
	if resUsage, ok := v.(*types.ResourceMetrics); ok && resUsage != nil {
		copyUsage := *resUsage
		return &copyUsage
	}
	return nil
}

// SetMongoCollection 设置 MongoDB 导出目标集合名。
func (s *spanImpl) SetMongoCollection(name string) trace.Span {
	state := s.lockState()
	if state == nil {
		return s
	}
	defer state.lifecycleMu.RUnlock()
	state.mongoMu.Lock()
	state.mongoCollection = name
	state.mongoMu.Unlock()
	return s
}

// GetMongoCollection 返回 MongoDB 导出目标集合名。
func (s *spanImpl) GetMongoCollection() string {
	state := s.lockState()
	if state == nil {
		return ""
	}
	defer state.lifecycleMu.RUnlock()
	state.mongoMu.RLock()
	name := state.mongoCollection
	state.mongoMu.RUnlock()
	return name
}

// GetSnapshot 返回当前 Span 快照
func (s *spanImpl) GetSnapshot() trace.SpanSnapshot {
	snapshot := s.snapshot.Load()
	if snapshot == nil {
		return nil
	}
	return snapshot
}

// checkSpanEndStatus 检查当前 Span 状态是否被设置
func (s *spanImpl) checkSpanEndStatus(state *spanState) {
	if state.status.Load() != nil {
		return
	}

	v := state.errorDetail.Load()
	if errDetail, ok := v.(*types.ErrorDetail); ok && errDetail != nil {
		state.status.Store(defaultErrorStatus)
		return
	}

	state.logMu.RLock()
	defer state.logMu.RUnlock()
	for _, logEntry := range state.logs {
		switch logEntry.Severity {
		case types.SpanLogSeverityError, types.SpanLogSeverityFatal, types.SpanLogSeverityPanic:
			state.status.Store(defaultErrorStatus)
			return
		}
	}

	state.status.Store(defaultSuccessStatus)
}
