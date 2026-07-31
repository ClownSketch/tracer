package mock

import (
	"fmt"
	"time"

	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// SpanSnapshotMock 用于测试的 SpanSnapshot 实现
// 提供了完整的接口实现，方便在测试中使用
// 保留完整的 span 数据，与 span_snapshot_impl.go 中的字段保持一致
type SpanSnapshotMock struct {
	// 基础信息（与 span_snapshot_impl.go 保持一致）
	StartTime        time.Time         // 开始时间
	EndTime          time.Time         // 结束时间
	SpanContext      types.SpanContext // 当前 Span 上下文
	SpanName         string            // 当前 Span 名称
	SpanKind         types.SpanKind    // 当前 Span 类型
	SpanTraceID      string            // 当前 Span 的 TraceID
	SpanParentSpanID string            // 当前 Span 的 ParentSpanID

	// 扩展信息（与 span_snapshot_impl.go 保持一致）
	LinkedSpans     []types.SpanContext    // 关联 Span
	Attributes      map[string]any         // 属性
	Events          []types.SpanEvent      // 事件
	Logs            []types.SpanLog        // 日志
	Status          types.SpanStatus       // 当前 Span 状态
	ErrorDetail     *types.ErrorDetail     // 错误详情
	Resource        *types.ResourceInfo    // 系统资源信息
	ResourceUsage   *types.ResourceMetrics // 资源使用情况
	MongoCollection string                 // MongoDB 导出目标集合名

	// 控制选项
	ReleaseFunc func() // 自定义 Release 函数，如果为 nil 则使用默认实现
}

// NewSpanSnapshotMock 创建新的 SpanSnapshotMock
// 提供默认值，方便快速创建测试用的 snapshot
func NewSpanSnapshotMock(id int) *SpanSnapshotMock {
	now := time.Now()
	spanID := fmt.Sprintf("span-%d", id)
	traceID := fmt.Sprintf("trace-%d", id)
	parentSpanID := fmt.Sprintf("parent-%d", id)

	return &SpanSnapshotMock{
		StartTime: now.Add(-10 * time.Millisecond),
		EndTime:   now,
		SpanContext: types.SpanContext{
			TraceID: traceID,
			SpanID:  spanID,
		},
		SpanName:         fmt.Sprintf("test-span-%d", id),
		SpanKind:         types.SpanKindInternal,
		SpanTraceID:      traceID,
		SpanParentSpanID: parentSpanID,
		Attributes: map[string]any{
			"test.key": "test.value",
			"test.id":  spanID,
		},
		Status: types.SpanStatus{
			Code:        types.StatusCodeOk,
			Description: "ok",
		},
	}
}

// NewSpanSnapshotMockWithOptions 使用选项模式创建 SpanSnapshotMock
func NewSpanSnapshotMockWithOptions(id int, opts ...SpanSnapshotMockOption) *SpanSnapshotMock {
	mock := NewSpanSnapshotMock(id)
	for _, opt := range opts {
		opt(mock)
	}
	return mock
}

// SpanSnapshotMockOption 配置选项函数类型
type SpanSnapshotMockOption func(*SpanSnapshotMock)

// WithStartTime 设置开始时间
func WithStartTime(t time.Time) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.StartTime = t
	}
}

// WithEndTime 设置结束时间
func WithEndTime(t time.Time) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.EndTime = t
	}
}

// WithSpanName 设置 Span 名称
func WithSpanName(name string) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.SpanName = name
	}
}

// WithSpanKind 设置 Span 类型
func WithSpanKind(kind types.SpanKind) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.SpanKind = kind
	}
}

// WithSpanContext 设置 Span 上下文
func WithSpanContext(spanContext types.SpanContext) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.SpanContext = spanContext
		// 同步更新 TraceID 和 SpanID（如果未单独设置）
		if m.SpanTraceID == "" {
			m.SpanTraceID = spanContext.TraceID
		}
	}
}

// WithTraceID 设置 TraceID
func WithTraceID(traceID string) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.SpanTraceID = traceID
		// 同步更新 SpanContext
		if m.SpanContext.TraceID == "" {
			m.SpanContext.TraceID = traceID
		}
	}
}

// WithSpanID 设置 SpanID
func WithSpanID(spanID string) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		// 同步更新 SpanContext
		m.SpanContext.SpanID = spanID
	}
}

// WithParentSpanID 设置 ParentSpanID
func WithParentSpanID(parentSpanID string) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.SpanParentSpanID = parentSpanID
	}
}

// WithAttributes 设置属性
func WithAttributes(attrs map[string]any) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.Attributes = attrs
	}
}

// WithEvents 设置事件
func WithEvents(events []types.SpanEvent) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.Events = events
	}
}

// WithLogs 设置日志
func WithLogs(logs []types.SpanLog) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.Logs = logs
	}
}

// WithErrorDetail 设置错误详情
func WithErrorDetail(errDetail *types.ErrorDetail) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.ErrorDetail = errDetail
	}
}

// WithStatus 设置状态
func WithStatus(status types.SpanStatus) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.Status = status
	}
}

// WithResource 设置资源信息
func WithResource(resource *types.ResourceInfo) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.Resource = resource
	}
}

// WithResourceUsage 设置资源使用情况
func WithResourceUsage(usage *types.ResourceMetrics) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.ResourceUsage = usage
	}
}

// WithMongoCollection 设置 MongoDB 导出目标集合名
func WithMongoCollection(name string) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.MongoCollection = name
	}
}

// WithLinkedSpans 设置关联 Span
func WithLinkedSpans(linkedSpans []types.SpanContext) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.LinkedSpans = linkedSpans
	}
}

// WithReleaseFunc 设置自定义 Release 函数
func WithReleaseFunc(fn func()) SpanSnapshotMockOption {
	return func(m *SpanSnapshotMock) {
		m.ReleaseFunc = fn
	}
}

// 实现 trace.SpanSnapshot 接口

// GetStartTime 获取开始时间
func (m *SpanSnapshotMock) GetStartTime() time.Time {
	return m.StartTime
}

// GetEndTime 获取结束时间
func (m *SpanSnapshotMock) GetEndTime() time.Time {
	return m.EndTime
}

// GetSpanName 获取 Span 名称
func (m *SpanSnapshotMock) GetSpanName() string {
	return m.SpanName
}

// GetSpanKind 获取 Span 类型
func (m *SpanSnapshotMock) GetSpanKind() types.SpanKind {
	return m.SpanKind
}

// GetSpanTraceID 获取 TraceID
func (m *SpanSnapshotMock) GetSpanTraceID() string {
	return m.SpanTraceID
}

// GetSpanID 获取 SpanID
// 注意：与 span_snapshot_impl.go 保持一致，从 SpanContext 获取
func (m *SpanSnapshotMock) GetSpanID() string {
	return m.SpanContext.SpanID
}

// GetSpanParentSpanID 获取 ParentSpanID
func (m *SpanSnapshotMock) GetSpanParentSpanID() string {
	return m.SpanParentSpanID
}

// GetLinkedSpans 获取关联 Span
func (m *SpanSnapshotMock) GetLinkedSpans() []types.SpanContext {
	if m.LinkedSpans == nil {
		return nil
	}
	return m.LinkedSpans
}

// GetAttributes 获取属性
func (m *SpanSnapshotMock) GetAttributes() map[string]any {
	if m.Attributes == nil {
		return nil
	}
	return m.Attributes
}

// GetEvents 获取事件
func (m *SpanSnapshotMock) GetEvents() []types.SpanEvent {
	if m.Events == nil {
		return nil
	}
	return m.Events
}

// GetLogs 获取日志
func (m *SpanSnapshotMock) GetLogs() []types.SpanLog {
	if m.Logs == nil {
		return nil
	}
	return m.Logs
}

// GetErrorDetail 获取错误详情
func (m *SpanSnapshotMock) GetErrorDetail() *types.ErrorDetail {
	return m.ErrorDetail
}

// GetStatus 获取状态
func (m *SpanSnapshotMock) GetStatus() types.SpanStatus {
	return m.Status
}

// GetResource 获取资源信息
func (m *SpanSnapshotMock) GetResource() *types.ResourceInfo {
	return m.Resource
}

// GetResourceUsage 获取资源使用情况
func (m *SpanSnapshotMock) GetResourceUsage() *types.ResourceMetrics {
	return m.ResourceUsage
}

// GetMongoCollection 获取 MongoDB 导出目标集合名
func (m *SpanSnapshotMock) GetMongoCollection() string {
	return m.MongoCollection
}

// Release 释放资源
// 如果设置了自定义 ReleaseFunc，则调用自定义函数
// 否则使用默认实现（no-op）
func (m *SpanSnapshotMock) Release() {
	if m.ReleaseFunc != nil {
		m.ReleaseFunc()
	}
	// 默认实现：no-op（mock 不需要实际释放资源）
}

// 确保 SpanSnapshotMock 实现了 trace.SpanSnapshot 接口
var _ trace.SpanSnapshot = (*SpanSnapshotMock)(nil)
