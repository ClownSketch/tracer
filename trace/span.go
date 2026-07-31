package trace

import (
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/types"
)

// Span 表示分布式追踪中的一个操作。
//
// Span 包含操作的开始时间、结束时间、属性、事件、日志、状态等信息。
// Span 必须通过调用 End() 方法来结束，以记录操作的完成时间。
//
// 示例:
//
//	ctx, span := tracer.Start(ctx, "database.query")
//	defer span.End()
//
//	span.SetAttributes(attribute.String("db.statement", "SELECT * FROM users"))
//	span.AddEvent("db.query.start", "sql", func() map[string]any {
//		return map[string]any{"query": "SELECT * FROM users"}
//	})
//
//	result, err := db.Query(ctx, "SELECT * FROM users")
//	if err != nil {
//		span.RecordError(err)
//		span.SetStatus(tracer.SpanStatus{Code: tracer.StatusCodeError})
//	}
type Span interface {
	// End 结束当前 Span 并记录结束时间。
	//
	// 此方法必须被调用以完成 Span 的生命周期。
	// 建议使用 defer 确保 Span 总是被结束。
	//
	// 示例:
	//
	//	ctx, span := tracer.Start(ctx, "operation")
	//	defer span.End()
	End()

	// GetStartTime 返回 Span 的开始时间。
	GetStartTime() time.Time

	// GetEndTime 返回 Span 的结束时间。
	//
	// 如果 Span 尚未结束，返回零值。
	GetEndTime() time.Time

	// WithForceRecord 标记 Span 需要强制记录，即使采样器决定不采样。
	//
	// 这对于重要的操作（如错误处理）很有用。
	WithForceRecord() Span

	// WithRecordOnError 标记 Span 仅在发生错误时导出（如 RecordError、WithError、StatusCodeError）。
	//
	// 适合在中间件根 Span 上设置一次：成功请求不落库，出错时自动保留链路。
	WithRecordOnError() Span

	// WithForceNotRecord 标记 Span 不导出（覆盖先前的导出策略）。
	WithForceNotRecord() Span

	// SpanContext 返回 Span 的上下文信息，包含 TraceID 和 SpanID。
	//
	// SpanContext 用于在服务间传播追踪信息。
	SpanContext() types.SpanContext

	// GetSpanName 返回 Span 的名称。
	GetSpanName() string

	// GetSpanKind 返回 Span 的类型（如 Server、Client、Internal 等）。
	GetSpanKind() types.SpanKind

	// GetSpanTraceID 返回 Span 的 TraceID。
	GetSpanTraceID() string

	// GetSpanParentSpanID 返回 Span 的父 SpanID。
	//
	// 如果这是根 Span，返回空字符串。
	GetSpanParentSpanID() string

	// AddLinkedSpan 添加关联的 Span。
	//
	// 关联 Span 用于表示因果关系，但不一定是父子关系。
	AddLinkedSpan(spanContext types.SpanContext)

	// GetLinkedSpans 返回所有关联的 Span。
	GetLinkedSpans() []types.SpanContext

	// SetAttributeConfig 设置属性，支持配置选项。
	//
	// 参数:
	//   - key: 属性键
	//   - value: 属性值
	//   - opts: 属性配置选项（如类型、元数据等）
	//
	// 示例:
	//
	//	span.SetAttributeConfig("user.id", attribute.StringValue("12345"),
	//		attribute.WithAttributeType(attribute.AttributeTypeString),
	//	)
	SetAttributeConfig(key string, value attribute.Value, opts ...attribute.AttributeOption)

	// SetAttribute 设置单个私有属性。
	//
	// 私有属性仅在此 Span 中可见，不会传播到子 Span。
	//
	// 参数:
	//   - key: 属性键
	//   - value: 属性值
	SetAttribute(key string, value attribute.Value)

	// SetAttributes 批量设置私有属性（键值对形式）。
	//
	// 私有属性仅在此 Span 中可见，不会传播到子 Span。
	//
	// 参数:
	//   - attrs: 属性键值对
	//
	// 示例:
	//
	//	span.SetAttributes(
	//		attribute.String("user.id", "12345"),
	//		attribute.Int("request.id", 67890),
	//	)
	SetAttributes(attrs ...attribute.KeyValue)

	// SetGlobalAttribute 设置单个全局属性。
	//
	// 全局属性会传播到所有子 Span。
	//
	// 参数:
	//   - key: 属性键
	//   - value: 属性值
	SetGlobalAttribute(key string, value attribute.Value)

	// SetGlobalAttributes 批量设置全局属性（键值对形式）。
	//
	// 全局属性会传播到所有子 Span。
	//
	// 参数:
	//   - attrs: 属性键值对
	//
	// 示例:
	//
	//	span.SetGlobalAttributes(
	//		attribute.String("service.name", "user-service"),
	//		attribute.String("service.version", "1.0.0"),
	//	)
	SetGlobalAttributes(attrs ...attribute.KeyValue)

	// GetGlobalAttributes 返回所有全局属性。
	GetGlobalAttributes() map[string]attribute.Attribute

	// SetInheritedAttribute 设置单个继承属性。
	//
	// 继承属性从父 Span 继承，并可以被子 Span 继承。
	//
	// 参数:
	//   - key: 属性键
	//   - value: 属性值
	SetInheritedAttribute(key string, value attribute.Value)

	// SetInheritedAttributes 批量设置继承属性（键值对形式）。
	//
	// 继承属性从父 Span 继承，并可以被子 Span 继承。
	//
	// 参数:
	//   - attrs: 属性键值对
	SetInheritedAttributes(attrs ...attribute.KeyValue)

	// GetInheritedAttributes 返回所有继承属性。
	GetInheritedAttributes() map[string]attribute.Attribute

	// GetAttributes 返回所有属性（包括私有、全局和继承属性）。
	GetAttributes() map[string]any

	// AddEvent 添加事件信息。
	//
	// 事件用于记录操作过程中的重要时刻。
	//
	// 参数:
	//   - name: 事件名称
	//   - eventType: 事件类型（如 "sql"、"http"、"cache" 等）
	//   - eventHandler: 事件处理函数，返回事件的详细信息
	//
	// 示例:
	//
	//	span.AddEvent("db.query", "sql", func() map[string]any {
	//		return map[string]any{
	//			"table": "users",
	//			"sql":   "SELECT * FROM users WHERE id = ?",
	//		}
	//	})
	AddEvent(name, eventType string, eventHandler types.Event)

	// GetEvents 返回所有事件信息。
	GetEvents() []types.SpanEvent

	// AddLog 添加日志信息。
	//
	// 参数:
	//   - log: 日志信息
	//
	// 返回值:
	//   - Span: 返回 Span 自身，支持链式调用
	//
	// 示例:
	//
	//	span.AddLog(types.SpanLog{
	//		Severity: types.SpanLogSeverityInfo,
	//		Message:  "Processing request",
	//		Time:     time.Now(),
	//	})
	AddLog(log types.SpanLog) Span

	// GetLogs 返回所有日志信息。
	GetLogs() []types.SpanLog

	// RecordError 快速记录错误信息。
	//
	// 此方法会自动提取错误的堆栈信息并设置 Span 状态为 Error。
	//
	// 参数:
	//   - err: 错误对象
	//
	// 示例:
	//
	//	if err != nil {
	//		span.RecordError(err)
	//	}
	RecordError(err error) Span

	// WithError 记录带有描述的错误信息。
	//
	// 参数:
	//   - err: 错误对象
	//   - message: 错误描述信息
	//
	// 返回值:
	//   - Span: 返回 Span 自身，支持链式调用
	//
	// 示例:
	//
	//	if err != nil {
	//		span.WithError(err, "数据库查询失败")
	//	}
	WithError(err error, message string) Span

	// GetErrorDetail 返回错误详情。
	//
	// 如果 Span 没有错误，返回 nil。
	GetErrorDetail() *types.ErrorDetail

	// SetStatus 设置 Span 的状态。
	//
	// 状态用于表示操作的执行结果（如成功、错误、警告等）。
	//
	// 参数:
	//   - status: Span 状态
	//
	// 示例:
	//
	//	span.SetStatus(types.SpanStatus{
	//		Code:    types.StatusCodeError,
	//		Message: "操作失败",
	//	})
	SetStatus(status types.SpanStatus)

	// SetResource 设置资源信息。
	//
	// 资源信息表示服务或实例的静态信息（如服务名称、版本、主机名等）。
	//
	// 参数:
	//   - resource: 资源信息
	SetResource(resource *types.ResourceInfo)

	// GetResource 返回资源信息。
	GetResource() *types.ResourceInfo

	// SetResourceUsage 设置资源使用情况。
	//
	// 资源使用情况表示操作执行时的资源消耗（如 CPU、内存、网络等）。
	//
	// 参数:
	//   - usage: 资源使用情况
	SetResourceUsage(usage *types.ResourceMetrics)

	// GetResourceUsage 返回资源使用情况。
	GetResourceUsage() *types.ResourceMetrics

	// SetMongoCollection 设置 MongoDB 导出目标集合名。
	// 空字符串表示清除显式设置，导出时由导出器使用默认集合。
	SetMongoCollection(name string) Span

	// GetMongoCollection 返回 MongoDB 导出目标集合名。
	// 空字符串表示未设置，导出时由导出器使用默认集合。
	GetMongoCollection() string

	// GetSnapshot 获取当前 Span 的快照。
	//
	// 快照包含 Span 的完整状态，用于手动导出到外部系统。
	// 配置了 SpanProcessor 时，快照所有权由 Processor 管理，本方法返回 nil。
	// 调用者必须在使用完快照后调用 span.Release() 释放资源。
	//
	// 返回值:
	//   - SpanSnapshot: Span 快照
	//
	// 示例:
	//
	//	snapshot := span.GetSnapshot()
	//	defer snapshot.Release()
	//	exporter.ExportSpan(snapshot)
	GetSnapshot() SpanSnapshot
}
