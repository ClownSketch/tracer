package trace

import (
	"time"

	"github.com/ClownSketch/tracer/types"
)

// SpanSnapshot 定义SpanSnapshot，属于Span快照接口
type SpanSnapshot interface {
	GetStartTime() time.Time                  // GetStartTime 获取当前 Span 开始时间
	GetEndTime() time.Time                    // GetEndTime 获取当前 Span 结束时间
	GetSpanName() string                      // GetSpanName 获取当前 Span 名称
	GetSpanKind() types.SpanKind              // GetSpanKind 获取当前 Span 类型
	GetSpanTraceID() string                   // GetSpanTraceID 获取当前 Span 的 TraceID
	GetSpanID() string                        // GetSpanID 获取当前 Span 的 SpanID
	GetSpanParentSpanID() string              // GetSpanParentSpanID 获取当前 Span 的 ParentSpanID
	GetLinkedSpans() []types.SpanContext      // GetLinkedSpans 获取关联 Span
	GetAttributes() map[string]any            // GetAttributes 获取所有属性
	GetEvents() []types.SpanEvent             // GetEvent 获取所有事件信息
	GetLogs() []types.SpanLog                 // GetLogs 获取所有日志信息
	GetErrorDetail() *types.ErrorDetail       // GetErrorDetail 获取错误详情
	GetStatus() types.SpanStatus              // GetStatus 获取当前 Span 的状态
	GetResource() *types.ResourceInfo         // GetResource 获取资源信息
	GetResourceUsage() *types.ResourceMetrics // GetResourceUsage 获取资源使用情况
	GetMongoCollection() string               // GetMongoCollection 获取 MongoDB 导出目标集合名，空表示使用导出器默认集合
	Release()                                 // Release 释放快照资源，将池化的容器归还到对象池中
}
