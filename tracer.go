// Package tracer provides a production-oriented distributed tracing library for Go.
//
// Current default design:
//   - Span.End only freezes a snapshot and hands it to the processor
//   - BatchSpanProcessor is the only async scheduling layer
//   - exporters receive a batch and write directly to the target backend
//   - fallback is handled uniformly by the processor
//
// For practical usage, prefer providers.InitTracerE(...) and see:
//   - README.md
//   - API.md
//   - https://pkg.go.dev/github.com/ClownSketch/tracer
package tracer

import (
	"time"

	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
	"github.com/ClownSketch/tracer/types/operation"
)

type (
	// Tracer 是用于创建和管理 Span 的接口。
	//
	// 每个服务应该有自己的 Tracer 实例，通常从 TracerProvider 获取。
	// 参见 trace.Tracer 接口的详细文档。
	Tracer = trace.Tracer

	// TracerProvider 是用于提供 Tracer 实例的接口。
	//
	// TracerProvider 管理 Tracer 及其关联资源的生命周期。
	// 参见 trace.TracerProvider 接口的详细文档。
	TracerProvider = trace.TracerProvider

	// SyncSpanExporter 是支持同步确认导出的导出器接口。
	//
	// 它适用于 WAL 这类需要“远端成功后再 ACK 本地日志”的场景。
	SyncSpanExporter = trace.SyncSpanExporter

	// SpanExporter 是用于将 Span 数据导出到外部系统的接口。
	SpanExporter = trace.SpanExporter

	// Span 表示分布式追踪中的一个操作。
	//
	// Span 包含操作的开始时间、结束时间、属性、事件、日志、状态等信息。
	// 参见 trace.Span 接口的详细文档。
	Span = trace.Span

	// ==================== 类型别名导出 ====================
	// 以下类型从 types 包导出，方便外部用户直接使用，无需导入 types 包

	// SpanOptions 是 Span 配置选项函数类型。
	//
	// 用于在创建 Span 时配置 Span 的属性（如 SpanKind、强制记录等）。
	//
	// 示例:
	//
	//	ctx, span := tracer.Start(ctx, "operation",
	//		tracer.WithSpanKind(tracer.SpanKindServer),
	//		tracer.WithForceRecord(),
	//	)
	SpanOptions = types.SpanOptions

	// SpanConfig 是 Span 配置结构体。
	//
	// 包含 Span 的所有配置信息，如名称、类型、属性、事件、状态等。
	SpanConfig = types.SpanConfig

	// SpanKind 表示 Span 在分布式系统中的类型。
	//
	// 常见的 SpanKind 包括：
	//   - SpanKindServer: 服务端 Span（接收请求）
	//   - SpanKindClient: 客户端 Span（发送请求）
	//   - SpanKindInternal: 内部 Span（服务内部操作）
	//   - SpanKindProducer: 生产者 Span（消息队列生产者）
	//   - SpanKindConsumer: 消费者 Span（消息队列消费者）
	//   - SpanKindCron: 定时任务 Span
	//   - SpanKindAsync: 异步 Span
	SpanKind = types.SpanKind

	// StatusCode 表示 Span 的执行状态码。
	//
	// 常见的状态码包括：
	//   - StatusCodeUnset: 未设置
	//   - StatusCodeOk: 成功
	//   - StatusCodeError: 错误
	//   - StatusCodeWarning: 警告
	//   - StatusCodeInfo: 信息
	//   - StatusCodeDebug: 调试
	StatusCode = types.StatusCode

	// SpanStatus 表示 Span 的执行状态。
	//
	// 包含状态码和状态消息。
	//
	// 示例:
	//
	//	span.SetStatus(tracer.SpanStatus{
	//		Code:    tracer.StatusCodeError,
	//		Message: "操作失败",
	//	})
	SpanStatus = types.SpanStatus

	// SpanContext 包含 Span 的上下文信息，用于在服务间传播追踪信息。
	//
	// SpanContext 包含 TraceID 和 SpanID，用于建立 Span 之间的父子关系。
	//
	// 示例:
	//
	//	spanCtx := span.SpanContext()
	//	traceID := spanCtx.TraceID
	//	spanID := spanCtx.SpanID
	SpanContext = types.SpanContext

	// SpanLog 表示 Span 的日志信息。
	//
	// 包含日志级别、消息、时间等信息。
	//
	// 示例:
	//
	//	span.AddLog(tracer.SpanLog{
	//		Severity: tracer.SpanLogSeverityInfo,
	//		Message:  "处理请求",
	//		Time:     time.Now(),
	//	})
	SpanLog = types.SpanLog

	// SpanLogSeverity 表示日志的严重程度级别。
	//
	// 常见的级别包括：Debug、Info、Warn、Error、Fatal、Panic 等。
	SpanLogSeverity = types.SpanLogSeverity

	// SpanEvent 表示 Span 的事件信息。
	//
	// 事件用于记录操作过程中的重要时刻。
	//
	// 示例:
	//
	//	span.AddEvent("db.query", "sql", func() map[string]any {
	//		return map[string]any{"query": "SELECT * FROM users"}
	//	})
	SpanEvent = types.SpanEvent

	// ErrorDetail 表示错误的详细信息。
	//
	// 包含错误消息、堆栈信息等。
	//
	// 示例:
	//
	//	if err != nil {
	//		span.RecordError(err)
	//		errorDetail := span.GetErrorDetail()
	//		if errorDetail != nil {
	//			log.Printf("错误: %s", errorDetail.Message)
	//		}
	//	}
	ErrorDetail = types.ErrorDetail

	// StackFrame 表示错误堆栈中的单个堆栈帧。
	//
	// 包含文件名、函数名、行号等信息。
	StackFrame = types.StackFrame

	// ResourceInfo 表示服务或实例的静态信息。
	//
	// 包含服务名称、版本、主机名、环境等信息。
	//
	// 示例:
	//
	//	span.SetResource(&tracer.ResourceInfo{
	//		ServiceName:    "user-service",
	//		ServiceVersion: "1.0.0",
	//		HostName:       "server-01",
	//		Environment:    "production",
	//	})
	ResourceInfo = types.ResourceInfo

	// ResourceMetrics 表示资源使用情况。
	//
	// 包含 CPU、内存、网络等资源的使用情况。
	//
	// 示例:
	//
	//	span.SetResourceUsage(&tracer.ResourceMetrics{
	//		CPUUsage:    0.5,
	//		MemoryUsage: 1024 * 1024 * 100, // 100MB
	//	})
	ResourceMetrics = types.ResourceMetrics

	// SamplingDecision 表示采样决策。
	//
	// 常见的决策包括：
	//   - SamplingDecisionRecordAndSample: 记录并采样
	//   - SamplingDecisionRecordOnly: 仅记录
	//   - SamplingDecisionDrop: 丢弃
	SamplingDecision = types.SamplingDecision

	// SamplingParameters 表示采样参数。
	//
	// 包含 TraceID、SpanID 等信息，用于采样决策。
	SamplingParameters = types.SamplingParameters

	// SamplingResult 表示采样结果。
	//
	// 包含采样决策和采样原因等信息。
	SamplingResult = types.SamplingResult

	// ==================== Operation 类型别名导出 ====================
	// 以下类型从 types/operation 包导出，方便外部用户直接使用，无需导入 types/operation 包

	// SQLOperationInfo 表示数据库操作信息。
	//
	// 包含 SQL 语句、表名、操作类型等信息。
	//
	// 示例:
	//
	//	span.AddEvent("db.query", "sql", func() map[string]any {
	//		return map[string]any{
	//			"sql":   "SELECT * FROM users WHERE id = ?",
	//			"table": "users",
	//		}
	//	})
	SQLOperationInfo = operation.SQLOperationInfo

	// RedisOperationInfo 表示 Redis 操作信息。
	//
	// 包含 Redis 命令、键名、操作类型等信息。
	RedisOperationInfo = operation.RedisOperationInfo

	// RequestInfo 表示 HTTP 请求信息。
	//
	// 包含请求方法、URL、头部、参数等信息。
	RequestInfo = operation.RequestInfo

	// ResponseInfo 表示 HTTP 响应信息。
	//
	// 包含响应状态码、头部、大小等信息。
	ResponseInfo = operation.ResponseInfo

	// ExternalCallInfo 表示外部或跨服务调用信息。
	//
	// 包含目标服务、调用方法、超时时间等信息。
	ExternalCallInfo = operation.ExternalCallInfo
)

const (
	// ==================== SpanKind 常量 ====================
	// SpanKindInternal 内部 Span
	SpanKindInternal = types.SpanKindInternal

	// SpanKindClient 客户端 Span
	SpanKindClient = types.SpanKindClient

	// SpanKindServer 服务端 Span
	SpanKindServer = types.SpanKindServer

	// SpanKindProducer 生产者 Span
	SpanKindProducer = types.SpanKindProducer

	// SpanKindConsumer 消费者 Span
	SpanKindConsumer = types.SpanKindConsumer

	// SpanKindCron 定时任务 Span
	SpanKindCron = types.SpanKindCron

	// SpanKindAsync 异步 Span
	SpanKindAsync = types.SpanKindAsync

	// ==================== StatusCode 常量 ====================
	// StatusCodeUnset 未设置
	StatusCodeUnset = types.StatusCodeUnset

	// StatusCodeOk 成功
	StatusCodeOk = types.StatusCodeOk

	// StatusCodeError 错误
	StatusCodeError = types.StatusCodeError

	// StatusCodeWarning 警告
	StatusCodeWarning = types.StatusCodeWarning

	// StatusCodeInfo 信息
	StatusCodeInfo = types.StatusCodeInfo

	// StatusCodeDebug 调试
	StatusCodeDebug = types.StatusCodeDebug

	// StatusCodeTrace 追踪
	StatusCodeTrace = types.StatusCodeTrace

	// StatusCodeMetric 指标
	StatusCodeMetric = types.StatusCodeMetric

	// StatusCodeUnknown 未知
	StatusCodeUnknown = types.StatusCodeUnknown

	// ==================== SpanLogSeverity 常量 ====================
	// SpanLogSeverityDebug 调试
	SpanLogSeverityDebug = types.SpanLogSeverityDebug

	// SpanLogSeverityInfo 信息
	SpanLogSeverityInfo = types.SpanLogSeverityInfo

	// SpanLogSeverityWarn 警告
	SpanLogSeverityWarn = types.SpanLogSeverityWarn

	// SpanLogSeverityError 错误
	SpanLogSeverityError = types.SpanLogSeverityError

	// SpanLogSeverityFatal 严重
	SpanLogSeverityFatal = types.SpanLogSeverityFatal

	// SpanLogSeverityPanic 恐慌
	SpanLogSeverityPanic = types.SpanLogSeverityPanic

	// SpanLogSeverityTrace 追踪
	SpanLogSeverityTrace = types.SpanLogSeverityTrace

	// SpanLogSeverityMetric 指标
	SpanLogSeverityMetric = types.SpanLogSeverityMetric

	// ==================== SamplingDecision 常量 ====================
	// SamplingDecisionRecordAndSample 记录并采样
	SamplingDecisionRecordAndSample = types.SamplingDecisionRecordAndSample

	// SamplingDecisionRecordOnly 仅记录
	SamplingDecisionRecordOnly = types.SamplingDecisionRecordOnly

	// SamplingDecisionDrop 丢弃
	SamplingDecisionDrop = types.SamplingDecisionDrop

	// RecordPolicyNone 默认不导出
	RecordPolicyNone = types.RecordPolicyNone

	// RecordPolicyAlways 始终导出
	RecordPolicyAlways = types.RecordPolicyAlways

	// RecordPolicyOnError 仅错误时导出
	RecordPolicyOnError = types.RecordPolicyOnError
)

// WithSpanKind 设置 Span 的类型。
//
// SpanKind 用于表示 Span 在分布式系统中的角色（如 Server、Client、Internal 等）。
//
// 参数:
//   - kind: 要设置的 Span 类型（如 SpanKindServer、SpanKindClient 等）
//
// 返回值:
//   - SpanOptions: Span 配置选项函数
//
// 示例:
//
//	ctx, span := tracer.Start(ctx, "http.request",
//		tracer.WithSpanKind(tracer.SpanKindServer),
//	)
func WithSpanKind(kind SpanKind) SpanOptions {
	return func(c *SpanConfig) {
		c.SpanKind = kind
	}
}

// WithForceRecord 设置 Span 强制记录，不管采样器是否决定采样。
//
// 这对于重要的操作（如错误处理）很有用，确保这些操作总是被记录。
//
// 返回值:
//   - SpanOptions: Span 配置选项函数
//
// 示例:
//
//	ctx, span := tracer.Start(ctx, "critical.operation",
//		tracer.WithForceRecord(),
//	)
func WithForceRecord() SpanOptions {
	return func(c *SpanConfig) {
		c.ForceRecord = types.RecordPolicyAlways
	}
}

// WithRecordOnError 设置 Span 仅在发生错误时导出。
//
// 适合在 HTTP 中间件等入口调用一次：默认成功请求不写后端，出错时自动保留完整 Span。
// 与 WithForceRecord 互斥时以后设置的 Start 选项为准；运行时可再用 span.WithRecordOnError() 调整。
//
// 示例:
//
//	ctx, span := tracer.Start(ctx, "http.request",
//		tracer.WithSpanKind(tracer.SpanKindServer),
//		tracer.WithRecordOnError(),
//	)
//	defer span.End()
func WithRecordOnError() SpanOptions {
	return func(c *SpanConfig) {
		c.ForceRecord = types.RecordPolicyOnError
	}
}

// WithMongoCollection 设置 Span 的 MongoDB 导出目标集合名。
//
// 未设置时，MongoDB 路由导出器会使用 TracerConfig.MongoDBCollection 对应的默认集合。
// 子 Span 会继承父 Span 的集合名，除非在 Start 时通过本选项显式覆盖。
func WithMongoCollection(name string) SpanOptions {
	return func(c *SpanConfig) {
		c.MongoCollection = name
	}
}

// WithAttributeType 设置属性类型。
//
// 属性类型用于指定属性的数据类型（如 String、Int、Bool 等）。
//
// 参数:
//   - attrType: 属性类型（如 AttributeTypeString、AttributeTypeInt 等）
//
// 返回值:
//   - attribute.AttributeOption: 属性配置选项函数
//
// 示例:
//
//	span.SetAttributeConfig("user.id", attribute.StringValue("12345"),
//		tracer.WithAttributeType(attribute.AttributeTypeString),
//	)
func WithAttributeType(attrType attribute.AttributeType) attribute.AttributeOption {
	return func(config *attribute.Config) {
		config.Type = attrType
	}
}

// WithAttributeMetadata 设置属性的元数据。
//
// 元数据用于存储属性的额外信息（如描述、单位、格式等）。
//
// 参数:
//   - metadata: 元数据映射（键值对形式）
//
// 返回值:
//   - attribute.AttributeOption: 属性配置选项函数
//
// 示例:
//
//	span.SetAttributeConfig("response.time", attribute.Float64Value(123.45),
//		tracer.WithAttributeMetadata(map[string]any{
//			"unit": "ms",
//			"description": "响应时间",
//		}),
//	)
func WithAttributeMetadata(metadata map[string]any) attribute.AttributeOption {
	return func(config *attribute.Config) {
		config.MetaData = metadata
	}
}

// ==================== Processor 相关方法 ====================

// BatchSpanProcessorOption 是批处理器的配置选项类型。
//
// 这是 processor.BatchSpanProcessorOption 的别名，方便用户直接通过 tracer 包使用。
type BatchSpanProcessorOption = processor.BatchSpanProcessorOption

// WALSpanProcessorOption 是 WAL 处理器的配置选项类型。
type WALSpanProcessorOption = processor.WALSpanProcessorOption

// NewBatchSpanProcessor 创建批处理器实例。
//
// 批处理器负责批量处理 Span 数据，提高导出效率。
// 这是 processor.NewBatchSpanProcessor 的便捷包装，用户可以直接通过 tracer 包调用。
//
// 参数:
//   - exporter: Span 导出器，用于导出处理后的 Span 数据
//   - opts: 批处理器配置选项（可选），如批次大小、工作协程数、刷新间隔等
//
// 返回值:
//   - trace.SpanProcessor: Span 处理器实例
//
// 示例:
//
//	processor := tracer.NewBatchSpanProcessor(
//		exporter,
//		tracer.WithBatchSize(100),
//		tracer.WithWorkers(5),
//		tracer.WithFlushInterval(2*time.Second),
//		tracer.WithQueueSize(1000),
//		tracer.WithFallbackDir("/tmp/tracer_fallback"),
//	)
//
// Deprecated: 生产代码应使用 NewBatchSpanProcessorE 接收初始化错误。
func NewBatchSpanProcessor(exporter trace.SpanExporter, opts ...BatchSpanProcessorOption) trace.SpanProcessor {
	return processor.NewBatchSpanProcessor(exporter, opts...)
}

// NewBatchSpanProcessorE 创建批处理器并返回初始化错误。
// 生产代码应使用该入口确保 fallback 目录错误在启动阶段暴露。
func NewBatchSpanProcessorE(exporter trace.SpanExporter, opts ...BatchSpanProcessorOption) (trace.SpanProcessor, error) {
	return processor.NewBatchSpanProcessorE(exporter, opts...)
}

// NewWALSpanProcessor 创建 WAL 主路径处理器。
// @return trace.SpanProcessor WAL 处理器
// @return error 初始化错误
func NewWALSpanProcessor(exporter trace.SyncSpanExporter, opts ...WALSpanProcessorOption) (trace.SpanProcessor, error) {
	return processor.NewWALSpanProcessor(exporter, opts...)
}

// WithBatchSize 设置批次大小。
//
// 批次大小决定了每次批量导出多少个 Span。
// 较大的批次大小可以提高导出效率，但会增加内存使用和延迟。
//
// 参数:
//   - batchSize: 批次大小（建议值：50-1000，根据 QPS 调整）
//
// 返回值:
//   - BatchSpanProcessorOption: 批处理器配置选项
//
// 示例:
//
//	processor := tracer.NewBatchSpanProcessor(exporter,
//		tracer.WithBatchSize(500), // 高 QPS 场景
//	)
func WithBatchSize(batchSize int) BatchSpanProcessorOption {
	return processor.WithBatchSize(batchSize)
}

// WithWorkers 设置工作协程数。
//
// 工作协程数决定了并发处理 Span 的 goroutine 数量。
// 建议设置为 CPU 核心数的 2 倍。
//
// 参数:
//   - workers: 工作协程数（建议值：CPU 核心数 * 2）
//
// 返回值:
//   - BatchSpanProcessorOption: 批处理器配置选项
//
// 示例:
//
//	processor := tracer.NewBatchSpanProcessor(exporter,
//		tracer.WithWorkers(10), // 5 核 CPU
//	)
func WithWorkers(workers int) BatchSpanProcessorOption {
	return processor.WithWorkers(workers)
}

// WithFlushInterval 设置刷新间隔。
//
// 刷新间隔决定了多长时间强制刷新一次批次，即使批次未满。
// 较短的刷新间隔可以减少延迟，但会增加网络开销。
//
// 参数:
//   - interval: 刷新间隔（建议值：500ms-5s，根据延迟要求调整）
//
// 返回值:
//   - BatchSpanProcessorOption: 批处理器配置选项
//
// 示例:
//
//	processor := tracer.NewBatchSpanProcessor(exporter,
//		tracer.WithFlushInterval(2*time.Second), // 2 秒刷新一次
//	)
func WithFlushInterval(interval time.Duration) BatchSpanProcessorOption {
	return processor.WithFlushInterval(interval)
}

// WithQueueSize 设置队列大小。
//
// 队列大小决定了可以缓存的 Span 数量。
// 队列大小应该 >= batchSize * workers * 2，以避免队列满的情况。
//
// 参数:
//   - size: 队列大小（建议值：batchSize * workers * 2）
//
// 返回值:
//   - BatchSpanProcessorOption: 批处理器配置选项
//
// 示例:
//
//	processor := tracer.NewBatchSpanProcessor(exporter,
//		tracer.WithQueueSize(10000), // 高并发场景
//	)
func WithQueueSize(size int) BatchSpanProcessorOption {
	return processor.WithQueueSize(size)
}

// WithQueueHighWaterMark 设置队列高水位线。
//
// 当队列长度超过高水位线时，会触发告警或降级策略。
//
// 参数:
//   - highWaterMark: 队列高水位线（建议值：队列大小的 80%）
//
// 返回值:
//   - BatchSpanProcessorOption: 批处理器配置选项
//
// 示例:
//
//	processor := tracer.NewBatchSpanProcessor(exporter,
//		tracer.WithQueueSize(10000),
//		tracer.WithQueueHighWaterMark(8000), // 80% 水位线
//	)
func WithQueueHighWaterMark(highWaterMark int) BatchSpanProcessorOption {
	return processor.WithQueueHighWaterMark(highWaterMark)
}

// WithFallbackDir 设置 Fallback 目录。
//
// 当导出失败时，Span 数据会自动写入 Fallback 目录中的文件。
// 这确保即使后端系统不可用，也不会丢失追踪数据。
//
// 参数:
//   - dir: Fallback 目录路径（必须存在且可写）
//
// 返回值:
//   - BatchSpanProcessorOption: 批处理器配置选项
//
// 示例:
//
//	processor := tracer.NewBatchSpanProcessor(exporter,
//		tracer.WithFallbackDir("/tmp/tracer_fallback"),
//	)
func WithFallbackDir(dir string) BatchSpanProcessorOption {
	return processor.WithFallbackDir(dir)
}

// WithWALDir 设置 WAL 目录。
func WithWALDir(dir string) WALSpanProcessorOption {
	return processor.WithWALDir(dir)
}

// WithWALSegmentSize 设置 WAL segment 大小。
func WithWALSegmentSize(size int64) WALSpanProcessorOption {
	return processor.WithWALSegmentSize(size)
}

// WithWALExportBatchSize 设置 WAL 后台投递批量大小。
func WithWALExportBatchSize(size int) WALSpanProcessorOption {
	return processor.WithWALExportBatchSize(size)
}

// WithWALPollInterval 设置 WAL 后台轮询间隔。
func WithWALPollInterval(interval time.Duration) WALSpanProcessorOption {
	return processor.WithWALPollInterval(interval)
}

// WithWALFlushInterval 设置 WAL 用户态缓冲刷新间隔。
func WithWALFlushInterval(interval time.Duration) WALSpanProcessorOption {
	return processor.WithWALFlushInterval(interval)
}

// WithWALBufferSize 设置 WAL 缓冲区大小。
func WithWALBufferSize(size int) WALSpanProcessorOption {
	return processor.WithWALBufferSize(size)
}

// WithWALSyncOnWrite 设置是否每条写入后立即 fsync。
func WithWALSyncOnWrite(syncOnWrite bool) WALSpanProcessorOption {
	return processor.WithWALSyncOnWrite(syncOnWrite)
}

// ==================== Sampler 相关方法 ====================

// NewAlwaysSampleSampler 创建一个总是采样的采样器。
//
// 此采样器会采样所有 Span，适用于开发环境或低 QPS 场景。
// 这是 sampler.NewAlwaysSampleSampler 的便捷包装，用户可以直接通过 tracer 包调用。
//
// 返回值:
//   - trace.SpanSampler: 采样器实例
//
// 示例:
//
//	sampler := tracer.NewAlwaysSampleSampler()
//	provider := providers.NewTracerProvider(
//		providers.WithSampler(sampler),
//	)
func NewAlwaysSampleSampler() trace.SpanSampler {
	return sampler.NewAlwaysSampleSampler()
}

// NewNeverSampleSampler 创建一个从不采样的采样器。
//
// 此采样器不会采样任何 Span，适用于禁用追踪的场景。
// 这是 sampler.NewNeverSampleSampler 的便捷包装，用户可以直接通过 tracer 包调用。
//
// 返回值:
//   - trace.SpanSampler: 采样器实例
//
// 示例:
//
//	sampler := tracer.NewNeverSampleSampler()
//	provider := providers.NewTracerProvider(
//		providers.WithSampler(sampler),
//	)
func NewNeverSampleSampler() trace.SpanSampler {
	return sampler.NewNeverSampleSampler()
}

// NewProbabilitySampler 创建一个概率采样器。
//
// 概率采样器根据指定的采样概率随机决定是否采样 Span。
// 这是 sampler.NewProbabilitySampler 的便捷包装，用户可以直接通过 tracer 包调用。
//
// 参数:
//   - probability: 采样概率（0.0-1.0），0.0 表示不采样，1.0 表示全采样
//
// 返回值:
//   - trace.SpanSampler: 采样器实例
//
// 示例:
//
//	sampler := tracer.NewProbabilitySampler(0.1) // 10% 采样率
//	provider := providers.NewTracerProvider(
//		providers.WithSampler(sampler),
//	)
func NewProbabilitySampler(probability float64) trace.SpanSampler {
	return sampler.NewProbabilitySampler(probability)
}
