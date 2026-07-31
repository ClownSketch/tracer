# Tracer 公开 API

本文只描述宿主项目应当依赖的公开接口。`core`、Processor 内部队列、fallback 文件格式和 MongoDB 文档转换结构都不是稳定的外部契约。

## 1. 初始化

### `providers.InitTracerE`

```go
func InitTracerE(config providers.TracerConfig) (trace.TracerProvider, error)
```

生产环境统一使用该入口。它会依次完成：

1. 校验并创建 Exporter。
2. 创建 Batch 或 WAL Processor。
3. 创建 Sampler。
4. 组装 TracerProvider。
5. 在 fallback、WAL、MongoDB 或文件初始化失败时同步返回错误。

```go
provider, err := providers.InitTracerE(config)
if err != nil {
	return fmt.Errorf("初始化 tracer: %w", err)
}
tracer.SetTracerProvider(provider, config.ServiceName)
```

### `providers.InitTracer`

兼容入口。初始化失败时记录错误并返回 no-op Provider。它不会把初始化失败反馈给调用者，生产服务不应使用。

### `tracer.SetTracerProvider`

```go
func SetTracerProvider(provider trace.TracerProvider, tracerName string)
```

设置进程级默认 Provider 和 Tracer。替换 Provider 时不会自动关闭旧实例，旧实例仍由创建它的宿主负责关闭。

### `tracer.GetTracer`

```go
func GetTracer(tracerName string) trace.Tracer
```

从当前 Provider 获取命名 Tracer。空名称使用 `default`。尚未初始化时返回 no-op Tracer。

### `tracer.GetTraceID`

```go
func GetTraceID(ctx context.Context) string
```

读取当前上下文中的 TraceID。不存在有效链路时返回空字符串。

## 2. 创建 Span

```go
ctx, span := tracer.GetTracer("gateway").Start(
	parentCtx,
	"payin.create",
	tracer.WithSpanKind(tracer.SpanKindInternal),
	tracer.WithForceRecord(),
)
defer span.End()
```

必须把返回的 `ctx` 继续传给后续调用。`Span.End()` 可重复调用，但业务代码仍应只结束一次。

### SpanKind

| 常量 | 用途 |
|---|---|
| `SpanKindServer` | 接收 HTTP/RPC 请求 |
| `SpanKindClient` | 调用外部服务 |
| `SpanKindInternal` | 服务内部操作 |
| `SpanKindProducer` | 发送消息 |
| `SpanKindConsumer` | 消费消息 |
| `SpanKindCron` | 定时任务 |
| `SpanKindAsync` | 异步后台任务 |

## 3. 记录策略

### `WithForceRecord`

无论采样器如何决策都记录当前 Span。适合支付命令、人工操作和异常处理等关键链路。

### `WithRecordOnError`

成功时不导出，发生以下情况时导出：

- 调用 `RecordError`。
- 调用 `WithError`。
- 设置 `StatusCodeError`。

### `WithForceNotRecord`

通过 `span.WithForceNotRecord()` 在运行时禁止导出当前 Span。

后设置的记录策略覆盖前面的策略。

## 4. 属性、事件与日志

### 属性

```go
span.SetAttributes(
	attribute.String("order.no", orderNo),
	attribute.Int64("amount", amount),
	attribute.Bool("retry", false),
)
```

- 私有属性只属于当前 Span。
- Global 属性会传给子 Span。
- Inherited 属性从父 Span 继承，并继续传给子 Span。

```go
span.SetGlobalAttributes(attribute.String("environment", "production"))
span.SetInheritedAttributes(attribute.String("merchant.no", "ME000001"))
```

### 事件

事件用于记录一个操作内部的阶段信息。

```go
span.AddEvent("channel.request", "http", func() map[string]any {
	return map[string]any{
		"channel_code": "UPI_001",
		"attempt":      1,
	}
})
```

### 日志

```go
span.AddLog(tracer.SpanLog{
	Timestamp: time.Now().Format(time.RFC3339Nano),
	Severity:  tracer.SpanLogSeverityInfo,
	Message:   "通道请求已受理",
	Fields: map[string]any{
		"channel_code": "UPI_001",
	},
})
```

Tracer 只负责记录调用方传入的数据，不负责业务脱敏。宿主项目必须在写入前决定哪些内容允许进入链路。

## 5. 错误和状态

```go
if err != nil {
	span.WithError(err, "创建代收订单失败")
	span.SetStatus(tracer.SpanStatus{
		Code:    tracer.StatusCodeError,
		Message: err.Error(),
	})
}
```

- `RecordError(err)`：记录错误和堆栈。
- `WithError(err, message)`：记录带业务描述的错误。
- `SetStatus(status)`：显式设置 Span 状态。

## 6. MongoDB 集合路由

仅 `mongodb_routing` Exporter 使用该能力。

```go
ctx, span := tracer.GetTracer("gateway").Start(
	ctx,
	"gateway.payin",
	tracer.WithMongoCollection("gp_traces_gateway"),
)
defer span.End()
```

也可以在创建后设置：

```go
span.SetMongoCollection("gp_traces_gateway")
```

子 Span 默认继承父 Span 的集合名称。生产环境应通过 `MongoDBAllowedCollections` 配置白名单，不能让请求参数直接决定集合名称。

## 7. HTTP Context 传播

### 服务端

```go
router.Use(ginmiddleware.GinCrossServiceMiddleware())
```

该中间件从请求 Header 提取 Trace Context 和 baggage，并把新 Span Context 写回请求上下文。

### 客户端

```go
req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
if err != nil {
	return err
}

propagator := propagationhttp.NewHTTPPropagator()
carrier := propagationhttp.NewHTTPHeaderCarrier(req.Header)
if err := propagator.Inject(ctx, carrier); err != nil {
	return err
}
```

下游服务必须使用支持 Extract 的中间件或 Propagator，才能继续同一条 Trace。

## 8. Processor

### Batch Processor

```go
processor, err := tracer.NewBatchSpanProcessorE(
	exporter,
	tracer.WithBatchSize(100),
	tracer.WithWorkers(4),
	tracer.WithQueueSize(4000),
	tracer.WithQueueHighWaterMark(3200),
	tracer.WithFlushInterval(2*time.Second),
	tracer.WithFallbackDir("./storage/fallback/gateway/node-1"),
)
```

生产代码必须使用 `NewBatchSpanProcessorE`。旧的 `NewBatchSpanProcessor` 无法返回 fallback 初始化错误。

如果需要运行状态，可以对 Processor 做具体类型断言：

```go
batch := processor.(*processorpkg.BatchSpanProcessor)
stats := batch.GetStats()
lastErr := batch.GetLastError()
```

统计字段包括 `accepted`、`exported`、`fallback`、`dropped` 和 `failures`。

### WAL Processor

```go
processor, err := tracer.NewWALSpanProcessor(
	syncExporter,
	tracer.WithWALDir("./storage/wal/gateway/node-1"),
	tracer.WithWALSegmentSize(32*1024*1024),
	tracer.WithWALExportBatchSize(100),
	tracer.WithWALPollInterval(200*time.Millisecond),
	tracer.WithWALFlushInterval(2*time.Millisecond),
	tracer.WithWALBufferSize(256*1024),
	tracer.WithWALSyncOnWrite(false),
)
```

WAL 要求 Exporter 同时实现 `trace.SpanExporter` 和 `trace.SyncSpanExporter`。

## 9. Exporter 契约

```go
type SpanExporter interface {
	ExportSpan(span SpanSnapshot) error
	ExportSpans(spans []SpanSnapshot) error
	Shutdown(ctx context.Context) error
}
```

约定：

- Exporter 不创建第二套异步队列。
- Exporter 不执行 fallback。
- 普通 `ExportSpans` 不释放快照，释放责任属于 Processor。
- 同步 WAL 接口在返回前完成远端确认。
- `Shutdown` 必须支持重复调用和超时等待。

### 自定义 Exporter

```go
type MyConfig struct{}

func (MyConfig) ExporterType() providers.ExporterType {
	return providers.ExporterType("my_exporter")
}

providers.RegisterExporter(func(cfg providers.ExporterConfig[MyConfig]) (trace.SpanExporter, error) {
	return newMyExporter(cfg.Options)
})
```

自定义 Exporter 需要自行编写批量写入、错误传播、重复关闭和并发测试。

## 10. Sampler

```go
sampler.NewAlwaysSampleSampler()
sampler.NewNeverSampleSampler()
sampler.NewProbabilitySampler(0.1)
sampler.NewDistributedSampler(0.1)
```

`InitTracerE` 中：

- `0 < SampleRate < 1` 使用 DistributedSampler。
- 其他值使用 AlwaysSampleSampler。
- `WithForceRecord` 可以覆盖采样结果。

## 11. 可选 Hook

Hook 默认编译为桩实现，通过 Build Tag 启用正式实现：

```bash
go build -tags "http_hook redis_hook gorm_hook sqlx_hook" ./cmd/your-app
```

接入方法见 [Hook 文档](./docs/hooks.md)。

## 12. 关闭

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()

if err := provider.Shutdown(shutdownCtx); err != nil {
	return fmt.Errorf("关闭 tracer: %w", err)
}
```

Provider 会级联等待 Processor 排空队列、恢复 fallback 或推进 WAL，并关闭由 Exporter 自己创建的连接。外部传入的 MongoDB Client 或 Collection 仍由宿主服务关闭。

第一次关闭即使超时，后台清理仍会继续；后续再次调用 `Shutdown` 可以继续等待同一次清理结果。
