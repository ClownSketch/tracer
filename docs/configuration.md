# Tracer 配置说明

## 1. 基础配置

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `ServiceName` | 空 | 服务名称，应显式设置 |
| `SampleRate` | `0` | `0` 不采样，`0 < value < 1` 使用分布式采样，`1` 全采样 |
| `IsDebug` | `false` | 在主 Processor 外增加 Console SimpleProcessor |
| `ExporterType` | `file` | 主 Exporter 类型 |

生产环境建议固定服务名称，避免同一实例运行期间变化。
`SampleRate` 必须位于 `0` 到 `1` 之间，超出范围会导致 `InitTracer` 初始化失败。

## 2. Batch Processor

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `BatchSize` | `50` | 每个导出批次最多包含的 Span 数 |
| `BatchInterval` | `2s` | 批次未满时的刷新间隔 |
| `Workers` | `2` | 并发导出批次数 |
| `QueueSize` | 最少 `10000` | Processor 入口队列容量 |
| `FallbackDir` | `./storage/fallback` | Batch 导出失败时的本地补偿目录 |

未设置 `QueueSize` 时，容量取 `BatchSize * Workers * 4`，并保证不小于 `10000`。

生产目录建议：

```text
/data/tracer/fallback/{service}/{instance}
```

同一目录只能由一个进程写入。

## 3. 文件 Exporter

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `LogFile` | `tracer.log` | 输出文件 |
| `FileAsyncBufferSize` | `1000` | `bufio.Writer` 缓冲区字节数 |
| `FileMaxFileSize` | `30 MiB` | 单文件轮转阈值 |
| `FileMaxBackups` | `5` | 保留备份数量 |

文件 Exporter 不再维护异步队列，`FileAsyncBufferSize` 只控制 writer 缓冲区。

## 4. MongoDB Exporter

连接来源按以下优先级选择：

1. `MongoDBCollectionObj`
2. `MongoDBClient + MongoDBDatabase + MongoDBCollection`
3. `MongoDBURI + MongoDBDatabase + MongoDBCollection`

| 字段 | 默认值 | 当前作用 |
|---|---:|---|
| `MongoDBURI` | 空 | 创建内部 MongoDB Client |
| `MongoDBDatabase` | 空 | 数据库名称 |
| `MongoDBCollection` | 空 | 默认集合名称 |
| `MongoDBClient` | `nil` | 复用外部 Driver v1 Client |
| `MongoDBCollectionObj` | `nil` | 复用外部 Driver v1 Collection |
| `MongoDBTimeout` | `10s` | 连接、索引和写入超时 |
| `MongoDBMaxRetries` | `3` | 可重试写错误的最大重试次数 |
| `MongoDBRetryDelay` | `200ms` | 初始退避时间 |
| `MongoDBMaxConcurrentWrites` | `10` | 当前用于估算内部连接池大小 |
| `MongoDBAllowedCollections` | 空 | Routing Exporter 集合白名单 |

下面三个字段是历史兼容入口，MongoDB Exporter 当前没有内部异步队列：

| 字段 | 状态 |
|---|---|
| `MongoDBBatchSize` | 兼容保留，批量大小由 BatchProcessor 控制 |
| `MongoDBFlushInterval` | 兼容保留，刷新间隔由 BatchProcessor 控制 |
| `MongoDBQueueSize` | 兼容保留，实际不创建 Exporter 队列 |

调优时优先调整 `BatchSize`、`BatchInterval`、`Workers` 和 `QueueSize`。

## 5. MongoDB Routing

```go
providers.TracerConfig{
	ExporterType:      providers.ExporterTypeMongoDBRouting,
	MongoDBCollection: "gp_traces_default",
	MongoDBAllowedCollections: []string{
		"gp_traces_gateway",
		"gp_traces_worker",
	},
}
```

集合白名单为空时不限制集合。生产环境必须配置白名单，并保证集合名称来自服务端常量。

## 6. WAL

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `UseWAL` | `false` | 使用 WAL 替代 Batch+fallback |
| `WALDir` | `./storage/wal` | WAL 目录 |
| `WALSegmentSize` | `32 MiB` | 单 segment 最大大小 |
| `WALExportBatchSize` | `100` | 后台同步导出批量 |
| `WALPollInterval` | `200ms` | dispatcher 轮询间隔 |
| `WALFlushInterval` | `2ms` | 用户态缓冲刷新间隔 |
| `WALBufferSize` | `256 KiB` | `bufio.Writer` 缓冲区大小 |
| `WALSyncOnWrite` | `false` | 每条记录后是否立即 fsync |

启用 `WALSyncOnWrite` 会显著增加请求延迟，只能在明确需要该持久化语义时开启。

## 7. 配置示例

```go
config := providers.TracerConfig{
	ServiceName:       "gateway",
	SampleRate:        0.1,
	ExporterType:      providers.ExporterTypeMongoDBRouting,
	BatchSize:         200,
	BatchInterval:     time.Second,
	Workers:           8,
	QueueSize:         20000,
	FallbackDir:       "/data/tracer/fallback/gateway/node-1",
	MongoDBURI:        os.Getenv("TRACER_MONGO_URI"),
	MongoDBDatabase:   "tracer",
	MongoDBCollection: "gp_traces_default",
	MongoDBAllowedCollections: []string{
		"gp_traces_gateway",
		"gp_traces_worker",
	},
	MongoDBTimeout:    10 * time.Second,
	MongoDBMaxRetries: 3,
	MongoDBRetryDelay: 200 * time.Millisecond,
}
```

参数必须通过压测和实例资源确定，不能直接把示例值视为所有服务的统一配置。
