# Tracer

[![CI](https://github.com/ClownSketch/tracer/actions/workflows/ci.yml/badge.svg)](https://github.com/ClownSketch/tracer/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ClownSketch/tracer.svg)](https://pkg.go.dev/github.com/ClownSketch/tracer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

`Tracer` 是面向 Go 服务的分布式链路追踪库。它负责创建和传播 Trace、记录 Span、批量导出数据，并在远端暂时不可用时提供本地补偿能力。

## 设计目标

- `Span.End()` 只冻结快照并交给 Processor，不执行远端 I/O。
- `BatchSpanProcessor` 是默认链路中唯一的异步调度层。
- Exporter 只负责把收到的批次同步写入目标后端。
- 导出失败或队列拥塞时，由 Processor 统一写入 fallback。
- 需要本地先持久化的场景，可以显式启用 WAL Processor。
- 初始化和关闭错误必须由宿主服务感知。

## 主链

```text
TracerProvider
  -> Tracer.Start(ctx, name)
  -> Span
  -> Span.End()
  -> SpanSnapshot
  -> SpanProcessor
       -> BatchProcessor -> Exporter
                         -> fallback
       -> WALProcessor   -> local WAL -> SyncSpanExporter
```

## 环境要求

- Go `1.25.12`
- MongoDB 可选，仅 MongoDB Exporter 和对应集成测试需要
- HTTP、Redis、GORM、SQLx Hook 通过 Build Tag 按需启用

## 安装

```bash
go get github.com/ClownSketch/tracer
```

## 快速开始

下面使用文件 Exporter，不依赖外部服务。

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/attribute"
	"github.com/ClownSketch/tracer/providers"
)

func main() {
	provider, err := providers.InitTracer(providers.TracerConfig{
		ServiceName:   "example-service",
		ExporterType:  providers.ExporterTypeFile,
		LogFile:       "./storage/log/traces.log",
		FallbackDir:   "./storage/fallback/example-service",
		SampleRate:    1,
		BatchSize:     100,
		BatchInterval: 2 * time.Second,
		Workers:       4,
		QueueSize:     4000,
	})
	if err != nil {
		log.Fatalf("初始化 Tracer 失败: %v", err)
	}

	tracer.SetTracerProvider(provider, "example-service")

	ctx, span := tracer.GetTracer("example-service").Start(
		context.Background(),
		"order.create",
		tracer.WithSpanKind(tracer.SpanKindInternal),
		tracer.WithForceRecord(),
	)
	span.SetAttributes(
		attribute.String("order.no", "P202607310001"),
		attribute.String("currency", "INR"),
	)
	span.End()

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		log.Printf("关闭 Tracer 失败: %v", err)
	}
}
```

`providers.InitTracer()` 是唯一的统一初始化入口。初始化失败会返回错误，宿主服务应根据自身启动策略决定终止启动或显式降级。

## 生产支持范围

| Exporter | 状态 | 用途 |
|---|---|---|
| `file` | 生产可用 | 本地文件与调试环境 |
| `mongodb` | 生产可用 | 固定集合存储 |
| `mongodb_routing` | 生产可用 | 按 Span 路由到受控集合 |
| `console` | 仅开发 | 终端调试 |
| `jaeger` | 实验实现 | `InitTracer` 拒绝生产初始化 |
| `zipkin` | 实验实现 | `InitTracer` 拒绝生产初始化 |
| `otlp` | 实验实现 | 尚未进入统一生产初始化 |

MongoDB 同时保留 Driver v1 和 v2 导出实现。统一 `providers.InitTracer()` 当前使用 Driver v1；Driver v2 由显式构造器接入。

## 可靠性模式

### Batch + fallback

默认模式。请求线程不等待远端确认，适合绝大多数业务链路追踪。

- 正常数据进入 BatchProcessor 队列。
- 批次导出失败或入口队列拥塞时写入 fallback 文件。
- fallback 在启动和运行期间重试恢复。
- 远端与 fallback 同时不可用时允许丢失链路数据，不反向阻塞宿主服务。

### WAL

Span 先写入本地 WAL，再由后台同步投递并推进 checkpoint。适合需要更强本地持久化语义的场景，但会增加磁盘 I/O 和运维成本。

WAL 不是业务事务日志，也不能替代支付系统自己的审计和账务数据。

## Gin 接入

```go
router.Use(ginmiddleware.GinMiddleware())
```

跨服务入口使用：

```go
router.Use(ginmiddleware.GinCrossServiceMiddleware())
```

前者创建本地根链路，后者会从请求 Header 提取上游 Trace Context 和 baggage。

## 文档

- [公开 API](./API.md)
- [架构设计](./docs/architecture.md)
- [配置说明](./docs/configuration.md)
- [Hook 接入](./docs/hooks.md)
- [可靠性与运维](./docs/reliability.md)
- [性能基线](./docs/performance.md)
- [发布检查](./docs/release-checklist.md)

## 可运行示例

- [基础文件导出](./examples/basic/main.go)
- [Gin 与 MongoDB](./examples/gin-mongodb/main.go)
- [MongoDB 集合路由](./examples/mongodb-routing/main.go)
- [WAL 模式](./examples/wal/main.go)

示例属于当前主模块，不包含独立 `go.mod`。`go test ./...` 会参与编译检查，避免示例长期失真。

## 验证

```bash
go test ./...
go test -race ./...
go test -tags "http_hook redis_hook gorm_hook sqlx_hook" ./...
go vet ./...
```

需要 MongoDB 的集成测试与高并发验证命令见 [发布检查](./docs/release-checklist.md)。

## License

本项目基于 [MIT License](./LICENSE) 开源。
