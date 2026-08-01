# 发布检查清单

## 1. 基础检查

```bash
go version
go mod verify
go vet ./...
go test ./...
```

项目要求 Go `1.25.12`。执行前确认 `go env GOVERSION` 与 `.go-version` 一致。

## 2. Hook 构建标签

```bash
go test -tags "http_hook redis_hook gorm_hook sqlx_hook" ./...
```

默认桩和正式 Hook 必须分别通过。

## 3. Race

```bash
go test -race ./...
go test -race -tags "http_hook redis_hook gorm_hook sqlx_hook" ./hooks ./middleware/...
```

## 4. MongoDB 真库

使用专门测试数据库：

```bash
export MONGO_URI='mongodb://127.0.0.1:27017'
export TRACER_E2E_MONGO_DATABASE='tracer_e2e_local'

go test ./integration \
  -run 'TestMongoDBExporter_(FullFlowE2E|WALFullFlowE2E|EventAttributesSchemaE2E)|TestMongoDBV2Exporter_EventAttributesSchemaE2E' \
  -count=1 -v
```

高并发 HTTP 全链测试：

```bash
TRACER_E2E_REQUESTS=8192 \
TRACER_E2E_CONCURRENCY=128 \
go test ./integration \
  -run '^TestMongoDBExporter_FullFlowE2E$' \
  -count=1 -v
```

MongoDB 路由性能测试会清理测试集合，只能使用专用数据库：

```bash
MONGO_URI='mongodb://127.0.0.1:27017' \
TRACER_BENCH_MONGO_DATABASE='tracer_routing_bench' \
go test ./exporter \
  -run '^$' \
  -bench '^BenchmarkMongoDBRouting_HighConcurrency$' \
  -benchmem -benchtime=1x -count=1
```

## 5. 核心基准

基准阶段必须使用 `-run '^$'`，防止再次执行普通压力测试。

```bash
go test ./core \
  -run '^$' -bench '^BenchmarkSpan_CoreLifecycle_' \
  -benchmem -benchtime=3s -count=3

go test ./core \
  -run '^$' -bench '^BenchmarkSpan_FullLifecycle_Parallel$' \
  -benchmem -benchtime=3s -count=3

go test ./processor \
  -run '^$' -bench '^(BenchmarkBatchProcessor_OnEnd|BenchmarkWALSpanProcessor_OnEnd)$' \
  -benchmem -benchtime=3s -count=3

go test ./exporter \
  -run '^$' -bench '^BenchmarkMongoDBExporter_' \
  -benchmem -benchtime=3s -count=3
```

基准结果用于比较版本变化，不能只看单次绝对数值。

## 6. 极端压力测试

百万级和五百万级场景默认跳过，只在独立压测机器上显式执行：

```bash
TRACER_STRESS_TESTS=1 go test ./core ./processor \
  -run 'TestSpan_FullLifecycle_(Performance|StressTest)|TestBatchProcessor_(PerformanceMetrics|ConcurrentStress)' \
  -count=1 -v
```

不要在开发机或常规 CI 与其他测试并行运行该命令。

## 7. 发布判定

必须满足：

- 默认和 Hook 标签测试通过。
- Race 无数据竞争。
- MongoDB Driver v1、v2 真库结构回归通过。
- Batch、fallback 和 WAL 恢复测试通过。
- `dropped` 路径有测试覆盖。
- Shutdown 超时后继续清理的行为通过。
- 高并发结果没有 Span 缺失。
- 依赖漏洞检查没有已知高危问题。
- README、API 和示例与当前公开接口一致。
- 自定义 Exporter 的值配置、指针配置和重复注册测试通过。
- `NewBatchSpanProcessor` 初始化错误能够返回给调用方。
- 分层基准与 [性能基线](./performance.md) 相比没有无法解释的明显退化。
- GitHub Actions 的质量、Race 和 MongoDB 真库任务全部通过。
