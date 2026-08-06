# Tracer 架构设计

## 1. 组件边界

| Package | 职责 |
|---|---|
| `trace` | 定义 Tracer、Span、Processor、Exporter、Sampler 等稳定接口 |
| `core` | 实现 Span 生命周期、快照冻结、对象池和记录策略 |
| `providers` | 组装 Sampler、Processor、Exporter，并管理 Provider 生命周期 |
| `processor` | 排队、聚合、并发导出、fallback、WAL 和快照释放 |
| `exporter` | 将批次同步写入文件、MongoDB 或其他后端 |
| `fallback` | SpanSnapshot 的补偿文件和 WAL 序列化格式 |
| `propagation` | 在载体中 Inject/Extract Trace Context 和 baggage |
| `middleware` | 为 Gin 等服务入口创建和结束 Span |
| `hooks` | 按 Build Tag 提供 HTTP、Redis、GORM、SQLx 自动事件记录 |
| `sampler` | 决定普通 Span 是否记录 |
| `types`、`attribute` | 定义链路数据模型和属性类型 |

依赖方向以 `trace` 接口为中心。Provider 是组合入口，可以依赖具体 Processor 和 Exporter；核心接口不能反向依赖 Provider。

## 2. Span 生命周期

```text
Tracer.Start
  -> 读取父 SpanContext
  -> 执行采样与记录策略
  -> 创建 SpanImpl
  -> 写入属性、事件、日志和错误
  -> Span.End
  -> 冻结 SpanSnapshot
  -> Processor.OnEnd
```

配置了 Processor 时，Snapshot 所有权在 `OnEnd` 后转移给 Processor。调用方不能再保存或释放该快照。

没有 Processor 的手动模式下，`GetSnapshot()` 返回快照，调用方使用后必须调用 `Release()`。

## 3. Batch 模式

```text
OnEnd
  -> admission lock
  -> queue
  -> 单聚合循环形成 batch
  -> export semaphore 控制并发
  -> Exporter.ExportSpans
       -> success: Release snapshots
       -> failure: fallback
           -> success: Release snapshots
           -> failure: 记录 dropped 并 Release
```

设计约束：

- Processor 是唯一异步调度层。
- Exporter 不创建内部队列。
- 队列到达高水位时触发提前 flush。
- 队列满时优先写 fallback。
- 远端和 fallback 同时失败时允许丢弃链路，不能阻塞业务主流程。

## 4. fallback

fallback 是 Batch 模式的本地补偿文件，不是 WAL。

- 初始化阶段验证目录可写性。
- 每条记录独立校验和序列化，单条非法 Span 不影响同批其他数据。
- 启动后立即恢复一次，之后按周期继续恢复。
- `.active` 文件在异常退出后能够重新参与恢复。
- 恢复成功后再删除对应本地记录。

fallback 目录必须按应用和实例隔离，不能让多个进程共享同一个写入目录。

## 5. WAL 模式

```text
OnEnd
  -> serialize
  -> append segment
  -> optional fsync
  -> release in-memory snapshot
  -> dispatcher reads WAL
  -> SyncSpanExporter ACK
  -> advance checkpoint
```

WAL 通过长度、CRC 和最大记录大小保护本地记录。远端未确认时不会推进 checkpoint。

WAL 写入失败时会尝试直接同步导出；如果本地和远端同时失败，链路可能丢失。

## 6. Exporter

Exporter 接收已经形成的批次并同步返回结果。连接管理遵循：

- Exporter 自己通过 URI 创建的连接，由 Exporter 在 Shutdown 时关闭。
- 宿主传入的 Client 或 Collection，由宿主关闭。
- MongoDB Exporter 使用无序批量写，并对可重试错误执行有限重试。
- MongoDB Routing Exporter 根据 Span 集合名选择目标集合，并支持白名单。

## 7. 传播

跨服务传播由 `propagation` 完成：

```text
上游 Span Context
  -> Inject HTTP Header
  -> 网络请求
  -> Extract HTTP Header
  -> 下游 Tracer.Start
```

未采样服务可以不生成本地 Span。下游是否继续记录由下游采样和记录策略决定。

## 8. 关闭

```text
Provider.Shutdown
  -> stop accepting new work
  -> wait processor queue/export
  -> recover/close fallback or WAL
  -> close managed exporter resources
```

关闭过程只启动一次。调用方的 Context 仅控制等待时间，后台清理不会因为第一次等待超时而停止。

## 9. 扩展边界

新增后端时实现 `trace.SpanExporter`，需要 WAL 时再实现 `trace.SyncSpanExporter`。不要在新 Exporter 内增加队列、fallback 或新的生命周期管理层。

新增框架接入时放在 `middleware` 或 `hooks`，不能把具体框架依赖引入 `trace` 和 `core`。

## 10. 非目标

- 不替代业务审计日志。
- 不承担宿主业务数据或业务状态的可靠存储。
- 不在库内部决定业务数据脱敏规则。
- 不保证远端 Exporter 与业务数据库事务一致。
