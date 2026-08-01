# 性能基线

## 1. 如何理解 Tracer 吞吐

Tracer 是嵌入业务进程的链路库，不是独立 HTTP 服务。性能需要分层观察：

1. Span 创建和结束。
2. Processor 接收和排队。
3. Exporter 序列化与路由。
4. MongoDB、文件或网络后端的最终写入。

端到端 QPS 同时受到业务请求包含的 Span 数量、Span 内容大小、批次参数、MongoDB 写入能力和测试机 TCP 参数影响，不能单独代表 Tracer 核心性能。

## 2. Go 1.25.12 基线

测试环境：Apple M2 Pro，Go 1.25.12，`-benchmem -benchtime=3s -count=3`。

| 分层 | 场景 | 耗时 | 内存 | 分配 |
| --- | --- | ---: | ---: | ---: |
| Core | 最小 Span 完整生命周期 | 1.507-1.515 us/op | 1090 B/op | 10 allocs/op |
| Core | 3 个私有属性 | 1.658-1.662 us/op | 1210 B/op | 13 allocs/op |
| Core | 4 个传播属性 | 2.122-2.124 us/op | 2597 B/op | 21 allocs/op |
| Core | 1 个事件和 1 条日志 | 2.322-2.335 us/op | 2244 B/op | 22 allocs/op |
| Core | 富 Span 完整生命周期 | 4.241-4.342 us/op | 约 7100 B/op | 77 allocs/op |
| Core | 富 Span 高并发 | 3.859-3.918 us/op | 约 7150 B/op | 77 allocs/op |
| Processor | Batch 接收 | 1.26-1.27 us/op | 832 B/op | 12 allocs/op |
| Processor | Batch 高并发接收 | 1.14-1.15 us/op | 833 B/op | 12 allocs/op |
| Processor | WAL 接收 | 7.14-7.73 us/op | 约 3340 B/op | 56 allocs/op |
| Processor | WAL 高并发接收 | 6.07-6.18 us/op | 约 3430 B/op | 57 allocs/op |
| Exporter | MongoDB 文档构建 | 389-395 ns/op | 1112 B/op | 15 allocs/op |
| Exporter | MongoDB 文档数组准备 | 416-421 ns/op | 24 B/op | 1 allocs/op |
| Exporter | 1-128 集合内存路由 | 1.06-1.11 us/op | 1606 B/op | 26 allocs/op |

Core 分层基准使用同步释放处理器，不包含 Exporter 和外部 I/O。富 Span 场景包含 9 个属性、2 个事件、2 条日志、业务侧 map 和时间字符串构造，以及 1 us 的模拟业务等待，因此不能把其全部分配归因于 Tracer。Batch 高频入口约可完成 79-87 万次 `OnEnd` 调用每秒。

当前版本通过传播属性延迟初始化、属性管理器原地更新、默认状态复用、快照容器整体复用，以及 Tracer 自有事件外层容器的所有权移交，将同一富 Span 基准从约 `9460 B/op、95 allocs/op` 降至约 `7100 B/op、77 allocs/op`。内存下降约 25%，分配次数下降约 19%，事件载荷、日志字段和普通可变属性仍会在 Span 结束时深拷贝，异步导出期间不会引用调用方后续修改的数据。

若继续保持公开 API、时间字段和导出文档结构不变，下一阶段合理目标约为 `70-75 allocs/op`。进一步明显下降需要重新设计事件时间类型、采样结果属性或导出器输入模型，这些属于公开协议变更，应在独立版本提案和兼容性评审后进行，不作为常规内部优化混入补丁。

## 3. 端到端结果

本地单节点 MongoDB 的路由写入结果：

- 1 个集合：约 13.1-13.3 万 Span/s。
- 8 个集合：约 5.6-9.6 万 Span/s。
- 32 个集合：约 2.9-5.0 万 Span/s。

结果范围来自 32/64 并发与 50/100 批次的两组参数。纯内存路由从 1 个集合增加到 128 个集合仍保持约 1.1 us/op，因此多集合下的下降主要来自 MongoDB 多集合写入和索引维护，不是路由表查询。

完整 HTTP 链路使用 8192 个请求、128 并发、每个请求 3 条富 Span，约 2.45 秒完成并精确写入 24576 条文档，对应约 3340 请求/s 和 1 万 Span/s。该结果包含 HTTP、Span 构建、Batch 排队和 MongoDB 最终持久化，不代表任一单层的独立上限。

## 4. 发布判定

- 不允许出现 Span 数据竞争。
- Batch 队列没有无原因丢弃。
- MongoDB E2E 文档数量必须与预期完全一致。
- 版本升级后使用相同工具链和参数对比三轮结果。
- 分别运行最小、属性、事件日志和富 Span 基准，避免用单一复杂场景替代 Core 成本判断。
- 高频业务优先减少单条 Span 的事件、日志和大字段数量，避免把响应正文等大对象重复写入多个 Span。
