# Changelog

本文档记录 Tracer 的公开接口、可靠性和兼容性变更。

## [Unreleased]

### Added

- 增加 MongoDB Driver v2 显式导出接口，并保留 Driver v1 接入能力。
- 增加 Batch fallback、WAL、MongoDB 真库和高并发链路测试。
- 增加自定义 Exporter 注册入口，允许主项目扩展导出目标。
- 增加 GitHub Actions 默认测试、Hook 标签测试、Race 和 MongoDB 真库回归。

### Changed

- `providers.InitTracer` 统一返回 `(trace.TracerProvider, error)`，初始化失败不再静默降级。
- `NewBatchSpanProcessor` 统一返回 `(trace.SpanProcessor, error)`，删除重复的 `NewBatchSpanProcessorE`。
- Exporter 注册改为显式声明配置样本，并拒绝空配置、类型不一致和重复注册。
- MongoDB 集合路由统一使用中立的 `ExportRoute` 语义。
- 项目目标工具链升级为 Go 1.25.12。
- 优化 Span 属性、快照和事件容器的生命周期；富 Span 基准内存下降约 25%，分配次数下降约 19%。
- 增加最小生命周期、私有属性、传播属性和事件日志的分层 Core 基准。

### Fixed

- 修复 Batch Processor 首次关闭超时后无法继续完成清理的问题。
- 修复 fallback 活跃文件在进程异常退出后无法恢复的问题。
- 修复单条非法 Span 导致整批数据被丢弃的问题。
- 修复 fallback 写入与恢复大小限制不一致的问题。
- 修复非 `map[string]any` 类型日志字段在 fallback/WAL 恢复时丢失的问题。

## [1.0.0-rc.1] - 待发布

- 首个候选发布版本；发布内容以 `Unreleased` 为准。
