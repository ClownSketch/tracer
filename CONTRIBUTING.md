# 参与开发

感谢参与 Tracer 的开发。该库位于应用请求和异步任务的观测路径，修改时需要优先保证兼容性、数据完整性和主程序性能。

## 开发环境

- 使用 `.go-version` 声明的 Go 工具链
- 默认测试不依赖外部数据库
- MongoDB 集成测试需要本地或 CI MongoDB 服务

## 开发流程

1. 从最新主分支创建功能分支。
2. 阅读相邻实现、可靠性文档和现有测试。
3. 保持修改范围与 Tracer 的记录和导出职责一致。
4. 为公开行为变化补充测试和 Changelog。
5. 执行默认测试、Hook 标签测试、Race 和必要的真库回归。

## 验证命令

```bash
go mod verify
go vet ./...
go test -count=1 ./...
go test -count=1 -tags "http_hook redis_hook gorm_hook sqlx_hook" ./...
go test -race -count=1 ./...
```

MongoDB 集成验证方法见 `docs/release-checklist.md`。

## 职责约束

- Tracer 只负责采集、传播、缓冲和导出链路数据。
- 不在核心库中加入业务脱敏、告警和权限规则。
- Exporter 扩展必须通过公开注册接口接入。
- fallback 和 WAL 修改必须覆盖异常退出与恢复测试。
- 不得用同步远端写入阻塞业务请求路径。

## 公共接口

新增、删除或重命名公开接口前，需要说明兼容性、性能影响和迁移方式。破坏兼容性的变更只能进入新的主版本。
