# 可靠性与运维

## 1. 可靠性边界

Tracer 的第一原则是不拖垮宿主服务。它会尽量保存链路数据，但不提供业务事务级强一致。

| 场景 | Batch + fallback | WAL |
|---|---|---|
| 正常主路径 | 内存排队后批量导出 | 本地 WAL 后后台导出 |
| 远端暂时失败 | 写入 fallback | 保留 checkpoint 前记录 |
| 进程异常退出 | 队列中尚未落盘的数据可能丢失 | 已刷新到 WAL 的数据可恢复 |
| 本地磁盘失败 | 尝试远端；两者都失败则丢失 | 尝试直接同步远端；失败则丢失 |
| 请求延迟 | 最低 | 较高，取决于缓冲与 fsync 策略 |

## 2. Batch Processor 状态

```go
stats := batch.GetStats()
lastErr := batch.GetLastError()
```

| 字段 | 含义 |
|---|---|
| `accepted` | Processor 已接受的快照数量 |
| `exported` | 成功写入主 Exporter 的数量 |
| `fallback` | 成功写入 fallback 的数量 |
| `dropped` | 主 Exporter 与本地补偿均无法保存的数量 |
| `failures` | Processor 观察到的错误次数 |

应重点告警：

- `dropped > 0`
- `fallback` 持续增长
- `failures` 短时间快速增长
- 队列长期接近容量
- fallback 目录文件持续堆积

## 3. WAL 状态

WAL Processor 的统计包含：

- `accepted`
- `appended`
- `direct_exported`
- `dropped`
- `failures`

WAL 目录需要监控磁盘容量、segment 数量、checkpoint 推进时间和最后错误。

## 4. 目录规划

```text
/data/tracer/
  fallback/
    gateway/node-1/
    worker/node-1/
  wal/
    audit/node-1/
```

约束：

- 每个应用、实例和 Processor 使用独立目录。
- 目录位于持久磁盘，不使用系统临时目录。
- 容器环境挂载持久卷。
- 定期清理已经确认无效的隔离文件，但不能直接删除 `.active` 或 WAL segment。

## 5. 容量估算

内存队列容量近似为：

```text
QueueSize * 平均 SpanSnapshot 大小
```

Span 体积主要受以下内容影响：

- 请求和响应 Body
- SQL 与 Redis Value
- 事件数量
- 日志 Fields
- 错误堆栈

调优顺序：

1. 控制单 Span 体积。
2. 观察 BatchProcessor 队列和 fallback。
3. 调整 `BatchSize` 与 `BatchInterval`。
4. 调整 `Workers` 与 `QueueSize`。
5. 最后调整 MongoDB 连接池和服务端容量。

## 6. MongoDB

- Tracer 自建 Client 时负责关闭连接。
- 外部 Client/Collection 由宿主负责关闭。
- Routing Exporter 使用固定白名单。
- 压测使用独立数据库，不能对生产集合运行测试。
- MongoDB 唯一键冲突按幂等成功处理，其他错误按配置重试。

## 7. 优雅关闭

宿主服务应先停止接收新请求，再关闭 Tracer：

```text
stop HTTP admission
  -> wait in-flight requests
  -> provider.Shutdown(timeout)
  -> close application-owned MongoDB client
  -> exit process
```

如果第一次 `Shutdown` 等待超时，后台清理仍在继续。宿主可以在退出前再次调用并继续等待，但必须设置进程整体退出上限。

## 8. 故障演练

发布前至少验证：

1. MongoDB 短暂不可用时进入 fallback。
2. MongoDB 恢复后 fallback 自动补投。
3. fallback 文件尾部不完整时恢复完整记录并隔离损坏部分。
4. WAL 重启后从 checkpoint 继续。
5. Shutdown 首次超时后第二次可以取得最终结果。
6. 高并发下无数据竞争、快照重复释放和 goroutine 泄漏。
