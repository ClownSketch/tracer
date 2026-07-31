# Hook 接入说明

## 1. 构建标签

Hook 默认使用桩实现。宿主应用通过 Build Tag 启用需要的正式实现：

| 标签 | 能力 |
|---|---|
| `http_hook` | HTTP 客户端调用事件 |
| `redis_hook` | go-redis 单命令与 Pipeline 事件 |
| `gorm_hook` | GORM Create、Query、Update、Delete、Raw、Row 事件 |
| `sqlx_hook` | SQLx Query、Exec、Get、Select、事务和 Statement 事件 |

```bash
go build -tags "redis_hook gorm_hook" ./cmd/gateway
go test -tags "http_hook redis_hook gorm_hook sqlx_hook" ./...
```

Build Tag 必须写入项目构建脚本和 CI，不能只配置在开发人员 IDE 中。

## 2. GORM

```go
if err := db.Use(&hooks.GormHookPlugin{}); err != nil {
	return fmt.Errorf("注册 tracer gorm hook: %w", err)
}
```

数据库调用必须继续传递业务上下文：

```go
if err := db.WithContext(ctx).First(&order, "out_trade_no = ?", orderNo).Error; err != nil {
	return err
}
```

没有有效 Span 的 Context 时，Hook 不记录事件。

## 3. Redis

```go
client := redis.NewClient(options)
client.AddHook(&hooks.RedisHookPlugin{DBIndex: "1"})
```

调用 Redis 时必须使用包含 Span 的 Context：

```go
value, err := client.Get(ctx, key).Result()
```

Hook 会记录命令、Key、Value、TTL、耗时、Pipeline 和事务标记。Tracer 不负责业务脱敏，宿主项目必须控制可以进入链路的数据。

## 4. SQLx

```go
rawDB, err := sqlx.ConnectContext(ctx, "mysql", dsn)
if err != nil {
	return err
}

db := hooks.NewTracedDB(rawDB)
```

之后使用包装器提供的 Context 方法：

```go
var order Order
if err := db.GetContext(ctx, &order, query, orderNo); err != nil {
	return err
}
```

未启用 `sqlx_hook` 时，`TracedDB` 直接嵌入原始 `sqlx.DB`，不会执行追踪逻辑。

## 5. HTTP 客户端

`HTTPHookMiddleware` 是通用生命周期适配器，不会自动替换 `http.Transport`。HTTP 客户端适配层需要在对应时机调用：

```go
hook := hooks.UseHTTPHook()

ctx, err = hook.BeforeRequest(ctx, req)
if err != nil {
	return err
}

resp, err := client.Do(req.WithContext(ctx))
if err != nil {
	_ = hook.OnError(ctx, req, err)
	return err
}

if err := hook.AfterResponse(ctx, req, resp); err != nil {
	return err
}
```

跨服务 Trace Context 的标准传播仍应显式使用 Propagator：

```go
propagator := propagationhttp.NewHTTPPropagator()
carrier := propagationhttp.NewHTTPHeaderCarrier(req.Header)
if err := propagator.Inject(ctx, carrier); err != nil {
	return err
}
```

## 6. 验证

默认桩与正式 Hook 都必须编译测试：

```bash
go test ./hooks ./middleware/...
go test -tags "http_hook redis_hook gorm_hook sqlx_hook" ./hooks ./middleware/...
go test -race -tags "http_hook redis_hook gorm_hook sqlx_hook" ./hooks ./middleware/...
```
