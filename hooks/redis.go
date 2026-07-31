//go:build redis_hook
// +build redis_hook

package hooks

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/types/operation"
	"github.com/redis/go-redis/v9"
)

// RedisHookPlugin 定义钩子插件
type RedisHookPlugin struct {
	DBIndex string
}

// DialHook 在连接到 Redis 服务器时调用
// @param hook redis.DialHook 钩子
// @return redis.DialHook 钩子
func (h *RedisHookPlugin) DialHook(hook redis.DialHook) redis.DialHook {
	// 打印连接信息
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		// 打印连接信息
		//fmt.Printf("开始连接 %s %s\n", network, addr)
		// 调用钩子
		conn, err := hook(ctx, network, addr)
		// 打印结束信息
		//fmt.Printf("结束连接 %s %s\n", network, addr)
		return conn, err
	}
}

// ProcessHook 在处理命令时调用
// @param hook redis.ProcessHook 钩子
// @return redis.ProcessHook 钩子
func (h *RedisHookPlugin) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	// 打印开始信息
	return func(ctx context.Context, cmd redis.Cmder) error {
		// 记录开始时间
		redisStartTime := time.Now()

		// 调用钩子
		err := hook(ctx, cmd)

		// 计算命令执行时间
		duration := time.Since(redisStartTime)

		// 从上下文中获取span
		span := baggage.GetSpanContext(ctx)
		// 如果span存在且不是 noop span，则添加事件
		if span != nil && span.GetSpanName() != "" {
			var errMsg string
			var stack string
			if err != nil {
				errMsg = err.Error()
				stack = fmt.Sprintf("%+v", err)
			}

			// 获取当前数据库索引（这里假设上下文中已经有数据库索引）
			dbIndex := h.DBIndex // 如果上下文中没有 dbIndex，默认为空字符串
			if dbIndex == "" {
				dbIndex = "0" // 默认数据库索引为 0
			}

			// 检查命令类型，判断是否为事务
			isTransaction := checkIfTransaction(cmd)

			// 记录redis操作日志
			// 注意：ProcessHook 处理的是单个命令，不是 Pipeline
			span.AddEvent("redis.operations", cmd.Name(), tracer.BuildRedisEvent(&operation.RedisOperationInfo{
				Timestamp:   time.Now().Format(time.RFC3339), // 记录时间
				Stack:       stack,                           // 堆栈信息
				IndexDb:     dbIndex,                         // 数据库索引，可能根据上下文动态获取
				Operation:   cmd.Name(),                      // 操作类型，如get、set、delete
				Key:         extractKey(cmd.Args()),          // 操作key
				Value:       extractValue(cmd.Args()),        // 操作值
				TTL:         extractTTL(cmd.Args()),          // 过期时间
				CostSeconds: duration.Seconds(),              // 执行时间（单位秒）
				Pipeline:    false,                           // ProcessHook 处理的是单个命令，不是 Pipeline
				Transaction: isTransaction,                   // 检查是否为事务
				Message:     errMsg,                          // 错误信息
				Success:     err == nil,                      // 是否成功
			}))
		}

		return err
	}
}

// ProcessPipelineHook 在处理管道命令时调用
// @param hook redis.ProcessPipelineHook 钩子
// @return redis.ProcessPipelineHook 钩子
func (h *RedisHookPlugin) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	// 打印开始信息
	return func(ctx context.Context, cmdS []redis.Cmder) error {
		// 记录管道开始时间
		startTime := time.Now()

		// 调用钩子
		err := hook(ctx, cmdS)

		// 记录管道总执行时间
		duration := time.Since(startTime)

		// 从上下文中获取span
		span := baggage.GetSpanContext(ctx)
		// 如果span存在且不是 noop span，则添加事件
		if span != nil && span.GetSpanName() != "" {
			// 定义错误信息
			var errMsg string
			var stack string
			if err != nil {
				errMsg = err.Error()
				stack = fmt.Sprintf("%+v", err)
			}

			// 获取当前数据库索引（这里假设上下文中已经有数据库索引）
			dbIndex := h.DBIndex // 如果上下文中没有 dbIndex，默认为空字符串
			if dbIndex == "" {
				dbIndex = "0" // 默认数据库索引为 0
			}

			// 检查是否为事务（通过检查命令名称）
			isTransaction := false
			for _, cmd := range cmdS {
				if cmd.Name() == "MULTI" || cmd.Name() == "EXEC" || cmd.Name() == "DISCARD" {
					isTransaction = true
					break
				}
			}

			// 循环处理管道中的所有命令
			for _, cmd := range cmdS {
				// 跳过事务控制命令（MULTI、EXEC、DISCARD）
				if cmd.Name() == "MULTI" || cmd.Name() == "EXEC" || cmd.Name() == "DISCARD" {
					continue
				}

				// 记录redis操作日志
				// ProcessPipelineHook 处理的是 Pipeline，所以 Pipeline=true
				span.AddEvent("redis.operations", cmd.Name(), tracer.BuildRedisEvent(&operation.RedisOperationInfo{
					Timestamp:   time.Now().Format(time.RFC3339), // 记录时间
					Stack:       stack,                           // 堆栈信息
					IndexDb:     dbIndex,                         // 数据库索引，可能根据上下文动态获取
					Operation:   cmd.Name(),                      // 操作类型，如get、set、delete
					Key:         extractKey(cmd.Args()),          // 操作key
					Value:       extractValue(cmd.Args()),        // 操作值
					TTL:         extractTTL(cmd.Args()),          // 过期时间(单位秒)
					CostSeconds: duration.Seconds(),              // 执行时间（单位秒，使用管道总时间）
					Pipeline:    true,                            // ProcessPipelineHook 处理的是 Pipeline
					Transaction: isTransaction,                   // 检查是否为事务
					Message:     errMsg,                          // 错误信息
					Success:     err == nil,                      // 是否成功
				}))
			}
		}

		return err
	}
}

// 提取 Key
func extractKey(args []any) string {
	if len(args) > 1 {
		// 如果 args[1] 是 interface 类型，则尝试进行类型断言
		if key, ok := args[1].(string); ok {
			return key
		}
	}
	return ""
}

// 提取 Value
func extractValue(args []any) any {
	if len(args) > 2 {
		return args[2]
	}
	return nil
}

// 提取 TTL（从命令参数中提取）
// 注意：TTL 的提取依赖于具体的 Redis 命令，不同命令的参数位置可能不同
// 例如：SET key value EX 10 中，TTL 在参数中的位置取决于命令类型
func extractTTL(args []any) float64 {
	// 遍历参数，查找可能的 TTL 值
	// 常见的 TTL 参数位置：EX、PX、EXAT、PXAT 等
	for i, arg := range args {
		if i > 0 { // 跳过命令名
			// 检查是否是 TTL 相关的参数
			if str, ok := arg.(string); ok {
				if str == "EX" || str == "PX" || str == "EXAT" || str == "PXAT" {
					// 下一个参数应该是 TTL 值
					if i+1 < len(args) {
						if ttl, ok := args[i+1].(int); ok {
							return float64(ttl)
						}
						if ttl, ok := args[i+1].(int64); ok {
							return float64(ttl)
						}
						if ttl, ok := args[i+1].(float64); ok {
							return ttl
						}
					}
				}
			}
		}
	}
	return 0
}

// checkIfTransaction 检查命令是否为事务
// 通过检查命令名称来判断是否为事务相关命令
func checkIfTransaction(cmd redis.Cmder) bool {
	name := cmd.Name()
	return name == "MULTI" || name == "EXEC" || name == "DISCARD" || name == "WATCH" || name == "UNWATCH"
}
