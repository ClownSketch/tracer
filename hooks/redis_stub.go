//go:build !redis_hook
// +build !redis_hook

package hooks

import "github.com/redis/go-redis/v9"

// RedisHookPlugin 是一个占位符，当未开启 redis_hook 构建标签时不会执行任何逻辑。
type RedisHookPlugin struct {
	DBIndex string
}

// DialHook 返回原始钩子。
func (h *RedisHookPlugin) DialHook(hook redis.DialHook) redis.DialHook {
	return hook
}

// ProcessHook 返回原始钩子。
func (h *RedisHookPlugin) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return hook
}

// ProcessPipelineHook 返回原始钩子。
func (h *RedisHookPlugin) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return hook
}
