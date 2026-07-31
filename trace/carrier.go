package trace

// ContextCarrier 定义上下文载体接口
type ContextCarrier interface {
	// Set 设置键值对
	Set(key string, value string)
	// Get 获取键值对
	Get(key string) string
	// Keys 获取所有键
	Keys() []string
}
