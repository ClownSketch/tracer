package operation

// RedisOperationInfo 定义Redis操作信息
type RedisOperationInfo struct {
	IndexDb     string  `json:"index_db"`          // 索引数据库
	Operation   string  `json:"operation"`         // 操作类型，如get、set、delete
	TTL         float64 `json:"ttl"`               // 缓存过期时间(单位秒)
	Key         string  `json:"key"`               // 缓存键
	Value       any     `json:"value"`             // 缓存值
	CostSeconds float64 `json:"cost_seconds"`      // 执行时间(单位秒)
	Success     bool    `json:"success"`           // 是否成功
	Transaction bool    `json:"transaction"`       // 是否是事务
	Pipeline    bool    `json:"pipeline"`          // 是否是管道
	Stack       string  `json:"stack,omitempty"`   // 堆栈信息
	Message     string  `json:"message,omitempty"` // 错误消息，用于记录错误信息
	Timestamp   string  `json:"timestamp"`         // 执行时间 (ISO 8601 格式)，如2021-01-01T00:00:00.000Z
}
