package types

// 添加自定义类型
type contextKey string

const (
	// 前置回调
	CallBackBeforeName = "tracing:before"
	// 后置回调
	CallBackAfterName = "tracing:after"
	// Gorm开始时间
	GormStartTime = "_start_time"
	// Redis数据库索引键
	RedisDbIndexKey contextKey = "clown_sketch_redis_db_index"
	// Redis管道键，用于标记当前上下文正在执行管道
	RedisPipelineKey contextKey = "clown_sketch_redis_pipeline"
	// HTTP开始时间
	HTTPStartTime contextKey = "clown_sketch_http_start_time"
	// HTTP外部调用信息键
	HTTPExternalCallInfoKey contextKey = "clown_sketch_http_external_call_info"
	// HTTP跳过追踪键
	HTTPSkipTraceKey contextKey = "clown_sketch_http_skip_trace"
	// HTTP最大请求体大小键
	HTTPMaxRequestBodySizeKey contextKey = "clown_sketch_http_max_request_body_size"
)
