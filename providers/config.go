package providers

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

// ConsoleExporterConfig Console导出器配置
type ConsoleExporterConfig struct {
	// Writer 输出目标，可以是文件路径（string）或 io.Writer
	// 如果为空，则使用默认的 os.Stdout
	Writer any `json:"writer,omitempty"` // string 或 io.Writer

	// PrettyPrint 是否美化输出
	PrettyPrint bool `json:"prettyPrint,omitempty"`

	// UseJSON 是否使用JSON格式输出
	UseJSON bool `json:"useJSON,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (c ConsoleExporterConfig) ExporterType() ExporterType {
	return ExporterTypeConsole
}

// FileExporterConfig File导出器配置
type FileExporterConfig struct {
	// FilePath 文件路径
	FilePath string `json:"filePath"`

	// MaxFileSize 最大文件大小（字节），超过此大小会轮转
	// 如果为0，则不按大小轮转
	MaxFileSize int64 `json:"maxFileSize,omitempty"`

	// RotateInterval 轮转间隔（按时间轮转）
	// 如果为0，则不按时间轮转
	RotateInterval time.Duration `json:"rotateInterval,omitempty"`

	// MaxBackups 最大备份文件数
	MaxBackups int `json:"maxBackups,omitempty"`

	// AsyncBufferSize 异步缓冲区大小
	AsyncBufferSize int `json:"asyncBufferSize,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (f FileExporterConfig) ExporterType() ExporterType {
	return ExporterTypeFile
}

// JaegerExporterConfig Jaeger导出器配置
type JaegerExporterConfig struct {
	// Endpoint Jaeger端点地址
	Endpoint string `json:"endpoint"`

	// Timeout 请求超时时间
	Timeout time.Duration `json:"timeout,omitempty"`

	// Headers 额外的请求头
	Headers map[string]string `json:"headers,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (j JaegerExporterConfig) ExporterType() ExporterType {
	return ExporterTypeJaeger
}

// ZipkinExporterConfig Zipkin导出器配置
type ZipkinExporterConfig struct {
	// Endpoint Zipkin端点地址
	Endpoint string `json:"endpoint"`

	// Timeout 请求超时时间
	Timeout time.Duration `json:"timeout,omitempty"`

	// Headers 额外的请求头
	Headers map[string]string `json:"headers,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (z ZipkinExporterConfig) ExporterType() ExporterType {
	return ExporterTypeZipkin
}

// MongoDBExporterConfig MongoDB导出器配置
// 配置方式有三种，按优先级从高到低：
// 1. CollectionObj：直接使用已有的集合对象（推荐，包含所有连接信息）
// 2. Client + Collection：使用已有的客户端和集合名称
// 3. URI + Database + Collection：通过连接字符串创建新连接
type MongoDBExporterConfig struct {
	// URI MongoDB连接URI（如 mongodb://127.0.0.1:27017）
	// 必须与 Database 和 Collection 一起使用
	URI string `json:"uri,omitempty"`

	// Database 数据库名称
	// 必须与 URI 和 Collection 一起使用
	Database string `json:"database,omitempty"`

	// Collection 集合（表）名称
	// 必须与 URI 和 Database 一起使用，或者与 Client 一起使用（使用 Client 时只需要 Collection）
	Collection string `json:"collection,omitempty"`

	// Client MongoDB客户端对象
	// 必须与 Collection 一起使用（Database 可以从配置中获取，如果配置中没有则必须提供）
	// 注意：此字段不会序列化到 JSON，仅用于程序内传递
	Client *mongo.Client `json:"-"`

	// CollectionObj MongoDB集合对象（推荐使用）
	// 已包含客户端、数据库和集合的所有信息，可独立使用
	// 注意：此字段不会序列化到 JSON，仅用于程序内传递
	CollectionObj *mongo.Collection `json:"-"`

	// Timeout 操作超时时间
	Timeout time.Duration `json:"timeout,omitempty"`

	// BatchSize 批量大小（默认50）
	BatchSize int `json:"batchSize,omitempty"`

	// FlushInterval 刷新间隔（默认2秒）
	FlushInterval time.Duration `json:"flushInterval,omitempty"`

	// QueueSize 队列大小（默认10000）
	QueueSize int `json:"queueSize,omitempty"`

	// MaxConcurrentWrites 最大并发写入数（默认10）
	// 限制同时写入 MongoDB 的 goroutine 数量，避免创建过多 goroutine
	MaxConcurrentWrites int `json:"maxConcurrentWrites,omitempty"`

	// MaxRetries 最大重试次数（默认3）
	MaxRetries int `json:"maxRetries,omitempty"`

	// RetryDelay 重试延迟（默认200ms）
	RetryDelay time.Duration `json:"retryDelay,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (m MongoDBExporterConfig) ExporterType() ExporterType {
	return ExporterTypeMongoDB
}

// MongoDBRoutingExporterConfig MongoDB 路由导出器配置。
// 连接与批量参数与 MongoDBExporterConfig 相同；Span 可通过 SetMongoCollection 指定目标集合。
type MongoDBRoutingExporterConfig struct {
	// URI MongoDB连接URI（如 mongodb://127.0.0.1:27017）
	URI string `json:"uri,omitempty"`

	// Database 数据库名称
	Database string `json:"database,omitempty"`

	// Collection 默认集合名称（Span 未指定 mongo 集合时使用）
	Collection string `json:"collection,omitempty"`

	// Client MongoDB客户端对象
	Client *mongo.Client `json:"-"`

	// CollectionObj MongoDB默认集合对象（推荐使用）
	CollectionObj *mongo.Collection `json:"-"`

	// AllowedCollections 允许写入的集合白名单；为空时不限制
	AllowedCollections []string `json:"allowedCollections,omitempty"`

	Timeout             time.Duration `json:"timeout,omitempty"`
	BatchSize           int           `json:"batchSize,omitempty"`
	FlushInterval       time.Duration `json:"flushInterval,omitempty"`
	QueueSize           int           `json:"queueSize,omitempty"`
	MaxConcurrentWrites int           `json:"maxConcurrentWrites,omitempty"`
	MaxRetries          int           `json:"maxRetries,omitempty"`
	RetryDelay          time.Duration `json:"retryDelay,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (m MongoDBRoutingExporterConfig) ExporterType() ExporterType {
	return ExporterTypeMongoDBRouting
}

// SimpleMongoDBExporterConfig 简单MongoDB导出器配置
type SimpleMongoDBExporterConfig struct {
	// URI MongoDB连接URI
	URI string `json:"uri"`

	// Database 数据库名称
	Database string `json:"database"`

	// Collection 集合名称
	Collection string `json:"collection"`

	// Timeout 连接超时时间
	Timeout time.Duration `json:"timeout,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (s SimpleMongoDBExporterConfig) ExporterType() ExporterType {
	return ExporterTypeSimpleMongoDB
}

// DirectMongoDBExporterConfig 直接MongoDB导出器配置
type DirectMongoDBExporterConfig struct {
	// URI MongoDB连接URI
	URI string `json:"uri"`

	// Database 数据库名称
	Database string `json:"database"`

	// Collection 集合名称
	Collection string `json:"collection"`

	// Timeout 连接超时时间
	Timeout time.Duration `json:"timeout,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (d DirectMongoDBExporterConfig) ExporterType() ExporterType {
	return ExporterTypeDirectMongoDB
}

// CustomExporterConfig 自定义导出器配置
// 用户可以实现自己的配置结构体并实现 ExporterOption 接口
type CustomExporterConfig struct {
	// Type 自定义导出器类型
	Type ExporterType `json:"type"`

	// Options 自定义选项
	Options map[string]any `json:"options,omitempty"`
}

// ExporterType 实现 ExporterOption 接口
func (c CustomExporterConfig) ExporterType() ExporterType {
	return c.Type
}

// GetDefaultTracerConfig 获取默认的tracer配置
func GetDefaultTracerConfig(serviceName string, isDebug bool) TracerConfig {
	return TracerConfig{
		ServiceName:       serviceName,                 // 服务名称
		SampleRate:        1.0,                         // 开发环境下全采样
		IsDebug:           isDebug,                     // 是否调试
		BatchSize:         100,                         // 批量大小
		BatchInterval:     5 * time.Second,             // 批量间隔
		ExporterType:      "file",                      // 导出器类型
		LogFile:           "./storage/log/traces.log",  // 日志文件
		FallbackDir:       "./storage/fallback",        // 回退目录
		MongoDBURI:        "mongodb://localhost:27017", // MongoDB连接URI
		MongoDBDatabase:   "tracer",                    // MongoDB数据库名称
		MongoDBCollection: "traces",                    // MongoDB集合名称
	}
}
