package providers

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/ClownSketch/tracer/trace"
)

// ExporterType 定义导出器类型
type ExporterType string

const (
	// ExporterTypeJaeger 定义Jaeger导出器类型
	ExporterTypeJaeger ExporterType = "jaeger"

	// ExporterTypeZipkin 定义Zipkin导出器类型
	ExporterTypeZipkin ExporterType = "zipkin"

	// ExporterTypeFile 定义文件导出器类型
	ExporterTypeFile ExporterType = "file"

	// ExporterTypeConsole 定义Console导出器类型
	ExporterTypeConsole ExporterType = "console"

	// ExporterTypeMongoDB 定义MongoDB导出器类型
	ExporterTypeMongoDB ExporterType = "mongodb"

	// ExporterTypeSimpleMongoDB 定义简单MongoDB导出器类型
	ExporterTypeSimpleMongoDB ExporterType = "simple_mongodb"

	// ExporterTypeDirectMongoDB 定义直接MongoDB导出器类型
	ExporterTypeDirectMongoDB ExporterType = "direct_mongodb"

	// ExporterTypeMongoDBRouting 定义支持 Span 集合路由的 MongoDB 导出器类型
	ExporterTypeMongoDBRouting ExporterType = "mongodb_routing"

	// 自定义导出器，允许用户自定义导出器类型
	ExporterTypeCustom ExporterType = "custom"
)

// TracerConfig 定义追踪器配置
type TracerConfig struct {
	ServiceName        string        // 服务名称
	SampleRate         float64       // 采样率
	IsDebug            bool          // 是否调试
	BatchSize          int           // 批量大小
	BatchInterval      time.Duration // 批量间隔
	Workers            int           // 工作协程数（批处理器）
	QueueSize          int           // BatchProcessor 队列大小（启动时静态配置，非运行时动态调整）
	ExporterType       ExporterType  // 导出器类型
	ExporterEndpoint   string        // Jaeger 或 Zipkin Collector 地址
	ExporterTimeout    time.Duration // HTTP 导出器请求超时
	ExporterHeaders    map[string]string
	LogFile            string        // 日志文件
	FallbackDir        string        // 回退目录
	UseWAL             bool          // 是否启用 WAL 主路径
	WALDir             string        // WAL 目录
	WALSegmentSize     int64         // WAL segment 大小
	WALExportBatchSize int           // WAL 后台投递批量大小
	WALPollInterval    time.Duration // WAL 后台轮询间隔
	WALFlushInterval   time.Duration // WAL 用户态缓冲刷新间隔
	WALBufferSize      int           // WAL 缓冲区大小
	WALSyncOnWrite     bool          // 是否每条写入后立即 fsync
	MongoDBURI         string        // MongoDB连接URI
	MongoDBDatabase    string        // MongoDB数据库名称
	MongoDBCollection  string        // MongoDB集合名称
	// MongoDB 连接对象（优先级高于 URI）
	// 如果设置了 MongoDBCollectionObj，则优先使用（推荐方式，包含所有连接信息）
	// 如果设置了 MongoDBClient，则使用已有的 Client（需要同时提供 Database 和 Collection）
	// 如果都未设置，则使用 URI + Database + Collection 创建新连接
	MongoDBClient        *mongo.Client     // MongoDB客户端对象，不会序列化到 JSON
	MongoDBCollectionObj *mongo.Collection // MongoDB集合对象，不会序列化到 JSON
	// 文件导出器配置
	FileAsyncBufferSize int   // 文件导出器异步缓冲区大小
	FileMaxFileSize     int64 // 文件导出器最大文件大小
	FileMaxBackups      int   // 文件导出器最大备份文件数
	// MongoDB 导出器配置
	MongoDBBatchSize           int           // MongoDB 导出器批量大小
	MongoDBFlushInterval       time.Duration // MongoDB 导出器刷新间隔
	MongoDBQueueSize           int           // MongoDB 导出器队列大小
	MongoDBMaxConcurrentWrites int           // MongoDB 导出器最大并发写入数（限制 goroutine 数量）
	MongoDBTimeout             time.Duration // MongoDB 导出器超时时间
	MongoDBMaxRetries          int           // MongoDB 导出器最大重试次数
	MongoDBRetryDelay          time.Duration // MongoDB 导出器重试延迟
	MongoDBAllowedCollections  []string      // MongoDB 路由导出器允许写入的集合
}

// ExporterOption 导出器配置选项接口，所有导出器配置必须实现此接口
type ExporterOption interface {
	// ExporterType 返回导出器类型
	ExporterType() ExporterType
}

// ExporterConfig 定义导出器配置（泛型版本，类型安全）
type ExporterConfig[T ExporterOption] struct {
	Type    ExporterType // 导出器类型
	Options T            // 导出器选项（类型安全）
}

// NewExporterConfig 创建导出器配置
func NewExporterConfig[T ExporterOption](options T) ExporterConfig[T] {
	return ExporterConfig[T]{
		Type:    options.ExporterType(),
		Options: options,
	}
}

// 兼容旧的 ExporterConfig（非泛型版本，用于向后兼容）
type LegacyExporterConfig struct {
	Type    ExporterType   // 导出器类型
	Options map[string]any // 导出器选项
}

// 注册导出器工厂函数类型（泛型版本）
type RegisterExporterFactory[T ExporterOption] func(config ExporterConfig[T]) (trace.SpanExporter, error)

// 内部使用的工厂函数类型（类型擦除，用于存储在 map 中）
type InternalExporterFactory func(opt ExporterOption) (trace.SpanExporter, error)
