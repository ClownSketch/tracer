package providers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/ClownSketch/tracer/exporter"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
)

var (
	// factoryMu 互斥锁,用于保护导出器工厂函数映射
	factoryMu sync.RWMutex

	// exporterFactories 导出器工厂函数映射,用于存储导出器工厂函数（内部使用类型擦除）
	exporterFactories = make(map[ExporterType]InternalExporterFactory)
)

// RegisterExporter 注册导出器工厂。
// option 只用于声明配置类型和导出器类型，不会作为运行时配置传给工厂。
func RegisterExporter[T ExporterOption](option T, factory RegisterExporterFactory[T]) error {
	if err := validateExporterOption(option); err != nil {
		return fmt.Errorf("注册导出器失败: %w", err)
	}
	if factory == nil {
		return errors.New("注册导出器失败: 工厂函数不能为空")
	}

	exporterType := option.ExporterType()
	factoryMu.Lock()
	defer factoryMu.Unlock()

	if _, exists := exporterFactories[exporterType]; exists {
		return fmt.Errorf("注册导出器失败: 类型 %q 已注册", exporterType)
	}

	exporterFactories[exporterType] = func(opt ExporterOption) (trace.SpanExporter, error) {
		config, ok := opt.(T)
		if !ok {
			return nil, fmt.Errorf("配置类型不匹配: 期望 %T, 实际 %T", option, opt)
		}
		return factory(ExporterConfig[T]{
			Type:    exporterType,
			Options: config,
		})
	}
	return nil
}

// mustRegisterExporter 注册内置导出器，注册失败表示程序定义冲突。
func mustRegisterExporter[T ExporterOption](option T, factory RegisterExporterFactory[T]) {
	if err := RegisterExporter(option, factory); err != nil {
		panic(err)
	}
}

// validateExporterOption 校验导出器配置是否可安全调用。
func validateExporterOption(option ExporterOption) error {
	if option == nil {
		return errors.New("导出器配置不能为空")
	}

	value := reflect.ValueOf(option)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return errors.New("导出器配置不能为空")
		}
	}

	if option.ExporterType() == "" {
		return errors.New("导出器类型不能为空")
	}
	return nil
}

// CreateExporter 创建导出器（泛型版本，类型安全）
func CreateExporter[T ExporterOption](config ExporterConfig[T]) (trace.SpanExporter, error) {
	if err := validateExporterOption(config.Options); err != nil {
		return nil, fmt.Errorf("创建导出器失败: %w", err)
	}
	optionType := config.Options.ExporterType()
	if config.Type != optionType {
		return nil, fmt.Errorf("创建导出器失败: 配置类型为 %q，选项类型为 %q", config.Type, optionType)
	}

	// 只在读取工厂时持有锁，导出器初始化不占用注册表锁。
	factoryMu.RLock()
	factory, ok := exporterFactories[config.Type]
	factoryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("导出器工厂函数未注册: %s", config.Type)
	}

	// 使用已注册的工厂创建导出器。
	return factory(config.Options)
}

// CreateExporterFromOption 从配置选项创建导出器（便捷方法）
func CreateExporterFromOption[T ExporterOption](option T) (trace.SpanExporter, error) {
	config, err := NewExporterConfig(option)
	if err != nil {
		return nil, err
	}
	return CreateExporter(config)
}

// 初始化注册导出器
func init() {
	// 注册控制台导出器（使用泛型配置）
	mustRegisterExporter(ConsoleExporterConfig{}, func(config ExporterConfig[ConsoleExporterConfig]) (trace.SpanExporter, error) {
		// 初始化控制台导出选项
		var opts []exporter.ConsoleExporterOption

		cfg := config.Options

		// 处理 writer 配置
		if cfg.Writer != nil {
			switch w := cfg.Writer.(type) {
			case string:
				// 如果提供了文件路径，打开文件
				if w != "" {
					file, err := os.OpenFile(w, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
					if err != nil {
						return nil, fmt.Errorf("打开文件失败: %w", err)
					}
					opts = append(opts, exporter.WithWriter(file))
				}
			case io.Writer:
				// 如果直接提供了 io.Writer
				opts = append(opts, exporter.WithWriter(w))
			}
		}
		// 如果没有指定 writer，使用默认的 os.Stdout（NewConsoleSpanExporter 的默认值）

		// 设置 prettyPrint
		opts = append(opts, exporter.WithPrettyPrint(cfg.PrettyPrint))

		// 设置 useJSON
		opts = append(opts, exporter.WithJSON(cfg.UseJSON))

		// 创建控制台导出器
		return exporter.NewConsoleSpanExporter(opts...), nil
	})

	// 注册文件导出器（使用泛型配置）
	mustRegisterExporter(FileExporterConfig{}, func(config ExporterConfig[FileExporterConfig]) (trace.SpanExporter, error) {
		cfg := config.Options

		// 初始化文件导出选项
		var opts []exporter.FileExporterOption

		// 设置文件路径
		if cfg.FilePath != "" {
			opts = append(opts, exporter.WithFilePath(cfg.FilePath))
		}

		// 设置最大文件大小
		if cfg.MaxFileSize > 0 {
			opts = append(opts, exporter.WithMaxFileSize(cfg.MaxFileSize))
		}

		// 设置轮转间隔
		if cfg.RotateInterval > 0 {
			opts = append(opts, exporter.WithRotateInterval(cfg.RotateInterval))
		}

		// 设置最大备份数
		if cfg.MaxBackups > 0 {
			opts = append(opts, exporter.WithMaxBackups(cfg.MaxBackups))
		}

		// 设置异步缓冲区大小
		if cfg.AsyncBufferSize > 0 {
			opts = append(opts, exporter.WithAsyncBufferSize(cfg.AsyncBufferSize))
		}

		// 创建文件导出器
		return exporter.NewFileSpanExporter(opts...)
	})

	// 注册 Jaeger 导出器。
	mustRegisterExporter(JaegerExporterConfig{}, func(config ExporterConfig[JaegerExporterConfig]) (trace.SpanExporter, error) {
		cfg := config.Options
		opts := make([]exporter.JaegerExporterOption, 0, 2)
		if cfg.Timeout > 0 {
			opts = append(opts, exporter.WithJaegerTimeout(cfg.Timeout))
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, exporter.WithJaegerHeaders(cfg.Headers))
		}
		return exporter.NewJaegerExporter(cfg.Endpoint, opts...)
	})

	// 注册 Zipkin 导出器。
	mustRegisterExporter(ZipkinExporterConfig{}, func(config ExporterConfig[ZipkinExporterConfig]) (trace.SpanExporter, error) {
		cfg := config.Options
		opts := make([]exporter.ZipkinExporterOption, 0, 2)
		if cfg.Timeout > 0 {
			opts = append(opts, exporter.WithZipkinTimeout(cfg.Timeout))
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, exporter.WithZipkinHeaders(cfg.Headers))
		}
		return exporter.NewZipkinExporter(cfg.Endpoint, opts...)
	})

	// 注册MongoDB导出器（使用泛型配置）
	mustRegisterExporter(MongoDBExporterConfig{}, func(config ExporterConfig[MongoDBExporterConfig]) (trace.SpanExporter, error) {
		cfg := config.Options

		// 初始化MongoDB导出器选项
		var opts []exporter.MongoDBExporterOption
		// 配置验证和优先级处理（按优先级从高到低）
		// 1. 优先使用 CollectionObj（推荐方式，包含所有连接信息）
		if cfg.CollectionObj != nil {
			// 直接使用，无需类型断言
			opts = append(opts, exporter.WithMongoDBCollection(cfg.CollectionObj))
		} else if cfg.Client != nil {
			// 2. 其次使用 Client + Collection（只需要 Collection，Database 可以从配置中获取）
			if cfg.Collection == "" {
				return nil, fmt.Errorf("使用 Client 时必须提供 Collection（集合名称）")
			}
			// Database 可以从配置中获取，如果配置中没有则必须提供
			if cfg.Database == "" {
				return nil, fmt.Errorf("使用 Client 时必须提供 Database（数据库名称），可以通过配置或参数传入")
			}
			// 直接使用，无需类型断言
			opts = append(opts, exporter.WithMongoDBClient(cfg.Client))
		} else {
			// 3. 使用 URI + Database + Collection（必须同时提供）
			if cfg.URI == "" || cfg.Database == "" || cfg.Collection == "" {
				return nil, fmt.Errorf("使用 URI 方式时必须同时提供 URI、Database 和 Collection")
			}
		}

		// 设置超时时间
		if cfg.Timeout > 0 {
			opts = append(opts, exporter.WithMongoDBTimeout(cfg.Timeout))
		}

		// 设置批量大小
		if cfg.BatchSize > 0 {
			opts = append(opts, exporter.WithMongoDBBatchSize(cfg.BatchSize))
		}

		// 设置刷新间隔
		if cfg.FlushInterval > 0 {
			opts = append(opts, exporter.WithMongoDBFlushInterval(cfg.FlushInterval))
		}

		// 设置队列大小
		if cfg.QueueSize > 0 {
			opts = append(opts, exporter.WithMongoDBQueueSize(cfg.QueueSize))
		}

		// 设置最大并发写入数
		if cfg.MaxConcurrentWrites > 0 {
			opts = append(opts, exporter.WithMongoDBMaxConcurrentWrites(cfg.MaxConcurrentWrites))
		}

		// 设置重试配置
		if cfg.MaxRetries > 0 || cfg.RetryDelay > 0 {
			maxRetries := cfg.MaxRetries
			if maxRetries == 0 {
				maxRetries = 3 // 默认值
			}
			retryDelay := cfg.RetryDelay
			if retryDelay == 0 {
				retryDelay = 200 * time.Millisecond // 默认值
			}
			opts = append(opts, exporter.WithMongoDBRetries(maxRetries, retryDelay))
		}

		// 创建MongoDB导出器
		// NewMongoDBExporter 的逻辑：
		// 1. 如果通过 WithMongoDBCollection 设置了 collection，直接返回，不会创建新连接（复用已有连接）
		// 2. 如果通过 WithMongoDBClient 设置了 client，但 collection 还是 nil，会复用已有的 client，使用传入的 database 和 collection 参数创建 collection
		// 3. 只有当 client == nil 时，才会创建新连接
		// 因此，无论传入什么参数，只要通过 opts 设置了 collection 或 client，就会复用已有连接，不会重新连接
		return exporter.NewMongoDBExporter(cfg.URI, cfg.Database, cfg.Collection, opts...)
	})

	// 简化版和直连版配置统一复用当前 MongoDB 导出器实现。
	mustRegisterExporter(SimpleMongoDBExporterConfig{}, func(config ExporterConfig[SimpleMongoDBExporterConfig]) (trace.SpanExporter, error) {
		cfg := config.Options
		opts := make([]exporter.MongoDBExporterOption, 0, 1)
		if cfg.Timeout > 0 {
			opts = append(opts, exporter.WithMongoDBTimeout(cfg.Timeout))
		}
		return exporter.NewMongoDBExporter(cfg.URI, cfg.Database, cfg.Collection, opts...)
	})
	mustRegisterExporter(DirectMongoDBExporterConfig{}, func(config ExporterConfig[DirectMongoDBExporterConfig]) (trace.SpanExporter, error) {
		cfg := config.Options
		opts := make([]exporter.MongoDBExporterOption, 0, 1)
		if cfg.Timeout > 0 {
			opts = append(opts, exporter.WithMongoDBTimeout(cfg.Timeout))
		}
		return exporter.NewMongoDBExporter(cfg.URI, cfg.Database, cfg.Collection, opts...)
	})

	// 注册 MongoDB 路由导出器（按 Span 集合名写入不同 collection）
	mustRegisterExporter(MongoDBRoutingExporterConfig{}, func(config ExporterConfig[MongoDBRoutingExporterConfig]) (trace.SpanExporter, error) {
		cfg := config.Options

		builderOpts := make([]exporter.MongoDBRoutingExporterOption, 0, 8)
		var mongoOpts []exporter.MongoDBExporterOption

		if cfg.CollectionObj != nil {
			mongoOpts = append(mongoOpts, exporter.WithMongoDBCollection(cfg.CollectionObj))
		} else if cfg.Client != nil {
			if cfg.Collection == "" {
				return nil, fmt.Errorf("使用 Client 时必须提供 Collection（默认集合名称）")
			}
			if cfg.Database == "" {
				return nil, fmt.Errorf("使用 Client 时必须提供 Database（数据库名称）")
			}
			mongoOpts = append(mongoOpts, exporter.WithMongoDBClient(cfg.Client))
		} else {
			if cfg.URI == "" || cfg.Database == "" || cfg.Collection == "" {
				return nil, fmt.Errorf("使用 URI 方式时必须同时提供 URI、Database 和 Collection")
			}
		}

		if cfg.Timeout > 0 {
			mongoOpts = append(mongoOpts, exporter.WithMongoDBTimeout(cfg.Timeout))
		}
		if cfg.BatchSize > 0 {
			mongoOpts = append(mongoOpts, exporter.WithMongoDBBatchSize(cfg.BatchSize))
		}
		if cfg.FlushInterval > 0 {
			mongoOpts = append(mongoOpts, exporter.WithMongoDBFlushInterval(cfg.FlushInterval))
		}
		if cfg.QueueSize > 0 {
			mongoOpts = append(mongoOpts, exporter.WithMongoDBQueueSize(cfg.QueueSize))
		}
		if cfg.MaxConcurrentWrites > 0 {
			mongoOpts = append(mongoOpts, exporter.WithMongoDBMaxConcurrentWrites(cfg.MaxConcurrentWrites))
		}
		if cfg.MaxRetries > 0 || cfg.RetryDelay > 0 {
			maxRetries := cfg.MaxRetries
			if maxRetries == 0 {
				maxRetries = 3
			}
			retryDelay := cfg.RetryDelay
			if retryDelay == 0 {
				retryDelay = 200 * time.Millisecond
			}
			mongoOpts = append(mongoOpts, exporter.WithMongoDBRetries(maxRetries, retryDelay))
		}

		for _, opt := range mongoOpts {
			builderOpts = append(builderOpts, exporter.RoutingWithMongoOption(opt))
		}
		if len(cfg.AllowedCollections) > 0 {
			builderOpts = append(builderOpts, exporter.WithMongoDBRoutingAllowedCollections(cfg.AllowedCollections...))
		}

		return exporter.NewMongoDBRoutingExporter(cfg.URI, cfg.Database, cfg.Collection, builderOpts...)
	})
}

// InitTracer 初始化追踪器并返回完整错误。
// 调用方必须处理初始化错误，避免链路系统静默降级。
func InitTracer(config TracerConfig) (trace.TracerProvider, error) {
	primaryExporter, err := createConfiguredExporter(config)
	if err != nil {
		return nil, fmt.Errorf("创建 Tracer 导出器失败: %w", err)
	}

	processors := make([]trace.SpanProcessor, 0, 2)
	if config.IsDebug && config.ExporterType != ExporterTypeConsole {
		debugExporter, debugErr := CreateExporterFromOption(ConsoleExporterConfig{UseJSON: true})
		if debugErr != nil {
			_ = primaryExporter.Shutdown(context.Background())
			return nil, fmt.Errorf("创建调试导出器失败: %w", debugErr)
		}
		processors = append(processors, processor.NewSimpleSpanProcessor(debugExporter))
	}

	primaryProcessor, err := createPrimaryProcessor(primaryExporter, config)
	if err != nil {
		for _, proc := range processors {
			_ = proc.Shutdown(context.Background())
		}
		_ = primaryExporter.Shutdown(context.Background())
		return nil, err
	}
	processors = append(processors, primaryProcessor)

	samplerInstance, err := createConfiguredSampler(config.SampleRate)
	if err != nil {
		for _, proc := range processors {
			_ = proc.Shutdown(context.Background())
		}
		return nil, err
	}

	opts := make([]TracerProviderOption, 0, len(processors)+2)
	for _, proc := range processors {
		opts = append(opts, WithSpanProcessor(proc))
	}
	opts = append(opts, WithSampler(samplerInstance), WithServiceName(config.ServiceName))
	return NewTracerProvider(opts...), nil
}

// createConfiguredExporter 根据统一配置创建导出器。
// @param config TracerConfig Tracer 配置
// @return result trace.SpanExporter 导出器
// @return err error 配置或构造错误
func createConfiguredExporter(config TracerConfig) (trace.SpanExporter, error) {
	if config.ExporterOption != nil {
		if err := validateExporterOption(config.ExporterOption); err != nil {
			return nil, fmt.Errorf("自定义导出器配置无效: %w", err)
		}
		exporterType := config.ExporterOption.ExporterType()
		if config.ExporterType != "" && config.ExporterType != exporterType {
			return nil, fmt.Errorf("导出器类型不一致: 配置为 %q，选项为 %q", config.ExporterType, exporterType)
		}
		return CreateExporterFromOption(config.ExporterOption)
	}

	switch config.ExporterType {
	case "", ExporterTypeFile:
		return CreateExporterFromOption(newFileExporterConfig(config))
	case ExporterTypeConsole:
		return CreateExporterFromOption(ConsoleExporterConfig{UseJSON: true})
	case ExporterTypeJaeger:
		return nil, errors.New("jaeger 导出器未接入 InitTracer，请使用显式构造器")
	case ExporterTypeZipkin:
		return nil, errors.New("zipkin 导出器未接入 InitTracer，请使用显式构造器")
	case ExporterTypeMongoDB:
		return CreateExporterFromOption(newMongoExporterConfig(config))
	case ExporterTypeMongoDBRouting:
		return CreateExporterFromOption(newMongoRoutingExporterConfig(config))
	case ExporterTypeSimpleMongoDB:
		return CreateExporterFromOption(SimpleMongoDBExporterConfig{
			URI:        config.MongoDBURI,
			Database:   config.MongoDBDatabase,
			Collection: config.MongoDBCollection,
			Timeout:    config.MongoDBTimeout,
		})
	case ExporterTypeDirectMongoDB:
		return CreateExporterFromOption(DirectMongoDBExporterConfig{
			URI:        config.MongoDBURI,
			Database:   config.MongoDBDatabase,
			Collection: config.MongoDBCollection,
			Timeout:    config.MongoDBTimeout,
		})
	default:
		return nil, fmt.Errorf("不支持的导出器类型: %q", config.ExporterType)
	}
}

// createConfiguredSampler 根据采样率创建采样器。
func createConfiguredSampler(sampleRate float64) (trace.SpanSampler, error) {
	if sampleRate < 0 || sampleRate > 1 {
		return nil, fmt.Errorf("采样率必须在 0 到 1 之间，当前值为 %v", sampleRate)
	}
	if sampleRate == 0 {
		return sampler.NewNeverSampler(), nil
	}
	if sampleRate == 1 {
		return sampler.NewAlwaysSampleSampler(), nil
	}
	return sampler.NewDistributedSampler(sampleRate), nil
}

// createPrimaryProcessor 根据可靠性模式创建主 Span 处理器。
// @param spanExporter trace.SpanExporter 导出器
// @param config TracerConfig Tracer 配置
// @return result trace.SpanProcessor Span 处理器
// @return err error 配置或构造错误
func createPrimaryProcessor(spanExporter trace.SpanExporter, config TracerConfig) (trace.SpanProcessor, error) {
	if config.UseWAL {
		syncExporter, ok := spanExporter.(trace.SyncSpanExporter)
		if !ok {
			return nil, fmt.Errorf("导出器 %T 不支持 WAL 同步确认", spanExporter)
		}
		walProcessor, err := processor.NewWALSpanProcessor(syncExporter, newWALProcessorOptions(config)...)
		if err != nil {
			return nil, fmt.Errorf("初始化 WAL 处理器失败: %w", err)
		}
		return walProcessor, nil
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}
	workers := config.Workers
	if workers <= 0 {
		workers = 2
	}
	queueSize := config.QueueSize
	if queueSize <= 0 {
		queueSize = batchSize * workers * 4
		if queueSize < 10000 {
			queueSize = 10000
		}
	}
	batchInterval := config.BatchInterval
	if batchInterval <= 0 {
		batchInterval = 2 * time.Second
	}
	fallbackDir := config.FallbackDir
	if fallbackDir == "" {
		fallbackDir = "./storage/fallback"
	}

	batchProcessor, err := processor.NewBatchSpanProcessor(
		spanExporter,
		processor.WithBatchSize(batchSize),
		processor.WithWorkers(workers),
		processor.WithQueueSize(queueSize),
		processor.WithFlushInterval(batchInterval),
		processor.WithFallbackDir(fallbackDir),
	)
	if err != nil {
		return nil, fmt.Errorf("初始化 Batch 处理器失败: %w", err)
	}
	return batchProcessor, nil
}

// newFileExporterConfig 从统一配置构建文件导出器配置。
// @param config TracerConfig Tracer 配置
// @return result FileExporterConfig 文件导出器配置
func newFileExporterConfig(config TracerConfig) FileExporterConfig {
	result := FileExporterConfig{
		FilePath:        config.LogFile,
		MaxFileSize:     config.FileMaxFileSize,
		RotateInterval:  24 * time.Hour,
		MaxBackups:      config.FileMaxBackups,
		AsyncBufferSize: config.FileAsyncBufferSize,
	}
	if result.MaxFileSize <= 0 {
		result.MaxFileSize = 30 * 1024 * 1024
	}
	if result.MaxBackups <= 0 {
		result.MaxBackups = 5
	}
	if result.AsyncBufferSize <= 0 {
		result.AsyncBufferSize = 1000
	}
	return result
}

// newMongoExporterConfig 从统一配置构建固定集合 MongoDB 配置。
// @param config TracerConfig Tracer 配置
// @return result MongoDBExporterConfig MongoDB 导出器配置
func newMongoExporterConfig(config TracerConfig) MongoDBExporterConfig {
	result := MongoDBExporterConfig{
		CollectionObj:       config.MongoDBCollectionObj,
		Client:              config.MongoDBClient,
		URI:                 config.MongoDBURI,
		Database:            config.MongoDBDatabase,
		Collection:          config.MongoDBCollection,
		BatchSize:           config.MongoDBBatchSize,
		FlushInterval:       config.MongoDBFlushInterval,
		QueueSize:           config.MongoDBQueueSize,
		MaxConcurrentWrites: config.MongoDBMaxConcurrentWrites,
		Timeout:             config.MongoDBTimeout,
		MaxRetries:          config.MongoDBMaxRetries,
		RetryDelay:          config.MongoDBRetryDelay,
	}
	if result.BatchSize <= 0 {
		result.BatchSize = 50
	}
	if result.FlushInterval <= 0 {
		result.FlushInterval = 2 * time.Second
	}
	if result.QueueSize <= 0 {
		result.QueueSize = 10000
	}
	if result.MaxConcurrentWrites <= 0 {
		result.MaxConcurrentWrites = 10
	}
	if result.Timeout <= 0 {
		result.Timeout = 10 * time.Second
	}
	if result.MaxRetries <= 0 {
		result.MaxRetries = 3
	}
	if result.RetryDelay <= 0 {
		result.RetryDelay = 200 * time.Millisecond
	}
	return result
}

// newMongoRoutingExporterConfig 从统一配置构建路由 MongoDB 配置。
// @param config TracerConfig Tracer 配置
// @return result MongoDBRoutingExporterConfig 路由导出器配置
func newMongoRoutingExporterConfig(config TracerConfig) MongoDBRoutingExporterConfig {
	result := MongoDBRoutingExporterConfig{
		CollectionObj:       config.MongoDBCollectionObj,
		Client:              config.MongoDBClient,
		URI:                 config.MongoDBURI,
		Database:            config.MongoDBDatabase,
		Collection:          config.MongoDBCollection,
		AllowedCollections:  config.MongoDBAllowedCollections,
		BatchSize:           config.MongoDBBatchSize,
		FlushInterval:       config.MongoDBFlushInterval,
		QueueSize:           config.MongoDBQueueSize,
		MaxConcurrentWrites: config.MongoDBMaxConcurrentWrites,
		Timeout:             config.MongoDBTimeout,
		MaxRetries:          config.MongoDBMaxRetries,
		RetryDelay:          config.MongoDBRetryDelay,
	}
	if result.BatchSize <= 0 {
		result.BatchSize = 50
	}
	if result.FlushInterval <= 0 {
		result.FlushInterval = 2 * time.Second
	}
	if result.QueueSize <= 0 {
		result.QueueSize = 10000
	}
	if result.MaxConcurrentWrites <= 0 {
		result.MaxConcurrentWrites = 10
	}
	if result.Timeout <= 0 {
		result.Timeout = 10 * time.Second
	}
	if result.MaxRetries <= 0 {
		result.MaxRetries = 3
	}
	if result.RetryDelay <= 0 {
		result.RetryDelay = 200 * time.Millisecond
	}
	return result
}

// newWALProcessorOptions 从统一配置构建 WAL 处理器选项。
// @param config TracerConfig Tracer 配置
// @return result []processor.WALSpanProcessorOption WAL 选项
func newWALProcessorOptions(config TracerConfig) []processor.WALSpanProcessorOption {
	opts := make([]processor.WALSpanProcessorOption, 0, 7)
	if config.WALDir != "" {
		opts = append(opts, processor.WithWALDir(config.WALDir))
	}
	if config.WALSegmentSize > 0 {
		opts = append(opts, processor.WithWALSegmentSize(config.WALSegmentSize))
	}
	if config.WALExportBatchSize > 0 {
		opts = append(opts, processor.WithWALExportBatchSize(config.WALExportBatchSize))
	}
	if config.WALPollInterval > 0 {
		opts = append(opts, processor.WithWALPollInterval(config.WALPollInterval))
	}
	if config.WALFlushInterval > 0 {
		opts = append(opts, processor.WithWALFlushInterval(config.WALFlushInterval))
	}
	if config.WALBufferSize > 0 {
		opts = append(opts, processor.WithWALBufferSize(config.WALBufferSize))
	}
	if config.WALSyncOnWrite {
		opts = append(opts, processor.WithWALSyncOnWrite(true))
	}
	return opts
}
