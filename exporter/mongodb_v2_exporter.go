package exporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/ClownSketch/tracer/trace"
)

type mongoV2QueueItem struct {
	doc any
}

type mongoV2SpanDocument struct {
	Name          string                        `bson:"name"`
	TraceID       string                        `bson:"trace_id"`
	SpanID        string                        `bson:"span_id"`
	ParentSpanID  string                        `bson:"parent_span_id"`
	Kind          any                           `bson:"kind"`
	StartTime     time.Time                     `bson:"start_time"`
	EndTime       time.Time                     `bson:"end_time"`
	Duration      int64                         `bson:"duration"`
	CreatedAt     int64                         `bson:"created_at"`
	RecordID      string                        `bson:"record_id,omitempty"`
	Status        *mongoV2StatusDocument        `bson:"status,omitempty"`
	Attributes    map[string]any                `bson:"attributes,omitempty"`
	Events        []bson.M                      `bson:"events,omitempty"`
	Logs          []mongoV2LogDocument          `bson:"logs,omitempty"`
	Error         *mongoV2ErrorDocument         `bson:"error,omitempty"`
	Resource      *mongoV2ResourceDocument      `bson:"resource,omitempty"`
	ResourceUsage *mongoV2ResourceUsageDocument `bson:"resource_usage,omitempty"`
	LinkedSpans   []mongoV2LinkedSpanDocument   `bson:"linked_spans,omitempty"`
}

type mongoV2StatusDocument struct {
	Code        any    `bson:"code,omitempty"`
	Description string `bson:"description,omitempty"`
}

type mongoV2LogDocument struct {
	Timestamp  string         `bson:"timestamp"`
	Message    string         `bson:"message"`
	Severity   any            `bson:"severity"`
	Attributes map[string]any `bson:"attributes,omitempty"`
	Fields     any            `bson:"fields,omitempty"`
	EventType  string         `bson:"eventType,omitempty"`
}

type mongoV2ErrorDocument struct {
	Code            string                      `bson:"code,omitempty"`
	Message         string                      `bson:"message"`
	BusinessCode    string                      `bson:"business_code,omitempty"`
	BusinessMessage []string                    `bson:"business_message,omitempty"`
	HttpCode        int                         `bson:"http_code,omitempty"`
	Timestamp       string                      `bson:"timestamp,omitempty"`
	MetaData        map[string]any              `bson:"meta_data,omitempty"`
	StackTrace      []mongoV2StackFrameDocument `bson:"stack_trace,omitempty"`
}

type mongoV2StackFrameDocument struct {
	File         string `bson:"file,omitempty"`
	FileName     string `bson:"file_name,omitempty"`
	FunctionName string `bson:"function_name,omitempty"`
	LineNumber   int    `bson:"line_number,omitempty"`
}

type mongoV2ResourceDocument struct {
	ServiceName string         `bson:"service_name,omitempty"`
	Host        string         `bson:"host,omitempty"`
	Attributes  map[string]any `bson:"attributes,omitempty"`
}

type mongoV2ResourceUsageDocument struct {
	CPUUsage    float64 `bson:"cpu_usage,omitempty"`
	MemoryUsage float64 `bson:"memory_usage,omitempty"`
	DiskUsage   float64 `bson:"disk_usage,omitempty"`
	NetworkIO   float64 `bson:"network_io,omitempty"`
}

type mongoV2LinkedSpanDocument struct {
	TraceID string `bson:"trace_id"`
	SpanID  string `bson:"span_id"`
}

// MongoDBV2Exporter MongoDB 导出器。
// BatchProcessor 负责异步调度；导出器收到 batch 后直接同步写 MongoDB。
type MongoDBV2Exporter struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	mu         sync.RWMutex // 保护client的读写锁

	// 连接管理标志
	clientManaged bool // 连接是否由导出器管理（true=导出器创建，false=外部传入）

	// 批量写入配置
	batchSize           int           // 批量大小
	flushInterval       time.Duration // 刷新间隔
	timeout             time.Duration // 操作超时时间
	maxRetries          int           // 最大重试次数
	retryDelay          time.Duration // 重试延迟
	maxConcurrentWrites int           // 最大并发写入数（限制同时写入的 goroutine 数量）
	stopOnce            sync.Once     // 确保只关闭一次
	shutdownDone        chan struct{} // 后台关闭完成信号
	shutdownErr         error         // 后台关闭错误
	docsPool            sync.Pool     // 复用 InsertMany 所需的 []any 装箱切片

	// 统计信息
	processedCount int64 // 处理数量
	droppedCount   int64 // 丢弃数量
	exportErrors   int64 // 导出错误数量
}

// MongoDBV2ExporterOption MongoDB导出器选项
type MongoDBV2ExporterOption func(*MongoDBV2Exporter)

// WithMongoDBV2Client 设置MongoDB客户端（如果已存在）
// 注意：如果使用此选项，连接由外部管理，导出器不会关闭连接
func WithMongoDBV2Client(client *mongo.Client) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {
		e.client = client
		e.clientManaged = false // 外部传入的客户端，不由导出器管理
	}
}

// WithMongoDBV2Collection 设置MongoDB集合（推荐方式，由外部管理连接）
// 这样可以复用已有的连接，支持使用 qmgo 等扩展库
// 注意：如果使用此选项，连接由外部管理，导出器不会关闭连接
func WithMongoDBV2Collection(collection *mongo.Collection) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {
		if collection != nil {
			e.collection = collection
			// 从 collection 获取 database 和 client（仅用于引用，不用于关闭）
			e.database = collection.Database()
			e.client = collection.Database().Client()
			e.clientManaged = false // 外部传入的 collection，连接由外部管理
		}
	}
}

// WithMongoDBV2Timeout 设置操作超时时间
func WithMongoDBV2Timeout(timeout time.Duration) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {
		e.timeout = timeout
	}
}

// WithMongoDBV2BatchSize 设置批量大小
func WithMongoDBV2BatchSize(size int) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {
		if size > 0 {
			e.batchSize = size
		}
	}
}

// WithMongoDBV2FlushInterval 设置刷新间隔
func WithMongoDBV2FlushInterval(interval time.Duration) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {
		if interval > 0 {
			e.flushInterval = interval
		}
	}
}

// WithMongoDBV2QueueSize 保留配置入口。
// 现在异步队列只存在于 BatchProcessor，这里不再维护 exporter 内部队列。
func WithMongoDBV2QueueSize(size int) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {}
}

// WithMongoDBV2MaxConcurrentWrites 设置最大并发写入数
func WithMongoDBV2MaxConcurrentWrites(max int) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {
		if max > 0 {
			e.maxConcurrentWrites = max
		}
	}
}

// WithMongoDBV2Retries 设置最大重试次数和重试延迟
func WithMongoDBV2Retries(maxRetries int, retryDelay time.Duration) MongoDBV2ExporterOption {
	return func(e *MongoDBV2Exporter) {
		if maxRetries > 0 {
			e.maxRetries = maxRetries
		}
		if retryDelay > 0 {
			e.retryDelay = retryDelay
		}
	}
}

// NewMongoDBV2Exporter 创建MongoDB导出器
// 支持三种方式：
// 1. 使用 WithMongoDBV2Collection 传入已初始化的 collection（推荐，可复用连接）
//   - 直接使用传入的 collection，导出器不负责关闭连接
//
// 2. 使用 WithMongoDBV2Client 传入已初始化的 client，配合 database 和 collection 参数
//   - 使用已有的 client，导出器自己获取 collection（不创建新连接）
//   - 导出器不负责关闭连接
//
// 3. 传入 URI、database、collection（内部创建连接）
//   - 导出器自己连接 mongo，维护连接池，负责关闭连接
//   - 适用于主项目没有使用 mongo 的场景
func NewMongoDBV2Exporter(uri, database, collection string, opts ...MongoDBV2ExporterOption) (*MongoDBV2Exporter, error) {
	// 初始化导出器默认配置
	e := newMongoDBV2ExporterWithDefaults()

	// 应用选项（可能在选项中已经设置了 collection 或 client）
	for _, opt := range opts {
		opt(e)
	}
	e.normalizeRuntimeConfig()

	// 方式1: 如果已经通过 WithMongoDBV2Collection 设置了 collection，直接复用
	if e.collection != nil {
		return e.initWithCollection()
	}

	// 方式2: 如果已经通过 WithMongoDBV2Client 设置了 client，复用已有 client
	if e.client != nil {
		return e.initWithClient(database, collection)
	}

	// 方式3: 创建新连接
	return e.initWithURI(uri, database, collection)
}

func (e *MongoDBV2Exporter) normalizeRuntimeConfig() {
	if e.maxConcurrentWrites <= 0 {
		e.maxConcurrentWrites = 1
	}
}

// newMongoDBV2ExporterWithDefaults 创建带有默认配置的 MongoDB 导出器
func newMongoDBV2ExporterWithDefaults() *MongoDBV2Exporter {
	e := &MongoDBV2Exporter{
		batchSize:           50,                     // 默认批量大小
		flushInterval:       2 * time.Second,        // 默认刷新间隔
		timeout:             10 * time.Second,       // 默认超时时间
		maxRetries:          3,                      // 默认最大重试次数
		retryDelay:          200 * time.Millisecond, // 默认重试延迟
		maxConcurrentWrites: 4,                      // 默认最大并发写入数
	}
	e.docsPool.New = func() any {
		buf := make([]any, 0, 512)
		return &buf
	}
	return e
}

// initWithCollection 使用已有的 collection 初始化导出器（复用已有连接）
// 直接使用传入的 collection，导出器不负责关闭连接
func (e *MongoDBV2Exporter) initWithCollection() (*MongoDBV2Exporter, error) {
	// 连接由外部管理，导出器不负责关闭
	e.clientManaged = false
	if err := e.createIndexes(); err != nil {
		return nil, err
	}
	return e, nil
}

// initWithClient 使用已有的 client 初始化导出器（复用已有连接）
// 使用已有的 client，导出器自己获取 collection（不创建新连接）
// 需要提供 database 和 collection 参数，因为 MongoDB API 需要 database 才能获取 collection
func (e *MongoDBV2Exporter) initWithClient(database, collection string) (*MongoDBV2Exporter, error) {
	// 验证参数
	if database == "" {
		return nil, fmt.Errorf("使用 WithMongoDBV2Client 时必须提供 database（数据库名称）")
	}
	if collection == "" {
		return nil, fmt.Errorf("使用 WithMongoDBV2Client 时必须提供 collection（集合名称）")
	}

	// 使用已有的 client，从 client 获取 database 和 collection（不创建新连接）
	e.database = e.client.Database(database)
	e.collection = e.database.Collection(collection)
	// 连接由外部管理，导出器不负责关闭
	e.clientManaged = false
	if err := e.createIndexes(); err != nil {
		return nil, err
	}

	return e, nil
}

// initWithURI 使用 URI 创建新连接并初始化导出器
// 导出器自己连接 mongo，维护连接池，负责关闭连接
// 适用于主项目没有使用 mongo 的场景
func (e *MongoDBV2Exporter) initWithURI(uri, database, collection string) (*MongoDBV2Exporter, error) {
	// 验证参数
	if err := e.validateURIParams(uri, database, collection); err != nil {
		return nil, err
	}

	// 创建新客户端
	client, err := e.createMongoClient(uri)
	if err != nil {
		return nil, err
	}

	e.client = client
	// 连接由导出器管理，需要负责关闭
	e.clientManaged = true

	// 使用新创建的 client 获取 database 和 collection
	e.database = e.client.Database(database)
	e.collection = e.database.Collection(collection)

	if err := e.createIndexes(); err != nil {
		_ = client.Disconnect(context.Background())
		e.client = nil
		e.database = nil
		e.collection = nil
		return nil, err
	}

	return e, nil
}

// validateURIParams 验证 URI 方式所需的参数
func (e *MongoDBV2Exporter) validateURIParams(uri, database, collection string) error {
	if uri == "" {
		return fmt.Errorf("MongoDB URI 不能为空（或使用 WithMongoDBV2Collection/WithMongoDBV2Client 传入 collection/client）")
	}
	if database == "" {
		return fmt.Errorf("数据库名称不能为空（或使用 WithMongoDBV2Collection/WithMongoDBV2Client 传入 collection/client）")
	}
	if collection == "" {
		return fmt.Errorf("集合名称不能为空（或使用 WithMongoDBV2Collection/WithMongoDBV2Client 传入 collection）")
	}
	return nil
}

// createMongoClient 创建 MongoDB 客户端连接
func (e *MongoDBV2Exporter) createMongoClient(uri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	// 配置客户端选项
	clientOptions := options.Client().ApplyURI(uri)
	poolSize := uint64(100)
	if e.maxConcurrentWrites > 0 {
		candidate := uint64(e.maxConcurrentWrites * 4)
		if candidate > poolSize {
			poolSize = candidate
		}
	}
	if poolSize > 512 {
		poolSize = 512
	}
	minPoolSize := uint64(10)
	if poolSize >= 128 {
		minPoolSize = poolSize / 8
	}
	// 设置连接池选项，优化并发性能
	clientOptions.SetMaxPoolSize(poolSize)             // 最大连接池大小
	clientOptions.SetMinPoolSize(minPoolSize)          // 最小连接池大小
	clientOptions.SetMaxConnIdleTime(30 * time.Minute) // 连接空闲超时

	// 创建连接
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("连接MongoDB失败: %w", err)
	}

	// 测试连接
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping MongoDB失败: %w", err)
	}

	return client, nil
}

// ExportSpan 同步导出单个 span。
func (e *MongoDBV2Exporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

// ExportSpanSync 同步导出单个 span。
// 该方法会在 MongoDB 真正完成写入（或确认重复键）后返回，适合 WAL 处理器使用。
func (e *MongoDBV2Exporter) ExportSpanSync(ctx context.Context, span trace.SpanSnapshot) error {
	return e.ExportSpansSync(ctx, []trace.SpanSnapshot{span})
}

// ExportSpans 同步批量写入 MongoDB。
// 当前方法只负责把一批 span 写入后端，不负责 fallback，也不释放快照。
func (e *MongoDBV2Exporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	items := make([]mongoV2QueueItem, 0, len(spans))
	validCount := 0
	for _, span := range spans {
		if span == nil {
			continue
		}
		validCount++
		items = append(items, e.buildQueueItem(span, false))
	}
	if len(items) == 0 {
		return nil
	}

	atomic.AddInt64(&e.processedCount, int64(validCount))
	if err := e.writeBatchWithRetry(context.Background(), items, false); err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(items)))
		return err
	}
	return nil
}

// ExportSpansSync 同步导出多个 spans。
// 成功返回 nil 时，表示这一批数据已经被 MongoDB 成功接收，
// 调用方可以安全推进 WAL ACK。
func (e *MongoDBV2Exporter) ExportSpansSync(ctx context.Context, spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	items := make([]mongoV2QueueItem, 0, len(spans))
	validCount := 0
	for _, span := range spans {
		if span == nil {
			continue
		}
		validCount++
		items = append(items, e.buildQueueItem(span, true))
	}
	defer releaseSpanSnapshotsV2(spans)

	if len(items) == 0 {
		return nil
	}

	atomic.AddInt64(&e.processedCount, int64(validCount))
	if err := e.writeBatchWithRetry(ctx, items, false); err != nil {
		atomic.AddInt64(&e.exportErrors, int64(len(items)))
		return err
	}
	return nil
}

func releaseSpanSnapshotsV2(spans []trace.SpanSnapshot) {
	for _, span := range spans {
		if span != nil {
			span.Release()
		}
	}
}

func (e *MongoDBV2Exporter) buildQueueItem(span trace.SpanSnapshot, includeRecordID bool) mongoV2QueueItem {
	return mongoV2QueueItem{
		doc: e.buildDocument(span, includeRecordID),
	}
}

// writeBatchWithRetry 批量写入MongoDB，带重试机制
// 接收的是已经转换好的文档，不需要再转换，也不需要释放快照资源（已经在 ExportSpans 中释放）。
func (e *MongoDBV2Exporter) writeBatchWithRetry(ctx context.Context, items []mongoV2QueueItem, _ bool) error {
	if len(items) == 0 {
		return nil
	}

	e.mu.RLock()
	collection := e.collection
	timeout := e.timeout
	e.mu.RUnlock()

	if collection == nil {
		return errors.New("mongodb collection is not available")
	}

	err := e.writeItemsWithRetry(ctx, collection, timeout, items)
	return err
}

func (e *MongoDBV2Exporter) writeItemsWithRetry(ctx context.Context, collection *mongo.Collection, timeout time.Duration, items []mongoV2QueueItem) error {
	if len(items) == 0 {
		return nil
	}

	const maxInsertBatch = 500
	for start := 0; start < len(items); start += maxInsertBatch {
		end := start + maxInsertBatch
		if end > len(items) {
			end = len(items)
		}
		if err := e.insertItemsChunkWithRetry(ctx, collection, timeout, items[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (e *MongoDBV2Exporter) insertItemsChunkWithRetry(ctx context.Context, collection *mongo.Collection, timeout time.Duration, items []mongoV2QueueItem) error {
	docs := e.borrowDocsBuffer(len(items))
	defer e.releaseDocsBuffer(docs)

	for i, item := range items {
		docs[i] = item.doc
	}

	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		opCtx, cancel := context.WithTimeout(ctxOrBackgroundV2(ctx), timeout)
		_, err := collection.InsertMany(opCtx, docs, options.InsertMany().SetOrdered(false))
		cancel()

		if err == nil || isMongoV2DuplicateOnlyInsertError(err) {
			return nil
		}

		lastErr = err
		if attempt >= e.maxRetries {
			break
		}
		if sleepErr := sleepWithContextV2(ctx, e.retryDelay*time.Duration(attempt+1)); sleepErr != nil {
			return sleepErr
		}
	}

	if lastErr == nil {
		lastErr = errors.New("mongodb write failed")
	}
	return lastErr
}

func (e *MongoDBV2Exporter) borrowDocsBuffer(size int) []any {
	bufPtr := e.docsPool.Get().(*[]any)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]any, size)
	} else {
		buf = buf[:size]
	}
	return buf
}

func (e *MongoDBV2Exporter) releaseDocsBuffer(buf []any) {
	if cap(buf) == 0 {
		return
	}
	for i := range buf {
		buf[i] = nil
	}
	buf = buf[:0]
	e.docsPool.Put(&buf)
}

func ctxOrBackgroundV2(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func sleepWithContextV2(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if ctx == nil {
		time.Sleep(delay)
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isMongoV2DuplicateOnlyInsertError(err error) bool {
	if err == nil {
		return false
	}

	var bulkErr mongo.BulkWriteException
	if errors.As(err, &bulkErr) {
		if bulkErr.WriteConcernError != nil || len(bulkErr.WriteErrors) == 0 {
			return false
		}
		for _, writeErr := range bulkErr.WriteErrors {
			if !isMongoV2DuplicateKeyCode(writeErr.Code) {
				return false
			}
		}
		return true
	}

	var writeErr mongo.WriteException
	if errors.As(err, &writeErr) {
		if writeErr.WriteConcernError != nil || len(writeErr.WriteErrors) == 0 {
			return false
		}
		for _, item := range writeErr.WriteErrors {
			if !isMongoV2DuplicateKeyCode(item.Code) {
				return false
			}
		}
		return true
	}

	return mongo.IsDuplicateKeyError(err)
}

func isMongoV2DuplicateKeyCode(code int) bool {
	switch code {
	case 11000, 11001, 12582, 16460:
		return true
	default:
		return false
	}
}

// buildDocument 构建 MongoDB 文档。
// 顶层和大部分嵌套结构使用 typed struct，减少 bson.M 带来的额外 map 分配。
func (e *MongoDBV2Exporter) buildDocument(span trace.SpanSnapshot, includeRecordID bool) any {
	doc := mongoV2SpanDocument{
		Name:         span.GetSpanName(),
		TraceID:      span.GetSpanTraceID(),
		SpanID:       span.GetSpanID(),
		ParentSpanID: span.GetSpanParentSpanID(),
		Kind:         span.GetSpanKind(),
		StartTime:    span.GetStartTime(),
		EndTime:      span.GetEndTime(),
		Duration:     span.GetEndTime().Sub(span.GetStartTime()).Nanoseconds(),
		CreatedAt:    span.GetStartTime().Unix(),
	}
	if includeRecordID {
		doc.RecordID = buildMongoV2RecordID(span)
	}

	status := span.GetStatus()
	if status.Code != "" || status.Description != "" {
		doc.Status = &mongoV2StatusDocument{
			Code:        status.Code,
			Description: status.Description,
		}
	}

	if attrs := span.GetAttributes(); len(attrs) > 0 {
		doc.Attributes = attrs
	}

	if events := span.GetEvents(); len(events) > 0 {
		eventDocs := make([]bson.M, len(events))
		for i, event := range events {
			ev := bson.M{
				"name":      event.Name,
				"timestamp": event.Timestamp,
			}
			if len(event.Attributes) > 0 {
				ev["attributes"] = event.Attributes
			}
			eventDocs[i] = ev
		}
		doc.Events = eventDocs
	}

	if logs := span.GetLogs(); len(logs) > 0 {
		logDocs := make([]mongoV2LogDocument, len(logs))
		for i, log := range logs {
			logDocs[i] = mongoV2LogDocument{
				Timestamp:  log.Timestamp,
				Message:    log.Message,
				Severity:   log.Severity,
				Attributes: log.Attributes,
				Fields:     log.Fields,
				EventType:  log.EventType,
			}
		}
		doc.Logs = logDocs
	}

	if errDetail := span.GetErrorDetail(); errDetail != nil {
		errorDoc := &mongoV2ErrorDocument{
			Code:            errDetail.Code,
			Message:         errDetail.Message,
			BusinessCode:    errDetail.BusinessCode,
			BusinessMessage: errDetail.BusinessMessage,
			HttpCode:        errDetail.HttpCode,
			Timestamp:       errDetail.Timestamp,
			MetaData:        errDetail.MetaData,
		}
		if len(errDetail.StackTrace) > 0 {
			stackTraceDocs := make([]mongoV2StackFrameDocument, len(errDetail.StackTrace))
			for i, v := range errDetail.StackTrace {
				stackTraceDocs[i] = mongoV2StackFrameDocument{
					File:         v.File,
					FileName:     v.FileName,
					FunctionName: v.FunctionName,
					LineNumber:   v.LineNumber,
				}
			}
			errorDoc.StackTrace = stackTraceDocs
		}
		doc.Error = errorDoc
	}

	if resource := span.GetResource(); resource != nil {
		resourceDoc := &mongoV2ResourceDocument{
			ServiceName: resource.ServiceName,
			Host:        resource.Host,
			Attributes:  resource.Attributes,
		}
		if resourceDoc.ServiceName != "" || resourceDoc.Host != "" || len(resourceDoc.Attributes) > 0 {
			doc.Resource = resourceDoc
		}
	}

	if usage := span.GetResourceUsage(); usage != nil {
		usageDoc := &mongoV2ResourceUsageDocument{
			CPUUsage:    usage.CPUUsage,
			MemoryUsage: usage.MemoryUsage,
			DiskUsage:   usage.DiskUsage,
			NetworkIO:   usage.NetworkIO,
		}
		if usageDoc.CPUUsage > 0 || usageDoc.MemoryUsage > 0 || usageDoc.DiskUsage > 0 || usageDoc.NetworkIO > 0 {
			doc.ResourceUsage = usageDoc
		}
	}

	if linkedSpans := span.GetLinkedSpans(); len(linkedSpans) > 0 {
		linkedSpanDocs := make([]mongoV2LinkedSpanDocument, len(linkedSpans))
		for i, linkedSpan := range linkedSpans {
			linkedSpanDocs[i] = mongoV2LinkedSpanDocument{
				TraceID: linkedSpan.TraceID,
				SpanID:  linkedSpan.SpanID,
			}
		}
		doc.LinkedSpans = linkedSpanDocs
	}

	return doc
}

func buildMongoV2RecordID(span trace.SpanSnapshot) string {
	return fmt.Sprintf("%s:%s:%d", span.GetSpanTraceID(), span.GetSpanID(), span.GetEndTime().UnixNano())
}

// createIndexes 创建MongoDB索引，优化查询性能
//
// 索引字段（共6个字段）：
//   - trace_id: 用于查询整个trace的所有spans
//   - span_id: 用于查询单个span（唯一索引）
//   - created_at: 用于时间范围查询
//   - parent_span_id: 用于查询子spans
//   - name: 用于按操作名称查询
//   - kind: 用于按span类型查询
//
// 索引列表（共9个索引）：
//
//	单字段索引（6个）：
//	  - idx_trace_id (trace_id: 1)
//	  - idx_span_id (span_id: 1, unique)
//	  - idx_created_at (created_at: -1)
//	  - idx_parent_span_id (parent_span_id: 1)
//	  - idx_name (name: 1)
//	  - idx_kind (kind: 1)
//	复合索引（3个）：
//	  - idx_trace_id_created_at (trace_id: 1, created_at: -1)
//	  - idx_name_created_at (name: 1, created_at: -1)
//	  - idx_kind_created_at (kind: 1, created_at: -1)
//
// 优化说明：
//   - 此方法会在每次启动时执行，但会先检查索引是否存在，只创建不存在的索引
//   - 这样可以避免每次启动都尝试创建已存在的索引，减少不必要的网络请求和 MongoDB 检查开销
//   - 复合索引支持前缀查询，例如 idx_trace_id_created_at 可以用于 { trace_id: "xxx" } 查询
//   - 当前配置存在冗余索引（trace_id、name、kind 既有单字段索引也有复合索引），
//     但为了保持查询灵活性，暂时保留所有索引
func (e *MongoDBV2Exporter) createIndexes() error {
	e.mu.RLock()
	collection := e.collection
	e.mu.RUnlock()

	if collection == nil {
		return errors.New("mongodb collection is not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 先获取已存在的索引列表，避免重复创建
	existingIndexes := make(map[string]bool)
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list mongodb indexes: %w", err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var indexSpec bson.M
		if err := cursor.Decode(&indexSpec); err != nil {
			return fmt.Errorf("decode mongodb index: %w", err)
		}
		if name, ok := indexSpec["name"].(string); ok {
			existingIndexes[name] = true
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("iterate mongodb indexes: %w", err)
	}

	// 定义索引模型及其稳定名称。
	indexes := []mongoV2NamedIndex{
		// record_id 索引（WAL 重放的幂等键）
		{
			name: "idx_record_id",
			model: mongo.IndexModel{
				Keys: bson.D{{Key: "record_id", Value: 1}},
				Options: options.Index().
					SetName("idx_record_id").
					SetUnique(true).
					SetPartialFilterExpression(bson.D{{Key: "record_id", Value: bson.D{{Key: "$exists", Value: true}}}}),
			},
		},
		// trace_id 索引（最常用，用于查询整个trace）
		{
			name: "idx_trace_id",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "trace_id", Value: 1}},
				Options: options.Index().SetName("idx_trace_id"),
			},
		},
		// span_id 索引（用于查询单个span）
		{
			name: "idx_span_id",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "span_id", Value: 1}},
				Options: options.Index().SetName("idx_span_id").SetUnique(true),
			},
		},
		// created_at 索引（用于时间范围查询）
		{
			name: "idx_created_at",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "created_at", Value: -1}},
				Options: options.Index().SetName("idx_created_at"),
			},
		},
		// parent_span_id 索引（用于查询子spans）
		{
			name: "idx_parent_span_id",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "parent_span_id", Value: 1}},
				Options: options.Index().SetName("idx_parent_span_id"),
			},
		},
		// name 索引（用于按操作名称查询）
		{
			name: "idx_name",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "name", Value: 1}},
				Options: options.Index().SetName("idx_name"),
			},
		},
		// kind 索引（用于按span类型查询）
		{
			name: "idx_kind",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "kind", Value: 1}},
				Options: options.Index().SetName("idx_kind"),
			},
		},
		// 复合索引: trace_id + created_at（用于trace查询和时间排序）
		{
			name: "idx_trace_id_created_at",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "trace_id", Value: 1},
					{Key: "created_at", Value: -1},
				},
				Options: options.Index().SetName("idx_trace_id_created_at"),
			},
		},
		// 复合索引: name + created_at（用于按操作名称和时间查询）
		{
			name: "idx_name_created_at",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "name", Value: 1},
					{Key: "created_at", Value: -1},
				},
				Options: options.Index().SetName("idx_name_created_at"),
			},
		},
		// 复合索引: kind + created_at（用于按span类型和时间查询）
		{
			name: "idx_kind_created_at",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "kind", Value: 1},
					{Key: "created_at", Value: -1},
				},
				Options: options.Index().SetName("idx_kind_created_at"),
			},
		},
	}

	// 只创建不存在的索引，减少不必要的网络请求和 MongoDB 检查开销
	for _, index := range indexes {
		// 如果索引已存在，跳过创建
		if existingIndexes[index.name] {
			continue
		}

		// 创建不存在的索引
		_, err := collection.Indexes().CreateOne(ctx, index.model)
		if err != nil {
			return fmt.Errorf("create mongodb index %q: %w", index.name, err)
		}
	}
	return nil
}

// mongoV2NamedIndex 保存索引模型及其用于存在性检查的名称。
type mongoV2NamedIndex struct {
	name  string
	model mongo.IndexModel
}

// Shutdown 关闭导出器并清理资源（优雅关闭）
func (e *MongoDBV2Exporter) Shutdown(ctx context.Context) error {
	e.stopOnce.Do(func() {
		e.shutdownDone = make(chan struct{})
		go e.finishShutdown()
	})

	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-e.shutdownDone:
		return e.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishShutdown 在后台关闭由导出器管理的 MongoDB 连接。
func (e *MongoDBV2Exporter) finishShutdown() {
	defer close(e.shutdownDone)

	e.mu.Lock()
	client := e.client
	clientManaged := e.clientManaged
	timeout := e.timeout
	e.client = nil
	e.database = nil
	e.collection = nil
	e.mu.Unlock()

	if client == nil || !clientManaged {
		return
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	disconnectCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := client.Disconnect(disconnectCtx); err != nil {
		e.shutdownErr = fmt.Errorf("断开MongoDB连接失败: %w", err)
	}
}

// GetStats 获取统计信息
func (e *MongoDBV2Exporter) GetStats() map[string]int64 {
	return map[string]int64{
		"processed": atomic.LoadInt64(&e.processedCount),
		"dropped":   atomic.LoadInt64(&e.droppedCount),
		"errors":    atomic.LoadInt64(&e.exportErrors),
		"queue_len": 0,
		"queue_cap": 0,
	}
}

// GetQueueLength 获取当前 exporter 队列长度。
// exporter 已不再维护内部队列，因此固定返回 0。
func (e *MongoDBV2Exporter) GetQueueLength() int {
	return 0
}

// GetMaxQueueSize 获取 exporter 队列最大容量。
// exporter 已不再维护内部队列，因此固定返回 0。
func (e *MongoDBV2Exporter) GetMaxQueueSize() int {
	return 0
}
