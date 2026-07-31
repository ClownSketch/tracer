package exporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ClownSketch/tracer/trace"
)

// MongoDBRoutingExporter 支持按 Span 指定的集合名路由写入 MongoDB。
// 未设置集合名的 Span 会写入初始化时的默认集合（MongoDBCollection）。
type MongoDBRoutingExporter struct {
	base *MongoDBExporter

	defaultCollectionName string
	allowedCollections    map[string]struct{}

	collectionCache    sync.Map // collectionName -> *mongo.Collection
	indexedCollections sync.Map // collectionName -> struct{}
	indexMu            sync.Mutex
}

type mongodbRoutingExporterBuilder struct {
	mongoOpts []MongoDBExporterOption
	allowed   map[string]struct{}
}

// MongoDBRoutingExporterOption MongoDB 路由导出器选项。
type MongoDBRoutingExporterOption func(*mongodbRoutingExporterBuilder)

// RoutingWithMongoOption 复用 MongoDBExporter 的选项（连接、超时、重试等）。
func RoutingWithMongoOption(opt MongoDBExporterOption) MongoDBRoutingExporterOption {
	return func(b *mongodbRoutingExporterBuilder) {
		b.mongoOpts = append(b.mongoOpts, opt)
	}
}

// WithMongoDBRoutingAllowedCollections 限制允许写入的集合名白名单。
// 未配置时不限制；Span 指定的集合不在白名单内时回退到默认集合。
func WithMongoDBRoutingAllowedCollections(names ...string) MongoDBRoutingExporterOption {
	return func(b *mongodbRoutingExporterBuilder) {
		if len(names) == 0 {
			return
		}
		if b.allowed == nil {
			b.allowed = make(map[string]struct{}, len(names))
		}
		for _, name := range names {
			if name != "" {
				b.allowed[name] = struct{}{}
			}
		}
	}
}

// NewMongoDBRoutingExporter 创建 MongoDB 路由导出器。
// uri、database、collection 与 MongoDBExporter 相同；collection 为默认集合。
func NewMongoDBRoutingExporter(uri, database, collection string, opts ...MongoDBRoutingExporterOption) (*MongoDBRoutingExporter, error) {
	builder := &mongodbRoutingExporterBuilder{}
	for _, opt := range opts {
		opt(builder)
	}

	base, err := NewMongoDBExporter(uri, database, collection, builder.mongoOpts...)
	if err != nil {
		return nil, err
	}

	e := &MongoDBRoutingExporter{
		base:                  base,
		defaultCollectionName: collection,
		allowedCollections:    builder.allowed,
	}

	if base.collection != nil && collection != "" {
		e.collectionCache.Store(collection, base.collection)
		e.indexedCollections.Store(collection, struct{}{})
	}

	return e, nil
}

// ExportSpan 同步导出单个 span。
func (e *MongoDBRoutingExporter) ExportSpan(span trace.SpanSnapshot) error {
	return e.ExportSpans([]trace.SpanSnapshot{span})
}

// ExportSpanSync 同步导出单个 span。
func (e *MongoDBRoutingExporter) ExportSpanSync(ctx context.Context, span trace.SpanSnapshot) error {
	return e.ExportSpansSync(ctx, []trace.SpanSnapshot{span})
}

// ExportSpans 同步批量写入 MongoDB，按 Span 集合名分组后批量 InsertMany。
func (e *MongoDBRoutingExporter) ExportSpans(spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}

	items := make([]mongoRoutingQueueItem, 0, len(spans))
	validCount := 0
	for _, span := range spans {
		if span == nil {
			continue
		}
		collection, err := e.resolveCollection(span)
		if err != nil {
			atomic.AddInt64(&e.base.exportErrors, 1)
			return err
		}
		validCount++
		items = append(items, mongoRoutingQueueItem{
			doc:        e.base.buildDocument(span, false),
			collection: collection,
		})
	}
	if len(items) == 0 {
		return nil
	}

	atomic.AddInt64(&e.base.processedCount, int64(validCount))
	if err := e.writeRoutingBatchWithRetry(context.Background(), items); err != nil {
		atomic.AddInt64(&e.base.exportErrors, int64(len(items)))
		return err
	}
	return nil
}

// ExportSpansSync 同步批量导出多个 spans。
func (e *MongoDBRoutingExporter) ExportSpansSync(ctx context.Context, spans []trace.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}
	defer releaseSpanSnapshots(spans)

	items := make([]mongoRoutingQueueItem, 0, len(spans))
	validCount := 0
	for _, span := range spans {
		if span == nil {
			continue
		}
		collection, err := e.resolveCollection(span)
		if err != nil {
			atomic.AddInt64(&e.base.exportErrors, 1)
			return err
		}
		validCount++
		items = append(items, mongoRoutingQueueItem{
			doc:        e.base.buildDocument(span, true),
			collection: collection,
		})
	}
	if len(items) == 0 {
		return nil
	}

	atomic.AddInt64(&e.base.processedCount, int64(validCount))
	if err := e.writeRoutingBatchWithRetry(ctx, items); err != nil {
		atomic.AddInt64(&e.base.exportErrors, int64(len(items)))
		return err
	}
	return nil
}

type mongoRoutingQueueItem struct {
	doc        any
	collection *mongo.Collection
}

func (e *MongoDBRoutingExporter) resolveCollection(span trace.SpanSnapshot) (*mongo.Collection, error) {
	name := span.GetMongoCollection()
	if name == "" {
		return e.defaultCollection()
	}
	if !e.isCollectionAllowed(name) {
		return e.defaultCollection()
	}
	return e.getOrCreateCollection(name)
}

func (e *MongoDBRoutingExporter) defaultCollection() (*mongo.Collection, error) {
	e.base.mu.RLock()
	collection := e.base.collection
	e.base.mu.RUnlock()
	if collection == nil {
		return nil, errors.New("mongodb default collection is not available")
	}
	return collection, nil
}

func (e *MongoDBRoutingExporter) isCollectionAllowed(name string) bool {
	if len(e.allowedCollections) == 0 {
		return true
	}
	_, ok := e.allowedCollections[name]
	return ok
}

func (e *MongoDBRoutingExporter) getOrCreateCollection(name string) (*mongo.Collection, error) {
	if cached, ok := e.collectionCache.Load(name); ok {
		collection := cached.(*mongo.Collection)
		if err := e.ensureRoutingCollectionIndexes(collection, name); err != nil {
			return nil, err
		}
		return collection, nil
	}

	e.base.mu.RLock()
	database := e.base.database
	e.base.mu.RUnlock()
	if database == nil {
		return nil, fmt.Errorf("mongodb database is not available for collection %q", name)
	}

	collection := database.Collection(name)
	actual, _ := e.collectionCache.LoadOrStore(name, collection)
	resolved := actual.(*mongo.Collection)
	if err := e.ensureRoutingCollectionIndexes(resolved, name); err != nil {
		return nil, err
	}
	return resolved, nil
}

func (e *MongoDBRoutingExporter) writeRoutingBatchWithRetry(ctx context.Context, items []mongoRoutingQueueItem) error {
	if len(items) == 0 {
		return nil
	}

	e.base.mu.RLock()
	timeout := e.base.timeout
	e.base.mu.RUnlock()

	first := items[0].collection
	for i := 1; i < len(items); i++ {
		if items[i].collection != first {
			return e.writeRoutingBatchGrouped(ctx, timeout, items)
		}
	}

	queueItems := make([]mongoQueueItem, len(items))
	for i, item := range items {
		queueItems[i] = mongoQueueItem{doc: item.doc}
	}
	return e.base.writeItemsWithRetry(ctx, first, timeout, queueItems)
}

func (e *MongoDBRoutingExporter) writeRoutingBatchGrouped(ctx context.Context, timeout time.Duration, items []mongoRoutingQueueItem) error {
	groups := make(map[*mongo.Collection][]mongoQueueItem)
	for _, item := range items {
		groups[item.collection] = append(groups[item.collection], mongoQueueItem{doc: item.doc})
	}

	for collection, chunk := range groups {
		if err := e.base.writeItemsWithRetry(ctx, collection, timeout, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (e *MongoDBRoutingExporter) ensureRoutingCollectionIndexes(collection *mongo.Collection, name string) error {
	if _, loaded := e.indexedCollections.Load(name); loaded {
		return nil
	}

	e.indexMu.Lock()
	defer e.indexMu.Unlock()
	if _, loaded := e.indexedCollections.Load(name); loaded {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existingIndexes := make(map[string]bool)
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list mongodb indexes for collection %q: %w", name, err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var indexSpec bson.M
		if err := cursor.Decode(&indexSpec); err != nil {
			return fmt.Errorf("decode mongodb index for collection %q: %w", name, err)
		}
		if indexName, ok := indexSpec["name"].(string); ok {
			existingIndexes[indexName] = true
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("iterate mongodb indexes for collection %q: %w", name, err)
	}

	indexes := routingCollectionIndexModels()
	for _, index := range indexes {
		indexName := index.Options.Name
		if indexName == nil {
			continue
		}
		if existingIndexes[*indexName] {
			continue
		}
		if _, err := collection.Indexes().CreateOne(ctx, index); err != nil {
			return fmt.Errorf("create mongodb index %q for collection %q: %w", *indexName, name, err)
		}
	}
	e.indexedCollections.Store(name, struct{}{})
	return nil
}

func routingCollectionIndexModels() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "record_id", Value: 1}},
			Options: options.Index().
				SetName("idx_record_id").
				SetUnique(true).
				SetPartialFilterExpression(bson.D{{Key: "record_id", Value: bson.D{{Key: "$exists", Value: true}}}}),
		},
		{
			Keys:    bson.D{{Key: "trace_id", Value: 1}},
			Options: options.Index().SetName("idx_trace_id"),
		},
		{
			Keys:    bson.D{{Key: "span_id", Value: 1}},
			Options: options.Index().SetName("idx_span_id").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "created_at", Value: -1}},
			Options: options.Index().SetName("idx_created_at"),
		},
		{
			Keys:    bson.D{{Key: "parent_span_id", Value: 1}},
			Options: options.Index().SetName("idx_parent_span_id"),
		},
		{
			Keys:    bson.D{{Key: "name", Value: 1}},
			Options: options.Index().SetName("idx_name"),
		},
		{
			Keys:    bson.D{{Key: "kind", Value: 1}},
			Options: options.Index().SetName("idx_kind"),
		},
		{
			Keys: bson.D{
				{Key: "trace_id", Value: 1},
				{Key: "created_at", Value: -1},
			},
			Options: options.Index().SetName("idx_trace_id_created_at"),
		},
		{
			Keys: bson.D{
				{Key: "name", Value: 1},
				{Key: "created_at", Value: -1},
			},
			Options: options.Index().SetName("idx_name_created_at"),
		},
		{
			Keys: bson.D{
				{Key: "kind", Value: 1},
				{Key: "created_at", Value: -1},
			},
			Options: options.Index().SetName("idx_kind_created_at"),
		},
	}
}

// Shutdown 关闭导出器并清理资源。
func (e *MongoDBRoutingExporter) Shutdown(ctx context.Context) error {
	if e.base == nil {
		return nil
	}
	return e.base.Shutdown(ctx)
}

// GetStats 获取统计信息。
func (e *MongoDBRoutingExporter) GetStats() map[string]int64 {
	if e.base == nil {
		return map[string]int64{}
	}
	return e.base.GetStats()
}

// GetQueueLength 获取当前 exporter 队列长度。
func (e *MongoDBRoutingExporter) GetQueueLength() int {
	if e.base == nil {
		return 0
	}
	return e.base.GetQueueLength()
}

// GetMaxQueueSize 获取 exporter 队列最大容量。
func (e *MongoDBRoutingExporter) GetMaxQueueSize() int {
	if e.base == nil {
		return 0
	}
	return e.base.GetMaxQueueSize()
}
