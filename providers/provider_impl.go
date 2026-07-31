package providers

import (
	"context"
	"errors"
	"sync"

	"github.com/ClownSketch/tracer/core"
	"github.com/ClownSketch/tracer/processor"
	"github.com/ClownSketch/tracer/sampler"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/types"
)

// TracerProviderConfig 定义追踪器提供者配置
type TracerProviderConfig struct {
	Processors []trace.SpanProcessor // 处理器列表
	Sampler    trace.SpanSampler     // 采样器
	Resource   *types.ResourceInfo   // 默认资源信息
}

// TracerProviderOption 定义追踪器提供者选项
type TracerProviderOption func(c *TracerProviderConfig)

// providerImpl 是 TracerProvider 的实现
type tracerProviderImpl struct {
	mu             sync.RWMutex            // 保护 tracers map 的读写锁
	tracers        map[string]trace.Tracer // Tracer 实例缓存，按名称存储
	spanProcessors []trace.SpanProcessor   // Span 处理器列表
	sampler        trace.SpanSampler       // 默认的采样器
	resource       *types.ResourceInfo     // 默认资源信息
	shutdownOnce   sync.Once               // 确保只关闭一次
	shutdown       chan struct{}           // 关闭信号通道
	shutdownDone   chan struct{}           // 后台关闭完成信号
	shutdownErr    error                   // 首次关闭的完整错误
}

// WithSpanProcessor 添加一个 SpanProcessor
func WithSpanProcessor(proc trace.SpanProcessor) TracerProviderOption {
	return func(cfg *TracerProviderConfig) {
		if proc == nil {
			return
		}
		// 添加处理器到处理器列表
		cfg.Processors = append(cfg.Processors, proc)
	}
}

// WithSampler 设置采样器
func WithSampler(sampler trace.SpanSampler) TracerProviderOption {
	return func(cfg *TracerProviderConfig) {
		if sampler == nil {
			return
		}
		// 设置采样器
		cfg.Sampler = sampler
	}
}

// WithServiceName 设置所有 Span 的默认服务名称。
func WithServiceName(serviceName string) TracerProviderOption {
	return func(cfg *TracerProviderConfig) {
		if serviceName == "" {
			return
		}
		cfg.Resource = &types.ResourceInfo{ServiceName: serviceName}
	}
}

// NewTracerProvider 创建一个新的 TracerProvider 实例
func NewTracerProvider(opts ...TracerProviderOption) trace.TracerProvider {
	cfg := &TracerProviderConfig{
		Processors: []trace.SpanProcessor{},          // 处理器列表
		Sampler:    sampler.NewAlwaysSampleSampler(), // 默认全采样
	}

	for _, opt := range opts {
		opt(cfg)
	}

	p := &tracerProviderImpl{
		tracers:        make(map[string]trace.Tracer), // 追踪器列表，初始化为空
		spanProcessors: cfg.Processors,                // 处理器列表
		sampler:        cfg.Sampler,                   // 采样器
		resource:       cfg.Resource,                  // 默认资源信息
		shutdown:       make(chan struct{}),           // 初始化关闭信号通道
		shutdownDone:   make(chan struct{}),           // 初始化关闭完成信号
	}

	return p
}

// GetTracer 获取一个命名的 Tracer 实例（若不存在则创建）
func (p *tracerProviderImpl) GetTracer(tracerName string) trace.Tracer {

	p.mu.RLock() // 加读锁
	if tracer, ok := p.tracers[tracerName]; ok {
		p.mu.RUnlock() // 解锁
		return tracer
	}
	p.mu.RUnlock() // 释放读锁

	p.mu.Lock()         // 加写锁
	defer p.mu.Unlock() // 解锁
	if tracer, ok := p.tracers[tracerName]; ok {
		return tracer
	}

	// 创建一个新的TracerImpl实例
	tracer := core.NewTracerImplWithResource(
		tracerName,
		p,
		processor.NewMultiSpanProcessor(p.spanProcessors...),
		p.sampler,
		p.resource,
	)

	// 将新的TracerImpl实例添加到追踪器列表中，键为追踪器名称，值为TracerImpl实例
	p.tracers[tracerName] = tracer

	return tracer // 返回新的TracerImpl实例
}

// Shutdown 优雅关闭所有 SpanProcessor
// @param ctx 上下文
// @return error 错误，如果关闭失败，则返回错误
func (p *tracerProviderImpl) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		close(p.shutdown) // 关闭关闭信号通道
		go p.finishShutdown()
	})

	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-p.shutdownDone:
		return p.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishShutdown 在后台完整关闭所有处理器。
func (p *tracerProviderImpl) finishShutdown() {
	defer close(p.shutdownDone)
	for _, proc := range p.spanProcessors {
		if proc == nil {
			continue
		}
		if err := proc.Shutdown(context.Background()); err != nil {
			p.shutdownErr = errors.Join(p.shutdownErr, err)
		}
	}
}
