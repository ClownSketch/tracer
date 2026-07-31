package processor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ClownSketch/tracer/trace"
)

// MultiSpanProcessor 是一个多Span处理器实现
type MultiSpanProcessor struct {
	// 读写锁，用于保护处理器列表
	mu sync.RWMutex
	// 处理器列表
	processors []trace.SpanProcessor
}

// sharedSpanSnapshot 让多个处理器共享同一份只读快照数据。
type sharedSpanSnapshot struct {
	trace.SpanSnapshot
	remaining atomic.Int32
}

// retainedSpanSnapshot 保存单个处理器对共享快照的释放状态。
type retainedSpanSnapshot struct {
	*sharedSpanSnapshot
	releaseOnce sync.Once
}

// Release 释放当前处理器持有的快照引用。
func (s *retainedSpanSnapshot) Release() {
	if s == nil || s.sharedSpanSnapshot == nil {
		return
	}
	s.releaseOnce.Do(func() {
		if s.remaining.Add(-1) == 0 && s.SpanSnapshot != nil {
			s.SpanSnapshot.Release()
		}
	})
}

// NewMultiSpanProcessor 创建一个新的MultiSpanProcessor
func NewMultiSpanProcessor(processors ...trace.SpanProcessor) trace.SpanProcessor {
	return &MultiSpanProcessor{processors: processors}
}

// AddProcessor 添加处理器
// @param proc 处理器
func (p *MultiSpanProcessor) AddProcessor(processor trace.SpanProcessor) {
	p.mu.Lock()         // 加写锁
	defer p.mu.Unlock() // 解锁
	// 添加处理器
	p.processors = append(p.processors, processor)
}

// RemoveProcessor 删除处理器

// OnStart 实现trace.SpanProcessor接口
// 主要用于在Span开始时执行，可以用于记录Span的开始时间、设置Span的标签、属性等
// @param ctx 上下文
// @param span Span实例
func (p *MultiSpanProcessor) OnStart(ctx context.Context, span trace.Span) {
	p.mu.RLock()         // 加读锁
	defer p.mu.RUnlock() //解锁
	// 遍历处理器列表，执行每个处理器的OnStart方法
	for _, processor := range p.processors {
		if processor != nil {
			processor.OnStart(ctx, span)
		}
	}

}

// OnEnd 实现trace.SpanProcessor接口
// 主要用于在Span结束时执行，可以用于记录Span的结束时间、设置Span的标签、属性等
// @param span Span快照
func (p *MultiSpanProcessor) OnEnd(span trace.SpanSnapshot) {
	if span == nil {
		return
	}

	p.mu.RLock()
	processors := append([]trace.SpanProcessor(nil), p.processors...)
	p.mu.RUnlock()

	if len(processors) == 0 {
		span.Release()
		return
	}

	shared := &sharedSpanSnapshot{SpanSnapshot: span}
	shared.remaining.Store(int32(len(processors)))
	for _, processor := range processors {
		retained := &retainedSpanSnapshot{sharedSpanSnapshot: shared}
		if processor == nil {
			retained.Release()
			continue
		}
		processor.OnEnd(retained)
	}
}

// Shutdown 实现trace.SpanProcessor接口
// 主要用于在追踪器关闭时执行，可以用于关闭Span处理器、关闭资源等
// @param ctx 上下文
// @return error 错误，如果关闭失败，则返回错误
func (p *MultiSpanProcessor) Shutdown(ctx context.Context) error {
	p.mu.RLock()
	processors := append([]trace.SpanProcessor(nil), p.processors...)
	p.mu.RUnlock()

	var shutdownErr error
	for _, processor := range processors {
		if processor != nil {
			shutdownErr = errors.Join(shutdownErr, processor.Shutdown(ctx))
		}
	}
	return shutdownErr
}
