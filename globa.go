package tracer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ClownSketch/tracer/baggage"
	"github.com/ClownSketch/tracer/trace"
	"github.com/ClownSketch/tracer/trace/noop"
)

// providerHolder 为 atomic.Value 提供固定的 Provider 存储类型。
type providerHolder struct {
	provider trace.TracerProvider
}

// tracerHolder 为 atomic.Value 提供固定的 Tracer 存储类型。
type tracerHolder struct {
	tracer trace.Tracer
}

var (
	// globalProvider 使用原子值存储全局追踪器提供者
	// 确保在并发访问时的线程安全性
	globalProvider atomic.Value

	// globalTracer 使用原子值存储全局追踪器
	// 提供快速访问的默认追踪器实例
	globalTracer atomic.Value

	// 定义业务项目根路径，在error获取堆栈的时候，会过滤不属于该路径的堆栈
	businessRootPath string
	// initRootOnce 确保路径只被初始化一次，彻底杜绝 Data Race
	initRootOnce sync.Once

	// 定义最大堆栈深度
	MaxStackTraceDepth = 10
)

// SetBusinessRootPath 设置业务项目根路径
// 参数:
//   - path: 业务项目根路径
func SetBusinessRootPath(path string) {
	initRootOnce.Do(func() {
		businessRootPath = filepath.ToSlash(strings.TrimSuffix(path, "/"))
	})
}

// GetBusinessRootPath 返回当前设置的业务项目根路径（若为空则尝试自动检测）
func GetBusinessRootPath() string {
	return businessRootPath
}

// detectBusinessRootPath 自动检测业务项目根路径（仅在未设置时触发）
func detectBusinessRootPath() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	for {
		// 探测当前目录是否存在 go.mod
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.ToSlash(dir)
		}

		// 向上层目录回溯
		parent := filepath.Dir(dir)
		// 到达根目录 (如 "/" 或 "C:\")
		if parent == dir {
			break
		}
		dir = parent
	}

	return ""
}

// SetTracerProvider 设置全局追踪器提供者
// 这个函数会:
// 1. 保存新的提供者
// 2. 创建默认追踪器
// 旧 Provider 的关闭由创建它的宿主负责，避免全局替换阻塞业务协程。
// 参数:
//   - provider: 新的追踪器提供者
func SetTracerProvider(provider trace.TracerProvider, tracerName string) {
	if tracerName == "" {
		tracerName = "default"
	}
	if provider == nil {
		provider = &noop.NoopTracerProvider{}
	}

	// 如果用户没有提前调用 SetBusinessRootPath 手动设置，则自动探测
	if businessRootPath == "" {
		SetBusinessRootPath(detectBusinessRootPath())
	}

	// 存储新的提供者
	globalProvider.Store(providerHolder{provider: provider})

	// 创建并存储默认追踪器
	tr := provider.GetTracer(tracerName)
	// 存储默认追踪器
	globalTracer.Store(tracerHolder{tracer: tr})

}

// GetTracerProvider 获取全局追踪器提供者
// 如果没有设置提供者，返回无操作提供者
// 返回:
//   - 当前的追踪器提供者
func GetTracerProvider() trace.TracerProvider {
	if provider := globalProvider.Load(); provider != nil {
		return provider.(providerHolder).provider
	}
	return &noop.NoopTracerProvider{}
}

// GetTracer 返回全局追踪器
// 这是获取追踪器的推荐方法
// 如果没有设置追踪器，返回无操作追踪器
// 返回:
//   - 当前的全局追踪器
func GetTracer(tracerName string) trace.Tracer {
	if tracerName == "" {
		tracerName = "default"
	}

	provider := GetTracerProvider()
	if provider != nil {
		return provider.GetTracer(tracerName)
	}
	return &noop.NoopTracer{}
}

// GetTraceID 从上下文中获取当前 TraceID
//
// 这是获取 TraceID 的推荐方法，适用于需要记录日志、错误追踪等场景。
// 方法会优先从当前活跃的 Span 获取 TraceID，如果 Span 不存在，则从 SpanContext 获取。
//
// 参数:
//   - ctx: 包含追踪信息的上下文
//
// 返回值:
//   - string: 当前 TraceID（16 进制字符串，长度为 32），如果不存在则返回空字符串
//
// 示例:
//
//	// 在业务代码中获取 TraceID 用于日志记录
//	func handleRequest(ctx context.Context) {
//		traceID := tracer.GetTraceID(ctx)
//		if traceID != "" {
//			log.Printf("[TraceID: %s] 处理请求", traceID)
//		}
//		// ... 业务逻辑
//	}
//
//	// 在错误处理中获取 TraceID
//	func handleError(ctx context.Context, err error) {
//		traceID := tracer.GetTraceID(ctx)
//		log.Printf("[TraceID: %s] 错误: %v", traceID, err)
//	}
//
//	// 在中间件中获取 TraceID 并添加到响应头
//	func traceMiddleware(ctx context.Context, w http.ResponseWriter) {
//		traceID := tracer.GetTraceID(ctx)
//		if traceID != "" {
//			w.Header().Set("X-Trace-ID", traceID)
//		}
//	}
func GetTraceID(ctx context.Context) string {
	return baggage.GetTraceIDFromContext(ctx)
}
