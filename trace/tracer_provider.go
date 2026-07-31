package trace

import "context"

// TracerProvider 是用于提供 Tracer 实例的接口。
//
// TracerProvider 管理 Tracer 及其关联资源的生命周期。
// 通常每个应用程序创建一个 TracerProvider，用于为不同服务获取 Tracer 实例。
//
// 示例:
//
//	provider := providers.NewTracerProvider(
//		providers.WithSpanProcessor(processor),
//		providers.WithSampler(sampler.NewProbabilitySampler(0.1)),
//	)
//	defer provider.Shutdown(context.Background())
//
//	tracer := provider.GetTracer("my-service")
type TracerProvider interface {
	// GetTracer 返回指定服务名称的 Tracer 实例。
	//
	// 相同的服务名称将始终返回相同的 Tracer 实例。
	// 不同的服务名称将返回不同的 Tracer 实例。
	//
	// 参数:
	//   - tracerName: 将使用此 Tracer 的服务或组件名称。
	//                 此名称通常用于过滤和分组追踪。
	//
	// 返回值:
	//   - Tracer: 指定服务名称的 Tracer 实例。
	//
	// 示例:
	//
	//	tracer := provider.GetTracer("user-service")
	//	ctx, span := tracer.Start(ctx, "get-user")
	//	defer span.End()
	GetTracer(tracerName string) Tracer

	// Shutdown 优雅地关闭 TracerProvider 及其所有关联资源。
	//
	// 当应用程序关闭时应该调用此方法，以确保所有待处理的 span 都被导出，
	// 并且资源被正确释放。
	//
	// 参数:
	//   - ctx: 用于控制关闭超时的 context。如果 context 被取消，关闭将被中止。
	//
	// 返回值:
	//   - error: 如果关闭失败或超时，返回错误。
	//
	// 示例:
	//
	//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	//	defer cancel()
	//	if err := provider.Shutdown(ctx); err != nil {
	//		log.Printf("关闭 TracerProvider 失败: %v", err)
	//	}
	Shutdown(ctx context.Context) error
}
