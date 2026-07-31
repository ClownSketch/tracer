//go:build !http_hook
// +build !http_hook

package hooks

import (
	"context"
	"net/http"
)

// HTTPHookMiddleware 是一个占位符，当未开启 http_hook 构建标签时不会执行任何逻辑。
type HTTPHookMiddleware struct{}

// BeforeRequest 在未启用 http_hook 时不执行任何操作。
func (h *HTTPHookMiddleware) BeforeRequest(ctx context.Context, req *http.Request) (context.Context, error) {
	return ctx, nil
}

// AfterResponse 在未启用 http_hook 时不执行任何操作。
// 注意：这里使用 interface{} 类型，因为 HTTP 客户端库的 Response 类型我们无法直接导入
func (h *HTTPHookMiddleware) AfterResponse(ctx context.Context, req *http.Request, resp interface{}) error {
	return nil
}

// OnError 在未启用 http_hook 时不执行任何操作。
func (h *HTTPHookMiddleware) OnError(ctx context.Context, req *http.Request, err error) error {
	return nil
}

// UseHTTPHook 返回 HTTP 追踪中间件（占位符版本）
func UseHTTPHook() *HTTPHookMiddleware {
	return &HTTPHookMiddleware{}
}
