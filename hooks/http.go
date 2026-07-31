//go:build http_hook
// +build http_hook

package hooks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/baggage"
	hooktypes "github.com/ClownSketch/tracer/hooks/type"
	"github.com/ClownSketch/tracer/types/operation"
)

// HTTPHookMiddleware HTTP 追踪中间件
// 用于自动追踪 HTTP 客户端请求，无需主项目实现适配器
type HTTPHookMiddleware struct {
	// 注意：这里需要导入 HTTP 客户端的 Middleware 接口
	// 但由于这是 tracer 库，不应该依赖 HTTP 客户端库
	// 所以我们需要定义一个兼容的接口，或者使用反射
}

// BeforeRequest 请求前处理
// 注意：这个方法签名需要与 HTTP 客户端的 Middleware 接口兼容
// 但由于这是 tracer 库，我们不能直接导入 HTTP 客户端库
// 所以这里使用通用的类型
func (h *HTTPHookMiddleware) BeforeRequest(ctx context.Context, req *http.Request) (context.Context, error) {
	// 1. 检查是否跳过追踪（通过 context 中的标志）
	if isSkipTrace(ctx) {
		return ctx, nil
	}

	// 2. 从 context 中获取 span
	span := baggage.GetSpanContext(ctx)
	// 如果 span 不存在或者是 noop span，则不记录
	if span == nil || span.GetSpanName() == "" {
		return ctx, nil
	}

	// 3. 记录开始时间
	startTime := time.Now()
	ctx = context.WithValue(ctx, hooktypes.HTTPStartTime, startTime)

	// 4. 初始化 ExternalCallInfo（用于记录请求和响应信息）
	externalCallInfo := &operation.ExternalCallInfo{
		Request: &operation.RequestInfo{
			Method:      req.Method,
			DecodedURL:  req.URL.Path,
			QueryString: req.URL.RawQuery,
			Headers:     req.Header.Clone(),
			Timestamp:   startTime.Format(time.RFC3339),
		},
		Response:    make([]*operation.ResponseInfo, 0),
		IsExternal:  true, // HTTP 客户端调用都是外部调用
		Timestamp:   startTime.Format(time.RFC3339),
		ServiceName: extractServiceName(req.URL.Host), // 从 URL 提取服务名称
	}

	// 5. 读取请求体（如果存在，带大小限制）
	if req.Body != nil {
		// 默认完整记录；只有调用方显式配置时才限制链路副本大小。
		maxRequestSize := getMaxRequestBodySize(ctx)

		bodyBytes, truncated, err := readRequestBodyWithoutConsuming(req, maxRequestSize)
		if err == nil {
			externalCallInfo.Request.Body = bodyBytes
			if truncated {
				if headers, ok := externalCallInfo.Request.Headers.(http.Header); ok {
					headers.Set("X-Request-Body-Truncated", "true")
				}
			}
		}
	}

	// 6. 设置 TraceID 到请求头（用于跨服务追踪）
	if traceID := span.SpanContext().TraceID; traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
		externalCallInfo.TraceID = traceID
	}

	// 7. 将 externalCallInfo 存储到 context
	ctx = context.WithValue(ctx, hooktypes.HTTPExternalCallInfoKey, externalCallInfo)

	return ctx, nil
}

// AfterResponse 响应后处理
// 注意：这里使用 interface{} 类型，因为 HTTP 客户端库的 Response 类型我们无法直接导入
// 主项目需要确保传入的 resp 类型有 StatusCode、Status、Headers、Body、Duration 字段
func (h *HTTPHookMiddleware) AfterResponse(ctx context.Context, req *http.Request, resp interface{}) error {
	// 1. 检查是否跳过追踪
	if isSkipTrace(ctx) {
		return nil
	}

	// 2. 从 context 中获取 span 和 externalCallInfo
	span := baggage.GetSpanContext(ctx)
	if span == nil || span.GetSpanName() == "" {
		return nil
	}

	_ts, isExist := ctx.Value(hooktypes.HTTPStartTime).(time.Time)
	if !isExist {
		return nil
	}
	startTime := _ts

	externalCallInfo, ok := ctx.Value(hooktypes.HTTPExternalCallInfoKey).(*operation.ExternalCallInfo)
	if !ok || externalCallInfo == nil {
		return nil
	}

	// 3. 使用反射或类型断言获取响应信息
	// 由于我们不能直接导入 HTTP 客户端库，这里使用类型断言
	// 主项目需要确保传入的 resp 实现了特定接口
	respInfo := extractResponseInfo(resp, req, time.Since(startTime))
	if respInfo == nil {
		return nil
	}

	// 4. 添加响应信息到 ExternalCallInfo（支持重试场景，多个响应）
	externalCallInfo.AddResponse(respInfo)

	// 5. 更新 ExternalCallInfo 的总体状态
	externalCallInfo.Success = respInfo.IsSuccess
	externalCallInfo.CostSeconds = time.Since(startTime).Seconds()

	// 6. 添加事件到 span
	span.AddEvent("external.call", "external", tracer.BuildExternalCallInfoEvent(externalCallInfo))

	// 7. 如果状态码 >= 400，记录错误
	if respInfo.HttpCode >= 400 {
		span.WithError(
			fmt.Errorf("HTTP请求失败，状态码: %d", respInfo.HttpCode),
			fmt.Sprintf("HTTP请求失败: %s", req.URL.String()),
		)
	}

	return nil
}

// OnError 错误处理
func (h *HTTPHookMiddleware) OnError(ctx context.Context, req *http.Request, err error) error {
	// 1. 检查是否跳过追踪
	if isSkipTrace(ctx) {
		return nil
	}

	// 2. 从 context 中获取 span 和 externalCallInfo
	span := baggage.GetSpanContext(ctx)
	if span == nil || span.GetSpanName() == "" {
		return nil
	}

	externalCallInfo, ok := ctx.Value(hooktypes.HTTPExternalCallInfoKey).(*operation.ExternalCallInfo)
	if !ok || externalCallInfo == nil {
		return nil
	}

	// 3. 记录错误到 span
	span.WithError(err, fmt.Sprintf("HTTP请求失败: %s", req.URL.String()))

	// 4. 记录错误到 ExternalCallInfo
	responseInfo := &operation.ResponseInfo{
		Method:          req.Method,
		Body:            err.Error(),
		BusinessCodeMsg: "请求失败",
		CostSeconds:     0, // 错误时无法计算耗时
		Timestamp:       time.Now().Format(time.RFC3339),
		IsSuccess:       false,
	}
	externalCallInfo.AddResponse(responseInfo)
	externalCallInfo.Success = false

	// 5. 添加事件（即使出错也记录）
	span.AddEvent("external.call", "external", tracer.BuildExternalCallInfoEvent(externalCallInfo))

	return nil
}

// extractServiceName 从 URL 提取服务名称
func extractServiceName(host string) string {
	if host == "" {
		return "unknown"
	}
	// 移除端口号
	if idx := len(host); idx > 0 {
		for i := 0; i < idx; i++ {
			if host[i] == ':' {
				return host[:i]
			}
		}
	}
	return host
}

// extractResponseInfo 从响应对象中提取响应信息
// 使用反射，支持多种响应类型（避免直接依赖 HTTP 客户端库）
func extractResponseInfo(resp interface{}, req *http.Request, duration time.Duration) *operation.ResponseInfo {
	if resp == nil {
		return nil
	}

	// 使用反射获取字段值
	// 假设 resp 有 StatusCode、Status、Headers、Body、Duration 字段
	// 这样可以避免直接依赖 HTTP 客户端库
	v := reflect.ValueOf(resp)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil
	}

	// 获取 StatusCode
	statusCodeField := v.FieldByName("StatusCode")
	if !statusCodeField.IsValid() || !statusCodeField.CanInterface() {
		return nil
	}
	statusCode, ok := statusCodeField.Interface().(int)
	if !ok {
		return nil
	}

	// 获取 Status
	statusField := v.FieldByName("Status")
	status := ""
	if statusField.IsValid() && statusField.CanInterface() {
		if s, ok := statusField.Interface().(string); ok {
			status = s
		}
	}

	// 获取 Headers
	headersField := v.FieldByName("Headers")
	headers := make(http.Header)
	if headersField.IsValid() && headersField.CanInterface() {
		if h, ok := headersField.Interface().(http.Header); ok {
			headers = h
		}
	}

	// 获取 Body
	bodyField := v.FieldByName("Body")
	body := ""
	if bodyField.IsValid() && bodyField.CanInterface() {
		if b, ok := bodyField.Interface().([]byte); ok {
			body = string(b)
		}
	}

	// 获取 Duration（如果可用）
	costSeconds := duration.Seconds()
	durationField := v.FieldByName("Duration")
	if durationField.IsValid() && durationField.CanInterface() {
		if d, ok := durationField.Interface().(time.Duration); ok && d > 0 {
			costSeconds = d.Seconds()
		}
	}

	return &operation.ResponseInfo{
		Method:      req.Method,
		HttpCode:    statusCode,
		HttpCodeMsg: status,
		Header:      headers,
		Body:        body,
		CostSeconds: costSeconds,
		Timestamp:   time.Now().Format(time.RFC3339),
		IsSuccess:   statusCode < 400,
	}
}

type requestBodyReadCloser struct {
	io.Reader
	io.Closer
}

// readRequestBodyWithoutConsuming 读取有限的追踪副本，并保持业务请求体完整。
func readRequestBodyWithoutConsuming(req *http.Request, maxSize int64) ([]byte, bool, error) {
	if req == nil || req.Body == nil {
		return nil, false, nil
	}

	if req.GetBody != nil {
		bodyCopy, err := req.GetBody()
		if err == nil {
			defer bodyCopy.Close()
			if maxSize <= 0 {
				data, readErr := io.ReadAll(bodyCopy)
				return data, false, readErr
			}
			return readLimitedBody(bodyCopy, maxSize)
		}
	}

	originalBody := req.Body
	var (
		consumed  []byte
		recorded  []byte
		truncated bool
		err       error
	)
	if maxSize > 0 {
		consumed, err = io.ReadAll(io.LimitReader(originalBody, maxSize+1))
		recorded = consumed
		if int64(len(recorded)) > maxSize {
			recorded = recorded[:maxSize]
			truncated = true
		}
	} else {
		consumed, err = io.ReadAll(originalBody)
		recorded = consumed
	}
	req.Body = &requestBodyReadCloser{
		Reader: io.MultiReader(bytes.NewReader(consumed), originalBody),
		Closer: originalBody,
	}
	if err != nil {
		return recorded, false, err
	}
	return recorded, truncated, nil
}

// readLimitedBody 多读一个字节用于判断是否截断。
func readLimitedBody(reader io.Reader, maxSize int64) ([]byte, bool, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxSize+1))
	if err != nil {
		return data, false, err
	}
	if int64(len(data)) > maxSize {
		return data[:maxSize], true, nil
	}
	return data, false, nil
}

// isSkipTrace 检查是否跳过追踪
func isSkipTrace(ctx context.Context) bool {
	// 检查 context 中是否有跳过追踪的标志
	// 这个标志由 HTTP 客户端库设置
	// 注意：HTTP 客户端库使用 "skip_trace" 作为 key，我们需要兼容它
	type skipTraceKeyType string
	skipTraceKey := skipTraceKeyType("skip_trace")

	// 优先检查 HTTP 客户端库使用的 key
	if skip, ok := ctx.Value(skipTraceKey).(bool); ok && skip {
		return true
	}

	// 兼容检查 hook 自己的 key（如果 HTTP 客户端库未来使用）
	if skip, ok := ctx.Value(hooktypes.HTTPSkipTraceKey).(bool); ok && skip {
		return true
	}

	return false
}

// getMaxRequestBodySize 从 context 中获取最大请求体大小
func getMaxRequestBodySize(ctx context.Context) int64 {
	if size, ok := ctx.Value(hooktypes.HTTPMaxRequestBodySizeKey).(int64); ok {
		return size
	}
	return 0
}

// UseHTTPHook 返回 HTTP 追踪中间件
// 主项目可以直接使用这个中间件，无需实现适配器
// 使用示例：
//
//	import (
//		httpClient "github.com/ClownSketch/client/http"
//		"github.com/ClownSketch/tracer/hooks"
//	)
//
//	client := httpClient.NewClient().
//		Use(hooks.UseHTTPHook())
func UseHTTPHook() *HTTPHookMiddleware {
	return &HTTPHookMiddleware{}
}
