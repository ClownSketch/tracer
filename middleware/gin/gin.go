package middleware

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/attribute"
	propagationHttp "github.com/ClownSketch/tracer/propagation/http"
	"github.com/ClownSketch/tracer/types"
	"github.com/ClownSketch/tracer/types/operation"
	"github.com/gin-gonic/gin"
)

// ginServerTraceConfig 是用于配置 Gin 中间件的配置
type ginServerTraceConfig struct {
	extractUpstream    bool               // 是否提取上游链路信息
	recordPolicy       tracer.SpanOptions // 记录策略
	spanNameFormatter  SpanNameFormatter  // Start 前解析 span 名称
	maxRequestBodySize int64              // 链路请求体上限，0 表示完整记录
}

/*
	 GinMiddleware 是用于单体服务的 Gin 中间件
	 它会在请求开始时创建一个 Span，在请求结束时结束 Span
	 它会在请求发生错误时记录错误日志
	 它会在请求结束时记录响应状态码
	 它会在请求结束时记录请求耗时
	 它会在请求结束时记录请求URL
	 它会在请求结束时记录请求方法

	 与 GinCrossServiceMiddleware 的区别：
	 - GinMiddleware: 用于单体服务，不提取上游链路信息，性能更好
	 - GinCrossServiceMiddleware: 用于跨服务调用，会提取上游链路信息和 baggage

	 示例:
		// 单体服务
		r.Use(middleware.GinMiddleware())
		// 自定义 span 名称
		r.Use(middleware.GinMiddleware(
			middleware.WithSpanNameFormatter(func(c *gin.Context) string {
				return "gateway.payin.unified"
			}),
		))
		// 跨服务调用
		r.Use(middleware.GinCrossServiceMiddleware())
		// 内部服务间调用
		r.Use(middleware.GinInternalServiceMiddleware())
*/
func GinMiddleware(opts ...GinOption) gin.HandlerFunc {
	cfg := ginServerTraceConfig{
		extractUpstream: false,
		recordPolicy:    tracer.WithForceRecord(),
	}
	applyGinOptions(&cfg, opts...)
	return ginServerTraceMiddleware(cfg)
}

// GinCrossServiceMiddleware 跨服务调用的 Gin 中间件
// 用于处理跨服务链路追踪，会自动从 HTTP Header 提取上游链路信息
// 适用于微服务架构中的服务端。
//
// 与 GinMiddleware 的区别：
// - GinMiddleware: 用于单体服务，不提取上游链路信息，性能更好
// - GinCrossServiceMiddleware: 用于跨服务调用，会提取上游链路信息和 baggage
func GinCrossServiceMiddleware(opts ...GinOption) gin.HandlerFunc {
	cfg := ginServerTraceConfig{
		extractUpstream: true,
		recordPolicy:    tracer.WithForceRecord(),
	}
	applyGinOptions(&cfg, opts...)
	return ginServerTraceMiddleware(cfg)
}

// GinInternalServiceMiddleware 内部服务间调用的 Gin 中间件
// 行为与 GinCrossServiceMiddleware 相同（Extract 上游 trace + baggage），
// 适用于 manager → gateway 等内部 HTTP。
func GinInternalServiceMiddleware(opts ...GinOption) gin.HandlerFunc {
	cfg := ginServerTraceConfig{
		extractUpstream: true,
		recordPolicy:    tracer.WithForceRecord(),
	}
	applyGinOptions(&cfg, opts...)
	return ginServerTraceMiddleware(cfg)
}

// ginServerTraceMiddleware 是用于 Gin 中间件的链路追踪中间件
func ginServerTraceMiddleware(cfg ginServerTraceConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 不追踪 OPTIONS 请求
		if c.Request.Method == http.MethodOptions {
			c.Next()
			return
		}

		// 获取请求的上下文
		baseCtx := c.Request.Context()
		// 如果需要提取上游链路信息，则提取上游链路信息
		if cfg.extractUpstream {
			// 创建 HTTP 头载体
			carrier := propagationHttp.NewHTTPHeaderCarrier(c.Request.Header)
			// 创建 HTTP 传播器
			propagator := propagationHttp.NewHTTPPropagator()
			// 提取上游链路信息
			baseCtx = propagator.Extract(baseCtx, carrier)
		}

		// 获取请求的路径
		route := c.FullPath()
		// 解析 span 名称
		spanName := resolveSpanName(c, cfg.spanNameFormatter)

		// 创建 span 选项
		startOpts := []tracer.SpanOptions{
			// 设置 span 类型
			tracer.WithSpanKind(types.SpanKindServer),
			// 设置记录策略
			cfg.recordPolicy,
		}
		// 创建 span
		spanCtx, span := tracer.GetTracer("").Start(baseCtx, spanName, startOpts...)
		// 更新请求的上下文
		c.Request = c.Request.WithContext(spanCtx)
		// 获取请求开始时间
		startTime := span.GetStartTime()

		// 在请求结束时结束 Span
		defer func() {
			if recovered := recover(); recovered != nil {
				// 定义错误
				var e error
				// 判断错误类型
				switch panicValue := recovered.(type) {
				case error:
					// 如果是 error 类型，则直接赋值
					e = panicValue
				default:
					// 如果是其他类型，则转换为 error 类型
					e = fmt.Errorf("%v", panicValue)
				}
				// 设置 HTTP 状态码
				httpCode := http.StatusInternalServerError
				// 设置 HTTP 状态码属性
				span.SetAttributes(attribute.Int("http.status_code", httpCode))
				// 记录错误
				span.RecordError(e)
				// 记录错误详情，并记录堆栈信息
				span.WithError(e, "处理请求时发生错误")
				// 记录错误日志事件
				span.AddLog(types.SpanLog{
					// 设置日志时间
					Timestamp: time.Now().Format(time.RFC3339Nano),
					// 设置日志级别
					Severity: types.SpanLogSeverityError,
					// 设置日志字段
					Fields: map[string]any{
						"error":      e.Error(),              // 设置错误信息
						"message":    e.Error(),              // 设置错误消息
						"stack":      string(debug.Stack()),  // 设置堆栈信息
						"event_type": "middleware.gin.error", // 设置事件类型
					},
				})

				span.End()
				panic(recovered)
			}

			// 结束 Span
			span.End()
		}()

		// 设置 span 到 gin 上下文
		c.Set("span", span)

		// 设置 Span 属性
		attrs := []attribute.KeyValue{
			attribute.String("http.method", c.Request.Method),          // 设置请求方法
			attribute.String("http.url", c.Request.URL.Path),           // 设置请求 URL
			attribute.String("http.host", c.Request.Host),              // 设置请求主机
			attribute.String("http.user_agent", c.Request.UserAgent()), // 设置请求 User-Agent
			attribute.String("http.referer", c.Request.Referer()),      // 设置请求 Referer
			attribute.String("http.remote_addr", c.ClientIP()),         // 设置请求 Remote Address
			attribute.String("http.protocol", c.Request.Proto),         // 设置请求协议
		}

		// 如果请求路径不为空，则设置请求路径
		if route != "" {
			// 设置请求路径
			attrs = append(attrs, attribute.String("http.route", route))
		}
		// 设置 Span 属性
		span.SetAttributes(attrs...)

		body, truncated, _ := readGinRequestBody(c.Request, cfg.maxRequestBodySize)
		requestHeaders := c.Request.Header.Clone()
		if truncated {
			requestHeaders.Set("X-Request-Body-Truncated", "true")
		}
		reqInfo := &operation.RequestInfo{
			Method:      c.Request.Method,
			DecodedURL:  c.Request.URL.Path,
			QueryString: c.Request.URL.RawQuery,
			Headers:     requestHeaders,
			ClientIP:    c.ClientIP(),
			UserAgent:   c.Request.UserAgent(),
			CostSeconds: time.Since(startTime).Seconds(),
			Timestamp:   startTime.Format(time.RFC3339),
		}
		if len(body) > 0 {
			reqInfo.Body = string(body)
		}
		span.AddEvent("http.request", "", tracer.BuildRequestEvent(reqInfo))
		// 继续处理请求
		c.Next()

		// 正常响应的 http.response 由业务 response 层（如 response.Success）写入，
		// 中间件仅补充 http.status_code 属性，避免与 response 层重复记录。
		span.SetAttributes(attribute.Int("http.status_code", c.Writer.Status()))
	}
}

type ginRequestBodyReadCloser struct {
	io.Reader
	io.Closer
}

// readGinRequestBody 读取有限的追踪副本，并把已读取字节完整放回业务请求。
func readGinRequestBody(request *http.Request, maxSize int64) ([]byte, bool, error) {
	if request == nil || request.Body == nil {
		return nil, false, nil
	}

	originalBody := request.Body
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
	request.Body = &ginRequestBodyReadCloser{
		Reader: io.MultiReader(bytes.NewReader(consumed), originalBody),
		Closer: originalBody,
	}
	if err != nil {
		return recorded, false, err
	}
	return recorded, truncated, nil
}
