package middleware

import (
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
)

// SpanNameFormatter 在 Start 前根据 gin.Context 解析 span 名称
// 参数：
// - c: gin 上下文
// 返回：
// - 解析后的 span 名称
type SpanNameFormatter func(*gin.Context) string

// GinOption 配置 Gin 链路中间件
type GinOption interface {
	apply(*ginServerTraceConfig) // 应用配置
}

// ginOptionFunc 是 GinOption 的函数实现
type ginOptionFunc func(*ginServerTraceConfig)

// apply 应用配置
func (f ginOptionFunc) apply(cfg *ginServerTraceConfig) {
	f(cfg)
}

// WithSpanNameFormatter 自定义 span 名称；未设置时使用 defaultSpanNameFormatter
// 参数：
// - formatter: 自定义 span 名称的函数
// 返回：
// - GinOption: 配置 Gin 链路中间件
func WithSpanNameFormatter(formatter SpanNameFormatter) GinOption {
	// 返回 GinOption 的函数实现
	return ginOptionFunc(func(cfg *ginServerTraceConfig) {
		if formatter != nil {
			// 设置自定义 span 名称的函数
			cfg.spanNameFormatter = formatter
		}
	})
}

// WithMaxRequestBodySize 设置链路中最多记录的请求体字节数；不设置时完整记录。
func WithMaxRequestBodySize(size int64) GinOption {
	return ginOptionFunc(func(cfg *ginServerTraceConfig) {
		if size > 0 {
			cfg.maxRequestBodySize = size
		}
	})
}

// applyGinOptions 应用配置
// 参数：
// - cfg: 配置
// - opts: 配置选项
// 返回：
// - 应用后的配置
func applyGinOptions(cfg *ginServerTraceConfig, opts ...GinOption) {
	// 应用配置选项
	for _, opt := range opts {
		opt.apply(cfg)
	}
	// 如果自定义 span 名称的函数为空，则使用默认的 span 名称格式化函数
	if cfg.spanNameFormatter == nil {
		// 设置默认的 span 名称格式化函数
		cfg.spanNameFormatter = defaultSpanNameFormatter
	}
}

// defaultSpanNameFormatter 对齐 OTel otelgin：{METHOD} {FullPath}。
// 参数：
// - c: gin 上下文
// 返回：
// - 解析后的 span 名称
func defaultSpanNameFormatter(c *gin.Context) string {
	// 获取请求方法
	method := strings.ToUpper(c.Request.Method)
	// 如果请求方法不是常用的 HTTP 方法，则使用 "HTTP"
	if !slices.Contains([]string{
		http.MethodGet, http.MethodHead, // Get 和 Head 方法不使用 "HTTP"
		http.MethodPost, http.MethodPut, // Post 和 Put 方法使用 "HTTP"
		http.MethodPatch, http.MethodDelete, // Patch 和 Delete 方法使用 "HTTP"
		http.MethodConnect, http.MethodOptions, // Connect 和 Options 方法使用 "HTTP"
		http.MethodTrace,
	}, method) {
		method = "HTTP"
	}
	// 获取请求路径
	if path := c.FullPath(); path != "" {
		// 返回请求方法和请求路径
		return method + " " + path
	}

	return method
}

// resolveSpanName 解析 span 名称
// 参数：
// - c: gin 上下文
// - formatter: 自定义 span 名称的函数
// 返回：
// - 解析后的 span 名称
func resolveSpanName(c *gin.Context, formatter SpanNameFormatter) string {
	// 如果自定义 span 名称的函数为空，则使用默认的 span 名称格式化函数
	if formatter == nil {
		formatter = defaultSpanNameFormatter
	}
	// 如果自定义 span 名称的函数不为空，则使用自定义的 span 名称格式化函数
	name := formatter(c)
	if name != "" {
		// 返回自定义的 span 名称
		return name
	}

	// 如果自定义 span 名称的函数为空，则使用默认的 span 名称格式化函数
	// 返回 "HTTP {METHOD} route not found"
	return "HTTP " + strings.ToUpper(c.Request.Method) + " route not found"
}
