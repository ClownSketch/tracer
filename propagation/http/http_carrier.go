package http

import (
	"net/http"
	"strings"

	"github.com/ClownSketch/tracer/trace"
)

// HTTPHeaderCarrier 是一个HTTP头载体实现
type HTTPHeaderCarrier struct {
	header http.Header // HTTP头
}

// NewHTTPHeaderCarrier 创建一个新的HTTPHeaderCarrier
func NewHTTPHeaderCarrier(header http.Header) trace.ContextCarrier {
	return &HTTPHeaderCarrier{header: header}
}

// Set 实现propagation.TextMapCarrier接口
func (c *HTTPHeaderCarrier) Set(key string, value string) {
	// 对 Key 和 Value 进行trim，如果为空，则返回
	trimmedKey := strings.TrimSpace(key)
	trimmedValue := strings.TrimSpace(value)
	if trimmedKey == "" || trimmedValue == "" {
		return
	}
	// 设置HTTP头（使用trim后的key和value）
	c.header.Set(trimmedKey, trimmedValue)
}

// Get 实现propagation.TextMapCarrier接口
func (c *HTTPHeaderCarrier) Get(key string) string {
	// 对 Key 进行trim，如果为空，则返回空字符串
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return ""
	}
	// 获取HTTP头（HTTP header的Get方法是大小写不敏感的）
	return c.header.Get(trimmedKey)
}

// Keys 实现propagation.TextMapCarrier接口
func (c *HTTPHeaderCarrier) Keys() []string {
	// 如果HTTP头为空，则返回空切片
	if len(c.header) == 0 {
		return []string{}
	}

	// 创建一个切片，长度为HTTP头的长度
	keys := make([]string, 0, len(c.header))
	for key := range c.header {
		// 将键添加到切片中
		keys = append(keys, key)
	}
	// 返回切片
	return keys
}

// 确保HTTPHeaderCarrier实现trace.ContextCarrier接口
var _ trace.ContextCarrier = (*HTTPHeaderCarrier)(nil)
