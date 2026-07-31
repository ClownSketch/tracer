package tracer

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	// ==================== Tracer Core Errors ====================
	ErrorCodeUnknown string = "unknown" // 未知错误

	// ==================== Trace & Span Errors ====================
	ErrorCodeTraceNotFound      string = "trace_not_found"      // 追踪不存在
	ErrorCodeSpanCreationFailed string = "span_creation_failed" // Span创建失败
	ErrorCodeSpanNotFound       string = "span_not_found"       // Span不存在
	ErrorCodeInvalidTraceID     string = "invalid_trace_id"     // 无效的TraceID
	ErrorCodeInvalidSpanID      string = "invalid_span_id"      // 无效的SpanID

	// ==================== Context Propagation Errors ====================
	ErrorCodeContextPropagationFailed string = "context_propagation_failed" // 上下文传播失败
	ErrorCodeContextExtractFailed     string = "context_extract_failed"     // 上下文提取失败
	ErrorCodeContextInjectFailed      string = "context_inject_failed"      // 上下文注入失败

	// ==================== Exporter Errors ====================
	ErrorCodeExporterNotFound       string = "exporter_not_found"       // 导出器不存在
	ErrorCodeExporterFailed         string = "exporter_failed"          // 导出器失败
	ErrorCodeExporterShutdownFailed string = "exporter_shutdown_failed" // 导出器关闭失败
	ErrorCodeExportDataFailed       string = "export_data_failed"       // 数据导出失败

	// ==================== Sampler Errors ====================
	ErrorCodeSamplerNotFound     string = "sampler_not_found"     // 采样器不存在
	ErrorCodeSamplerFailed       string = "sampler_failed"        // 采样器失败
	ErrorCodeSamplerConfigFailed string = "sampler_config_failed" // 采样器配置失败

	// ==================== Configuration Errors ====================
	ErrorCodeInvalidConfig    string = "invalid_config"     // 无效配置
	ErrorCodeConfigNotFound   string = "config_not_found"   // 配置不存在
	ErrorCodeConfigLoadFailed string = "config_load_failed" // 配置加载失败

	// ==================== Resource Errors ====================
	ErrorCodeResourceNotFound     string = "resource_not_found"     // 资源不存在
	ErrorCodeResourceCreateFailed string = "resource_create_failed" // 资源创建失败
	ErrorCodeResourceUpdateFailed string = "resource_update_failed" // 资源更新失败
	ErrorCodeResourceDeleteFailed string = "resource_delete_failed" // 资源删除失败

	// ==================== Instrumentation Errors ====================
	ErrorCodeInstrumentationFailed string = "instrumentation_failed" // 自动埋点失败
	ErrorCodeMiddlewareFailed      string = "middleware_failed"      // 中间件失败
	ErrorCodeHookFailed            string = "hook_failed"            // 钩子失败

	// ==================== General Errors ====================
	ErrorCodeInvalidParameter       string = "invalid_parameter"        // 无效参数
	ErrorCodeTimeout                string = "timeout"                  // 超时
	ErrorCodeConcurrentAccess       string = "concurrent_access"        // 并发访问错误
	ErrorCodeMemoryAllocationFailed string = "memory_allocation_failed" // 内存分配失败
)

// ClownError 定义错误接口
type ClownError interface {
	error          // 错误接口
	IsError() bool // 判断是否为错误

	Code() string                    // 获取错误码
	WithCode(code string) ClownError // 设置错误码

	BusinessCode() string                    // 获取业务错误码
	WithBusinessCode(code string) ClownError // 设置业务错误码

	Message() string                                 // 获取错误消息
	WithMessage(message string) ClownError           // 设置错误消息
	WithMessagef(format string, a ...any) ClownError // 设置错误消息

	BusinessMessage() []string                        // 获取业务错误消息
	FirstTip() string                                 // 获取第一个业务错误提示
	WithBusinessMessage(message ...string) ClownError // 设置业务错误消息

	Metadata() map[string]any                      // 获取元数据
	WithMetadata(key string, value any) ClownError // 设置元数据

	WithError(err error) ClownError // 设置堆栈错误
	StackError() error              // 获取堆栈错误

	HTTPCode() int                    // 获取HTTP状态码
	WithHTTPCode(code int) ClownError // 设置HTTP状态码

}

// clownErrorImpl 实现 ClownError 接口
type clownErrorImpl struct {
	code            string         // 错误码
	message         string         // 错误消息
	businessCode    string         // 业务错误码
	businessMessage []string       // 业务错误消息
	stackError      error          // 堆栈错误
	metadata        map[string]any // 元数据
	httpCode        int            // HTTP状态码
	timestamp       string         // 时间戳
}

// NewClownError 创建新的错误实例
func NewClownError() ClownError {
	return &clownErrorImpl{
		// 默认400错误
		httpCode: http.StatusBadRequest,
		// 初始化创建时间
		timestamp: time.Now().Format(time.RFC3339Nano),
	}
}

// IsError 实现错误接口
func (c *clownErrorImpl) IsError() bool {
	// 有堆栈错误，肯定是错误
	if c.stackError != nil {
		return true
	}

	// 有明确的错误码（非Unknown），是错误
	if c.code != "" && c.code != ErrorCodeUnknown {
		return true
	}

	// HTTP状态码 >= 400，是错误
	if c.httpCode >= 400 {
		return true
	}

	// 有错误消息，是错误
	if c.message != "" {
		return true
	}

	return false
}

// Error 实现错误接口
func (c *clownErrorImpl) Error() string {
	// 如果堆栈错误不为空，则返回堆栈错误
	if c.stackError != nil {
		return c.stackError.Error()
	}

	// 获取最合适的错误消息
	errorMsg := c.getErrorMessage()

	// 如果错误码不为空，则返回错误码和消息（使用 strings.Builder 优化性能）
	if c.code != "" && c.code != ErrorCodeUnknown {
		var builder strings.Builder
		builder.Grow(len(c.code) + len(errorMsg) + 3) // 预分配容量（": " + 消息）
		builder.WriteString(c.code)
		builder.WriteString(": ")
		builder.WriteString(errorMsg)
		return builder.String()
	}

	return errorMsg
}

// getErrorMessage 获取最合适的错误消息
func (c *clownErrorImpl) getErrorMessage() string {
	// 优先级：message > businessMessage[0] > 默认消息
	if c.message != "" {
		return c.message
	}

	if len(c.businessMessage) > 0 {
		return c.businessMessage[0]
	}

	return "未知错误"
}

// Code 实现错误码接口
func (c *clownErrorImpl) Code() string {
	return c.code
}

// WithCode 实现错误码接口
func (c *clownErrorImpl) WithCode(code string) ClownError {
	c.code = code
	return c
}

// BusinessCode 实现业务错误码接口
func (c *clownErrorImpl) BusinessCode() string {
	return c.businessCode
}

// WithBusinessCode 实现业务错误码接口
func (c *clownErrorImpl) WithBusinessCode(code string) ClownError {
	c.businessCode = code
	return c
}

// Message 实现错误消息接口
func (c *clownErrorImpl) Message() string {
	return c.message
}

// WithMessage 实现错误消息接口
func (c *clownErrorImpl) WithMessage(message string) ClownError {
	c.message = message
	return c
}

// WithMessagef 实现错误消息接口
func (c *clownErrorImpl) WithMessagef(format string, a ...any) ClownError {
	c.message = fmt.Sprintf(format, a...)
	return c
}

// BusinessMessage 实现业务错误消息接口
func (c *clownErrorImpl) BusinessMessage() []string {
	return c.businessMessage
}

// FirstTip 实现业务错误消息接口
func (c *clownErrorImpl) FirstTip() string {
	if len(c.businessMessage) > 0 {
		return c.businessMessage[0]
	}
	return ""
}

// WithBusinessMessage 实现业务错误消息接口
func (c *clownErrorImpl) WithBusinessMessage(message ...string) ClownError {
	if len(message) > 0 {
		c.businessMessage = message
	}
	return c
}

// Metadata 实现元数据接口
func (c *clownErrorImpl) Metadata() map[string]any {
	return c.metadata
}

// WithMetadata 实现元数据接口
func (c *clownErrorImpl) WithMetadata(key string, value any) ClownError {

	// 如果key为空或value为空，则返回当前错误
	if key == "" || value == nil {
		return c
	}

	// 如果元数据为空，则初始化元数据map
	// 之所以没有在初始化时创建，是因为很多时候用不上，这里创建可以减少内存分配
	if c.metadata == nil {
		c.metadata = make(map[string]any)
	}

	// 设置元数据
	c.metadata[key] = value
	return c
}

// WithError 实现错误接口
func (c *clownErrorImpl) WithError(err error) ClownError {
	if err != nil {
		// 判断是否为ClownError类型
		if clownErr, ok := err.(ClownError); ok {
			// 如果ClownError类型，则直接使用StackError
			c.stackError = clownErr.StackError()
		} else {
			// 检查错误是否已经包含堆栈信息
			if _, ok := err.(interface{ StackTrace() errors.StackTrace }); ok {
				c.stackError = err
			} else {
				c.stackError = errors.WithStack(err)
			}
		}
	}

	return c
}

// StackError 实现错误接口
func (c *clownErrorImpl) StackError() error {
	return c.stackError
}

// HTTPCode 实现错误接口
func (c *clownErrorImpl) HTTPCode() int {
	return c.httpCode
}

// WithHTTPCode 实现错误接口
func (c *clownErrorImpl) WithHTTPCode(code int) ClownError {
	c.httpCode = code
	return c
}
