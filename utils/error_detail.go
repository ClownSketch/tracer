package utils

import (
	"net/http"
	"runtime"
	"strings"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/types"
	"github.com/pkg/errors"
)

const unknownErrorCode = "unknown"

// structuredError 描述 tracer 可以读取的结构化错误信息。
// 宿主错误库只需实现这些方法，无需依赖 tracer。
type structuredError interface {
	Code() string
	BusinessCode() string
	BusinessMessage() []string
	Metadata() map[string]any
	StackError() error
	HTTPCode() int
}

// CreateErrorDetail 创建错误详情
func CreateErrorDetail(err error, customMessage string) *types.ErrorDetail {
	// 判断 err 是否存在
	if err == nil {
		// 没有错误，不生成错误详情
		return nil
	}

	// 定义错误消息和堆栈信息
	var (
		errMsg       = err.Error()                    // 获取错误消息
		stackTrace   []types.StackFrame               // 初始化堆栈信息
		code         = unknownErrorCode               // 初始化错误码
		httpCode     = http.StatusInternalServerError // 初始化HTTP状态码
		businessCode string                           // 初始化业务错误码
		tipsMsg      = []string{customMessage}        // 初始化业务错误消息
		metadata     map[string]any                   // 初始化元数据
	)

	// 如果是结构化错误，提取可记录的业务信息
	if structuredErr, ok := err.(structuredError); ok {

		code = structuredErr.Code()                 // 获取错误码
		httpCode = structuredErr.HTTPCode()         // 获取HTTP状态码
		businessCode = structuredErr.BusinessCode() // 获取业务错误码
		tipsMsg = structuredErr.BusinessMessage()   // 获取业务错误消息
		metadata = structuredErr.Metadata()         // 获取元数据

		// 获取堆栈信息
		if stackErr := structuredErr.StackError(); stackErr != nil {
			// 获取堆栈错误消息
			errMsg = stackErr.Error()
			// 判断错误是否包含 pkg/errors 堆栈
			if st, ok := stackErr.(interface{ StackTrace() errors.StackTrace }); ok {
				// 转换堆栈信息
				stackTrace = convertStackTrace(st.StackTrace())
			}
		}
	} else {
		// 对于普通错误，尝试获取已有堆栈信息
		if stackErr, ok := err.(interface{ StackTrace() errors.StackTrace }); ok {
			// 转换堆栈信息
			stackTrace = convertStackTrace(stackErr.StackTrace())
		}
	}

	// 兜底赋值，如果 tipsMsg 为空，则设置为 customMessage
	if len(tipsMsg) == 0 && customMessage != "" {
		tipsMsg = []string{customMessage}
	}

	// 如果 err 中没有堆栈信息，则通过调用栈获取
	if len(stackTrace) == 0 {
		// 通过调用栈获取堆栈信息
		stackTrace = getStackTrace()
	}

	// 返回错误详情
	return &types.ErrorDetail{
		Code:            code,           // 设置错误码
		Message:         errMsg,         // 设置错误消息
		BusinessCode:    businessCode,   // 设置业务错误码
		BusinessMessage: tipsMsg,        // 设置业务错误消息
		HttpCode:        httpCode,       // 设置HTTP状态码
		StackTrace:      stackTrace,     // 设置堆栈信息
		MetaData:        metadata,       // 设置元数据
		Timestamp:       GetTimestamp(), // 设置时间戳
	}
}

// convertStackTrace 转换堆栈信息
func convertStackTrace(st errors.StackTrace) []types.StackFrame {
	// 定义堆栈信息
	var stackTrace []types.StackFrame
	// 自动获取 RootPath
	rootPath := tracer.GetBusinessRootPath()

	// 循环遍历堆栈信息
	for _, frame := range st {
		// 如果获取的堆栈信息长度大于最大堆栈深度，则跳出循环
		if len(stackTrace) >= tracer.MaxStackTraceDepth {
			break
		}
		// 获取函数指针
		pc := uintptr(frame) - 1
		// 获取函数信息
		fn := runtime.FuncForPC(pc)
		// 如果函数信息为空，则跳过
		if fn == nil {
			continue
		}
		// 获取文件信息和行号
		file, line := fn.FileLine(pc)
		// 如果根路径为空，或者文件路径包含根路径，则将堆栈信息添加到堆栈信息切片中
		if rootPath == "" || strings.Contains(file, rootPath) {
			stackTrace = append(stackTrace, types.StackFrame{
				FunctionName: fn.Name(), // 函数名称
				File:         file,      // 文件路径
				LineNumber:   line,      // 行号
			})
		}
	}
	return stackTrace
}

// getStackTrace 获取堆栈信息
func getStackTrace() []types.StackFrame {
	// 定义堆栈信息切片
	var stackTrace []types.StackFrame
	// 定义堆栈信息切片，一次性抓取足够多的原始堆栈（32层），为过滤留出缓冲量
	pc := make([]uintptr, 32)
	// 过 runtime.Callers, getStackTrace, CreateErrorDetail 本身
	n := runtime.Callers(3, pc)
	// 如果堆栈帧为空，则返回空切片
	if n == 0 {
		return nil
	}
	// 自动获取 RootPath
	rootPath := tracer.GetBusinessRootPath()
	// 使用 runtime.CallersFrames 解析堆栈帧，并过滤掉不属于业务项目根路径的堆栈帧
	frames := runtime.CallersFrames(pc[:n])
	for {
		// 获取堆栈帧
		frame, more := frames.Next()
		// 如果根路径为空，或者文件路径包含根路径，则将堆栈信息添加到堆栈信息切片中
		if rootPath == "" || strings.Contains(frame.File, rootPath) {
			// 将堆栈帧添加到堆栈信息切片中
			stackTrace = append(stackTrace, types.StackFrame{
				FunctionName: frame.Function, // 函数名称
				File:         frame.File,     // 文件路径
				LineNumber:   frame.Line,     // 行号
			})
		}

		// 如果堆栈信息长度大于最大堆栈深度，或者没有更多堆栈帧，则跳出循环
		if len(stackTrace) >= tracer.MaxStackTraceDepth || !more {
			break
		}

	}
	// 返回堆栈信息切片
	return stackTrace
}
