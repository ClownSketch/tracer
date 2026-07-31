package types

// 定义错误详情
type ErrorDetail struct {
	Code            string         `json:"code"`             // 错误码，用于记录错误码
	Message         string         `json:"message"`          // 错误消息，用于记录错误信息
	BusinessCode    string         `json:"business_code"`    // 业务错误码，响应给用户使用
	BusinessMessage []string       `json:"business_message"` // 业务错误消息，响应给用户使用
	MetaData        map[string]any `json:"meta_data"`        // 元数据，用于记录错误元数据，可用于存储自定义数据
	StackTrace      []StackFrame   `json:"stack_trace"`      // 错误堆栈，用于记录错误堆栈
	HttpCode        int            `json:"http_code"`        // HTTP状态码
	Timestamp       string         `json:"timestamp"`        // 错误时间，用于记录错误时间
}

// StackFrame 定义错误堆栈，表示单个堆栈帧
type StackFrame struct {
	File         string `json:"file"`          // 文件路径
	FileName     string `json:"file_name"`     // 文件名
	FunctionName string `json:"function_name"` // 函数名称
	LineNumber   int    `json:"line_number"`   // 行号
}
