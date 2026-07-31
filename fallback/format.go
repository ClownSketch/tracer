package fallback

// SpanData 定义统一的 fallback 数据格式
// 所有导出器都使用这个格式写入本地磁盘，批处理器读取时转换为 span 快照格式
type SpanData struct {
	// 导出路由
	MongoCollection string `json:"mongoCollection,omitempty"` // MongoDB 导出目标集合名

	// 基本信息
	Name         string `json:"name"`         // Span 名称
	TraceID      string `json:"traceID"`      // TraceID
	SpanID       string `json:"spanID"`       // SpanID
	ParentSpanID string `json:"parentSpanID"` // ParentSpanID
	Kind         string `json:"kind"`         // Span 类型
	StartTime    string `json:"startTime"`    // 开始时间 (RFC3339Nano)
	EndTime      string `json:"endTime"`      // 结束时间 (RFC3339Nano)
	Duration     string `json:"duration"`     // 持续时间 (纳秒字符串)

	// 状态
	Status *Status `json:"status,omitempty"`

	// 属性
	Attributes map[string]any `json:"attributes,omitempty"`

	// 事件
	Events []Event `json:"events,omitempty"`

	// 日志
	Logs []Log `json:"logs,omitempty"`

	// 错误详情
	Error *Error `json:"error,omitempty"`

	// 资源信息
	Resource *Resource `json:"resource,omitempty"`

	// 资源使用情况
	ResourceUsage *ResourceUsage `json:"resourceUsage,omitempty"`

	// 关联 Span
	Links []Link `json:"links,omitempty"`
}

// Status 定义状态格式
type Status struct {
	Code        string `json:"code"`        // 状态码
	Description string `json:"description"` // 状态描述
}

// Event 定义事件格式
type Event struct {
	Name       string         `json:"name"`                 // 事件名称
	Timestamp  string         `json:"timestamp"`            // 时间戳 (RFC3339Nano)
	Attributes map[string]any `json:"attributes,omitempty"` // 事件属性
}

// Log 定义日志格式
type Log struct {
	Timestamp  string         `json:"timestamp"`            // 时间戳 (RFC3339Nano)
	Message    string         `json:"message"`              // 日志消息
	Severity   string         `json:"severity"`             // 日志级别
	Attributes map[string]any `json:"attributes,omitempty"` // 日志属性
	Fields     any            `json:"fields,omitempty"`     // 日志字段
	EventType  string         `json:"eventType,omitempty"`  // 事件类型
}

// Error 定义错误详情格式
type Error struct {
	Code            string         `json:"code,omitempty"`            // 错误码
	Message         string         `json:"message"`                   // 错误消息
	BusinessCode    string         `json:"businessCode,omitempty"`    // 业务错误码
	BusinessMessage []string       `json:"businessMessage,omitempty"` // 业务错误消息
	HttpCode        int            `json:"httpCode,omitempty"`        // HTTP状态码
	Timestamp       string         `json:"timestamp,omitempty"`       // 错误时间
	MetaData        map[string]any `json:"metaData,omitempty"`        // 元数据
	StackTrace      []StackFrame   `json:"stackTrace,omitempty"`      // 堆栈信息
}

// StackFrame 定义堆栈帧格式
type StackFrame struct {
	File         string `json:"file"`         // 文件路径
	FileName     string `json:"fileName"`     // 文件名
	FunctionName string `json:"functionName"` // 函数名称
	LineNumber   int    `json:"lineNumber"`   // 行号
}

// Resource 定义资源信息格式
type Resource struct {
	ServiceName string         `json:"serviceName,omitempty"` // 服务名称
	Host        string         `json:"host,omitempty"`        // 主机名
	Attributes  map[string]any `json:"attributes,omitempty"`  // 资源属性
}

// ResourceUsage 定义资源使用情况格式
type ResourceUsage struct {
	CPUUsage    float64 `json:"cpuUsage,omitempty"`    // CPU 使用率（百分比）
	MemoryUsage float64 `json:"memoryUsage,omitempty"` // 内存使用率（MB）
	DiskUsage   float64 `json:"diskUsage,omitempty"`   // 磁盘使用率（MB）
	NetworkIO   float64 `json:"networkIO,omitempty"`   // 网络 IO（MB）
}

// Link 定义关联 Span 格式
type Link struct {
	TraceID      string `json:"traceID"`                // TraceID
	SpanID       string `json:"spanID"`                 // SpanID
	ParentSpanID string `json:"parentSpanID,omitempty"` // ParentSpanID
}
