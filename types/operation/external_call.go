package operation

import "sync"

// ExternalCallInfo 定义外部或者跨服务调用信息
type ExternalCallInfo struct {
	Mutex       sync.RWMutex    // 读写锁
	ServiceName string          `json:"service_name"`          // 服务名称
	SpanID      string          `json:"span_id,omitempty"`     // 调用链ID
	TraceID     string          `json:"trace_id,omitempty"`    // 调用链ID
	CallerName  string          `json:"caller_name,omitempty"` // 调用方名称
	Request     *RequestInfo    `json:"request,omitempty"`     // 请求信息
	Response    []*ResponseInfo `json:"response,omitempty"`    // 响应信息, 这里记录每次调用的响应信息
	Success     bool            `json:"success"`               // 是否成功
	CostSeconds float64         `json:"cost_seconds"`          // 执行时间(单位秒)
	FailCount   int             `json:"fail_count"`            // 失败次数
	FailReason  []string        `json:"fail_reason,omitempty"` // 失败原因，这里记录每次失败的原因
	IsExternal  bool            `json:"is_external"`           // 是否为外部调用，true表示为外部调用，false表示为内部调用
	Timestamp   string          `json:"timestamp"`             // 执行时间 (ISO 8601 格式)，如2021-01-01T00:00:00.000Z
}

// 添加响应信息
func (e *ExternalCallInfo) AddResponse(response *ResponseInfo) {
	// 判断响应信息是否为空
	if response == nil {
		return
	}

	e.Mutex.Lock()         // 增加写锁，防止并发写入响应信息
	defer e.Mutex.Unlock() // 释放写锁
	// 判断响应信息列表是否为空，如果为空，则初始化响应信息列表
	if e.Response == nil {
		e.Response = make([]*ResponseInfo, 0)
	}
	// 将响应信息添加到响应信息列表中
	e.Response = append(e.Response, response)

	// 判断响应信息是否成功
	if !response.IsSuccess {
		// 更新失败次数
		e.FailCount++
		if response.BusinessCodeMsg != "" {
			// 更新失败原因
			e.FailReason = append(e.FailReason, response.BusinessCodeMsg)
		} else {
			// 更新失败原因
			e.FailReason = append(e.FailReason, response.HttpCodeMsg)
		}
	}
}
