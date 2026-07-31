package operation

// RequestInfo 定义请求信息
type RequestInfo struct {
	TTL         int     `json:"ttl"`                    // 请求超时时间(单位秒)
	Method      string  `json:"method"`                 // 请求方式，如GET、POST、PUT、DELETE
	DecodedURL  string  `json:"decoded_url"`            // 请求地址（不含查询参数）
	QueryString string  `json:"query_string,omitempty"` // URL 查询参数（GET 等请求的传参，如 ?name=foo&age=18）
	Headers     any     `json:"header"`                 // 请求 Header 信息
	Body        any     `json:"body"`                   // 请求 Body 信息（POST/PUT/PATCH 等）
	ClientIP    string  `json:"client_ip,omitempty"`    // 请求的 IP 地址
	UserAgent   string  `json:"user_agent,omitempty"`   // 用户代理
	CostSeconds float64 `json:"cost_seconds"`           // 执行时间(单位秒)
	Timestamp   string  `json:"timestamp"`              // 请求时间 (ISO 8601 格式)，如2021-01-01T00:00:00.000Z
}

// ResponseInfo 定义响应信息
type ResponseInfo struct {
	RetryID         string  `json:"retry_id,omitempty"`          // 标识每次响应，区分重试
	Method          string  `json:"method"`                      // 请求方式，如GET、POST、PUT、DELETE
	Header          any     `json:"header"`                      // Header 信息
	Body            any     `json:"body"`                        // Body 信息
	BusinessCode    string  `json:"business_code,omitempty"`     // 业务码
	BusinessCodeMsg string  `json:"business_code_msg,omitempty"` // 提示信息
	HttpCode        int     `json:"http_code"`                   // HTTP 状态码
	HttpCodeMsg     string  `json:"http_code_msg"`               // HTTP 状态码信息
	CostSeconds     float64 `json:"cost_seconds"`                // 执行时间(单位秒)
	Timestamp       string  `json:"timestamp"`                   // 响应时间 (ISO 8601 格式)，如2021-01-01T00:00:00.000Z
	IsSuccess       bool    `json:"is_success"`                  // 是否成功
}
