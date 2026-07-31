package operation

// SQLOperationInfo 定义数据库操作信息
type SQLOperationInfo struct {
	Table       string  `json:"table"`             // 表名
	Operation   string  `json:"operation"`         // 操作类型，如select、insert、update、delete
	Rows        int64   `json:"rows"`              // 返回的行数
	SQL         string  `json:"sql"`               // SQL 语句，这里记录的是完整的SQL语句，包括参数
	Stack       string  `json:"stack,omitempty"`   // 堆栈信息
	Message     string  `json:"message,omitempty"` // 错误消息，用于记录错误信息
	CostSeconds float64 `json:"cost_seconds"`      // 执行时间(单位秒)
	Success     bool    `json:"success"`           // 是否成功
	Transaction bool    `json:"transaction"`       // 是否是事务
	Timestamp   string  `json:"timestamp"`         // 执行时间 (ISO 8601 格式)，如2021-01-01T00:00:00.000Z
}
