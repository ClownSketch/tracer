package types

// Event 定义事件
type Event func() map[string]any

// SpanEvent 定义事件
type SpanEvent struct {
	Name       string         `json:"name"`       // 事件名称
	Event      Event          `json:"event"`      // 事件
	Timestamp  string         `json:"timestamp"`  // 事件时间，格式为ISO 8601，如2021-01-01T00:00:00.000Z
	Attributes map[string]any `json:"attributes"` // 事件属性
}
