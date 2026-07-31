package text

import "strings"

// TextMapCarrier 是一个文本映射载体实现
type TextMapCarrier struct {
	carrier map[string]string
}

// NewTextMapCarrier 创建一个新的TextMapCarrier
func NewTextMapCarrier() *TextMapCarrier {
	return &TextMapCarrier{carrier: make(map[string]string)}
}

// Set 设置键值对
func (c *TextMapCarrier) Set(key string, value string) {
	// 忽略空键或空值
	if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
		return
	}
	c.carrier[key] = value
}

// Get 获取键对应的值
func (c *TextMapCarrier) Get(key string) string {
	if strings.TrimSpace(key) == "" {
		return ""
	}
	return c.carrier[key]
}

// Keys 返回所有键
func (c *TextMapCarrier) Keys() []string {
	if len(c.carrier) == 0 {
		return []string{}
	}
	keys := make([]string, 0, len(c.carrier))
	for k := range c.carrier {
		keys = append(keys, k)
	}
	return keys
}
