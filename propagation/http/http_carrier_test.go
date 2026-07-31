package http

import (
	"net/http"
	"strings"
	"testing"

	"github.com/ClownSketch/tracer/trace"
)

func TestHTTPHeaderCarrier_Set(t *testing.T) {
	header := make(http.Header)
	carrier := NewHTTPHeaderCarrier(header)

	tests := []struct {
		name     string
		key      string
		value    string
		expected string
	}{
		{"正常设置", "trace-id", "123", "123"},
		{"空键", "", "value", ""},
		{"空值", "key", "", ""},
		{"带空格", "  key  ", "  value  ", "value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			carrier.Set(tt.key, tt.value)
			if tt.expected != "" {
				// 对于带空格的 key，需要使用 trim 后的 key 来获取
				trimmedKey := strings.TrimSpace(tt.key)
				if trimmedKey == "" {
					trimmedKey = tt.key
				}
				if got := header.Get(trimmedKey); got != tt.expected {
					t.Errorf("Set() = %v, want %v", got, tt.expected)
				}
			} else {
				// 对于空键值，应该没有设置
				trimmedKey := strings.TrimSpace(tt.key)
				if trimmedKey == "" {
					trimmedKey = tt.key
				}
				if _, exists := header[trimmedKey]; exists {
					t.Errorf("Empty key/value should not be set")
				}
			}
		})
	}
}

func TestHTTPHeaderCarrier_Get(t *testing.T) {
	header := make(http.Header)
	header.Set("trace-id", "123")
	header.Set("span-id", "456")
	carrier := NewHTTPHeaderCarrier(header)

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{"正常获取", "trace-id", "123"},
		{"获取不存在的键", "nonexistent", ""},
		{"空键", "", ""},
		{"带空格键", "  trace-id  ", "123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := carrier.Get(tt.key); got != tt.expected {
				t.Errorf("Get() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHTTPHeaderCarrier_Keys(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string]string
		expected []string
	}{
		{"空头部", map[string]string{}, []string{}},
		{"单个头部", map[string]string{"trace-id": "123"}, []string{"trace-id"}},
		{"多个头部", map[string]string{"trace-id": "123", "span-id": "456"}, []string{"trace-id", "span-id"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := make(http.Header)
			for k, v := range tt.headers {
				header.Set(k, v)
			}
			carrier := NewHTTPHeaderCarrier(header)

			keys := carrier.Keys()
			if len(keys) != len(tt.expected) {
				t.Errorf("Keys() length = %v, want %v", len(keys), len(tt.expected))
			}

			// 转换为 map 便于比较（HTTP header 的 key 是大小写不敏感的）
			keyMap := make(map[string]bool)
			for _, k := range keys {
				// 转换为小写进行比较（HTTP header 的 key 是大小写不敏感的）
				keyMap[strings.ToLower(k)] = true
			}

			for _, expectedKey := range tt.expected {
				// 转换为小写进行比较
				if !keyMap[strings.ToLower(expectedKey)] {
					t.Errorf("Keys() missing key: %s (got keys: %v)", expectedKey, keys)
				}
			}
		})
	}
}

func TestHTTPHeaderCarrier_Interface(t *testing.T) {
	header := make(http.Header)
	carrier := NewHTTPHeaderCarrier(header)

	// 测试接口实现
	var _ trace.ContextCarrier = carrier
	var _ trace.ContextCarrier = (*HTTPHeaderCarrier)(nil)
}
