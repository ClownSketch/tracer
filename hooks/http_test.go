//go:build http_hook

package hooks

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

func TestReadRequestBodyWithoutConsuming(t *testing.T) {
	original := bytes.Repeat([]byte("a"), 1024+17)
	request, err := http.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader(original))
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}
	request.GetBody = nil

	recorded, truncated, err := readRequestBodyWithoutConsuming(request, 1024)
	if err != nil {
		t.Fatalf("读取追踪副本失败: %v", err)
	}
	if !truncated || len(recorded) != 1024 {
		t.Fatalf("截断信息错误: truncated=%v len=%d", truncated, len(recorded))
	}
	actual, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("读取恢复后的请求体失败: %v", err)
	}
	if !bytes.Equal(actual, original) {
		t.Fatalf("HTTP hook 修改了实际业务请求体: got=%d want=%d", len(actual), len(original))
	}
}

func TestReadRequestBodyWithoutLimit(t *testing.T) {
	original := bytes.Repeat([]byte("b"), 1024*1024+17)
	request, err := http.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader(original))
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}
	request.GetBody = nil

	recorded, truncated, err := readRequestBodyWithoutConsuming(request, 0)
	if err != nil {
		t.Fatalf("读取追踪副本失败: %v", err)
	}
	if truncated || !bytes.Equal(recorded, original) {
		t.Fatalf("未配置上限时请求体记录不完整: truncated=%v got=%d want=%d", truncated, len(recorded), len(original))
	}
	actual, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("读取恢复后的请求体失败: %v", err)
	}
	if !bytes.Equal(actual, original) {
		t.Fatalf("HTTP hook 修改了实际业务请求体: got=%d want=%d", len(actual), len(original))
	}
}
