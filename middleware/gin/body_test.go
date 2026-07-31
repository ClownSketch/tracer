package middleware

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

func TestReadGinRequestBodyPreservesCompleteBody(t *testing.T) {
	original := bytes.Repeat([]byte("b"), 64*1024+31)
	request, err := http.NewRequest(http.MethodPost, "/payin", bytes.NewReader(original))
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}

	recorded, truncated, err := readGinRequestBody(request, 64*1024)
	if err != nil {
		t.Fatalf("读取追踪副本失败: %v", err)
	}
	if !truncated || len(recorded) != 64*1024 {
		t.Fatalf("截断信息错误: truncated=%v len=%d", truncated, len(recorded))
	}
	actual, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("读取恢复后的请求体失败: %v", err)
	}
	if !bytes.Equal(actual, original) {
		t.Fatalf("Gin 中间件修改了实际业务请求体: got=%d want=%d", len(actual), len(original))
	}
}

func TestReadGinRequestBodyWithoutLimit(t *testing.T) {
	original := bytes.Repeat([]byte("c"), 1024*1024+31)
	request, err := http.NewRequest(http.MethodPost, "/payin", bytes.NewReader(original))
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}

	recorded, truncated, err := readGinRequestBody(request, 0)
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
		t.Fatalf("Gin 中间件修改了实际业务请求体: got=%d want=%d", len(actual), len(original))
	}
}
