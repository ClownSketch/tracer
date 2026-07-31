package utils

import (
	"errors"
	"net/http"
	"testing"

	pkgerrors "github.com/pkg/errors"
)

// structuredTestError 模拟宿主项目提供的结构化错误。
type structuredTestError struct {
	stackErr error
}

// Error 返回原始错误文本。
func (e *structuredTestError) Error() string {
	return e.stackErr.Error()
}

// Code 返回系统错误码。
func (e *structuredTestError) Code() string {
	return "DATABASE_ERROR"
}

// BusinessCode 返回业务错误码。
func (e *structuredTestError) BusinessCode() string {
	return "BUSINESS_ERROR"
}

// BusinessMessage 返回业务提示信息。
func (e *structuredTestError) BusinessMessage() []string {
	return []string{"系统繁忙"}
}

// Metadata 返回错误元数据。
func (e *structuredTestError) Metadata() map[string]any {
	return map[string]any{"order_no": "PI10001"}
}

// StackError 返回带堆栈的原始错误。
func (e *structuredTestError) StackError() error {
	return e.stackErr
}

// HTTPCode 返回 HTTP 状态码。
func (e *structuredTestError) HTTPCode() int {
	return http.StatusInternalServerError
}

// TestCreateErrorDetailWithStructuredError 验证 tracer 可以读取任意结构化错误实现。
func TestCreateErrorDetailWithStructuredError(t *testing.T) {
	err := &structuredTestError{stackErr: pkgerrors.WithStack(errors.New("database unavailable"))}
	detail := CreateErrorDetail(err, "执行数据库操作失败")

	if detail == nil {
		t.Fatal("结构化错误应生成错误详情")
	}
	if detail.Code != "DATABASE_ERROR" || detail.BusinessCode != "BUSINESS_ERROR" {
		t.Fatalf("错误码记录不正确: code=%s business_code=%s", detail.Code, detail.BusinessCode)
	}
	if detail.HttpCode != http.StatusInternalServerError {
		t.Fatalf("HTTP 状态码记录不正确: %d", detail.HttpCode)
	}
	if len(detail.BusinessMessage) != 1 || detail.BusinessMessage[0] != "系统繁忙" {
		t.Fatalf("业务提示记录不正确: %#v", detail.BusinessMessage)
	}
	if detail.MetaData["order_no"] != "PI10001" {
		t.Fatalf("错误元数据记录不正确: %#v", detail.MetaData)
	}
	if detail.Message != "database unavailable" || len(detail.StackTrace) == 0 {
		t.Fatalf("错误堆栈记录不正确: message=%s stack=%#v", detail.Message, detail.StackTrace)
	}
}
