package tracer_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/ClownSketch/tracer"
	"github.com/ClownSketch/tracer/utils"
)

// TestCreateErrorDetailWithClownError 验证 ClownError 的业务信息和堆栈可被完整记录。
func TestCreateErrorDetailWithClownError(t *testing.T) {
	err := tracer.NewClownError().
		WithCode("ORDER_NOT_FOUND").
		WithBusinessCode("PAYIN_ORDER_NOT_FOUND").
		WithBusinessMessage("目标资源不存在").
		WithMetadata("out_trade_no", "PI10001").
		WithHTTPCode(http.StatusBadRequest).
		WithError(errors.New("order not found"))

	detail := utils.CreateErrorDetail(err, "查询目标资源失败")
	if detail == nil {
		t.Fatal("ClownError 应生成错误详情")
	}
	if detail.Code != "ORDER_NOT_FOUND" || detail.BusinessCode != "PAYIN_ORDER_NOT_FOUND" {
		t.Fatalf("错误码记录不正确: code=%s business_code=%s", detail.Code, detail.BusinessCode)
	}
	if detail.HttpCode != http.StatusBadRequest {
		t.Fatalf("HTTP 状态码记录不正确: %d", detail.HttpCode)
	}
	if len(detail.BusinessMessage) != 1 || detail.BusinessMessage[0] != "目标资源不存在" {
		t.Fatalf("业务提示记录不正确: %#v", detail.BusinessMessage)
	}
	if detail.MetaData["out_trade_no"] != "PI10001" {
		t.Fatalf("错误元数据记录不正确: %#v", detail.MetaData)
	}
	if detail.Message != "order not found" || len(detail.StackTrace) == 0 {
		t.Fatalf("错误堆栈记录不正确: message=%s stack=%#v", detail.Message, detail.StackTrace)
	}
}
