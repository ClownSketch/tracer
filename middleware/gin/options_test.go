package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestDefaultSpanNameFormatter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	var captured *gin.Context
	r.POST("/api/v1/payin/unified", func(ctx *gin.Context) {
		captured = ctx
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payin/unified", nil)
	r.ServeHTTP(w, req)

	if captured == nil {
		t.Fatal("handler was not invoked")
	}

	got := defaultSpanNameFormatter(captured)
	want := "POST /api/v1/payin/unified"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestResolveSpanNameWithCustomFormatter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/confirm", nil)

	formatter := func(_ *gin.Context) string {
		return "gateway.manual.payin.confirm"
	}

	got := resolveSpanName(c, formatter)
	if got != "gateway.manual.payin.confirm" {
		t.Fatalf("unexpected span name: %q", got)
	}
}

func TestResolveSpanNameEmptyFormatterFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/missing", nil)

	got := resolveSpanName(c, func(*gin.Context) string { return "" })
	want := "HTTP GET route not found"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
