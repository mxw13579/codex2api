package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestShouldCheckJSONDepth(t *testing.T) {
	cases := []struct {
		ct   string
		body string
		want bool
	}{
		{"application/json", `{"a":1}`, true},
		{"application/json; charset=utf-8", `x`, true},
		{"application/vnd.api+json", `x`, true},
		{"", `{"a":1}`, true},
		{"text/plain", `hello`, true},
		{"multipart/form-data; boundary=x", "--x\r\n", false},
		{"multipart/form-data; boundary=x", " \n{\"a\":1}", true}, // 伪造类型仍检查
		{"application/x-www-form-urlencoded", "a=1", false},
		{"application/octet-stream", "\x89PNG", false},
		{"image/png", "[", true},
		{"image/png", "\x89PNG[[[", false},
	}
	for _, tc := range cases {
		if got := shouldCheckJSONDepth(tc.ct, []byte(tc.body)); got != tc.want {
			t.Fatalf("shouldCheckJSONDepth(%q, %q)=%v want %v", tc.ct, tc.body, got, tc.want)
		}
	}
}

func TestJSONDepthGuardMiddlewareSkipsImportAndPassesNormal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	deep := []byte(`{"a":` + strings.Repeat("[", 20000))
	for _, tc := range []struct {
		path string
		body []byte
		want int
	}{
		{"/v1/responses", []byte(`{"model":"m"}`), http.StatusNoContent},
		{"/v1/responses", deep, http.StatusBadRequest},
		// 导入端点不缓存 raw body，由 handler 流式解析 multipart，不在这里检查。
		{"/api/admin/accounts/import", deep, http.StatusNoContent},
	} {
		router := gin.New()
		router.Use(func(c *gin.Context) {
			if c.Request.URL.Path != "/api/admin/accounts/import" {
				c.Set("raw_body", tc.body)
			}
			c.Next()
		})
		router.Use(JSONDepthGuardMiddleware())
		router.POST(tc.path, func(c *gin.Context) { c.Status(http.StatusNoContent) })
		req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("%s: status=%d want %d body=%s", tc.path, rec.Code, tc.want, rec.Body.String())
		}
	}
}
