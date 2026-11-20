package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestJSONDepthExemptUpload(t *testing.T) {
	cases := []struct {
		method, path, ct string
		want             bool
	}{
		{"POST", "/v1/images/edits", "multipart/form-data; boundary=x", true},
		{"POST", "/images/edits", "Multipart/Form-Data; boundary=x", true},
		{"POST", "/v1/live", "multipart/form-data; boundary=x", true},
		{"POST", "/live", "multipart/form-data; boundary=x", true},
		{"POST", "/backend-api/codex/realtime/calls", "multipart/form-data; boundary=x", true},
		{"POST", "/api/admin/settings/background-upload", "multipart/form-data; boundary=x", true},
		{"POST", "/v1/images/edits", "application/json", false},
		{"POST", "/v1/images/edits", "", false},
		{"POST", "/v1/live", "application/sdp", false},
		{"POST", "/v1/responses", "multipart/form-data; boundary=x", false},
		{"POST", "/v1/chat/completions", "application/x-www-form-urlencoded", false},
		{"POST", "/v1/messages", "application/octet-stream", false},
		{"PUT", "/v1/images/edits", "multipart/form-data; boundary=x", false},
	}
	for _, tc := range cases {
		if got := jsonDepthExemptUpload(tc.method, tc.path, tc.ct); got != tc.want {
			t.Fatalf("jsonDepthExemptUpload(%s %s %q)=%v want %v", tc.method, tc.path, tc.ct, got, tc.want)
		}
	}
}

func TestJSONDepthGuardMiddlewareStatuses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	deepValid := []byte(`{"a":` + strings.Repeat("[", 20000) + strings.Repeat("]", 20000) + `}`)
	deepOpen := []byte(`{"a":` + strings.Repeat("[", 20000))
	invalidPrefix := []byte(`{"a":01,"b":` + strings.Repeat("[", 20000))
	controlPrefix := append([]byte("\x0c"), deepValid...)
	binaryLike := append([]byte("\x89PNG\r\n"), bytes.Repeat([]byte("[{"), 20000)...)
	const tooDeep = "Request JSON exceeds the maximum nesting depth of 10000"
	const unverifiable = "Request body is not valid JSON and its nesting depth could not be verified to be within the maximum of 10000"
	for _, tc := range []struct {
		name, path, ct string
		body           []byte
		wantStatus     int
		wantMessage    string
	}{
		{"normal", "/v1/responses", "application/json", []byte(`{"model":"m"}`), http.StatusNoContent, ""},
		{"valid deep", "/v1/responses", "application/json", deepValid, http.StatusBadRequest, tooDeep},
		{"unclosed deep", "/v1/responses", "application/json", deepOpen, http.StatusBadRequest, tooDeep},
		{"invalid prefix", "/v1/responses", "application/json", invalidPrefix, http.StatusBadRequest, unverifiable},
		{"control byte prefix form ct", "/v1/responses", "application/x-www-form-urlencoded", controlPrefix, http.StatusBadRequest, unverifiable},
		{"octet-stream to json route", "/v1/chat/completions", "application/octet-stream", binaryLike, http.StatusBadRequest, unverifiable},
		{"multipart to json route", "/v1/responses", "multipart/form-data; boundary=x", deepValid, http.StatusBadRequest, tooDeep},
		{"exempt multipart upload", "/v1/images/edits", "multipart/form-data; boundary=x", binaryLike, http.StatusNoContent, ""},
		// 导入端点不缓存 raw body，由 handler 流式解析 multipart，不在这里检查。
		{"import path", "/api/admin/accounts/import", "multipart/form-data; boundary=x", deepValid, http.StatusNoContent, ""},
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
		req.Header.Set("Content-Type", tc.ct)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != tc.wantStatus {
			t.Fatalf("%s: status=%d want %d body=%.200s", tc.name, rec.Code, tc.wantStatus, rec.Body.String())
		}
		if tc.wantMessage != "" {
			body := rec.Body.Bytes()
			if gjson.GetBytes(body, "error.message").String() != tc.wantMessage || gjson.GetBytes(body, "error.code").String() != "json_nesting_too_deep" {
				t.Fatalf("%s: body=%s", tc.name, body)
			}
		}
	}
}
