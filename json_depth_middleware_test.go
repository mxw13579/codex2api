package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codex2api/api"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// configureRequestBodyMiddleware 必须在任何 handler 解析 JSON 之前拦截超深嵌套；
// 这里的 handler 直接做 gjson 递归校验，若防护缺失整个测试进程会栈溢出崩溃。
func TestJSONDepthRequestBodyMiddlewareRejectsDeepBeforeHandlers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(api.RecoveryMiddleware())
	configureRequestBodyMiddleware(router, 48<<20)
	reached := 0
	handler := func(c *gin.Context) {
		reached++
		_ = gjson.ValidBytes(api.GetRawBody(c))
		c.Status(http.StatusNoContent)
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages", "/messages", "/v1/messages/count_tokens", "/v1beta/models/x:generateContent", "/api/admin/settings"} {
		router.POST(path, handler)
	}

	const levels = 6_000_000
	open := strings.Repeat("[", levels)
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, _ = zw.Write([]byte(`{"model":"m","input":` + open))
	_ = zw.Close()

	cases := []struct {
		name, path, encoding string
		body                 []byte
		anthropic            bool
	}{
		{"chat open", "/v1/chat/completions", "", []byte(`{"model":"m","messages":` + open), false},
		{"responses gzip", "/v1/responses", "gzip", compressed.Bytes(), false},
		{"messages", "/v1/messages", "", []byte(`{"model":"m","messages":` + open), true},
		{"messages no prefix", "/messages", "", []byte(`{"model":"m","messages":` + open), true},
		{"count tokens uses openai format", "/v1/messages/count_tokens", "", []byte(`{"messages":` + open), false},
		{"gemini", "/v1beta/models/x:generateContent", "", []byte(`{"contents":` + open), false},
		{"admin", "/api/admin/settings", "", []byte(`{"a":` + open), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%.300s", rec.Code, rec.Body.String())
			}
			body := rec.Body.Bytes()
			if tc.anthropic {
				if gjson.GetBytes(body, "type").String() != "error" || gjson.GetBytes(body, "error.type").String() != "invalid_request_error" || gjson.GetBytes(body, "error.message").String() == "" {
					t.Fatalf("anthropic error body = %s", body)
				}
			} else if gjson.GetBytes(body, "error.code").String() != "json_nesting_too_deep" || gjson.GetBytes(body, "error.type").String() != "invalid_request_error" {
				t.Fatalf("error body = %s", body)
			}
		})
	}
	if reached != 0 {
		t.Fatalf("handler reached %d times", reached)
	}

	// 正常请求不受影响，且 handler 读到的 body 与原文一致。
	normal := `{"model":"m","input":[{"role":"user","content":[{"type":"input_text","text":"[[[{{{"}]}]}`
	var seen []byte
	router.POST("/echo", func(c *gin.Context) {
		seen, _ = io.ReadAll(c.Request.Body)
		c.Status(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader(normal))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || string(seen) != normal {
		t.Fatalf("normal request changed: status=%d body=%q", rec.Code, seen)
	}
}

// 管理端背景上传是豁免路由：图片/视频文件里大量 '['/'{' 不能被误拒。
func TestJSONDepthBackgroundUploadMultipartIsExempt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	configureRequestBodyMiddleware(router, 48<<20)
	var gotSize int64
	router.POST("/api/admin/settings/background-upload", func(c *gin.Context) {
		fh, err := c.FormFile("file")
		if err != nil {
			c.Status(http.StatusBadRequest)
			return
		}
		gotSize = fh.Size
		c.Status(http.StatusNoContent)
	})
	file := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte("[[{{"), 20000)...)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", "bg.png")
	_, _ = fw.Write(file)
	_ = mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/settings/background-upload", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || gotSize != int64(len(file)) {
		t.Fatalf("background upload rejected: status=%d body=%.200s", rec.Code, rec.Body.String())
	}
	// 同样的文件字节当成 JSON 接口的请求体则必须拒绝（非豁免路由不看 Content-Type）。
	router.POST("/api/admin/settings", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	req = httptest.NewRequest(http.MethodPost, "/api/admin/settings", bytes.NewReader(file))
	req.Header.Set("Content-Type", "application/octet-stream")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("non-exempt binary body must be checked: status=%d", rec.Code)
	}
}
