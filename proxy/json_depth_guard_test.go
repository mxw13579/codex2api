package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/codex2api/security"
)

// 600 万层：gjson 的递归校验每层约 178 字节栈，超过约 570 万层即超过 1GB 栈上限，
// 进程以 fatal error: stack overflow 退出。这些用例若入口防护失效会让整个测试进程崩溃。
const jsonDepthCrashLevels = 6_000_000

type jsonDepthGuardHarness struct {
	server        *httptest.Server
	upstreamCalls *atomic.Int32
}

// newJSONDepthGuardHarness 复刻 main.go 的请求体中间件链（Recovery → CORS →
// RequestSizeLimiter → RequestBodyDecompressor → BodyCache → JSONDepthGuard），
// 再挂上真实的 proxy 路由；上游全部指向本地计数服务器。
func newJSONDepthGuardHarness(t *testing.T) *jsonDepthGuardHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	calls := &atomic.Int32{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writePinnedSuccessSSE(w)
	}))
	t.Cleanup(upstream.Close)
	previousResin := resinCfg.Load()
	t.Cleanup(func() { resinCfg.Store(previousResin) })
	SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "test"})
	previousExec := WebsocketExecuteFunc
	t.Cleanup(func() { WebsocketExecuteFunc = previousExec })
	WebsocketExecuteFunc = func(ctx context.Context, account *auth.Account, requestBody []byte, sessionID string, proxyOverride string, apiKey string, deviceCfg *DeviceProfileConfig, headers http.Header, poolRouteKey string) (*http.Response, error) {
		calls.Add(1)
		sse := `data: {"type":"response.completed","response":{"id":"resp_1","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(sse))}, nil
	}

	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 2, TestConcurrency: 1, TestModel: "gpt-5.5"})
	t.Cleanup(store.Stop)
	store.AddAccount(&auth.Account{DBID: 1, AccessToken: "at", PlanType: "pro", AccountID: "acct-depth"})
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)

	router := gin.New()
	router.Use(api.RecoveryMiddleware())
	router.Use(api.CORSMiddleware())
	router.Use(security.RequestSizeLimiter(int64(security.DefaultMaxRequestBodySize)))
	router.Use(security.RequestBodyDecompressor(int64(security.DefaultMaxRequestBodySize)))
	router.Use(api.BodyCacheMiddleware())
	router.Use(api.JSONDepthGuardMiddleware())
	handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return &jsonDepthGuardHarness{server: server, upstreamCalls: calls}
}

func deepClosed(levels int) string {
	return strings.Repeat("[", levels) + strings.Repeat("]", levels)
}

func deepOpen(levels int) string { return strings.Repeat("[", levels) }

func jsonDepthGzip(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestJSONDepthGuardRejectsDeepBodiesBeforeHandlers(t *testing.T) {
	h := newJSONDepthGuardHarness(t)
	closed := deepClosed(jsonDepthCrashLevels)
	open := deepOpen(jsonDepthCrashLevels)

	type route struct {
		path      string
		anthropic bool
		bodies    map[string]string
	}
	routes := []route{
		{path: "/v1/chat/completions", bodies: map[string]string{
			"closed messages":         `{"model":"gpt-5.5","messages":` + closed + `}`,
			"open messages":           `{"model":"gpt-5.5","messages":` + open,
			"invalid prefix + deep":   `{"model":"gpt-5.5","x":01,"messages":` + open,
			"tools":                   `{"model":"gpt-5.5","messages":[{"role":"user","content":"hi"}],"tools":` + closed + `}`,
			"metadata":                `{"model":"gpt-5.5","messages":[{"role":"user","content":"hi"}],"metadata":{"a":` + closed + `}}`,
			"content":                 `{"model":"gpt-5.5","messages":[{"role":"user","content":` + closed + `}]}`,
			"tonum quote desync":      `{"model":"gpt-5.5","messages":[1" ,` + open,
			"top-level array":         closed,
			"non-json content-type ⇒": `{"model":"gpt-5.5","messages":` + open,
		}},
		{path: "/v1/responses", bodies: map[string]string{
			"closed input":          `{"model":"gpt-5.5","input":` + closed + `}`,
			"open input":            `{"model":"gpt-5.5","input":` + open,
			"invalid prefix + deep": `{"model":"gpt-5.5","x":01,"input":` + open,
			"tools":                 `{"model":"gpt-5.5","input":"hi","tools":` + closed + `}`,
			"metadata":              `{"model":"gpt-5.5","input":"hi","metadata":` + closed + `}`,
			"content":               `{"model":"gpt-5.5","input":[{"role":"user","content":` + closed + `}]}`,
		}},
		{path: "/v1/messages", anthropic: true, bodies: map[string]string{
			"closed messages":       `{"model":"claude-sonnet-4-5","max_tokens":16,"messages":` + closed + `}`,
			"open messages":         `{"model":"claude-sonnet-4-5","max_tokens":16,"messages":` + open,
			"invalid prefix + deep": `{"model":"claude-sonnet-4-5","x":01,"messages":` + open,
			"tools":                 `{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}],"tools":` + closed + `}`,
			"metadata":              `{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}],"metadata":` + closed + `}`,
			"content":               `{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":` + closed + `}]}`,
		}},
	}
	for _, rt := range routes {
		for name, body := range rt.bodies {
			for _, compressed := range []bool{false, true} {
				payload := []byte(body)
				if compressed {
					payload = jsonDepthGzip(t, payload)
				}
				req, err := http.NewRequest(http.MethodPost, h.server.URL+rt.path, bytes.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				if strings.HasPrefix(name, "non-json") {
					// 伪造非 JSON 的 Content-Type 不能绕过：首字节为 '{' 仍检查。
					req.Header.Set("Content-Type", "multipart/form-data; boundary=x")
				}
				if compressed {
					req.Header.Set("Content-Encoding", "gzip")
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("%s %s gzip=%v: %v", rt.path, name, compressed, err)
				}
				respBody, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("%s %s gzip=%v: status=%d body=%.300s", rt.path, name, compressed, resp.StatusCode, respBody)
				}
				if rt.anthropic {
					if gjson.GetBytes(respBody, "type").String() != "error" ||
						gjson.GetBytes(respBody, "error.type").String() != "invalid_request_error" ||
						!strings.Contains(gjson.GetBytes(respBody, "error.message").String(), "maximum nesting depth of 10000") {
						t.Fatalf("%s %s: anthropic error body = %s", rt.path, name, respBody)
					}
				} else {
					const want = `{"error":{"code":"json_nesting_too_deep","message":"Request JSON exceeds the maximum nesting depth of 10000","type":"invalid_request_error"}}`
					if strings.TrimSpace(string(respBody)) != want {
						t.Fatalf("%s %s: body = %s, want %s", rt.path, name, respBody, want)
					}
				}
				if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
					t.Fatalf("%s %s: CORS header missing on early rejection", rt.path, name)
				}
			}
		}
	}
	if calls := h.upstreamCalls.Load(); calls != 0 {
		t.Fatalf("upstream calls = %d, want 0", calls)
	}
}

func TestJSONDepthGuardBoundaryAndPassThrough(t *testing.T) {
	h := newJSONDepthGuardHarness(t)
	post := func(path, contentType string, body []byte) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, h.server.URL+path, bytes.NewReader(body))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, data
	}
	// 10000 层（含外层对象）恰好在上限内：不被入口拦截（可能被后续校验拒绝，但不是嵌套错误）。
	atLimit := `{"model":"gpt-5.5","input":"hi","metadata":` + deepClosed(9999) + `}`
	if code, body := post("/v1/responses", "application/json", []byte(atLimit)); gjson.GetBytes(body, "error.code").String() == "json_nesting_too_deep" {
		t.Fatalf("depth 10000 must pass the guard: status=%d body=%.200s", code, body)
	}
	overLimit := `{"model":"gpt-5.5","input":"hi","metadata":` + deepClosed(10000) + `}`
	if code, body := post("/v1/responses", "application/json", []byte(overLimit)); code != http.StatusBadRequest || gjson.GetBytes(body, "error.code").String() != "json_nesting_too_deep" {
		t.Fatalf("depth 10001 must be rejected: status=%d body=%.200s", code, body)
	}
	// 括号在字符串里不计入深度。
	inString := `{"model":"gpt-5.5","input":"` + strings.Repeat("[{", 50000) + `"}`
	if _, body := post("/v1/responses", "application/json", []byte(inString)); gjson.GetBytes(body, "error.code").String() == "json_nesting_too_deep" {
		t.Fatalf("brackets inside strings must not be counted: %.200s", body)
	}
	// 真正的 multipart 请求体（首字节不是 '{'/'['）跳过检查，即使含大量括号字节。
	multipartBody := "--x\r\nContent-Disposition: form-data; name=\"image\"; filename=\"a.png\"\r\n\r\n" + deepOpen(50000) + "\r\n--x--\r\n"
	if _, body := post("/v1/images/edits", "multipart/form-data; boundary=x", []byte(multipartBody)); gjson.GetBytes(body, "error.code").String() == "json_nesting_too_deep" {
		t.Fatalf("multipart body must not be depth-checked: %.200s", body)
	}
	// 空 body 不受影响。
	if _, body := post("/v1/responses", "application/json", nil); gjson.GetBytes(body, "error.code").String() == "json_nesting_too_deep" {
		t.Fatalf("empty body must not be depth-checked: %.200s", body)
	}
}

func dialJSONDepthWS(t *testing.T, h *jsonDepthGuardHarness, path string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(h.server.URL, "http") + path
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial %s: %v status=%d", path, err, status)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readWSFrameOfType(t *testing.T, conn *websocket.Conn, wantType string) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read websocket frame: %v", err)
		}
		if gjson.GetBytes(frame, "type").String() == wantType {
			return frame
		}
	}
}

func TestJSONDepthGuardWebSocketFrames(t *testing.T) {
	h := newJSONDepthGuardHarness(t)
	frames := map[string]string{
		"closed":         `{"type":"response.create","model":"gpt-5.5","input":` + deepClosed(jsonDepthCrashLevels) + `}`,
		"open":           `{"type":"response.create","model":"gpt-5.5","input":` + deepOpen(jsonDepthCrashLevels),
		"invalid prefix": `{"type":"response.create","x":01,"input":` + deepOpen(jsonDepthCrashLevels),
	}
	for _, path := range []string{"/v1/responses", "/backend-api/codex/responses", "/v1/realtime?model=gpt-5.5"} {
		conn := dialJSONDepthWS(t, h, path)
		if strings.HasPrefix(path, "/v1/realtime") {
			readWSFrameOfType(t, conn, "session.created")
		}
		for name, frame := range frames {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
				t.Fatalf("%s %s: write: %v", path, name, err)
			}
			errFrame := readWSFrameOfType(t, conn, "error")
			if got := gjson.GetBytes(errFrame, "error.code").String(); got != "json_nesting_too_deep" {
				t.Fatalf("%s %s: error frame = %s", path, name, errFrame)
			}
			if got := gjson.GetBytes(errFrame, "status").Int(); got != http.StatusBadRequest {
				t.Fatalf("%s %s: error frame status = %d frame=%s", path, name, got, errFrame)
			}
		}
		// 连接仍可用：后续合法但不完整的帧得到普通的校验错误，而不是断开。
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`[]`)); err != nil {
			t.Fatalf("%s: connection should stay open: %v", path, err)
		}
		errFrame := readWSFrameOfType(t, conn, "error")
		if gjson.GetBytes(errFrame, "error.code").String() == "json_nesting_too_deep" {
			t.Fatalf("%s: shallow frame misclassified: %s", path, errFrame)
		}
	}
	if calls := h.upstreamCalls.Load(); calls != 0 {
		t.Fatalf("upstream calls = %d, want 0", calls)
	}
}
