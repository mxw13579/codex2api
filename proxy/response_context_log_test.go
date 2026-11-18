package proxy

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) contextLines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var lines []string
	for _, line := range strings.Split(b.buf.String(), "\n") {
		if strings.Contains(line, "[ResponseContext] unavailable") {
			lines = append(lines, line)
		}
	}
	return lines
}

func captureResponseContextLogs(t *testing.T) *lockedLogBuffer {
	t.Helper()
	previousWriter, previousFlags := log.Writer(), log.Flags()
	buffer := &lockedLogBuffer{}
	log.SetOutput(buffer)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})
	return buffer
}

func requireSingleResponseContextLine(t *testing.T, logs *lockedLogBuffer, want ...string) string {
	t.Helper()
	lines := logs.contextLines()
	if len(lines) != 1 {
		t.Fatalf("[ResponseContext] lines = %q, want exactly one", lines)
	}
	for _, fragment := range want {
		if !strings.Contains(lines[0], fragment) {
			t.Fatalf("log line %q missing %q", lines[0], fragment)
		}
	}
	return lines[0]
}

func TestResponseContextUnavailableHTTPLogsReasonOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetResponseCacheStateForTest(testResponseCacheConfig())
	logs := captureResponseContextLogs(t)
	handler := NewHandler(newContinuationCodexStore(), nil, nil, nil)
	raw := []byte(`{"model":"gpt-5.5","previous_response_id":"resp_missing_secret","input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}],"stream":true}`)

	recorder := invokeResponsesHandler(t, handler.Responses, raw)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if gjson.Get(body, "error.code").String() != string(api.ErrCodeResponseContextUnavailable) ||
		gjson.Get(body, "error.message").String() != "Previous response context is unavailable" ||
		gjson.Get(body, "error.details.field").String() != "previous_response_id" ||
		gjson.Get(body, "error.details.message").String() != "missing_required_call_context" {
		t.Fatalf("response body changed: %s", body)
	}
	line := requireSingleResponseContextLine(t, logs,
		"status=409 ", "reason=missing_required_call_context ", "endpoint=/v1/responses ", "transport=http ", "account=0")
	if strings.Contains(line, "resp_missing_secret") {
		t.Fatalf("log line leaked previous_response_id: %q", line)
	}
}

func TestResponseContextUnavailableCompactSearchIDLogsReason(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetResponseCacheStateForTest(testResponseCacheConfig())
	logs := captureResponseContextLogs(t)
	handler := NewHandler(newContinuationCodexStore(), nil, nil, nil)
	raw := []byte(`{"model":"gpt-5.5","previous_response_id":"` + codexSearchResponseIDPrefix + `abc","input":"hi"}`)

	recorder := invokeResponsesHandler(t, handler.ResponsesCompact, raw)

	if recorder.Code != http.StatusConflict ||
		gjson.Get(recorder.Body.String(), "error.code").String() != string(api.ErrCodeResponseContextUnavailable) ||
		gjson.Get(recorder.Body.String(), "error.details.message").String() != "search_context_requires_responses" {
		t.Fatalf("compact response changed: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	requireSingleResponseContextLine(t, logs, "status=409 ", "reason=search_context_requires_responses ", "transport=http ", "account=0")
}

func TestResponseContextUnavailableHTTPContinuationLostLogsAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetResponseCacheStateForTest(testResponseCacheConfig())
	previousSettings := CurrentRuntimeSettings()
	nextSettings := previousSettings
	nextSettings.CodexForceWebsocket = true
	ApplyRuntimeSettings(nextSettings)
	previousExec := WebsocketExecuteFunc
	t.Cleanup(func() {
		WebsocketExecuteFunc = previousExec
		ApplyRuntimeSettings(previousSettings)
	})
	var calls int
	WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
		calls++
		return nil, &ResponsesContinuationLostError{Reason: "original_connection_unavailable"}
	}
	logs := captureResponseContextLogs(t)
	handler := NewHandler(newContinuationCodexStore(), nil, nil, nil)

	recorder := invokeResponsesHandler(t, handler.Responses, []byte(`{"model":"gpt-5.5","input":"hi"}`))

	if calls == 0 {
		t.Fatal("upstream websocket executor was not called")
	}
	if recorder.Code != http.StatusConflict ||
		gjson.Get(recorder.Body.String(), "error.code").String() != "previous_response_not_found" ||
		gjson.Get(recorder.Body.String(), "error.message").String() != "Previous response context is unavailable" {
		t.Fatalf("continuation-lost response changed: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	requireSingleResponseContextLine(t, logs, "status=409 ", "reason=original_connection_unavailable ", "transport=http ", "account=1")
}

func TestResponseContextUnavailableReasonRecognizesOnlyLocalContextErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    *api.APIError
		reason string
		ok     bool
	}{
		{"conflict", responsesWSContextUnavailable(http.StatusConflict, "nonportable_encrypted_context"), "nonportable_encrypted_context", true},
		{"native", nativeResponsesWSContextError(responsesWSContextUnavailable(http.StatusConflict, "original_connection_unavailable")), "original_connection_unavailable", true},
		{"backend", nativeResponsesWSContextError(responsesWSContextUnavailable(http.StatusServiceUnavailable, "context_commit_pending")), "context_commit_pending", true},
		{"upstream not found", api.NewAPIError(api.ErrorCode("previous_response_not_found"), "Previous response not found", api.ErrorTypeInvalidRequest), "", false},
		{"other 503", api.NewAPIError(api.ErrCodeServiceUnavailable, "no account", api.ErrorTypeServer), "", false},
	} {
		reason, ok := responseContextUnavailableReason(tc.err)
		if reason != tc.reason || ok != tc.ok {
			t.Fatalf("%s: responseContextUnavailableReason = (%q, %v), want (%q, %v)", tc.name, reason, ok, tc.reason, tc.ok)
		}
	}
}

func TestResponseContextUnavailableWSLogsReasonOnce(t *testing.T) {
	fixture := newNativeWSFixture(t, func(_ context.Context, body []byte) (*http.Response, error) {
		if gjson.GetBytes(body, "previous_response_id").Exists() {
			return nil, &ResponsesContinuationLostError{Reason: "original_connection_unavailable"}
		}
		return nativeWSResponse("fresh"), nil
	})
	logs := captureResponseContextLogs(t)
	conn := fixture.connect(t)

	terminal := nativeWSTurn(t, conn, `{"previous_response_id":"resp_ws_secret","input":[{"role":"user","content":"next"}]}`)

	if gjson.GetBytes(terminal, "error.code").String() != "previous_response_not_found" || gjson.GetBytes(terminal, "status").Int() != http.StatusConflict {
		t.Fatalf("missing continuation classification changed: %s", terminal)
	}
	reason := gjson.GetBytes(terminal, "error.details.message").String()
	if reason == "" {
		t.Fatalf("terminal has no reason detail: %s", terminal)
	}
	line := requireSingleResponseContextLine(t, logs,
		"status=409 ", "reason="+reason+" ", "endpoint=/v1/responses ", "transport=ws ", "account=1")
	if strings.Contains(line, "resp_ws_secret") {
		t.Fatalf("log line leaked previous_response_id: %q", line)
	}
	t.Logf("terminal=%s log=%q", terminal, line)
	assertNativeWSSuccess(t, nativeWSTurn(t, conn, `{"input":[{"role":"user","content":"full history"}]}`))
	if got := len(logs.contextLines()); got != 1 {
		t.Fatalf("[ResponseContext] lines after successful turn = %d, want 1", got)
	}
}

// 同一个 gin context 跨轮复用（realtime 路径）时，上一轮记下的账号不能串到下一轮
// 选号之前就返回的"上下文不可用"日志里。
func TestResponseContextUnavailableWSResetsAccountPerTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetResponseCacheForTest()
	t.Cleanup(resetResponseCacheForTest)
	previousExec := WebsocketExecuteFunc
	t.Cleanup(func() { WebsocketExecuteFunc = previousExec })
	WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
		return nativeWSResponse("resp_first_turn"), nil
	}
	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1})
	t.Cleanup(store.Stop)
	store.AddAccount(&auth.Account{DBID: 7, AccessToken: "fixture", PlanType: "plus"})
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
	turnDone := make(chan error, 2)
	router := gin.New()
	router.GET("/v1/realtime", func(c *gin.Context) {
		conn, err := responsesWSUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			turnDone <- err
			return
		}
		defer conn.Close()
		for range 2 {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				turnDone <- err
				return
			}
			// 与 realtime_ws.go 一致：每轮复用同一个 c。
			turnDone <- handler.forwardResponsesWebSocketTurn(c, conn, payload, "account-reset", nil)
		}
	})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	logs := captureResponseContextLogs(t)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime", http.Header{"Session-Id": {"account-reset"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	assertNativeWSSuccess(t, nativeWSTurn(t, conn, `{"input":[{"role":"user","content":"first"}]}`))
	if err := <-turnDone; err != nil {
		t.Fatalf("first turn error: %v", err)
	}
	terminal := nativeWSTurn(t, conn, `{"previous_response_id":"`+codexSearchResponseIDPrefix+`missing","input":[{"role":"user","content":"second"}]}`)
	<-turnDone

	reason := gjson.GetBytes(terminal, "error.details.message").String()
	if reason == "" {
		t.Fatalf("second turn did not fail with a context reason: %s", terminal)
	}
	requireSingleResponseContextLine(t, logs, "reason="+reason+" ", "transport=ws ", "account=0")
}
