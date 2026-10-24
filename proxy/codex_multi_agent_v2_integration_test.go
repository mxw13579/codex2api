package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

const codexMultiAgentV2TestBody = `{
	"model":"gpt-5.5",
	"stream":true,
	"input":[
		{"type":"additional_tools","role":"developer","tools":[
			{"type":"namespace","name":"collaboration","tools":[
				{"type":"function","name":"followup_task","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}},
				{"type":"function","name":"send_message","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}},
				{"type":"function","name":"spawn_agent","parameters":{"type":"object","properties":{"task_name":{"type":"string"},"message":{"type":"string","encrypted":true}},"required":["task_name","message"]}}
			]},
			{"type":"function","name":"business_tool","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}}
		]},
		{"type":"message","role":"user","content":[{"type":"input_text","text":"delegate this task"}]}
	]
}`

func newCodexMultiAgentV2TestHandler(t *testing.T, upstream http.Handler) *Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	server := httptest.NewServer(upstream)
	t.Cleanup(server.Close)
	previousResin := resinCfg.Load()
	SetResinConfig(&ResinConfig{BaseURL: server.URL, PlatformName: "test"})
	t.Cleanup(func() { resinCfg.Store(previousResin) })
	previousSettings := CurrentRuntimeSettings()
	nextSettings := previousSettings
	nextSettings.CodexRequestCompression = false
	ApplyRuntimeSettings(nextSettings)
	t.Cleanup(func() { ApplyRuntimeSettings(previousSettings) })

	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1, TestConcurrency: 1, TestModel: "gpt-5.5"})
	account := &auth.Account{DBID: 1, AccessToken: "at-1", PlanType: "pro", AccountID: "acct-1"}
	account.SetDispatchCountLimit(1)
	store.AddAccount(account)
	return NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
}

func runCodexMultiAgentV2Responses(t *testing.T, handler *Handler, body []byte, userAgent string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = req
	handler.Responses(ctx)
	return recorder
}

func codexMultiAgentV2UpstreamSSE(optimized bool) string {
	namespace := codexCollaborationNamespace
	if optimized {
		namespace = codexOptimizedCollaborationNamespace
	}
	return `data: {"type":"response.created","response":{"id":"resp_ma"}}` + "\n\n" +
		`data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_ma","namespace":"` + namespace + `","name":"spawn_agent","arguments":"{\"task_name\":\"probe\",\"message\":\"PLAINTEXT_TASK\"}","encrypted_function_args":[]}}` + "\n\n" +
		`data: {"type":"response.completed","response":{"id":"resp_ma","status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"
}

func TestResponsesCodexMultiAgentV2CompatRewritesParentAndRestoresResponse(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "1")
	upstreamBodies := make(chan []byte, 1)
	handler := newCodexMultiAgentV2TestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		upstreamBodies <- body
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(codexMultiAgentV2UpstreamSSE(true)))
	}))

	recorder := runCodexMultiAgentV2Responses(t, handler, []byte(codexMultiAgentV2TestBody), "codex_exec/0.162.0-alpha.17.2")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	upstreamBody := <-upstreamBodies
	if ns := gjson.GetBytes(upstreamBody, "input.0.tools.0.name").String(); ns != codexOptimizedCollaborationNamespace {
		t.Fatalf("upstream namespace = %q, want %q; body=%s", ns, codexOptimizedCollaborationNamespace, upstreamBody)
	}
	for _, path := range []string{
		"input.0.tools.0.tools.0.parameters.properties.message.encrypted",
		"input.0.tools.0.tools.1.parameters.properties.message.encrypted",
		"input.0.tools.0.tools.2.parameters.properties.message.encrypted",
	} {
		if gjson.GetBytes(upstreamBody, path).Exists() {
			t.Fatalf("upstream still has %s: %s", path, upstreamBody)
		}
	}
	if !gjson.GetBytes(upstreamBody, "input.0.tools.1.parameters.properties.message.encrypted").Exists() {
		t.Fatalf("unrelated tool encrypted marker was removed: %s", upstreamBody)
	}

	downstream := recorder.Body.String()
	if strings.Contains(downstream, codexOptimizedCollaborationNamespace) {
		t.Fatalf("downstream leaked optimized namespace: %s", downstream)
	}
	if !strings.Contains(downstream, `"namespace":"collaboration"`) {
		t.Fatalf("downstream namespace was not restored: %s", downstream)
	}
	if !strings.Contains(downstream, `"encrypted_function_args":[]`) {
		t.Fatalf("downstream plaintext marker was lost: %s", downstream)
	}
}

func codexMultiAgentV2UpstreamSSEWithoutMarker(namespace string) string {
	return `data: {"type":"response.created","response":{"id":"resp_ma"}}` + "\n\n" +
		`data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_ma","namespace":"` + namespace + `","name":"spawn_agent","arguments":"{\"task_name\":\"probe\",\"message\":\"PLAINTEXT_TASK\"}"}}` + "\n\n" +
		`data: {"type":"response.completed","response":{"id":"resp_ma","status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"
}

func TestResponsesCodexMultiAgentV2CompatInjectsMissingMarker(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "1")
	handler := newCodexMultiAgentV2TestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(codexMultiAgentV2UpstreamSSEWithoutMarker(codexOptimizedCollaborationNamespace)))
	}))

	recorder := runCodexMultiAgentV2Responses(t, handler, []byte(codexMultiAgentV2TestBody), "codex_exec/0.162.0-alpha.17.2")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	downstream := recorder.Body.String()
	if strings.Contains(downstream, codexOptimizedCollaborationNamespace) {
		t.Fatalf("downstream leaked optimized namespace: %s", downstream)
	}
	if !strings.Contains(downstream, `"namespace":"collaboration"`) {
		t.Fatalf("downstream namespace was not restored: %s", downstream)
	}
	if !strings.Contains(downstream, `"encrypted_function_args":[]`) {
		t.Fatalf("downstream plaintext marker was not injected: %s", downstream)
	}
}

func TestResponsesCodexMultiAgentV2CompatInjectsMarkerWithCollaborationNamespace(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "1")
	handler := newCodexMultiAgentV2TestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(codexMultiAgentV2UpstreamSSEWithoutMarker(codexCollaborationNamespace)))
	}))

	recorder := runCodexMultiAgentV2Responses(t, handler, []byte(codexMultiAgentV2TestBody), "codex_exec/0.162.0-alpha.17.2")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	downstream := recorder.Body.String()
	if !strings.Contains(downstream, `"namespace":"collaboration"`) {
		t.Fatalf("downstream namespace = %s", downstream)
	}
	if !strings.Contains(downstream, `"encrypted_function_args":[]`) {
		t.Fatalf("downstream plaintext marker was not injected: %s", downstream)
	}
}

func TestResponsesCodexMultiAgentV2CompatDisabledLeavesRequestAndResponseUntouched(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "0")
	upstreamBodies := make(chan []byte, 1)
	handler := newCodexMultiAgentV2TestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		upstreamBodies <- body
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(codexMultiAgentV2UpstreamSSE(true)))
	}))

	recorder := runCodexMultiAgentV2Responses(t, handler, []byte(codexMultiAgentV2TestBody), "codex_exec/0.162.0-alpha.17.2")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	upstreamBody := <-upstreamBodies
	if ns := gjson.GetBytes(upstreamBody, "input.0.tools.0.name").String(); ns != codexCollaborationNamespace {
		t.Fatalf("disabled compat changed upstream namespace: %q", ns)
	}
	if !gjson.GetBytes(upstreamBody, "input.0.tools.0.tools.2.parameters.properties.message.encrypted").Exists() {
		t.Fatalf("disabled compat removed encrypted marker: %s", upstreamBody)
	}
	if !strings.Contains(recorder.Body.String(), codexOptimizedCollaborationNamespace) {
		t.Fatalf("disabled compat unexpectedly restored response: %s", recorder.Body.String())
	}
}

func TestResponsesCodexMultiAgentV2CompatNonStreamRestoresResponse(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "1")
	handler := newCodexMultiAgentV2TestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(codexMultiAgentV2UpstreamSSE(true)))
	}))

	body := []byte(strings.Replace(codexMultiAgentV2TestBody, `"stream":true`, `"stream":false`, 1))
	recorder := runCodexMultiAgentV2Responses(t, handler, body, "codex_exec/0.162.0-alpha.17.2")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := recorder.Body.Bytes()
	if strings.Contains(string(response), codexOptimizedCollaborationNamespace) {
		t.Fatalf("non-stream response leaked optimized namespace: %s", response)
	}
	if ns := gjson.GetBytes(response, "output.0.namespace").String(); ns != codexCollaborationNamespace {
		t.Fatalf("non-stream namespace = %q, want %q; body=%s", ns, codexCollaborationNamespace, response)
	}
	if marker := gjson.GetBytes(response, "output.0.encrypted_function_args"); !marker.IsArray() || marker.Raw != "[]" {
		t.Fatalf("non-stream plaintext marker = %s, want []", marker.Raw)
	}
}

func TestResponsesCodexMultiAgentV2CompatConflictFailsClosed(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "1")
	upstreamBodies := make(chan []byte, 1)
	handler := newCodexMultiAgentV2TestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		upstreamBodies <- body
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(codexMultiAgentV2UpstreamSSE(false)))
	}))

	conflict := strings.Replace(codexMultiAgentV2TestBody,
		`{"type":"function","name":"business_tool"`,
		`{"type":"namespace","name":"collaboration-optimize","tools":[]},{"type":"function","name":"business_tool"`, 1)
	recorder := runCodexMultiAgentV2Responses(t, handler, []byte(conflict), "codex_exec/0.162.0-alpha.17.2")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	upstreamBody := <-upstreamBodies
	if ns := gjson.GetBytes(upstreamBody, "input.0.tools.0.name").String(); ns != codexCollaborationNamespace {
		t.Fatalf("conflict changed upstream namespace: %q", ns)
	}
	if !gjson.GetBytes(upstreamBody, "input.0.tools.0.tools.2.parameters.properties.message.encrypted").Exists() {
		t.Fatalf("conflict removed encrypted marker: %s", upstreamBody)
	}
}

func TestResponsesCodexMultiAgentV2CompatIgnoresOtherClients(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "1")
	upstreamBodies := make(chan []byte, 1)
	handler := newCodexMultiAgentV2TestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		upstreamBodies <- body
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(codexMultiAgentV2UpstreamSSE(false)))
	}))

	recorder := runCodexMultiAgentV2Responses(t, handler, []byte(codexMultiAgentV2TestBody), "curl/8.7.1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	upstreamBody := <-upstreamBodies
	if ns := gjson.GetBytes(upstreamBody, "input.0.tools.0.name").String(); ns != codexCollaborationNamespace {
		t.Fatalf("non-Codex client was rewritten: %q", ns)
	}
}

func TestResponsesWebSocketCodexMultiAgentV2CompatRewritesAndRestores(t *testing.T) {
	t.Setenv(codexMultiAgentV2CompatEnv, "1")
	gin.SetMode(gin.TestMode)

	previousExec := WebsocketExecuteFunc
	t.Cleanup(func() { WebsocketExecuteFunc = previousExec })
	bodyCh := make(chan []byte, 1)
	WebsocketExecuteFunc = func(ctx context.Context, account *auth.Account, requestBody []byte, sessionID string, proxyOverride string, apiKey string, deviceCfg *DeviceProfileConfig, headers http.Header, poolRouteKey string) (*http.Response, error) {
		bodyCh <- append([]byte(nil), requestBody...)
		sse := codexMultiAgentV2UpstreamSSE(true)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(sse)),
		}, nil
	}

	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 2, TestConcurrency: 1, TestModel: "gpt-5.5"})
	store.AddAccount(&auth.Account{DBID: 1, AccessToken: "at", PlanType: "plus", AccountID: "acct-1"})
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
	router := gin.New()
	handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses"
	headers := http.Header{"User-Agent": []string{"codex_exec/0.162.0-alpha.17.2"}}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err != nil {
		if resp != nil {
			t.Fatalf("dial websocket failed: %v status=%d", err, resp.StatusCode)
		}
		t.Fatalf("dial websocket failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"model":"gpt-5.5","stream":true,"input":[
		{"type":"additional_tools","role":"developer","tools":[
			{"type":"namespace","name":"collaboration","tools":[
				{"type":"function","name":"spawn_agent","parameters":{"type":"object","properties":{"task_name":{"type":"string"},"message":{"type":"string","encrypted":true}}}}
			]}
		]},
		{"type":"message","role":"user","content":[{"type":"input_text","text":"delegate"}]}
	]}`)); err != nil {
		t.Fatalf("write request: %v", err)
	}

	select {
	case gotBody := <-bodyCh:
		if ns := gjson.GetBytes(gotBody, "input.0.tools.0.name").String(); ns != codexOptimizedCollaborationNamespace {
			t.Fatalf("upstream WS namespace = %q; body=%s", ns, gotBody)
		}
		if gjson.GetBytes(gotBody, "input.0.tools.0.tools.0.parameters.properties.message.encrypted").Exists() {
			t.Fatalf("upstream WS encrypted marker was not removed: %s", gotBody)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream WS request")
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read downstream message: %v", err)
		}
		if eventType := gjson.GetBytes(message, "type").String(); eventType == "response.output_item.done" {
			if ns := gjson.GetBytes(message, "item.namespace").String(); ns != codexCollaborationNamespace {
				t.Fatalf("downstream WS namespace = %q; message=%s", ns, message)
			}
			if marker := gjson.GetBytes(message, "item.encrypted_function_args"); !marker.IsArray() || marker.Raw != "[]" {
				t.Fatalf("downstream WS plaintext marker = %s; message=%s", marker.Raw, message)
			}
			return
		}
	}
}
