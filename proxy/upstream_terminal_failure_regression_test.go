package proxy

import (
	"context"
	"fmt"
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

func isolateTerminalFailureRuntime(t *testing.T) {
	t.Helper()
	previous := CurrentRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previous) })
	next := previous
	next.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	next.CodexWSSilentRetry = false
	next.CodexWSHideErrors = false
	next.CodexForceWebsocket = false
	ApplyRuntimeSettings(next)
}

func TestChatNonStreamPrematureEOFDoesNotReturnSuccess(t *testing.T) {
	isolateTerminalFailureRuntime(t)
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			events := []string{`{"type":"response.created","response":{"id":"resp_unfinished"}}`}
			if partial {
				events = append(events, `{"type":"response.output_text.delta","delta":"unfinished text"}`)
			}
			handler, _ := newChatStreamTerminalTestHandler(t, events)
			recorder := invokeChatCompletionsWithBody(t, handler, `{"model":"gpt-5.5","stream":false,"messages":[{"role":"user","content":"hello"}]}`)
			if recorder.Code != http.StatusBadGateway || gjson.GetBytes(recorder.Body.Bytes(), "error.code").String() != ErrorCodeUpstreamStreamBreak {
				t.Fatalf("premature EOF returned status=%d body=%s; want 502 stream-break error", recorder.Code, recorder.Body.String())
			}
			if strings.Contains(recorder.Body.String(), `"choices"`) || strings.Contains(recorder.Body.String(), `"finish_reason":"stop"`) {
				t.Fatalf("failed non-stream response fabricated success: %s", recorder.Body.String())
			}
		})
	}
}

func TestHTTPResponsesTerminalFailureCannotBecomeSuccess(t *testing.T) {
	isolateTerminalFailureRuntime(t)
	for _, stream := range []bool{false, true} {
		for _, endpoint := range []string{"responses", "chat"} {
			for _, terminalType := range []string{"response.completed", "response.done", "response.failed"} {
				t.Run(fmt.Sprintf("%s/%v/%s", endpoint, stream, terminalType), func(t *testing.T) {
					events := []string{
						`{"type":"response.created","response":{"id":"resp_failed"}}`,
						fmt.Sprintf(`{"type":%q,"response":{"id":"resp_failed","status":"failed","status_code":503,"output":[],"error":{"code":"server_is_overloaded","message":"synthetic overload"}}}`, terminalType),
					}
					handler, _ := newChatStreamTerminalTestHandler(t, events)
					var recorder *httptest.ResponseRecorder
					if endpoint == "chat" {
						recorder = invokeChatCompletionsWithBody(t, handler, fmt.Sprintf(`{"model":"gpt-5.5","stream":%v,"messages":[{"role":"user","content":"hello"}]}`, stream))
					} else {
						recorder = invokeResponsesWithBody(t, handler, fmt.Sprintf(`{"model":"gpt-5.5","stream":%v,"input":"hello"}`, stream))
					}
					if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "synthetic overload") {
						t.Fatalf("status=%d body=%s; want explicit 503 overload error", recorder.Code, recorder.Body.String())
					}
					if strings.Contains(recorder.Body.String(), "[DONE]") || strings.Contains(recorder.Body.String(), `"choices"`) {
						t.Fatalf("failed attempt emitted success: %s", recorder.Body.String())
					}
				})
			}
		}
	}
}

func TestResponseFailureNumericStatusAliases(t *testing.T) {
	for _, fields := range []string{
		`"status":503`,
		`"status":"503"`,
		`"response":{"status":503}`,
		`"response":{"error":{"status":503}}`,
		`"response":{"status_details":{"error":{"status":503}}}`,
		`"error":{"status":503}`,
	} {
		t.Run(fields, func(t *testing.T) {
			payload := []byte(`{"type":"response.failed",` + fields + `}`)
			if got := responseFailedStatusCode(payload); got != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503 for %s", got, payload)
			}
		})
	}
}

// Exercise real WS framing through an OpenAI Responses relay, including a clean
// socket close before any terminal. No production credentials or upstreams.
func TestWebsocketUpstreamChatFailureWireStatus(t *testing.T) {
	isolateTerminalFailureRuntime(t)
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, frame string
		want        int
	}{
		{"completed_overload", `{"type":"response.completed","response":{"status":"failed","status_code":503,"output":[],"error":{"code":"server_is_overloaded","message":"synthetic overload"}}}`, 503},
		{"done_overload", `{"type":"response.done","response":{"status":"failed","status_code":503,"output":[],"error":{"code":"server_is_overloaded","message":"synthetic overload"}}}`, 503},
		{"error_numeric_status", `{"type":"error","status":503,"error":{"message":"synthetic overload"}}`, 503},
		{"handshake_503", "", 503},
		{"premature_close", "", 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetOpenAIResponsesWebsocketPool()
			t.Cleanup(resetOpenAIResponsesWebsocketPool)
			upgrader := websocket.Upgrader{}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.name == "handshake_503" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `{"error":{"code":"server_is_overloaded","message":"synthetic overload"}}`)
					return
				}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Errorf("upgrade: %v", err)
					return
				}
				defer conn.Close()
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
					if tc.frame == "" {
						_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "fixture complete"), time.Now().Add(time.Second))
						return
					}
					if err := conn.WriteMessage(websocket.TextMessage, []byte(tc.frame)); err != nil {
						return
					}
				}
			}))
			t.Cleanup(upstream.Close)
			store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1, TestConcurrency: 1, TestModel: "gpt-5.5", MaxRetries: 0})
			t.Cleanup(store.Stop)
			store.AddAccount(&auth.Account{DBID: 1, UpstreamType: auth.UpstreamOpenAIResponses, BaseURL: upstream.URL, APIKey: "synthetic-key", Models: []string{"gpt-5.5"}, ResponsesUpstreamTransport: auth.OpenAIResponsesTransportWebsocket})
			handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-5.5","stream":false,"messages":[{"role":"user","content":"hello"}]}`)).WithContext(ctx)
			handler.ChatCompletions(c)
			if recorder.Code != tc.want || !gjson.GetBytes(recorder.Body.Bytes(), "error").Exists() {
				t.Fatalf("status=%d body=%s, want %d with error", recorder.Code, recorder.Body.String(), tc.want)
			}
		})
	}
}

func TestNativeWebsocketTerminalFailureVisible(t *testing.T) {
	isolateTerminalFailureRuntime(t)
	for _, partial := range []bool{false, true} {
		for _, terminalType := range []string{"response.completed", "response.done"} {
			t.Run(fmt.Sprintf("%v/%s", partial, terminalType), func(t *testing.T) {
				fixture := newNativeWSFixture(t, func(context.Context, []byte) (*http.Response, error) {
					wire := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bad\"}}\n\n"
					if partial {
						wire += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
					}
					wire += fmt.Sprintf("data: {\"type\":%q,\"response\":{\"id\":\"resp_bad\",\"status\":\"failed\",\"status_code\":503,\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"synthetic overload\"}}}\n\n", terminalType)
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(wire))}, nil
				})
				terminal := nativeWSTurn(t, fixture.connect(t), `{"input":"hello"}`)
				if gjson.GetBytes(terminal, "type").String() != "response.failed" || gjson.GetBytes(terminal, "response.status_code").Int() != 503 {
					t.Fatalf("WS failure was not visible: %s", terminal)
				}
			})
		}
	}
}

func TestHTTPCommittedTerminalFailureHasNoSuccessSentinel(t *testing.T) {
	isolateTerminalFailureRuntime(t)
	for _, endpoint := range []string{"responses", "chat"} {
		t.Run(endpoint, func(t *testing.T) {
			handler, _ := newChatStreamTerminalTestHandler(t, []string{
				`{"type":"response.output_text.delta","delta":"partial"}`,
				`{"type":"response.completed","response":{"status":"failed","status_code":503,"error":{"code":"server_is_overloaded","message":"synthetic overload"}}}`,
			})
			var recorder *httptest.ResponseRecorder
			if endpoint == "chat" {
				recorder = invokeChatCompletionsWithBody(t, handler, `{"model":"gpt-5.5","stream":true,"messages":[{"role":"user","content":"hello"}]}`)
			} else {
				recorder = invokeResponsesWithBody(t, handler, `{"model":"gpt-5.5","stream":true,"input":"hello"}`)
			}
			body := recorder.Body.String()
			if recorder.Code != http.StatusOK || !strings.Contains(body, "partial") || !strings.Contains(body, "synthetic overload") || !strings.Contains(body, `"error"`) {
				t.Fatalf("already-committed stream must carry its error: status=%d body=%s", recorder.Code, body)
			}
			if strings.Contains(body, "[DONE]") || strings.Contains(body, `"finish_reason":"stop"`) || strings.Contains(body, `"type":"response.completed"`) {
				t.Fatalf("failed stream emitted a success terminal: %s", body)
			}
		})
	}
}

func TestInvalidResponsesTerminalCannotReturn200(t *testing.T) {
	isolateTerminalFailureRuntime(t)
	for _, kind := range []string{"response.done", "response.completed"} {
		t.Run(kind, func(t *testing.T) {
			handler, _ := newChatStreamTerminalTestHandler(t, []string{
				`{"type":"` + kind + `","response":{"status":"failed","error":{"code":"server_is_overloaded"}}} INVALID-SYNTHETIC-TRAILER`,
			})
			recorder := invokeChatCompletionsWithBody(t, handler, `{"model":"gpt-5.5","stream":false,"messages":[{"role":"user","content":"hello"}]}`)
			if recorder.Code != http.StatusBadGateway || !gjson.GetBytes(recorder.Body.Bytes(), "error").Exists() || strings.Contains(recorder.Body.String(), "INVALID-SYNTHETIC-TRAILER") {
				t.Fatalf("malformed terminal returned status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestReadSSETerminalFailureNormalization(t *testing.T) {
	for _, tc := range []struct {
		name, wire, wantEvent, wantType string
	}{
		{"named_without_type", "event: response.completed\ndata: {\"response\":{\"status\":\"failed\",\"status_code\":503}}\n\n", "response.failed", "response.failed"},
		{"data_only", "data: {\"type\":\"response.done\",\"response\":{\"status\":\"failed\"}}\n\n", "", "response.failed"},
		{"malformed_named_terminal", "event: response.completed\ndata: {\"response\":{}\n\n", "response.failed", "response.failed"},
		{"malformed_completed", "data: {\"type\":\"response.completed\"} trailing-garbage\n\n", "", "response.failed"},
		{"incomplete", "event: response.done\ndata: {\"response\":{\"status\":\"incomplete\"}}\n\n", "response.incomplete", "response.incomplete"},
		{"explicit_error_wins", "event: error\ndata: {\"type\":\"response.done\",\"response\":{\"status\":\"completed\"}}\n\n", "error", "response.done"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := ReadSSEStreamWithEvent(strings.NewReader(tc.wire), func(event string, data []byte) bool {
				calls++
				if event != tc.wantEvent || gjson.GetBytes(data, "type").String() != tc.wantType {
					t.Errorf("event=%q data=%s, want %q/%q", event, data, tc.wantEvent, tc.wantType)
				}
				return true
			})
			if err != nil && err != io.EOF || calls != 1 {
				t.Fatalf("read err=%v calls=%d", err, calls)
			}
		})
	}
}
