package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func TestCodexSearchInterruptRetryToNativeRelayKeepsSequence(t *testing.T) {
	for _, streamFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "http_status", true: "pre_output_stream_failure"}[streamFailure], func(t *testing.T) {
			setupSearchContextLoopback(t)
			settings := CurrentRuntimeSettings()
			settings.CodexWSSilentRetry = true
			settings.CodexWSSilentRetries = 2
			ApplyRuntimeSettings(settings)
			resetOpenAIResponsesWebsocketPool()
			t.Cleanup(resetOpenAIResponsesWebsocketPool)
			// Verify native frames byte-for-byte, including integers above 2^53.
			frames := []string{
				`{"type":"response.created","sequence_number":9007199254740993,"response":{"id":"resp_native_relay","status":"in_progress"}}`,
				`{"type":"response.output_text.delta","sequence_number":9007199254740994,"delta":"synthetic relay output","provider_metadata":{"counter":9007199254740995}}`,
				`{"type":"response.completed","sequence_number":9007199254740995,"response":{"id":"resp_native_relay","status":"completed","output":[]}}`,
			}
			var relayCalls atomic.Int32
			upgrader := websocket.Upgrader{}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				socket, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer socket.Close()
				_ = socket.SetReadDeadline(time.Now().Add(5 * time.Second))
				if _, _, err := socket.ReadMessage(); err != nil {
					t.Error(err)
					return
				}
				relayCalls.Add(1)
				for _, frame := range frames {
					if err := socket.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
						t.Error(err)
						return
					}
				}
			}))
			t.Cleanup(upstream.Close)
			store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1, RetryIntervalMS: 1})
			t.Cleanup(store.Stop)
			store.AddAccount(&auth.Account{DBID: 1, AccessToken: "synthetic-token", PlanType: "plus"})
			relay := &auth.Account{DBID: 2, UpstreamType: auth.UpstreamOpenAIResponses, BaseURL: upstream.URL, APIKey: "synthetic-key", Models: []string{"gpt-5.5"}, ResponsesUpstreamTransport: auth.OpenAIResponsesTransportWebsocket}
			previous := WebsocketExecuteFunc
			t.Cleanup(func() { WebsocketExecuteFunc = previous })
			var codexCalls atomic.Int32
			WebsocketExecuteFunc = func(_ context.Context, _ *auth.Account, _ []byte, _, _, _ string, _ *DeviceProfileConfig, _ http.Header, _ string) (*http.Response, error) {
				if codexCalls.Add(1) != 1 {
					t.Error("retried the failed Codex fixture instead of the newly available relay")
				}
				store.AddAccount(relay)
				if streamFailure {
					return codexSearchTestResponse(searchInterruptCreated("resp_private_failed"), `{"type":"response.failed","response":{"id":"resp_private_failed","status":"failed","error":{"type":"server_error","code":"server_error","message":"synthetic failure"},"output":[]}}`), nil
				}
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"type":"server_error","message":"synthetic failure"}}`))}, nil
			}
			handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
			router := gin.New()
			handler.RegisterRoutes(router)
			server := httptest.NewServer(router)
			t.Cleanup(server.Close)
			fixture := nativeWSFixture{url: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses"}
			conn := fixture.connect(t)
			_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.5","input":[{"role":"user","content":"synthetic request"}],"tools":[{"type":"web_search"}]}`)); err != nil {
				t.Fatal(err)
			}
			for _, want := range frames {
				_, got, err := conn.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != want {
					t.Fatalf("search projection leaked into native retry:\n got %s\nwant %s", got, want)
				}
			}
			if codexCalls.Load() != 1 || relayCalls.Load() != 1 {
				t.Fatal("unexpected retry count")
			}
		})
	}
}
