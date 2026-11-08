package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestFastTierRequiresHTTP(t *testing.T) {
	for tier, want := range map[string]bool{
		"priority": true, "fast": true, "ultrafast": true, " Priority ": true, "ULTRAFAST": true,
		"": false, "default": false, "auto": false, "flex": false, "scale": false,
	} {
		if got := fastTierRequiresHTTP(tier); got != want {
			t.Errorf("fastTierRequiresHTTP(%q) = %v, want %v", tier, got, want)
		}
	}
}

const fastTierCompletedSSE = "event: response.completed\n" +
	`data: {"type":"response.completed","response":{"id":"resp_fast_tier","object":"response","status":"completed","model":"gpt-5.6-sol",` +
	`"output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok","annotations":[]}]}],` +
	`"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}` + "\n\n"

// 全局强制上游 WS 时，加速档请求必须走 HTTP 且携带与自身一致的 routing hint；
// 默认档请求仍走 WS。
func TestFastTierRequestsBypassUpstreamWebsocket(t *testing.T) {
	type upstreamCall struct{ transport, tier, hint string }
	var mu sync.Mutex
	var calls []upstreamCall
	record := func(c upstreamCall) { mu.Lock(); calls = append(calls, c); mu.Unlock() }

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readUpstreamRequestBody(r)
		record(upstreamCall{"http", gjson.GetBytes(body, "service_tier").String(), r.Header.Get("X-Codex-Routing-Hint")})
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, fastTierCompletedSSE)
	}))
	t.Cleanup(server.Close)

	previousRuntime, previousWS, previousResin := CurrentRuntimeSettings(), WebsocketExecuteFunc, resinCfg.Load()
	t.Cleanup(func() {
		ApplyRuntimeSettings(previousRuntime)
		WebsocketExecuteFunc = previousWS
		resinCfg.Store(previousResin)
	})
	runtime := previousRuntime
	runtime.CodexForceWebsocket = true
	ApplyRuntimeSettings(runtime)
	SetResinConfig(&ResinConfig{BaseURL: server.URL, PlatformName: "test"})
	WebsocketExecuteFunc = func(_ context.Context, _ *auth.Account, body []byte, _, _, _ string, _ *DeviceProfileConfig, _ http.Header, _ string) (*http.Response, error) {
		record(upstreamCall{"ws", gjson.GetBytes(body, "service_tier").String(), ""})
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(strings.NewReader(fastTierCompletedSSE))}, nil
	}

	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 8})
	t.Cleanup(store.Stop)
	store.AddAccount(&auth.Account{DBID: 914501, AccessToken: "test-token", Status: auth.StatusReady})
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)

	endpoints := []struct {
		name, path string
		body       func(tier string) string
		serve      func(*gin.Context)
	}{
		{"responses", "/v1/responses", func(tier string) string {
			return `{"model":"gpt-5.6-sol","input":"hi","stream":true` + tier + `}`
		}, handler.Responses},
		{"chat", "/v1/chat/completions", func(tier string) string {
			return `{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"hi"}],"stream":true` + tier + `}`
		}, handler.ChatCompletions},
	}
	cases := []struct {
		tier, wantTransport, wantHint string
	}{
		{"", "ws", ""},
		{"priority", "http", "model=gpt-5.6-sol;tier=priority"},
		{"fast", "http", "model=gpt-5.6-sol;tier=priority"},
		{"ultrafast", "http", "model=gpt-5.6-sol;tier=ultrafast"},
	}
	for _, ep := range endpoints {
		for _, tc := range cases {
			t.Run(ep.name+"/"+tc.tier, func(t *testing.T) {
				mu.Lock()
				calls = nil
				mu.Unlock()
				field := ""
				if tc.tier != "" {
					field = `,"service_tier":"` + tc.tier + `"`
				}
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, ep.path, strings.NewReader(ep.body(field)))
				c.Request.Header.Set("Content-Type", "application/json")
				ep.serve(c)
				if recorder.Code != http.StatusOK {
					t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
				}
				mu.Lock()
				defer mu.Unlock()
				if len(calls) != 1 {
					t.Fatalf("upstream calls = %+v, want exactly one", calls)
				}
				got := calls[0]
				if got.transport != tc.wantTransport {
					t.Fatalf("transport = %s, want %s (%+v)", got.transport, tc.wantTransport, got)
				}
				if tc.wantTransport == "http" {
					if got.hint != tc.wantHint {
						t.Fatalf("routing hint = %q, want %q", got.hint, tc.wantHint)
					}
					if got.tier == "" {
						t.Fatalf("service_tier dropped on HTTP path: %+v", got)
					}
				}
			})
		}
	}
}
