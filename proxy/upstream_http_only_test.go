package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

func TestUpstreamHTTPOnlyContext(t *testing.T) {
	if isUpstreamHTTPOnly(nil) || isUpstreamHTTPOnly(context.Background()) || isUpstreamHTTPOnly(WithCodexDetectorRequest(nil)) {
		t.Fatal("ordinary and detector contexts must not implicitly force HTTP")
	}
	if !isUpstreamHTTPOnly(WithUpstreamHTTPOnly(nil)) {
		t.Fatal("nil parent must be supported")
	}
	type testKey struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), testKey{}, "kept"), time.Minute)
	defer cancel()
	ctx := WithUpstreamHTTPOnly(parent)
	if isUpstreamHTTPOnly(parent) || isCodexDetectorRequest(ctx) {
		t.Fatal("HTTP-only must neither mutate the parent nor imply a detector probe")
	}
	ctx = WithUpstreamHTTPOnly(WithAccountBoundEgress(WithCodexDetectorRequest(ctx), "http://127.0.0.1:12345"))
	if !isUpstreamHTTPOnly(ctx) || !isCodexDetectorRequest(ctx) || ctx.Value(testKey{}) != "kept" {
		t.Fatal("wrapping the context lost request state")
	}
	if route, ok := AccountBoundEgress(ctx); !ok || route != "http://127.0.0.1:12345" {
		t.Fatal("wrapping the context lost the frozen egress route")
	}
	deadline, _ := parent.Deadline()
	if got, ok := ctx.Deadline(); !ok || got != deadline || ctx.Done() != parent.Done() {
		t.Fatal("HTTP-only changed the parent deadline or cancellation channel")
	}
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("cancellation lost: %v", ctx.Err())
	}
}

func TestUpstreamHTTPOnlyExecuteRequestPrecedence(t *testing.T) {
	previous, previousWS := CurrentRuntimeSettings(), WebsocketExecuteFunc
	t.Cleanup(func() {
		ApplyRuntimeSettings(previous)
		WebsocketExecuteFunc = previousWS
	})
	// No credential: the HTTP path stops before creating a client. The WS path
	// is a stub, so this complete precedence matrix cannot make network calls.
	account := &auth.Account{DBID: 998141}
	wsCalls := 0
	WebsocketExecuteFunc = func(_ context.Context, got *auth.Account, _ []byte, _, _, _ string, _ *DeviceProfileConfig, _ http.Header, _ string) (*http.Response, error) {
		wsCalls++
		if got != account {
			t.Error("executor replaced the account")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	for _, forceWS := range []bool{false, true} {
		settings := previous
		settings.CodexForceWebsocket = forceWS
		settings.CodexTelemetryEnabled = false
		ApplyRuntimeSettings(settings)
		for _, override := range []struct {
			name  string
			value []bool
		}{{"default", nil}, {"explicit_http", []bool{false}}, {"explicit_ws", []bool{true}}} {
			for _, marker := range []string{"ordinary", "detector_only", "http_only"} {
				t.Run(fmt.Sprintf("global_ws_%t/%s/%s", forceWS, override.name, marker), func(t *testing.T) {
					ctx := context.Background()
					switch marker {
					case "detector_only":
						ctx = WithCodexDetectorRequest(ctx)
					case "http_only":
						ctx = WithUpstreamHTTPOnly(ctx)
					}
					wantWS := forceWS
					if len(override.value) != 0 {
						wantWS = override.value[0]
					}
					wantWS = wantWS && marker != "http_only"
					before := wsCalls
					resp, err := ExecuteRequest(ctx, account, []byte(`{"model":"gpt-5.4","input":"fixture"}`), "", "", "", nil, nil, override.value...)
					if resp != nil {
						defer resp.Body.Close()
					}
					if wantWS {
						if err != nil || resp == nil || wsCalls != before+1 {
							t.Fatalf("unmarked WS selection changed: calls=%d err=%v", wsCalls-before, err)
						}
					} else {
						var upstreamErr *Error
						if wsCalls != before || resp != nil || !errors.As(err, &upstreamErr) || upstreamErr.Code != ErrorCodeNoAvailableAccount {
							t.Fatalf("expected HTTP credential gate without a WS attempt: calls=%d err=%v", wsCalls-before, err)
						}
					}
					if CurrentRuntimeSettings().CodexForceWebsocket != forceWS {
						t.Fatal("request changed global transport settings")
					}
				})
			}
		}
	}
}

func TestUpstreamHTTPOnlyRelayLeavesOtherRequestsOnWebsocket(t *testing.T) {
	t.Setenv("CODEX_TRANSPORT_MODE", "standard")
	oldPool := openAIResponsesWebsocketPool
	openAIResponsesWebsocketPool = newOpenAIResponsesWSPool()
	t.Cleanup(func() {
		resetOpenAIResponsesWebsocketPool()
		openAIResponsesWebsocketPool = oldPool
	})
	const terminal = `{"type":"response.completed","response":{"id":"local-http-only","status":"completed"}}`
	var httpCalls, wsCalls, handshakes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer local-relay-only" {
			t.Error("relay target or identity changed")
		}
		if websocket.IsWebSocketUpgrade(r) {
			handshakes.Add(1)
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
				wsCalls.Add(1)
				if err := conn.WriteMessage(websocket.TextMessage, []byte(terminal)); err != nil {
					return
				}
			}
		}
		httpCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || !gjson.GetBytes(body, "stream").Bool() {
			t.Error("HTTP-only must retain a streaming POST")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", terminal)
	}))
	defer server.Close()
	account := &auth.Account{
		DBID: 998142, UpstreamType: auth.UpstreamOpenAIResponses,
		BaseURL: server.URL, APIKey: "local-relay-only",
		ResponsesUpstreamTransport: auth.OpenAIResponsesTransportWebsocket,
	}
	parent, cancel := context.WithTimeout(WithAccountBoundEgress(context.Background(), ""), 5*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		ws   bool
	}{
		{"ordinary_before", parent, true},
		{"http_only_with_pooled_ws", WithUpstreamHTTPOnly(parent), false},
		{"detector_only", WithCodexDetectorRequest(parent), true},
		{"http_only_detector", WithUpstreamHTTPOnly(WithCodexDetectorRequest(parent)), false},
		{"ordinary_after", parent, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeHTTP, beforeWS, beforeHandshakes := httpCalls.Load(), wsCalls.Load(), handshakes.Load()
			resp, err := ExecuteRelayStyleRequest(tc.ctx, account, []byte(`{"model":"gpt-5.4","input":"fixture","stream":true}`), "", nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") || !strings.Contains(string(body), "response.completed") {
				t.Fatalf("stream failed: status=%d err=%v", resp.StatusCode, err)
			}
			if tc.ws {
				if wsCalls.Load() != beforeWS+1 || httpCalls.Load() != beforeHTTP {
					t.Fatal("unmarked request no longer uses WS")
				}
			} else if httpCalls.Load() != beforeHTTP+1 || wsCalls.Load() != beforeWS || handshakes.Load() != beforeHandshakes {
				t.Fatal("HTTP-only request attempted a WS handshake or reused a WS connection")
			}
			if !account.OpenAIResponsesUsesUpstreamWebsocket() {
				t.Fatal("request changed the account transport setting")
			}
		})
	}
	if handshakes.Load() != 1 {
		t.Fatalf("ordinary WS requests should still reuse the connection: handshakes=%d", handshakes.Load())
	}
}
