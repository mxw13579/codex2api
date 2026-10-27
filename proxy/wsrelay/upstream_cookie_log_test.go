package wsrelay

import (
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

func websocketCookieLogSetup(t *testing.T) context.Context {
	t.Helper()
	previous := proxy.CurrentRuntimeSettings()
	t.Cleanup(func() { proxy.ApplyRuntimeSettings(previous) })
	proxy.UpdateRuntimeSettings(func(s proxy.RuntimeSettings) proxy.RuntimeSettings {
		s.CodexUpstreamCookieLogEnabled = true
		return s
	})
	return proxy.WithUpstreamCookieLog(context.Background())
}
func websocketCookieToken() string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(`{"host":"chat.gateway.unified-183.api.openai.com","iat":123,"exp":456}`)) + ".signature"
}
func websocketCookieEntries(t *testing.T, ctx context.Context) []gjson.Result {
	t.Helper()
	raw := proxy.SnapshotUpstreamCookieLog(ctx)
	if !gjson.Valid(raw) {
		t.Fatal("missing or invalid usage diagnostics")
	}
	return gjson.Get(raw, "entries").Array()
}

func TestUpstreamCookieFailedWebsocketHandshake(t *testing.T) {
	for _, status := range []int{403, 429} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			baseCtx := websocketCookieLogSetup(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add("Set-Cookie", "__oailb="+websocketCookieToken()+"; Path=/; HttpOnly")
				w.Header().Add("Set-Cookie", "__cflb=load-balancer; Secure")
				w.Header().Set("Cf-Ray", "failed-handshake-ray")
				w.WriteHeader(status)
				io.WriteString(w, `{"error":{"code":"rate_limit_error"}}`)
			}))
			defer server.Close()
			m := NewManager()
			defer m.Stop()
			ctx, cancel := context.WithTimeout(proxy.WithAccountBoundEgress(baseCtx, ""), time.Second*3)
			defer cancel()
			_, _, err := m.AcquireConnection(ctx, &auth.Account{DBID: 4288}, "ws"+strings.TrimPrefix(server.URL, "http"), "handshake-test", http.Header{"Cookie": {"__cflb=outbound"}, "Authorization": {"Bearer do-not-log-token"}}, "")
			if err == nil {
				t.Fatal("expected handshake failure")
			}
			es := websocketCookieEntries(t, ctx)
			if len(es) != 1 {
				t.Fatalf("entries=%d", len(es))
			}
			e := es[0]
			if e.Get("status").Int() != int64(status) || e.Get("unified_host").String() != "chat.gateway.unified-183.api.openai.com" || e.Get("connection_id").String() == "" || e.Get("cookie.0").String() != "__cflb=outbound" || len(e.Get("set_cookie").Array()) != 2 {
				t.Fatalf("handshake capture: %s", e.Raw)
			}
			if strings.Contains(e.Raw, "do-not-log-token") {
				t.Fatal("authorization logged")
			}
		})
	}
}

func TestUpstreamCookieReusedWebsocketLogsSuccessAnd429(t *testing.T) {
	websocketCookieLogSetup(t)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		if r.Header.Get("Cookie") != "__cflb=original-request" {
			t.Error("unexpected sent cookie")
		}
		headers := http.Header{"Set-Cookie": {"__oailb=" + websocketCookieToken() + "; Path=/; HttpOnly", "__cf_bm=bot-cookie; Secure"}, "Cf-Ray": {"handshake-ray"}, "X-Request-Id": {"handshake-id"}}
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, headers)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		for i := 0; i < 2; i++ {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
			if i == 0 {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"codex.response.metadata","headers":{"cf-ray":"first-turn-ray","x-request-id":"first-turn-id"}}`))
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"resp-success","status":"completed"}}`))
			} else {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status_code":429,"error":{"code":"turn_rate_limit_exceeded","type":"rate_limit_error"}}`))
			}
		}
		// Hold the connection open until the manager closes it, so the client can
		// consume the terminal frame before the read pump observes socket EOF.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	m := NewManager()
	defer m.Stop()
	m.dialer.NetDialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(server.URL, "http://"))
	}
	m.probeFunc = func(*WsConnection) bool { return true }
	e := NewExecutorWithManager(m)
	account := &auth.Account{DBID: 4288, AccessToken: "do-not-log-token", CustomHeaders: map[string]string{"Cookie": "__cflb=original-request"}}
	ctx, cancel := context.WithTimeout(proxy.WithAccountBoundEgress(context.Background(), ""), 5*time.Second)
	defer cancel()
	var firstConnection *WsConnection
	var es []gjson.Result
	for i := 0; i < 2; i++ {
		if i == 1 {
			account.CustomHeaders = map[string]string{"Cookie": "__cflb=not-sent-on-reuse"}
		}
		turnCtx := proxy.WithUpstreamCookieLog(ctx)
		response, err := e.ExecuteRequestViaWebsocket(turnCtx, account, []byte(`{"model":"gpt-6.1-sol","input":"synthetic test"}`), "cookie-test-session", "", "", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstConnection = response.conn
		} else if response.conn != firstConnection {
			t.Fatal("expected connection reuse")
		}
		err = response.ReadStream(func(data []byte) bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		if err = response.Close(); err != nil {
			t.Fatal(err)
		}
		entries := websocketCookieEntries(t, turnCtx)
		want := 2
		if i == 0 {
			want = 3
		}
		if len(entries) != want {
			t.Fatalf("turn %d entries=%d want=%d", i, len(entries), want)
		}
		// Compare handshake and terminal events; start events have no outcome.
		for _, entry := range entries {
			if entry.Get("event").String() != "websocket_turn" {
				es = append(es, entry)
			}
		}
	}
	if connections.Load() != 1 {
		t.Fatalf("connections=%d", connections.Load())
	}
	if len(es) != 3 {
		t.Fatalf("entries=%d", len(es))
	}
	if es[1].Get("status").Int() != 200 || es[2].Get("status").Int() != 429 || es[2].Get("error_code").String() != "turn_rate_limit_exceeded" {
		t.Fatalf("outcomes: %v", es)
	}
	for _, entry := range es {
		if entry.Get("connection_id").String() != es[0].Get("connection_id").String() || entry.Get("unified_host").String() != "chat.gateway.unified-183.api.openai.com" || entry.Get("cookie.0").String() != "__cflb=original-request" {
			t.Fatalf("reused snapshot lost: %s", entry.Raw)
		}
	}
	if es[1].Get("attempt_id").String() == es[2].Get("attempt_id").String() || es[1].Get("cf_ray").String() != "first-turn-ray" || es[2].Get("cf_ray").Exists() || es[2].Get("upstream_request_id").Exists() {
		t.Fatal("turn metadata mixed with handshake or previous turn")
	}
}
