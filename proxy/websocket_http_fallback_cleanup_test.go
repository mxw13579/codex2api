package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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

type fallbackCleanupRoute struct {
	name, path, body string
	nativeWS         bool
	invoke           func(*Handler, *gin.Context, *websocket.Conn) error
}

func fallbackCleanupRoutes() []fallbackCleanupRoute {
	return []fallbackCleanupRoute{
		{
			name: "responses", path: "/v1/responses",
			body: `{"model":"gpt-5.5","input":"hello"}`,
			invoke: func(h *Handler, c *gin.Context, _ *websocket.Conn) error {
				h.Responses(c)
				return nil
			},
		},
		{
			name: "chat", path: "/v1/chat/completions",
			body: `{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}]}`,
			invoke: func(h *Handler, c *gin.Context, _ *websocket.Conn) error {
				h.ChatCompletions(c)
				return nil
			},
		},
		{
			name: "messages", path: "/v1/messages",
			body: `{"model":"claude-opus-4-6","max_tokens":128,"messages":[{"role":"user","content":"hello"}]}`,
			invoke: func(h *Handler, c *gin.Context, _ *websocket.Conn) error {
				h.Messages(c)
				return nil
			},
		},
		{
			name: "native_ws", path: "/v1/responses", nativeWS: true,
			body: `{"type":"response.create","model":"gpt-5.5","input":[{"role":"user","content":"hello"}]}`,
			invoke: func(h *Handler, c *gin.Context, conn *websocket.Conn) error {
				return h.forwardResponsesWebSocketTurn(c, conn, []byte(`{"type":"response.create","model":"gpt-5.5","input":[{"role":"user","content":"hello"}]}`), "", nil)
			},
		},
	}
}

func (route fallbackCleanupRoute) request(ctx context.Context) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	method := http.MethodPost
	if route.nativeWS {
		method = http.MethodGet
	}
	c.Request = httptest.NewRequest(method, route.path, strings.NewReader(route.body)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")
	return c, recorder
}

// All credentials and routing here are synthetic. HTTP uses an in-memory
// RoundTripper; only the successful native-WS downstream uses a loopback socket.
// These tests replace process globals and must not run in parallel.
func newFallbackCleanupHandler(t *testing.T) (*Handler, *auth.Account) {
	t.Helper()
	previousExec := WebsocketExecuteFunc
	previousSettings := CurrentRuntimeSettings()
	previousResin := resinCfg.Load()
	previousResinEnabled := auth.ResinEgressEnabled()
	previousLog := log.Writer()
	previousGinMode := gin.Mode()
	t.Cleanup(func() {
		WebsocketExecuteFunc = previousExec
		ApplyRuntimeSettings(previousSettings)
		resinCfg.Store(previousResin)
		auth.SetResinEgressEnabled(previousResinEnabled)
		log.SetOutput(previousLog)
		gin.SetMode(previousGinMode)
	})
	gin.SetMode(gin.TestMode)
	log.SetOutput(io.Discard)
	settings := DefaultRuntimeSettings()
	settings.CodexForceWebsocket = true
	settings.CodexWSSizeRouter = false
	settings.CodexWSSilentRetry = false
	settings.CodexTelemetryEnabled = false
	settings.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	ApplyRuntimeSettings(settings)
	SetResinConfig(&ResinConfig{BaseURL: "http://127.0.0.1:1", PlatformName: "lease-cleanup-fixture"})

	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1, TestConcurrency: 1, TestModel: "gpt-5.5"})
	t.Cleanup(store.Stop)
	store.SetMaxRetries(0)
	account := &auth.Account{DBID: 940001, AccessToken: "synthetic-token", PlanType: "pro", AccountID: "synthetic-account"}
	// A fresh scheduler acquisition after the WS attempt must not be possible.
	account.SetDispatchCountLimit(1)
	store.AddAccount(account)
	return NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil), account
}

type fallbackCleanupRoundTripper func(*http.Request) (*http.Response, error)

func (f fallbackCleanupRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func mockFallbackCleanupHTTP(t *testing.T, account *auth.Account, execute fallbackCleanupRoundTripper) {
	t.Helper()
	key := fmt.Sprintf("resin|%d", account.ID())
	entry := &poolEntry{client: &http.Client{Transport: execute}}
	entry.touch()
	previous, loaded := clientPool.Swap(key, entry)
	t.Cleanup(func() {
		if loaded {
			clientPool.Store(key, previous)
		} else {
			clientPool.Delete(key)
		}
	})
}

type fallbackCleanupErrorBody struct{ onClose func() }

func (fallbackCleanupErrorBody) Read([]byte) (int, error) {
	return 0, errors.New("websocket read error: websocket: close 1009 (message too big)")
}
func (b fallbackCleanupErrorBody) Close() error {
	if b.onClose != nil {
		b.onClose()
	}
	return nil
}

func fallbackCleanupWSFailure(kind string) (*http.Response, error) {
	switch kind {
	case "handshake_403":
		// Use the real dial-error spelling, not a synthetic HTTP-status wrapper.
		return nil, errors.New("websocket handshake failed: websocket: bad handshake (HTTP 403 Forbidden)")
	case "request_1009":
		return nil, errors.New("websocket send error: websocket: close 1009 (message too big)")
	default: // stream_1009: the stream helper must hand the acquired lease back.
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: fallbackCleanupErrorBody{}}, nil
	}
}

type cancelAfterFallbackRetainWriter struct {
	cancel   context.CancelFunc
	account  *auth.Account
	hits     atomic.Int64
	active   atomic.Int64
	occupied atomic.Int64
}

func (w *cancelAfterFallbackRetainWriter) Write(p []byte) (int, error) {
	line := string(p)
	if strings.Contains(line, "fallback_id=") &&
		(strings.Contains(line, "保留账号租约并降级 HTTP") || strings.Contains(line, "retaining account lease and falling back to HTTP")) {
		if w.hits.Add(1) == 1 {
			w.active.Store(w.account.GetActiveRequests())
			w.occupied.Store(w.account.OccupiedRequests.Load())
			// Retain has completed, but continue and the next Take have not.
			w.cancel()
		}
	}
	return len(p), nil
}

func TestWebsocketHTTPFallbackCancellationReleasesRetainedLease(t *testing.T) {
	for _, route := range fallbackCleanupRoutes() {
		for _, failure := range []string{"handshake_403", "request_1009", "stream_1009"} {
			t.Run(route.name+"/"+failure, func(t *testing.T) {
				handler, account := newFallbackCleanupHandler(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				writer := &cancelAfterFallbackRetainWriter{cancel: cancel, account: account}
				log.SetOutput(writer)

				var wsCalls, httpCalls atomic.Int64
				WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
					wsCalls.Add(1)
					return fallbackCleanupWSFailure(failure)
				}
				mockFallbackCleanupHTTP(t, account, func(*http.Request) (*http.Response, error) {
					httpCalls.Add(1)
					return nil, ErrBadRequest("unexpected HTTP attempt after cancellation")
				})

				c, _ := route.request(ctx)
				err := route.invoke(handler, c, nil)
				if writer.hits.Load() != 1 || !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("Retain cancellation window not reached: hits=%d canceled=%v", writer.hits.Load(), ctx.Err() != nil)
				}
				if writer.active.Load() != 1 || writer.occupied.Load() != 1 {
					t.Fatalf("lease at Retain: active=%d occupied=%d, want 1 each", writer.active.Load(), writer.occupied.Load())
				}
				if route.nativeWS && !errors.Is(err, errResponsesWSClientGone) {
					t.Fatalf("native WS exit = %v, want client gone", err)
				}
				if wsCalls.Load() != 1 || httpCalls.Load() != 0 || account.TotalRequests.Load() != 1 {
					t.Errorf("dispatches: WS=%d HTTP=%d logical=%d, want 1/0/1", wsCalls.Load(), httpCalls.Load(), account.TotalRequests.Load())
				}
				if active, occupied := account.GetActiveRequests(), account.OccupiedRequests.Load(); active != 0 || occupied != 0 {
					t.Errorf("lease leaked after cancellation: active=%d occupied=%d", active, occupied)
				}
			})
		}
	}
}

func TestWebsocketHTTPFallbackStreamHandoffDeadlineReleasesLease(t *testing.T) {
	handler, account := newFallbackCleanupHandler(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var wsCalls, httpCalls, closes atomic.Int64
	WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
		wsCalls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: fallbackCleanupErrorBody{onClose: func() {
				closes.Add(1)
				// The stream helper has decided to hand the live 1009 lease to
				// the caller. Expire the deadline before that caller can Retain.
				cancel(errContinuousRetryDeadlineExceeded)
			}},
		}, nil
	}
	mockFallbackCleanupHTTP(t, account, func(*http.Request) (*http.Response, error) {
		httpCalls.Add(1)
		return nil, ErrBadRequest("unexpected HTTP attempt after deadline")
	})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil).WithContext(ctx)
	err := handler.forwardResponsesWebSocketTurn(c, nil, []byte(`{"type":"response.create","model":"gpt-5.5","input":[{"role":"user","content":"hello"}]}`), "", nil)
	if closes.Load() == 0 || !continuousRetryDeadlineExceeded(ctx) || !errors.Is(err, errResponsesWSClientGone) {
		t.Fatalf("stream handoff window not reached: closes=%d deadline=%v err=%v", closes.Load(), continuousRetryDeadlineExceeded(ctx), err)
	}
	if wsCalls.Load() != 1 || httpCalls.Load() != 0 || account.TotalRequests.Load() != 1 {
		t.Errorf("dispatches: WS=%d HTTP=%d logical=%d, want 1/0/1", wsCalls.Load(), httpCalls.Load(), account.TotalRequests.Load())
	}
	if active, occupied := account.GetActiveRequests(), account.OccupiedRequests.Load(); active != 0 || occupied != 0 {
		t.Errorf("stream handoff leaked lease: active=%d occupied=%d", active, occupied)
	}
}

func fallbackCleanupSuccessResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader(
			`data: {"type":"response.created","response":{"id":"resp_lease_cleanup"}}` + "\n\n" +
				`data: {"type":"response.output_item.added","item":{"type":"message"}}` + "\n\n" +
				`data: {"type":"response.output_text.delta","delta":"fallback-completed"}` + "\n\n" +
				`data: {"type":"response.output_text.done"}` + "\n\n" +
				`data: {"type":"response.completed","response":{"id":"resp_lease_cleanup","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fallback-completed"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n")),
	}
}

// Only downstream writes need a real WS connection; there is no upstream dial.
func fallbackCleanupDownstream(t *testing.T) (serverConn, clientConn *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			accepted <- conn
		}
	}))
	t.Cleanup(server.Close)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second} // no environment proxy
	clientConn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientConn.Close() })
	serverConn = <-accepted
	t.Cleanup(func() { _ = serverConn.Close() })
	return serverConn, clientConn
}

func TestWebsocketHTTPFallbackTakenLeaseReleasedExactlyOnce(t *testing.T) {
	for _, route := range fallbackCleanupRoutes() {
		for _, failure := range []string{"handshake_403", "request_1009", "stream_1009"} {
			for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
				t.Run(fmt.Sprintf("%s/%s/http_%d", route.name, failure, status), func(t *testing.T) {
					handler, account := newFallbackCleanupHandler(t)
					handler.store.SetMaxConcurrency(2) // room for the other synthetic lease
					var wsCalls, httpCalls atomic.Int64
					var otherLease atomic.Bool
					t.Cleanup(func() {
						if otherLease.Load() {
							handler.store.Release(account)
						}
					})
					WebsocketExecuteFunc = func(_ context.Context, selected *auth.Account, _ []byte, _, _, _ string, _ *DeviceProfileConfig, _ http.Header, _ string) (*http.Response, error) {
						wsCalls.Add(1)
						if selected != account {
							t.Error("WS attempt selected another account")
						}
						return fallbackCleanupWSFailure(failure)
					}
					mockFallbackCleanupHTTP(t, account, func(r *http.Request) (*http.Response, error) {
						if httpCalls.Add(1) != 1 {
							return nil, ErrBadRequest("unexpected additional HTTP attempt")
						}
						if r.URL.Host != "127.0.0.1:1" || r.Header.Get("X-Resin-Account") != fmt.Sprint(account.ID()) {
							t.Error("HTTP fallback did not keep the synthetic account/egress")
						}
						if account.GetActiveRequests() != 1 || account.OccupiedRequests.Load() != 1 {
							t.Error("HTTP fallback did not inherit exactly one acquired lease")
						}
						// Model another request holding a slot on the same account. The
						// store floors counters at zero, so zero alone cannot detect a
						// double release: deferred cleanup must leave this slot intact.
						account.ActiveRequests.Add(1)
						account.OccupiedRequests.Add(1)
						otherLease.Store(true)
						if status == http.StatusOK {
							return fallbackCleanupSuccessResponse(), nil
						}
						return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"message":"synthetic terminal failure","type":"invalid_request_error"}}`))}, nil
					})

					var downstream, client *websocket.Conn
					if route.nativeWS && status == http.StatusOK {
						downstream, client = fallbackCleanupDownstream(t)
					}
					c, recorder := route.request(context.Background())
					err := route.invoke(handler, c, downstream)
					if route.nativeWS {
						if status == http.StatusOK {
							if err != nil {
								t.Fatalf("native fallback success returned %v", err)
							}
							_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
							for {
								_, payload, readErr := client.ReadMessage()
								if readErr != nil {
									t.Fatal(readErr)
								}
								if strings.Contains(string(payload), `"type":"response.completed"`) {
									break
								}
							}
						} else {
							var closeErr *responsesWSCloseError
							if !errors.As(err, &closeErr) {
								t.Fatalf("native terminal failure returned %v, want close error", err)
							}
						}
					} else if recorder.Code != status || (status == http.StatusOK && !strings.Contains(recorder.Body.String(), "fallback-completed")) {
						t.Errorf("HTTP fallback status=%d, want %d with expected result", recorder.Code, status)
					}
					if wsCalls.Load() != 1 || httpCalls.Load() != 1 || account.TotalRequests.Load() != 1 {
						t.Errorf("dispatches: WS=%d HTTP=%d logical=%d, want 1/1/1", wsCalls.Load(), httpCalls.Load(), account.TotalRequests.Load())
					}
					if active, occupied := account.GetActiveRequests(), account.OccupiedRequests.Load(); active != 1 || occupied != 1 {
						t.Errorf("fallback leaked or double-released: active=%d occupied=%d, want only the other lease", active, occupied)
					}
					handler.store.Release(account)
					otherLease.Store(false)
					if account.GetActiveRequests() != 0 || account.OccupiedRequests.Load() != 0 {
						t.Error("account still occupied after releasing the other lease")
					}
				})
			}
		}
	}
}
