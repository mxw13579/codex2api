package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

const endpointBareBadRequest = `{"detail":"Bad Request"}`

// All upstreams are loopback fixtures; credentials, account IDs and payloads
// are synthetic. Never install a real upstream WebSocket executor in these tests.
func newBadRequestEndpointFixture(t *testing.T, policy database.BadRequestRetryPolicy, catchAll bool, count int, serve func(http.ResponseWriter, *http.Request)) (*Handler, []*auth.Account) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	oldRuntime, oldResin, oldWS := CurrentRuntimeSettings(), resinCfg.Load(), WebsocketExecuteFunc
	runtime := oldRuntime
	runtime.CodexForceWebsocket = false
	runtime.CodexWSSilentRetry = false
	runtime.CodexWSSilentRetries = 0
	runtime.CodexWSHideErrors = false
	runtime.ContinuousRetryPolicy = database.ContinuousRetryPolicy{Enabled: catchAll, CatchAll: catchAll}
	ApplyRuntimeSettings(runtime)
	t.Cleanup(func() {
		ApplyRuntimeSettings(oldRuntime)
		resinCfg.Store(oldResin)
		WebsocketExecuteFunc = oldWS
	})
	WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
		t.Error("dedicated HTTP retry unexpectedly selected WebSocket")
		return nil, context.Canceled
	}
	upstream := httptest.NewServer(http.HandlerFunc(serve))
	t.Cleanup(upstream.Close)
	SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "bad-request-loopback-fixture"})
	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1, TestConcurrency: 1, TestModel: "gpt-5.5", MaxRetries: 0, MaxRateLimitRetries: 0})
	t.Cleanup(store.Stop)
	store.SetBadRequestRetryPolicy(policy)
	store.SetRetryIntervalMS(0)
	accounts := make([]*auth.Account, 0, count)
	for i := 1; i <= count; i++ {
		account := &auth.Account{
			DBID: int64(i), AccountID: fmt.Sprintf("synthetic-bad-request-%d", i),
			AccessToken: "synthetic-fixture-token", PlanType: "plus", Status: auth.StatusReady,
			SchedulerPriority: int64(count - i),
		}
		store.AddAccount(account)
		accounts = append(accounts, account)
	}
	return NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil), accounts
}

// The WS connection is joined before restoring global fixture configuration.
func dialBadRequestEndpointWS(t *testing.T, h *Handler, ctx context.Context, prepare ...func(*gin.Context)) (*websocket.Conn, func()) {
	t.Helper()
	done := make(chan struct{})
	router := gin.New()
	router.GET("/v1/responses", func(c *gin.Context) {
		defer close(done)
		c.Request = c.Request.WithContext(ctx)
		for _, setup := range prepare {
			setup(c)
		}
		h.ResponsesWebSocket(c)
	})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	dialer := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = conn.Close()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("WebSocket handler did not release its turn after disconnect")
			}
		})
	}
	t.Cleanup(stop)
	return conn, stop
}

func invokeBadRequestEndpoint(t *testing.T, h *Handler, ctx context.Context, endpoint string, prepare ...func(*gin.Context)) (int, string) {
	t.Helper()
	if endpoint == "ws" {
		conn, stop := dialBadRequestEndpointWS(t, h, ctx, prepare...)
		defer stop()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.5","input":"synthetic request","service_tier":"priority"}`)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		// Cancellation intentionally produces no terminal event.
		for i := 0; i < 16; i++ {
			_, event, err := conn.ReadMessage()
			if err != nil {
				if ctx.Err() != nil {
					return 0, ""
				}
				t.Fatal(err)
			}
			switch gjson.GetBytes(event, "type").String() {
			case "error":
				if gjson.GetBytes(event, "status").Int() != 400 || gjson.GetBytes(event, "error.code").String() != "invalid_request" {
					t.Fatalf("wrong upstream error: %s", event)
				}
				return http.StatusBadRequest, string(event)
			case "response.completed":
				return http.StatusOK, string(event)
			}
		}
		t.Fatal("no terminal WS event")
	}
	path, body := "/v1/messages", `{"model":"gpt-5.5","max_tokens":32,"messages":[{"role":"user","content":"synthetic request"}]}`
	serve := h.Messages
	switch endpoint {
	case "images", "images_stream":
		path, body = "/v1/images/generations", fmt.Sprintf(`{"model":"gpt-image-2","prompt":"synthetic landscape","response_format":"b64_json","stream":%t}`, endpoint == "images_stream")
		serve = h.ImagesGenerations
	case "edits", "edits_multipart", "edits_json_direct", "edits_multipart_direct":
		path = "/v1/images/edits"
		body = `{"model":"gpt-image-2","prompt":"synthetic landscape","images":[{"image_url":"data:image/png;base64,` + tinyPNGBase64 + `"}],"response_format":"b64_json"}`
		serve = h.ImagesEdits
		if endpoint == "edits_json_direct" {
			serve = h.imagesEditsFromJSON
		} else if endpoint == "edits_multipart_direct" {
			serve = h.imagesEditsFromMultipart
		}
	}
	contentType := "application/json"
	if endpoint == "edits_multipart" || endpoint == "edits_multipart_direct" {
		var buffer bytes.Buffer
		writer := multipart.NewWriter(&buffer)
		for _, field := range [][2]string{{"model", "gpt-image-2"}, {"prompt", "synthetic landscape"}, {"response_format", "b64_json"}} {
			if err := writer.WriteField(field[0], field[1]); err != nil {
				t.Fatal(err)
			}
		}
		part, err := writer.CreateFormFile("image", "synthetic.png")
		if err != nil {
			t.Fatal(err)
		}
		image, err := base64.StdEncoding.DecodeString(tinyPNGBase64)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(image); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		body, contentType = buffer.String(), writer.FormDataContentType()
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", contentType)
	for _, setup := range prepare {
		setup(c)
	}
	serve(c)
	return recorder.Code, recorder.Body.String()
}

func TestBadRequestRetryEndpointSnapshotBeforeBodyRead(t *testing.T) {
	for _, endpoint := range []string{"messages", "images", "edits", "edits_multipart", "edits_json_direct", "edits_multipart_direct"} {
		t.Run(endpoint, func(t *testing.T) {
			var calls atomic.Int32
			h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}, false, 3, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Chatgpt-Account-Id") != accountsIDForBadRequestRoute(1) {
					t.Error("body-read policy update changed snapshot from same to rotate")
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			h.store.SetMaxRetries(0)
			h.store.SetMaxRateLimitRetries(0)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			readHookCalled := false
			status, body := invokeBadRequestEndpoint(t, h, ctx, endpoint, func(c *gin.Context) {
				c.Request.Body = &badRequestRouteReadHook{ReadCloser: c.Request.Body, hook: func() {
					readHookCalled = true
					h.store.SetBadRequestRetryPolicy(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true})
				}}
			})
			if !readHookCalled || calls.Load() != 2 || status != http.StatusBadRequest {
				t.Fatalf("hook=%t calls=%d want=2 status=%d body=%s", readHookCalled, calls.Load(), status, body)
			}
			for _, account := range accounts {
				if account.ActiveRequests.Load() != 0 {
					t.Fatal("body-read snapshot retry leaked an account lease")
				}
			}
		})
	}
}

// Images deliberately select with an empty affinity key. This is not an
// affinity-overflow test: a saturated A forces B, but no binding guard is used.
func TestBadRequestRetryImagesDoNotRewriteSessionBinding(t *testing.T) {
	for _, endpoint := range []string{"images", "edits", "edits_multipart"} {
		for _, success := range []bool{false, true} {
			t.Run(endpoint+"/success="+strconv.FormatBool(success), func(t *testing.T) {
				var calls atomic.Int32
				h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}, false, 2, func(w http.ResponseWriter, r *http.Request) {
					n := calls.Add(1)
					if r.Header.Get("Chatgpt-Account-Id") != accountsIDForBadRequestRoute(2) {
						t.Error("expected saturated A to leave only B available")
					}
					if n == 2 && success {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"created_at":1710000000,"output":[{"type":"image_generation_call","result":"`+tinyPNGBase64+`","output_format":"png"}]}}`+"\n\n")
						return
					}
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, endpointBareBadRequest)
				})
				h.store.SetMaxRetries(0)
				h.store.SetMaxRateLimitRetries(0)
				headers := make(http.Header)
				headers.Set(downstreamAffinityHeader, "synthetic-images-existing-session")
				affinityKey := sessionAffinityKey(resolveDownstreamAffinityID(headers), 0)
				h.store.BindSessionAffinity(affinityKey, accounts[0], "")
				if id, ok := h.store.SessionAffinityAccountID(affinityKey); !ok || id != accounts[0].ID() {
					t.Fatal("missing initial A binding")
				}
				held := h.store.TakePreferredAccountWithDispatch(accounts[0].ID(), 0, nil, nil, auth.DispatchPolicyStandard)
				if held != accounts[0] || held.ActiveRequests.Load() != 1 {
					t.Fatal("failed to saturate synthetic A")
				}
				defer h.store.Release(held)
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				status, body := invokeBadRequestEndpoint(t, h, ctx, endpoint, func(c *gin.Context) {
					c.Request.Header.Set(downstreamAffinityHeader, headers.Get(downstreamAffinityHeader))
				})
				want := http.StatusBadRequest
				if success {
					want = http.StatusOK
				}
				if status != want || calls.Load() != 2 {
					t.Fatalf("calls=%d want=2 status=%d want=%d body=%s", calls.Load(), status, want, body)
				}
				if id, ok := h.store.SessionAffinityAccountID(affinityKey); !ok || id != accounts[0].ID() {
					t.Fatalf("Images changed existing A binding: id=%d exists=%t", id, ok)
				}
				if accounts[0].ActiveRequests.Load() != 1 || accounts[1].ActiveRequests.Load() != 0 {
					t.Fatal("Images changed held A lease or leaked B lease")
				}
			})
		}
	}
}

func TestBadRequestRetryEndpointMatrix(t *testing.T) {
	for _, endpoint := range []string{"messages", "images", "edits", "ws"} {
		for _, tc := range []struct {
			name                string
			budget, pool, calls int
			rotate, catchAll    bool
			disabled, revoke    bool
			revokeKey           bool
			readFailure, cancel bool
			snapshot, forceHTTP bool
			scope               bool
			body                string
		}{
			{name: "same", budget: 2, pool: 2, calls: 3},
			{name: "same_scope_one", budget: 2, pool: 2, calls: 3, scope: true},
			{name: "rotate_scope_one", budget: 2, pool: 4, calls: 3, rotate: true, scope: true},
			{name: "no_candidates_scope_one", budget: 2, pool: 1, calls: 1, rotate: true, scope: true},
			{name: "independent_max_budget", budget: 10, pool: 1, calls: 11, catchAll: true},
			{name: "rotate", budget: 2, pool: 4, calls: 3, rotate: true, catchAll: true},
			{name: "no_candidates", budget: 10, pool: 1, calls: 1, rotate: true, catchAll: true},
			{name: "same_unavailable_no_fallback", budget: 2, pool: 2, calls: 1, revoke: true, catchAll: true},
			{name: "same_key_revoked_no_fallback", budget: 2, pool: 2, calls: 1, revokeKey: true, catchAll: true},
			{name: "zero_owns_catchall", pool: 2, calls: 1, catchAll: true},
			{name: "disabled", budget: 2, pool: 2, calls: 1, disabled: true},
			{name: "detailed_400", budget: 2, pool: 2, calls: 1, body: `{"detail":"Bad Request","reason":"invalid input"}`},
			{name: "complexity_400", budget: 2, pool: 2, calls: 1, body: `{"error":{"message":"Invalid image complexity: medium","type":"invalid_request_error"}}`},
			{name: "read_error_owns_catchall", budget: 2, pool: 2, calls: 1, catchAll: true, readFailure: true},
			{name: "cancel", budget: 10, pool: 2, calls: 1, catchAll: true, cancel: true},
			{name: "snapshot", budget: 1, pool: 1, calls: 2, snapshot: true},
			{name: "force_http", budget: 2, pool: 1, calls: 3, forceHTTP: true},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				var mu sync.Mutex
				var seen []string
				var h *Handler
				var accounts []*auth.Account
				const scopeKeyID, scopeGroupID = int64(76244), int64(76245)
				h, accounts = newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: !tc.disabled, MaxRetries: tc.budget, SwitchAccount: tc.rotate}, tc.catchAll, tc.pool, func(w http.ResponseWriter, r *http.Request) {
					id := r.Header.Get("Chatgpt-Account-Id")
					if tc.scope {
						if got := APIKeyScopeInflight(scopeKeyID, database.APIKeyScopeTypeGroup, scopeGroupID); got != 1 {
							t.Errorf("scope lease during dispatch=%d want=1", got)
						}
						for _, account := range accounts {
							if account.AccountID == id && account.ActiveRequests.Load() != 1 {
								t.Error("dispatched account must hold exactly one lease")
							}
						}
					}
					mu.Lock()
					seen = append(seen, id)
					n := len(seen)
					mu.Unlock()
					if tc.revoke || tc.revokeKey {
						for _, account := range accounts {
							if account.AccountID == id {
								if tc.revokeKey {
									account.SetAllowedAPIKeyIDs([]int64{987654})
								} else {
									atomic.StoreInt32(&account.DispatchPaused, 1)
								}
							}
						}
					}
					if tc.snapshot && n == 1 {
						h.store.SetBadRequestRetryPolicy(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10})
					}
					if tc.forceHTTP {
						UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings { s.CodexForceWebsocket = true; return s })
					}
					body := tc.body
					if body == "" {
						body = endpointBareBadRequest
					}
					if tc.readFailure {
						w.Header().Set("Content-Length", strconv.Itoa(len(body)+10))
					}
					if !tc.disabled && tc.body == "" {
						// The narrow error must not generalize unrelated quota headers
						// into an account cooldown or an unhealthy scheduling score.
						w.Header().Set("x-codex-primary-used-percent", "100")
						w.Header().Set("x-codex-primary-window-minutes", "300")
						w.Header().Set("x-codex-primary-reset-after-seconds", "3600")
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, body)
					if tc.cancel {
						cancel()
					}
				})
				if tc.scope {
					for _, account := range accounts {
						account.Mu().Lock()
						account.GroupIDs = []int64{scopeGroupID}
						account.Mu().Unlock()
					}
				}
				status, body := invokeBadRequestEndpoint(t, h, ctx, endpoint, func(c *gin.Context) {
					if tc.scope {
						c.Set(contextScopeBudgetGate, newScopeBudgetGate(scopeKeyID, []database.APIKeyScopeLimit{{
							ScopeType: database.APIKeyScopeTypeGroup, ScopeID: scopeGroupID, MaxConcurrency: 1,
						}}))
					}
				})
				if got := APIKeyScopeInflight(scopeKeyID, database.APIKeyScopeTypeGroup, scopeGroupID); got != 0 {
					t.Errorf("scope leases after request=%d want=0", got)
				}
				mu.Lock()
				got := append([]string(nil), seen...)
				mu.Unlock()
				if len(got) != tc.calls {
					t.Fatalf("upstream calls=%d want=%d; downstream=%d %s", len(got), tc.calls, status, body)
				}
				if !tc.cancel && status != http.StatusBadRequest {
					t.Fatalf("status=%d want 400; body=%s", status, body)
				}
				for i, id := range got {
					if id == "" {
						t.Fatal("missing synthetic upstream account identity")
					}
					for j := 0; j < i; j++ {
						if tc.rotate && id == got[j] || !tc.rotate && id != got[j] {
							t.Fatalf("wrong account retry policy: %v", got)
						}
					}
				}
				if !tc.disabled && tc.body == "" {
					for _, account := range accounts {
						account.Mu().RLock()
						failed := account.FailureStreak != 0 || !account.LastFailureAt.IsZero()
						account.Mu().RUnlock()
						if failed || account.HasActiveCooldown() || account.IsModelRateLimited("gpt-image-2") {
							t.Fatal("bare Bad Request penalized account health or cooldown")
						}
						if account.ActiveRequests.Load() != 0 {
							t.Fatal("account lease was not released")
						}
					}
				}
			})
		}
	}
}

// The six dedicated dispatches must not consume the ordinary five-attempt
// ceiling or the general retry budget. Both image collectors share this rule.
func TestBadRequestRetryImagesIndependentAttemptCap(t *testing.T) {
	for _, endpoint := range []string{"images", "edits", "images_stream"} {
		for _, tc := range []struct {
			name                    string
			bare, failures, general int
			calls, status           int
			streamFailure, catchAll bool
		}{
			{name: "mixed_500_recovery", bare: 6, failures: 1, general: 1, calls: 8, status: 200},
			{name: "mixed_stream_recovery", bare: 6, failures: 1, general: 1, calls: 8, status: 200, streamFailure: true},
			{name: "mixed_general_exhausted", bare: 6, failures: 2, general: 1, calls: 8, status: 500},
			{name: "mixed_ordinary_cap", bare: 6, failures: 6, general: 20, calls: 11, status: 500},
			{name: "pure_ordinary_cap", failures: 6, general: 20, calls: 5, status: 500},
			{name: "pure_stream_cap", failures: 6, general: 20, calls: 5, status: 502, streamFailure: true},
			{name: "catchall_500", failures: 5, calls: 6, status: 200, catchAll: true},
			{name: "catchall_stream", failures: 5, calls: 6, status: 200, catchAll: true, streamFailure: true},
		} {
			if tc.catchAll && endpoint != "images" {
				continue // shared loop; avoid repeating real exponential backoff
			}
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				var calls atomic.Int32
				h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 6}, tc.catchAll, 12, func(w http.ResponseWriter, r *http.Request) {
					n := int(calls.Add(1))
					if n <= tc.bare {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, endpointBareBadRequest)
						return
					}
					if n <= tc.bare+tc.failures {
						if tc.streamFailure {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, `data: {"type":"response.failed","response":{"status":"failed","status_code":503,"error":{"code":"server_error","message":"synthetic transient stream failure"}}}`+"\n\n")
						} else {
							w.WriteHeader(http.StatusInternalServerError)
							_, _ = io.WriteString(w, `{"error":{"code":"server_error","message":"synthetic transient HTTP failure"}}`)
						}
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"created_at":1710000000,"output":[{"type":"image_generation_call","result":"`+tinyPNGBase64+`","output_format":"png"}]}}`+"\n\n")
				})
				h.store.SetMaxRetries(tc.general)
				db, err := database.New("sqlite", filepath.Join(t.TempDir(), "synthetic-attempts.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				h.db = db
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				status, body := invokeBadRequestEndpoint(t, h, ctx, endpoint)
				if ctx.Err() != nil || int(calls.Load()) != tc.calls {
					t.Fatalf("calls=%d want=%d ctx=%v status=%d body=%s", calls.Load(), tc.calls, ctx.Err(), status, body)
				}
				if endpoint == "images_stream" && tc.streamFailure && tc.status != http.StatusOK {
					// The connected comment may already commit HTTP 200. The final
					// SSE error, rather than that status, describes the failure.
					if !strings.Contains(body, "event: error\n") || strings.Contains(body, tinyPNGBase64) {
						t.Fatalf("missing final stream error: status=%d body=%s", status, body)
					}
				} else if status != tc.status || (tc.status == http.StatusOK && !strings.Contains(body, tinyPNGBase64)) {
					t.Fatalf("status=%d want=%d body=%s", status, tc.status, body)
				}
				db.FlushUsageLogs()
				logs, err := db.ListRecentUsageLogs(context.Background(), 30)
				if err != nil || len(logs) != tc.calls {
					t.Fatalf("usage count=%d want=%d err=%v", len(logs), tc.calls, err)
				}
				seen := make(map[int]bool)
				for _, entry := range logs {
					if entry.AttemptIndex < 1 || entry.AttemptIndex > tc.calls || seen[entry.AttemptIndex] || entry.IsRetryAttempt != (entry.AttemptIndex < tc.calls) {
						t.Fatalf("usage status=%d attempt=%d retry=%t; want unique real dispatch indices 1..%d", entry.StatusCode, entry.AttemptIndex, entry.IsRetryAttempt, tc.calls)
					}
					seen[entry.AttemptIndex] = true
					wantStatus := http.StatusBadRequest
					if entry.AttemptIndex > tc.bare {
						wantStatus = http.StatusInternalServerError
						if tc.streamFailure {
							wantStatus = http.StatusBadGateway
						}
					}
					if entry.AttemptIndex > tc.bare+tc.failures {
						wantStatus = http.StatusOK
					}
					if entry.StatusCode != wantStatus {
						t.Errorf("attempt %d status=%d want=%d", entry.AttemptIndex, entry.StatusCode, wantStatus)
					}
				}
				for _, account := range accounts {
					if got := account.ActiveRequests.Load(); got != 0 {
						t.Errorf("account %d leaked %d leases", account.ID(), got)
					}
				}
			})
		}
	}
}

func TestBadRequestRetryWebSocketBudgetIsPerTurn(t *testing.T) {
	var calls atomic.Int32
	h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}, false, 1, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n%2 == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, endpointBareBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, wsContextTestSSE(fmt.Sprintf("resp_synthetic_retry_%d", n)))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, stop := dialBadRequestEndpointWS(t, h, ctx)
	defer stop()
	for turn := 0; turn < 2; turn++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.5","input":"independent synthetic turn","service_tier":"priority"}`)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		terminal := readResponsesWSTerminalEvent(t, conn)
		if gjson.GetBytes(terminal, "type").String() != "response.completed" {
			t.Fatalf("turn %d failed: %s", turn, terminal)
		}
	}
	if calls.Load() != 4 {
		t.Fatalf("calls=%d want=4 (one retry in each turn)", calls.Load())
	}
}

func TestBadRequestRetryEndpointUsage(t *testing.T) {
	for _, endpoint := range []string{"messages", "images", "edits", "ws"} {
		t.Run(endpoint, func(t *testing.T) {
			h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2}, true, 1, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			db, err := database.New("sqlite", filepath.Join(t.TempDir(), "synthetic-usage.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			h.db = db
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			status, body := invokeBadRequestEndpoint(t, h, ctx, endpoint)
			if status != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", status, body)
			}
			db.FlushUsageLogs()
			logs, err := db.ListRecentUsageLogs(context.Background(), 10)
			if err != nil || len(logs) != 3 {
				t.Fatalf("usage count=%d want=3 err=%v", len(logs), err)
			}
			for _, entry := range logs {
				if entry.StatusCode != 400 || entry.AttemptIndex < 1 || entry.AttemptIndex > 3 || entry.IsRetryAttempt != (entry.AttemptIndex < 3) {
					t.Fatalf("usage status=%d attempt=%d retry=%t", entry.StatusCode, entry.AttemptIndex, entry.IsRetryAttempt)
				}
			}
		})
	}
}

func TestBadRequestRetryMessagesPinOptsOut(t *testing.T) {
	var calls atomic.Int32
	h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true}, true, 2, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, endpointBareBadRequest)
	})
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set(contextPinnedAccountID, int64(1))
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gpt-5.5","messages":[{"role":"user","content":"synthetic request"}]}`))
	h.Messages(c)
	if calls.Load() != 1 || recorder.Code != http.StatusBadRequest {
		t.Fatalf("pinned calls=%d status=%d", calls.Load(), recorder.Code)
	}
}

func TestBadRequestRetryEndpointBackoffReleasesLeaseAndCancels(t *testing.T) {
	for _, endpoint := range []string{"messages", "images", "edits", "ws"} {
		t.Run(endpoint, func(t *testing.T) {
			var calls atomic.Int32
			h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2}, false, 1, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			h.store.SetRetryIntervalMS(5000)
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			start := time.Now()
			invokeBadRequestEndpoint(t, h, ctx, endpoint)
			if calls.Load() != 1 || accounts[0].ActiveRequests.Load() != 0 || time.Since(start) > 2*time.Second {
				t.Fatalf("canceled retry: calls=%d active=%d elapsed=%s", calls.Load(), accounts[0].ActiveRequests.Load(), time.Since(start))
			}
		})
	}
}

func TestBadRequestRetryCommittedHTTPDoesNotReplay(t *testing.T) {
	for _, endpoint := range []string{"messages", "images"} {
		t.Run(endpoint, func(t *testing.T) {
			var calls atomic.Int32
			h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2}, true, 1, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gpt-5.5","stream":true,"messages":[{"role":"user","content":"synthetic request"}]}`))
			_, _ = c.Writer.WriteString("data: synthetic already committed output\n\n")
			if endpoint == "messages" {
				h.Messages(c)
			} else {
				h.forwardImagesRequest(c, "/v1/images/generations", "gpt-image-2", "gpt-image-2", "gpt-image-2", []byte(`{"model":"gpt-5.5","input":"synthetic image","tools":[{"type":"image_generation","model":"gpt-image-2"}]}`), "b64_json", "image_generation", true)
			}
			if calls.Load() != 1 {
				t.Fatalf("committed output retried: calls=%d", calls.Load())
			}
		})
	}
}

func TestBadRequestRetryUpstreamWebSocketDoesNotMatchHTTPPolicy(t *testing.T) {
	for _, endpoint := range []string{"messages", "ws"} {
		t.Run(endpoint, func(t *testing.T) {
			h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2}, false, 1, func(http.ResponseWriter, *http.Request) {
				t.Error("WebSocket upstream error unexpectedly fell back to HTTP")
			})
			UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings { s.CodexForceWebsocket = true; return s })
			var calls atomic.Int32
			WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(endpointBareBadRequest))}, nil
			}
			if endpoint == "messages" {
				invokeBadRequestEndpoint(t, h, context.Background(), endpoint)
			} else {
				conn, stop := dialBadRequestEndpointWS(t, h, context.Background())
				defer stop()
				if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.5","input":"synthetic request"}`)); err != nil {
					t.Fatal(err)
				}
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				readResponsesWSTerminalEvent(t, conn)
			}
			if calls.Load() != 1 {
				t.Fatalf("non-HTTP failure used dedicated retry: calls=%d", calls.Load())
			}
		})
	}
}

func TestBadRequestRetryWebSocketContinuationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		rotate, pinned, previous, complete bool
		wantCalls                          int32
	}{
		{name: "pinned_rotate_stops", rotate: true, pinned: true, wantCalls: 1},
		{name: "pinned_same_allowed", pinned: true, wantCalls: 3},
		{name: "missing_history_rotate_stops", rotate: true, previous: true, wantCalls: 0},
		{name: "missing_history_same_stops", previous: true, wantCalls: 0},
		{name: "complete_history_same_preserved", previous: true, complete: true, wantCalls: 3},
		{name: "complete_history_rotate_preserved", rotate: true, previous: true, complete: true, wantCalls: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetResponseCacheForTest()
			t.Cleanup(resetResponseCacheForTest)
			if tc.complete {
				setResponseCache("anon", "resp_synthetic_parent", []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","content":"synthetic ancestor"}`)})
			}
			var calls atomic.Int32
			h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2, SwitchAccount: tc.rotate}, true, 3, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body := readUpstreamRequestBody(r)
				if tc.complete && !strings.Contains(string(body), "synthetic ancestor") {
					t.Error("same-account retry lost the admitted ancestor")
				}
				if (!tc.rotate || tc.pinned) && r.Header.Get("Chatgpt-Account-Id") != "synthetic-bad-request-1" {
					t.Error("continuation moved to another account")
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			h.store.BindSessionAffinity("synthetic-continuation", accounts[0], "")
			payload := map[string]any{
				"type": "response.create", "model": "gpt-5.5", "service_tier": "priority",
				"input": "synthetic new input", "prompt_cache_key": "synthetic-continuation",
			}
			if tc.pinned {
				payload["client_metadata"] = map[string]string{"x-codex-turn-state": "synthetic-turn"}
			}
			if tc.previous {
				payload["previous_response_id"] = "resp_synthetic_parent"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			conn, stop := dialBadRequestEndpointWS(t, h, ctx)
			defer stop()
			if err := conn.WriteJSON(payload); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			terminal := readResponsesWSTerminalEvent(t, conn)
			wantStatus := int64(http.StatusBadRequest)
			if tc.previous && !tc.complete {
				// Existing fail-closed recovery refuses even the first request;
				// the new retry policy must not manufacture a lossy continuation.
				wantStatus = http.StatusConflict
			}
			if gjson.GetBytes(terminal, "status").Int() != wantStatus || calls.Load() != tc.wantCalls {
				t.Fatalf("calls=%d want=%d terminal=%s", calls.Load(), tc.wantCalls, terminal)
			}
		})
	}
}
