package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// These route tests use only the synthetic loopback fixture. In particular,
// invoking a handler here never uses a production account or remote upstream.
func invokeBadRequestRoute(t *testing.T, h *Handler, ctx context.Context, route, body string, setup func(*gin.Context)) (int, string) {
	t.Helper()
	path := "/v1/responses"
	serve := h.Responses
	switch route {
	case "chat":
		path, serve = "/v1/chat/completions", h.ChatCompletions
	case "compact":
		path, serve = "/v1/responses/compact", h.ResponsesCompact
	}
	if body == "" {
		body = `{"model":"gpt-5.5","input":[{"role":"user","content":"synthetic request"}]}`
		if route == "chat" {
			body = `{"model":"gpt-5.5","messages":[{"role":"user","content":"synthetic request"}]}`
		}
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")
	if setup != nil {
		setup(c)
	}
	serve(c)
	return recorder.Code, recorder.Body.String()
}

func TestBadRequestRetryRoutesProMaxAndScope(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		for _, scenario := range []string{"rotate_promax", "same_plan_revoked", "rotate_group_denied", "same_group_revoked", "rotate_scope_denied", "same_scope_revoked", "same_scope_concurrency_one", "rotate_scope_concurrency_one"} {
			t.Run(route+"/"+scenario, func(t *testing.T) {
				rotate := strings.HasPrefix(scenario, "rotate_")
				var calls atomic.Int32
				var h *Handler
				var accounts []*auth.Account
				h, accounts = newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2, SwitchAccount: rotate}, true, 3, func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if scenario == "rotate_promax" && r.Header.Get("Chatgpt-Account-Id") == accountsIDForBadRequestRoute(2) {
						t.Error("ultrafast retry selected a non-ProMax account")
					}
					switch scenario {
					case "same_plan_revoked":
						h.store.UpdateAccountPlanType(accounts[0], "plus")
					case "same_group_revoked", "same_scope_revoked":
						h.store.ApplyAccountGroups(accounts[0].ID(), []int64{20})
					}
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, endpointBareBadRequest)
				})
				// Runtime setters make the ordinary budgets explicitly zero; NewStore
				// can interpret a persisted zero as its compatibility default.
				h.store.SetMaxRetries(0)
				h.store.SetMaxRateLimitRetries(0)
				for _, account := range accounts {
					h.store.ApplyAccountGroups(account.ID(), []int64{20})
				}
				h.store.ApplyAccountGroups(accounts[0].ID(), []int64{10})
				body := ""
				if scenario == "rotate_promax" || scenario == "same_plan_revoked" {
					h.store.UpdateAccountPlanType(accounts[0], "promax")
					h.store.UpdateAccountPlanType(accounts[2], "promax")
					body = `{"model":"gpt-5.5","service_tier":"ultrafast","input":[{"role":"user","content":"synthetic request"}]}`
					if route == "chat" {
						body = `{"model":"gpt-5.5","service_tier":"ultrafast","messages":[{"role":"user","content":"synthetic request"}]}`
					}
				}
				if scenario == "rotate_group_denied" || scenario == "same_group_revoked" {
					h.store.SetAPIKeyAllowedGroups(4242, []int64{10})
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				status, result := invokeBadRequestRoute(t, h, ctx, route, body, func(c *gin.Context) {
					c.Set(contextAPIKeyID, int64(4242))
					switch scenario {
					case "rotate_scope_denied", "same_scope_revoked":
						c.Set(contextScopeBudgetGate, &scopeBudgetGate{blockedGroups: map[int64]struct{}{20: {}}, message: "synthetic scope exhausted"})
					case "same_scope_concurrency_one", "rotate_scope_concurrency_one":
						for _, account := range accounts {
							h.store.ApplyAccountGroups(account.ID(), []int64{10})
						}
						c.Set(contextScopeBudgetGate, newScopeBudgetGate(4242, []database.APIKeyScopeLimit{{ScopeType: database.APIKeyScopeTypeGroup, ScopeID: 10, MaxConcurrency: 1}}))
					}
				})
				want := int32(1)
				if scenario == "rotate_promax" {
					want = 2
				} else if strings.HasSuffix(scenario, "concurrency_one") {
					want = 3
				}
				if status != 400 || calls.Load() != want {
					t.Fatalf("calls=%d want=%d status=%d body=%s", calls.Load(), want, status, result)
				}
				if inflight := APIKeyScopeInflight(4242, database.APIKeyScopeTypeGroup, 10); inflight != 0 {
					t.Fatalf("scope lease leaked: %d", inflight)
				}
			})
		}
	}
}

func TestBadRequestRetryRoutesRelay(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		for _, rotate := range []bool{false, true} {
			t.Run(route+"/rotate="+strconv.FormatBool(rotate), func(t *testing.T) {
				var mu sync.Mutex
				var seen []string
				serve := func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					seen = append(seen, r.Header.Get("Authorization"))
					mu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, endpointBareBadRequest)
				}
				h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2, SwitchAccount: rotate}, true, 3, serve)
				h.store.SetMaxRetries(0)
				h.store.SetMaxRateLimitRetries(0)
				upstream := httptest.NewServer(http.HandlerFunc(serve))
				t.Cleanup(upstream.Close)
				for _, account := range accounts {
					account.UpstreamType = auth.UpstreamOpenAIResponses
					account.BaseURL, account.APIKey = upstream.URL, account.AccountID
					account.Models = []string{"gpt-5.5"}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				status, body := invokeBadRequestRoute(t, h, ctx, route, "", nil)
				mu.Lock()
				defer mu.Unlock()
				if status != 400 || len(seen) != 3 {
					t.Fatalf("relay calls=%d want=3 status=%d body=%s", len(seen), status, body)
				}
				for i := 1; i < len(seen); i++ {
					if (seen[i] != seen[0]) != rotate {
						t.Error("relay did not follow same/rotate policy")
					}
				}
			})
		}
	}
}

func TestBadRequestRetryRoutesRotationSkipsForcedWSRelay(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		t.Run(route, func(t *testing.T) {
			var calls atomic.Int32
			h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true}, true, 3, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Chatgpt-Account-Id") == "" {
					t.Error("retry silently changed the forced-WS relay's transport policy")
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			accounts[1].UpstreamType = auth.UpstreamOpenAIResponses
			accounts[1].BaseURL = resinCfg.Load().BaseURL
			accounts[1].APIKey = "synthetic-forced-ws-key"
			accounts[1].Models = []string{"gpt-5.5"}
			accounts[1].ResponsesUpstreamTransport = auth.OpenAIResponsesTransportWebsocket
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			status, body := invokeBadRequestRoute(t, h, ctx, route, "", nil)
			if calls.Load() != 2 || status != 400 {
				t.Fatalf("calls=%d want=2 status=%d body=%s", calls.Load(), status, body)
			}
		})
	}
}

func TestBadRequestRetryRoutesSafePreflightBounds(t *testing.T) {
	for _, scenario := range []string{"same_three_attempt_cap", "window_expired", "rate_pinned_then_http400"} {
		t.Run(scenario, func(t *testing.T) {
			var harness *spAcceptanceHarness
			generalLimit := 0
			if scenario == "rate_pinned_then_http400" {
				generalLimit = 1
			}
			harness = spAcceptanceNewHarness(t, generalLimit, nil, func(w http.ResponseWriter, _ *http.Request, n int) {
				if scenario == "rate_pinned_then_http400" && n == 1 {
					spAcceptanceFlush(w, spAcceptanceCreated("resp_synthetic_A")+spAcceptanceRate+spAcceptanceFrame(spAcceptanceFailed))
					return
				}
				if scenario == "window_expired" {
					harness.mu.Lock()
					state := harness.sends[0].state
					harness.mu.Unlock()
					if state == nil {
						t.Error("expected an active safe-preflight state")
					} else {
						state.mu.Lock()
						state.started = time.Now().Add(-time.Hour)
						state.mu.Unlock()
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			harness.store.SetBadRequestRetryPolicy(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: scenario == "rate_pinned_then_http400"})
			resp, reader, _ := harness.request(t)
			wire := spAcceptanceReadRest(t, reader)
			harness.wait(t)
			want := 3
			if scenario == "window_expired" {
				want = 1
			} else if scenario == "rate_pinned_then_http400" {
				want = 2
			}
			harness.provePath(t, want)
			if scenario != "rate_pinned_then_http400" && resp.StatusCode != 400 {
				t.Fatalf("status=%d want=400 body=%s", resp.StatusCode, wire)
			}
		})
	}
}

func TestBadRequestRetryRoutesBudgetsRemainIndependent(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		for _, otherStatus := range []int{http.StatusInternalServerError, http.StatusTooManyRequests} {
			t.Run(route+"/"+strconv.Itoa(otherStatus), func(t *testing.T) {
				var calls atomic.Int32
				h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2}, false, 3, func(w http.ResponseWriter, _ *http.Request) {
					n := calls.Add(1)
					if n%2 == 1 {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, endpointBareBadRequest)
					} else {
						w.WriteHeader(otherStatus)
						_, _ = io.WriteString(w, `{"error":{"message":"synthetic transient failure"}}`)
					}
				})
				h.store.SetMaxRetries(0)
				h.store.SetMaxRateLimitRetries(0)
				if otherStatus == http.StatusTooManyRequests {
					h.store.SetMaxRateLimitRetries(1)
				} else {
					h.store.SetMaxRetries(1)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				status, body := invokeBadRequestRoute(t, h, ctx, route, "", nil)
				if calls.Load() != 4 || status != otherStatus {
					t.Fatalf("independent budgets: calls=%d want=4 status=%d want=%d body=%s", calls.Load(), status, otherStatus, body)
				}
			})
		}
	}
}

type badRequestRouteReadHook struct {
	io.ReadCloser
	hook func()
}

func (r *badRequestRouteReadHook) Read(p []byte) (int, error) {
	if r.hook != nil {
		r.hook()
		r.hook = nil
	}
	return r.ReadCloser.Read(p)
}

func TestBadRequestRetryRoutesSnapshotBeforeBodyRead(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact", "body_signal_compact"} {
		t.Run(route, func(t *testing.T) {
			var calls atomic.Int32
			h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}, false, 3, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Chatgpt-Account-Id") != accountsIDForBadRequestRoute(1) {
					t.Error("midflight policy update changed snapshot account mode")
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			h.store.SetMaxRetries(0)
			h.store.SetMaxRateLimitRetries(0)
			body := ""
			if route == "body_signal_compact" {
				body = `{"model":"gpt-5.5","input":[{"role":"user","content":"synthetic request"},{"type":"compaction_trigger"}]}`
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			status, result := invokeBadRequestRoute(t, h, ctx, route, body, func(c *gin.Context) {
				c.Request.Body = &badRequestRouteReadHook{ReadCloser: c.Request.Body, hook: func() {
					h.store.SetBadRequestRetryPolicy(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true})
				}}
			})
			if calls.Load() != 2 || status != 400 {
				t.Fatalf("snapshot calls=%d want=2 status=%d body=%s", calls.Load(), status, result)
			}
		})
	}
}

// This exercises real capacity overflow, not ordinary same-account affinity:
// A is bound and leased to its limit; selection must borrow B with a nonzero
// guard. Both the retry's pre-dispatch bind and its terminal release must keep A.
func TestBadRequestRetryRoutesCapacityOverflowKeepsBinding(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact", "messages", "ws"} {
		for _, success := range []bool{false, true} {
			t.Run(route+"/success="+strconv.FormatBool(success), func(t *testing.T) {
				var calls atomic.Int32
				var h *Handler
				var affinityKey string
				h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}, false, 2, func(w http.ResponseWriter, r *http.Request) {
					n := calls.Add(1)
					if r.Header.Get("Chatgpt-Account-Id") != accountsIDForBadRequestRoute(2) {
						t.Error("both overflow attempts must dispatch B, never A")
					}
					if id, ok := h.store.SessionAffinityAccountID(affinityKey); !ok || id != 1 {
						t.Errorf("attempt %d already overwrote A binding: id=%d exists=%t", n, id, ok)
					}
					if n == 2 && success {
						if route == "compact" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"id":"compact_synthetic_overflow","object":"response.compaction","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, wsContextTestSSE("resp_synthetic_overflow"))
						}
						return
					}
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, endpointBareBadRequest)
				})
				h.store.SetMaxRetries(0)
				h.store.SetMaxRateLimitRetries(0)
				headers := make(http.Header)
				headers.Set(downstreamAffinityHeader, "synthetic-capacity-overflow")
				identity := resolveRequestSessionIdentity(headers, nil)
				if route == "messages" {
					identity = resolveClaudeRequestSessionIdentity(headers, nil)
				}
				affinityKey = sessionAffinityKey(identity.affinityID, 0)
				if affinityKey == "" {
					t.Fatal("fixture requires an explicit session affinity key")
				}
				h.store.BindSessionAffinity(affinityKey, accounts[0], "")
				if id, ok := h.store.SessionAffinityAccountID(affinityKey); !ok || id != accounts[0].ID() {
					t.Fatal("fixture must start bound to A")
				}
				// Take a real lease, keeping A saturated for the whole request.
				held, _, initialGuard := h.nextAccountForSessionWithDispatchGuard(affinityKey, 0, nil, nil, auth.SelectionOptions{Policy: auth.DispatchPolicyStandard})
				if held != accounts[0] || !reflect.ValueOf(initialGuard).IsZero() {
					t.Fatal("initial selection must use the bound A without overflow")
				}
				defer h.store.Release(held)
				borrowed, _, overflowGuard := h.nextAccountForSessionWithDispatchGuard(affinityKey, 0, nil, nil, auth.SelectionOptions{Policy: auth.DispatchPolicyStandard})
				if borrowed != nil {
					h.store.Release(borrowed)
				}
				if borrowed != accounts[1] || reflect.ValueOf(overflowGuard).IsZero() || held.ActiveRequests.Load() != 1 {
					t.Fatal("fixture must produce a guarded capacity borrow of B")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				setup := func(c *gin.Context) {
					c.Request.Header.Set(downstreamAffinityHeader, headers.Get(downstreamAffinityHeader))
				}
				var status int
				var body string
				if route == "messages" || route == "ws" {
					status, body = invokeBadRequestEndpoint(t, h, ctx, route, setup)
				} else {
					status, body = invokeBadRequestRoute(t, h, ctx, route, "", setup)
				}
				want := http.StatusBadRequest
				if success {
					want = http.StatusOK
				}
				if status != want || calls.Load() != 2 {
					t.Fatalf("calls=%d want=2 status=%d want=%d body=%s", calls.Load(), status, want, body)
				}
				if id, ok := h.store.SessionAffinityAccountID(affinityKey); !ok || id != accounts[0].ID() {
					t.Errorf("final binding must remain A: id=%d exists=%t", id, ok)
				}
				if accounts[0].ActiveRequests.Load() != 1 || accounts[1].ActiveRequests.Load() != 0 {
					t.Error("request changed held A lease or leaked borrowed B lease")
				}
			})
		}
	}
}

func TestBadRequestRetryRoutesUpstreamWSDoesNotMatch(t *testing.T) {
	for _, route := range []string{"responses", "chat"} {
		t.Run(route, func(t *testing.T) {
			h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2}, false, 1, func(http.ResponseWriter, *http.Request) {
				t.Error("WebSocket failure used the HTTP retry policy")
			})
			h.store.SetMaxRetries(0)
			h.store.SetMaxRateLimitRetries(0)
			UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings { s.CodexForceWebsocket = true; return s })
			var calls atomic.Int32
			WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(endpointBareBadRequest))}, nil
			}
			status, body := invokeBadRequestRoute(t, h, context.Background(), route, "", nil)
			if calls.Load() != 1 || status != 400 {
				t.Fatalf("non-HTTP calls=%d want=1 status=%d body=%s", calls.Load(), status, body)
			}
		})
	}
}

func TestBadRequestRetryRoutesRecoverSuccessfully(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		for _, rotate := range []bool{false, true} {
			t.Run(route+"/rotate="+strconv.FormatBool(rotate), func(t *testing.T) {
				var calls atomic.Int32
				h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1, SwitchAccount: rotate}, false, 2, func(w http.ResponseWriter, r *http.Request) {
					if calls.Add(1) == 1 {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, endpointBareBadRequest)
						return
					}
					wantID := 1
					if rotate {
						wantID = 2
					}
					if r.Header.Get("Chatgpt-Account-Id") != accountsIDForBadRequestRoute(wantID) {
						t.Error("successful retry used the wrong account")
					}
					if route == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"compact_synthetic_success","object":"response.compaction","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, wsContextTestSSE("resp_synthetic_success"))
					}
				})
				h.store.SetMaxRetries(0)
				h.store.SetMaxRateLimitRetries(0)
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				status, body := invokeBadRequestRoute(t, h, ctx, route, "", nil)
				if status != 200 || calls.Load() != 2 || !gjson.Valid(body) {
					t.Fatalf("recovery calls=%d status=%d body=%s", calls.Load(), status, body)
				}
				for _, account := range accounts {
					if account.ActiveRequests.Load() != 0 {
						t.Fatal("successful retry leaked a lease")
					}
				}
			})
		}
	}
}

func TestBadRequestRetryRoutesRelayContinuation(t *testing.T) {
	for _, route := range []string{"responses", "compact"} {
		for _, complete := range []bool{false, true} {
			for _, rotate := range []bool{false, true} {
				t.Run(route+"/complete="+strconv.FormatBool(complete)+"/rotate="+strconv.FormatBool(rotate), func(t *testing.T) {
					resetResponseCacheForTest()
					t.Cleanup(resetResponseCacheForTest)
					if complete {
						setResponseCache("anon", "resp_synthetic_relay_parent", []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","content":"synthetic relay ancestor"}`)})
					}
					var calls atomic.Int32
					serve := func(w http.ResponseWriter, r *http.Request) {
						n := calls.Add(1)
						body := readUpstreamRequestBody(r)
						if rotate && n > 1 {
							if !strings.Contains(string(body), "synthetic relay ancestor") || gjson.GetBytes(body, "previous_response_id").Exists() {
								t.Error("relay rotation must use complete history rather than a cross-account continuation")
							}
						} else if gjson.GetBytes(body, "previous_response_id").String() != "resp_synthetic_relay_parent" {
							t.Error("same-account relay retry lost its original continuation ID")
						}
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, endpointBareBadRequest)
					}
					h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2, SwitchAccount: rotate}, true, 3, serve)
					upstream := httptest.NewServer(http.HandlerFunc(serve))
					t.Cleanup(upstream.Close)
					for _, account := range accounts {
						account.UpstreamType = auth.UpstreamOpenAIResponses
						account.BaseURL, account.APIKey = upstream.URL, account.AccountID
						account.Models = []string{"gpt-5.5"}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
					defer cancel()
					status, body := invokeBadRequestRoute(t, h, ctx, route, `{"model":"gpt-5.5","previous_response_id":"resp_synthetic_relay_parent","input":[{"role":"user","content":"synthetic new input"}]}`, nil)
					want := int32(1)
					if complete {
						want = 3
					}
					if calls.Load() != want || status != 400 {
						t.Fatalf("calls=%d want=%d status=%d body=%s", calls.Load(), want, status, body)
					}
				})
			}
		}
	}
}

func TestBadRequestRetryRoutesMatrix(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		for _, tc := range []struct {
			name                string
			budget, pool, calls int
			rotate, catchAll    bool
			disabled, revoke    bool
			revokeKey           bool
			readFailure, cancel bool
			snapshot, forceHTTP bool
			body                string
		}{
			{name: "same", budget: 2, pool: 2, calls: 3},
			{name: "independent_max_budget", budget: 10, pool: 1, calls: 11, catchAll: true},
			{name: "rotate", budget: 2, pool: 4, calls: 3, rotate: true, catchAll: true},
			{name: "no_candidates", budget: 10, pool: 1, calls: 1, rotate: true, catchAll: true},
			{name: "same_unavailable_no_fallback", budget: 2, pool: 2, calls: 1, revoke: true, catchAll: true},
			{name: "same_key_revoked_no_fallback", budget: 2, pool: 2, calls: 1, revokeKey: true, catchAll: true},
			{name: "zero_owns_catchall", pool: 2, calls: 1, catchAll: true},
			{name: "disabled", budget: 2, pool: 2, calls: 1, disabled: true},
			{name: "detailed_400", budget: 2, pool: 2, calls: 1, body: `{"detail":"Bad Request","reason":"invalid input"}`},
			{name: "validation_400", budget: 2, pool: 2, calls: 1, body: `{"error":{"message":"Bad Request: invalid input","type":"invalid_request_error"}}`},
			{name: "read_error_owns_catchall", budget: 2, pool: 2, calls: 1, catchAll: true, readFailure: true},
			{name: "cancel", budget: 10, pool: 2, calls: 1, catchAll: true, cancel: true},
			{name: "snapshot", budget: 1, pool: 1, calls: 2, snapshot: true},
			{name: "force_http", budget: 2, pool: 1, calls: 3, forceHTTP: true},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				var mu sync.Mutex
				var seen []string
				var h *Handler
				var accounts []*auth.Account
				h, accounts = newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: !tc.disabled, MaxRetries: tc.budget, SwitchAccount: tc.rotate}, tc.catchAll, tc.pool, func(w http.ResponseWriter, r *http.Request) {
					id := r.Header.Get("Chatgpt-Account-Id")
					mu.Lock()
					seen = append(seen, id)
					n := len(seen)
					mu.Unlock()
					for _, account := range accounts {
						if account.AccountID == id {
							if tc.revoke {
								atomic.StoreInt32(&account.DispatchPaused, 1)
							}
							if tc.revokeKey {
								account.SetAllowedAPIKeyIDs([]int64{987654})
							}
						}
					}
					if tc.snapshot && n == 1 {
						h.store.SetBadRequestRetryPolicy(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true})
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
				h.store.SetMaxRetries(0)
				h.store.SetMaxRateLimitRetries(0)
				status, body := invokeBadRequestRoute(t, h, ctx, route, "", nil)
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
				for _, account := range accounts {
					if account.ActiveRequests.Load() != 0 {
						t.Fatal("account lease was not released")
					}
					if !tc.disabled && tc.body == "" && !tc.cancel {
						account.Mu().RLock()
						failed := account.FailureStreak != 0 || !account.LastFailureAt.IsZero()
						account.Mu().RUnlock()
						if failed || account.HasActiveCooldown() || account.IsModelRateLimited("gpt-5.5") {
							t.Fatal("bare Bad Request penalized health or cooldown")
						}
					}
				}
			})
		}
	}
}

func TestBadRequestRetryRoutesUsage(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		t.Run(route, func(t *testing.T) {
			h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2}, true, 1, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, endpointBareBadRequest)
			})
			db, err := database.New("sqlite", filepath.Join(t.TempDir(), "synthetic-routes-usage.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			h.db = db
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			status, body := invokeBadRequestRoute(t, h, ctx, route, "", nil)
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

func TestBadRequestRetryRoutesPinAndCommit(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		for _, pin := range []bool{false, true} {
			t.Run(route+"/pin="+strconv.FormatBool(pin), func(t *testing.T) {
				var calls atomic.Int32
				h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true}, true, 2, func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, endpointBareBadRequest)
				})
				status, body := invokeBadRequestRoute(t, h, context.Background(), route, "", func(c *gin.Context) {
					if pin {
						c.Set(contextPinnedAccountID, int64(1))
					} else {
						_, _ = c.Writer.WriteString("synthetic already committed output\n")
					}
				})
				wantCalls, wantStatus := int32(1), http.StatusOK
				if pin {
					wantStatus = http.StatusBadRequest
					if route == "compact" {
						wantCalls, wantStatus = 0, http.StatusForbidden
					}
				}
				if calls.Load() != wantCalls || status != wantStatus {
					t.Fatalf("calls=%d want=%d status=%d want=%d body=%s", calls.Load(), wantCalls, status, wantStatus, body)
				}
			})
		}
	}
}

func TestBadRequestRetryRoutesBackoffCancellation(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		t.Run(route, func(t *testing.T) {
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
			invokeBadRequestRoute(t, h, ctx, route, "", nil)
			if calls.Load() != 1 || accounts[0].ActiveRequests.Load() != 0 || time.Since(start) > 2*time.Second {
				t.Fatalf("calls=%d active=%d elapsed=%s", calls.Load(), accounts[0].ActiveRequests.Load(), time.Since(start))
			}
		})
	}
}

func TestBadRequestRetryRoutesContinuation(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		for _, tc := range []struct {
			name                     string
			rotate, pinned, previous bool
			complete                 bool
			calls                    int32
		}{
			{name: "turn_pinned_rotation_stops", rotate: true, pinned: true, calls: 1},
			{name: "turn_pinned_same_allowed", pinned: true, calls: 3},
			{name: "complete_history_same", previous: true, complete: true, calls: 3},
			{name: "complete_history_rotation", previous: true, complete: true, rotate: true, calls: 3},
			{name: "missing_history_same_stops", previous: true, calls: 0},
			{name: "missing_history_rotation_stops", previous: true, rotate: true, calls: 0},
		} {
			if route == "chat" && tc.previous {
				continue // Chat carries complete messages rather than Responses IDs.
			}
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				resetResponseCacheForTest()
				t.Cleanup(resetResponseCacheForTest)
				if tc.complete {
					setResponseCache("anon", "resp_synthetic_routes_parent", []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","content":"synthetic ancestor"}`)})
				}
				var calls atomic.Int32
				h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2, SwitchAccount: tc.rotate}, true, 3, func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body := readUpstreamRequestBody(r)
					if tc.complete && !strings.Contains(string(body), "synthetic ancestor") {
						t.Error("retry lost the complete ancestor history")
					}
					if !tc.rotate && r.Header.Get("Chatgpt-Account-Id") != accountsIDForBadRequestRoute(1) {
						t.Error("same-account continuation rotated")
					}
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, endpointBareBadRequest)
				})
				h.store.BindSessionAffinity("synthetic-routes-continuation", accounts[0], "")
				payload := map[string]any{
					"model": "gpt-5.5", "prompt_cache_key": "synthetic-routes-continuation",
					"input": []map[string]string{{"role": "user", "content": "synthetic new input"}},
				}
				if route == "chat" {
					payload["messages"] = payload["input"]
					delete(payload, "input")
				}
				if tc.previous {
					payload["previous_response_id"] = "resp_synthetic_routes_parent"
				}
				body, _ := json.Marshal(payload)
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				status, result := invokeBadRequestRoute(t, h, ctx, route, string(body), func(c *gin.Context) {
					if route == "chat" {
						// Chat translation does not carry the Responses prompt_cache_key.
						c.Request.Header.Set(downstreamAffinityHeader, "synthetic-routes-chat-continuation")
						h.store.BindSessionAffinity(resolveDownstreamAffinityID(c.Request.Header), accounts[0], "")
					}
					if tc.pinned {
						c.Request.Header.Set("x-codex-turn-state", "synthetic-turn")
					}
				})
				wantStatus := http.StatusBadRequest
				if tc.previous && !tc.complete {
					wantStatus = http.StatusConflict // Existing fail-closed context admission.
				}
				if calls.Load() != tc.calls || status != wantStatus {
					t.Fatalf("calls=%d want=%d status=%d body=%s", calls.Load(), tc.calls, status, result)
				}
			})
		}
	}
}

func accountsIDForBadRequestRoute(id int) string {
	return "synthetic-bad-request-" + strconv.Itoa(id)
}

func TestBadRequestRetryRoutesSSEFailureNotBareHTTP(t *testing.T) {
	for _, route := range []string{"responses", "chat", "compact"} {
		t.Run(route, func(t *testing.T) {
			var calls atomic.Int32
			h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10}, false, 1, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_synthetic_sse\",\"status\":\"failed\",\"error\":{\"code\":\"invalid_request\",\"message\":\"Bad Request\",\"status_code\":400}}}\n\n")
			})
			UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings { s.CompactViaResponses = true; return s })
			_, body := invokeBadRequestRoute(t, h, context.Background(), route, "", nil)
			if calls.Load() != 1 {
				t.Fatalf("SSE pseudo-400 retried: calls=%d body=%s", calls.Load(), body)
			}
			if gjson.Get(body, "error.message").String() == "" {
				t.Fatalf("expected final SSE error conversion: %s", body)
			}
		})
	}
}
