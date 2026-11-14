package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Real handler/executor paths, but only synthetic identities and loopback HTTP.
// The HTTP controls and late Payload transport switches share the same budget.
func TestAuthorReviewBadRequestAfterPayloadTransportSwitch(t *testing.T) {
	for _, forceWS := range []bool{false, true} {
		for _, catchAll := range []bool{false, true} {
			for _, route := range []string{"responses", "chat", "messages"} {
				t.Run(fmt.Sprintf("ws=%t/catch_all=%t/%s", forceWS, catchAll, route), func(t *testing.T) {
					var httpCalls atomic.Int32
					h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}, catchAll, 1, func(w http.ResponseWriter, r *http.Request) {
						httpCalls.Add(1)
						body := readUpstreamRequestBody(r)
						if r.Method != http.MethodPost || gjson.GetBytes(body, "service_tier").String() != "ultrafast" {
							t.Error("loopback did not receive the final accelerated HTTP POST")
						}
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, endpointBareBadRequest)
					})
					h.store.UpdateAccountPlanType(accounts[0], "promax")
					runtime := CurrentRuntimeSettings()
					runtime.CodexForceWebsocket = forceWS
					ApplyRuntimeSettings(runtime)
					withPayloadRules(t, `{"override":[{"params":{"service_tier":"ultrafast"}}]}`)
					ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
					defer cancel()
					var status int
					if route == "messages" {
						status, _ = invokeBadRequestEndpoint(t, h, ctx, route)
					} else {
						status, _ = invokeBadRequestRoute(t, h, ctx, route, "", nil)
					}
					if status != http.StatusBadRequest || httpCalls.Load() != 2 {
						t.Fatalf("HTTP calls=%d status=%d, want exactly 2 calls and 400", httpCalls.Load(), status)
					}
				})
			}
		}
	}
}

func TestAuthorReviewBadRequestDoesNotRetrySyntheticWS400(t *testing.T) {
	for _, handshake := range []bool{false, true} {
		for _, route := range []string{"responses", "chat", "messages"} {
			t.Run(fmt.Sprintf("handshake=%t/%s", handshake, route), func(t *testing.T) {
				var wsCalls, httpCalls atomic.Int32
				h, _ := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}, false, 1, func(w http.ResponseWriter, _ *http.Request) {
					httpCalls.Add(1)
					w.WriteHeader(http.StatusBadRequest)
				})
				runtime := CurrentRuntimeSettings()
				runtime.CodexForceWebsocket = true
				ApplyRuntimeSettings(runtime)
				withPayloadRules(t, `{}`)
				WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
					wsCalls.Add(1)
					resp := &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(endpointBareBadRequest))}
					if handshake {
						resp.Request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1/unused-handshake", nil)
					}
					return resp, nil
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				var status int
				if route == "messages" {
					status, _ = invokeBadRequestEndpoint(t, h, ctx, route)
				} else {
					status, _ = invokeBadRequestRoute(t, h, ctx, route, "", nil)
				}
				if status != http.StatusBadRequest || wsCalls.Load() != 1 || httpCalls.Load() != 0 {
					t.Fatalf("status=%d WS calls=%d HTTP calls=%d, want 400/1/0", status, wsCalls.Load(), httpCalls.Load())
				}
			})
		}
	}
}

func TestAuthorReviewSearchContinuationKeepsCompactedInput(t *testing.T) {
	for _, scenario := range []string{"http_error", "stream_failure", "credential_changed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("CODEX_OVERFLOW_COMPACT_TAIL_KB", "1")
			resetResponseCacheStateForTest(testResponseCacheConfig())
			t.Cleanup(resetResponseCacheForTest)
			withPayloadRules(t, `{}`)
			const summary = "SYNTHETIC_COMPACT_SUMMARY"
			latest := "LATEST_SYNTHETIC_TURN_" + strings.Repeat("x", 1200)
			var calls atomic.Int32
			var account *auth.Account
			h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{}, false, 1, func(w http.ResponseWriter, r *http.Request) {
				body := readUpstreamRequestBody(r)
				switch calls.Add(1) {
				case 1:
					if !strings.Contains(string(body), "fixture-opaque") || !strings.Contains(string(body), "SEARCH_FIXTURE") || !strings.Contains(string(body), latest) {
						t.Error("first attempt did not materialize complete private search history")
					}
					if gjson.GetBytes(body, "previous_response_id").Exists() {
						t.Error("logical response ID leaked upstream")
					}
					if scenario == "stream_failure" {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"status_code\":400,\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"context length exceeded\"}}}\n\n")
					} else {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, `{"error":{"code":"context_length_exceeded","message":"context length exceeded"}}`)
					}
				case 2:
					if !strings.Contains(string(body), "conversation compaction assistant") {
						t.Error("second request must be the existing internal summary call")
					}
					if scenario == "credential_changed" {
						account.Mu().Lock()
						account.CredentialGeneration++
						account.Mu().Unlock()
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n", summary)
					_, _ = io.WriteString(w, searchContextSSE("resp_synthetic_summary", searchContextMessage(summary)))
				case 3:
					input := gjson.GetBytes(body, "input").Array()
					if len(input) != 2 || input[0].Get("role").String() != "developer" || input[0].Get("content.0.text").String() != overflowCompactSummaryPrefix+summary || !strings.Contains(input[1].Raw, latest) {
						t.Error("retry replaced the compacted summary/tail with the original replay")
					}
					compactedInput := gjson.GetBytes(body, "input").Raw
					if strings.Contains(compactedInput, "encrypted_content") || strings.Contains(compactedInput, "function_call_output") {
						t.Error("retry restored compacted private reasoning/tool history")
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, searchContextSSE("resp_synthetic_final", searchContextMessage("finished")))
				default:
					t.Error("unexpected extra summary or retry")
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			account = accounts[0]
			runtime := CurrentRuntimeSettings()
			runtime.CodexPreflightSSEPassthrough = false
			ApplyRuntimeSettings(runtime)
			previous := seedSearchContext(t, responseCacheOwner(0), account, true)
			raw, err := json.Marshal(map[string]any{
				"model": "gpt-5.5", "stream": scenario == "stream_failure", "previous_response_id": previous,
				"input": []any{map[string]any{"role": "user", "content": latest}},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			status, _ := invokeBadRequestRoute(t, h, ctx, "responses", string(raw), func(c *gin.Context) {
				c.Set(contextAPIKeyRow, &database.APIKeyRow{Limits: database.APIKeyLimits{AutoCompactOnOverflow: true}})
			})
			wantStatus, wantCalls := http.StatusOK, int32(3)
			if scenario == "credential_changed" {
				wantStatus, wantCalls = http.StatusConflict, 2
			}
			if status != wantStatus || calls.Load() != wantCalls {
				t.Fatalf("status=%d calls=%d, want %d/%d", status, calls.Load(), wantStatus, wantCalls)
			}
		})
	}
}

// Exercise cancellation from the HTTP request through the actual affinity
// selector, including release of the temporary borrowed slot.
func TestAuthorReviewAffinityOverflowWaitRequestCancellation(t *testing.T) {
	for _, cancelBeforeSelection := range []bool{false, true} {
		t.Run(fmt.Sprintf("already_canceled=%t", cancelBeforeSelection), func(t *testing.T) {
			t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MS", "2000")
			t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MIN_BYTES", "0")
			store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1})
			t.Cleanup(store.Stop)
			bound := &auth.Account{DBID: 1, AccessToken: "synthetic-bound", Status: auth.StatusReady}
			fallback := &auth.Account{DBID: 2, AccessToken: "synthetic-fallback", Status: auth.StatusReady}
			store.AddAccount(bound)
			store.AddAccount(fallback)
			store.BindSessionAffinity("synthetic-affinity", bound, "")
			if held := store.TakePreferredAccountWithDispatch(bound.ID(), 0, nil, nil, auth.DispatchPolicyStandard); held != bound {
				t.Fatal("could not occupy the bound account")
			}
			defer store.Release(bound)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelBeforeSelection {
				cancel()
			} else {
				timer := time.AfterFunc(50*time.Millisecond, cancel)
				defer timer.Stop()
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`)).WithContext(ctx)
			opts := selectionOptionsForRequest(c, "gpt-5.5", auth.DispatchPolicyStandard)
			started := time.Now()
			h := NewHandler(store, nil, nil, nil)
			selected, _, _ := h.nextAccountForSessionWithDispatchGuard(ctx, "synthetic-affinity", 0, nil, nil, opts)
			if selected != nil {
				store.Release(selected)
				t.Error("canceled request borrowed an account")
			}
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Errorf("cancellation did not interrupt the affinity wait: %s", elapsed)
			}
			lease := store.TakePreferredAccountWithDispatch(fallback.ID(), 0, nil, nil, auth.DispatchPolicyStandard)
			if lease != fallback {
				t.Fatal("canceled wait leaked the temporary borrowed slot")
			}
			store.Release(lease)
			if id, ok := store.SessionAffinityAccountID("synthetic-affinity"); !ok || id != bound.ID() {
				t.Error("canceled wait changed the original binding")
			}
		})
	}
}
