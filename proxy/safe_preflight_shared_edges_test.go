package proxy

// Shared Handler boundary regressions. All outbound traffic uses the acceptance
// fixture's certificate-verified TLS server and explicit loopback CONNECT proxy.
// No fake RoundTripper, executor replacement, authentication, tool execution, or
// continuation is used. The request classifier matrix is only a unit test; each
// HTTP case separately requires the actual outbound shared-path marker.
//
// These tests are deliberately serial. The long-generation case really waits
// beyond 60 seconds; the separate quota-window case explicitly ages only the
// retry-admission timestamp, and is NOT evidence of a long-lived generation.

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestSafePreflightSharedEdgesRequestBodyFormats(t *testing.T) {
	const base = `{"model":"gpt-5.5","input":"synthetic-input","stream":true`
	for _, tc := range []struct {
		name  string
		extra string
		want  bool
	}{
		{"plain_text_default", ``, true},
		{"explicit_plain_text", `,"text":{"format":{"type":"text"}}`, true},
		{"json_object", `,"text":{"format":{"type":"json_object"}}`, true},
		{"json_object_with_verbosity", `,"text":{"format":{"type":"json_object"},"verbosity":"low"}`, true},
		{"empty_tools", `,"tools":[],"text":{"format":{"type":"json_object"}}`, true},
		{"unknown_format", `,"text":{"format":{"type":"synthetic_future_format"}}`, false},
		{"json_schema_even_without_schema", `,"text":{"format":{"type":"json_schema"}}`, false},
		{"json_schema", `,"text":{"format":{"type":"json_schema","name":"synthetic","schema":{"type":"object"}}}`, false},
		{"known_format_unknown_field", `,"text":{"format":{"type":"json_object","strict":true}}`, false},
		{"plain_text_unknown_field", `,"text":{"format":{"type":"text","schema":{}}}`, false},
		{"missing_format_type", `,"text":{"format":{}}`, false},
		{"format_string", `,"text":{"format":"text"}`, false},
		{"format_array", `,"text":{"format":[]}`, false},
		{"format_type_number", `,"text":{"format":{"type":1}}`, false},
		{"text_string", `,"text":"synthetic"`, false},
		{"text_unknown_control", `,"text":{"format":{"type":"json_object"},"synthetic_control":true}`, false},
		{"verbosity_object", `,"text":{"verbosity":{}}`, false},
		{"duplicate_format_type", `,"text":{"format":{"type":"text","type":"json_object"}}`, false},
		{"escaped_duplicate_type", `,"text":{"format":{"type":"text","\u0074ype":"json_object"}}`, false},
		{"tools_object", `,"tools":{}`, false},
		{"tools_string", `,"tools":"none"`, false},
		{"tools_boolean", `,"tools":false`, false},
		{"tools_null_element", `,"tools":[null]`, false},
		{"hosted_tool", `,"tools":[{"type":"web_search"}]`, false},
		{"client_function_conservatively_excluded", `,"tools":[{"type":"function","name":"synthetic","parameters":{"type":"object"}}]`, false},
		{"tool_choice", `,"tool_choice":"none"`, false},
		{"previous_response", `,"previous_response_id":"resp_synthetic_previous"`, false},
		{"even_null_previous_response", `,"previous_response_id":null`, false},
		{"opaque_conversation", `,"conversation":"synthetic-conversation"`, false},
		{"context_management", `,"context_management":[{"type":"compaction"}]`, false},
		{"continue_thinking", `,"continue_thinking":true`, false},
		{"unknown_include", `,"include":["synthetic_control"]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := safePreflightRequestBody([]byte(base + tc.extra + `}`)); got != tc.want {
				t.Errorf("request eligibility=%t, want %t", got, tc.want)
			}
		})
	}
	for _, input := range []string{
		`[{"type":"function_call_output","call_id":"call_synthetic","output":"synthetic"}]`,
		`[{"type":"compaction","encrypted_content":"synthetic-not-a-real-continuation"}]`,
		`[{"role":"user","content":[{"type":"input_image","image_url":"synthetic-not-a-url"}]}]`,
	} {
		body := `{"model":"gpt-5.5","stream":true,"input":` + input + `,"text":{"format":{"type":"json_object"}}}`
		if safePreflightRequestBody([]byte(body)) {
			t.Error("tool-result/opaque/non-text input admitted to safe preflight")
		}
	}
}

// This only changes the synthetic downstream request. The TLS/proxy client and
// production Handler remain those of spAcceptanceNewHarness.
func spSharedEdgesRequest(t *testing.T, h *spAcceptanceHarness, body string, timeout time.Duration) (*http.Response, *bufio.Reader, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.downstream.URL+"/v1/responses", strings.NewReader(body))
	if err != nil {
		t.Fatal("could not construct synthetic edge request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(downstreamAffinityHeader, h.affinitySignal)
	client := *h.client
	client.Timeout = timeout
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal("loopback edge request failed (details deliberately redacted)")
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp, bufio.NewReader(resp.Body), cancel
}

func TestSafePreflightSharedEdgesKnownFormatsEnterRealHandler(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"json_object", spAcceptanceBody},
		// Spark is the existing text-only model: unlike an ordinary gpt-5.5
		// plain request, its translator does not inject a hosted image tool.
		// Do not strip that tool or change eligibility just for this test.
		{"plain_text_default", `{"model":"gpt-5.3-codex-spark","input":"synthetic-input","stream":true}`},
		{"explicit_plain_text", `{"model":"gpt-5.3-codex-spark","input":"synthetic-input","stream":true,"text":{"format":{"type":"text"}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					spAcceptanceFlush(w, spAcceptanceCreated("resp_synthetic_A")+spAcceptanceRate)
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
					}
					return
				}
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_B"))
			})
			t.Cleanup(gate.open)
			resp, reader, _ := spSharedEdgesRequest(t, h, tc.body, 10*time.Second)
			first := spAcceptanceReadEnvelope(t, reader)
			if first != spAcceptanceRate || h.calls.Load() != 1 {
				t.Fatal("known format did not publish original rate before retry")
			}
			gate.open()
			wire := first + spAcceptanceReadRest(t, reader)
			h.wait(t)
			h.provePath(t, 2)
			h.assertStableIdentity(t)
			if resp.StatusCode != http.StatusOK {
				t.Error("known no-tool format lost the streaming success status")
			}
			h.mu.Lock()
			for _, request := range h.wire {
				tools := gjson.GetBytes(request.body, "tools")
				if tools.Exists() && (!tools.IsArray() || len(tools.Array()) != 0) {
					t.Error("known-format fixture unexpectedly sent a tool declaration")
				}
			}
			h.mu.Unlock()
			spAcceptanceNoPrivateA(t, wire)
			spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_B")
		})
	}
}

// Install synthetic admission context before entering the same Handler method.
// Routes are built before the first request, rather than adding middleware to
// an already-registered Gin route (which would silently fail to exercise it).
// The acceptance fixture's completion/cancellation observations are retained.
func spSharedEdgesAdmission(t *testing.T, h *spAcceptanceHarness, prepare func(*gin.Context)) {
	t.Helper()
	router := gin.New()
	router.POST("/v1/responses", func(c *gin.Context) {
		stop := context.AfterFunc(c.Request.Context(), func() { h.cancelOnce.Do(func() { close(h.canceled) }) })
		defer stop()
		prepare(c)
		h.handler.Responses(c)
		h.done <- spAcceptanceResult{c.Writer.Status(), c.GetInt(AccessLogStatusContextKey)}
	})
	// Never mutate http.Server.Handler after Serve has started. The original
	// unused fixture listener keeps its registered cleanup; this listener is
	// configured before startup and still uses the acceptance HTTP client.
	h.downstream.Close()
	h.router = router
	h.downstream = httptest.NewServer(router)
	t.Cleanup(h.downstream.Close)
}

func TestSafePreflightSharedEdgesScopeConcurrencyOneReacquiresOwnLease(t *testing.T) {
	const keyID, otherKeyID = int64(972041), int64(972042)
	rateRead := spAcceptanceNewGate()
	type snapshot struct {
		leaseID            uint64
		leases, own, other int
	}
	observed := make(chan snapshot, 2)
	var scope database.APIKeyScopeLimit
	var scopeGate *scopeBudgetGate
	h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, r *http.Request, n int) {
		s := snapshot{own: APIKeyScopeInflight(keyID, scope.ScopeType, scope.ScopeID), other: APIKeyScopeInflight(otherKeyID, scope.ScopeType, scope.ScopeID)}
		scopeGate.concurrencyMu.Lock()
		s.leases = len(scopeGate.leases)
		if s.leases == 1 {
			s.leaseID = scopeGate.leases[0].id
		}
		scopeGate.concurrencyMu.Unlock()
		select {
		case observed <- s:
		default:
		}
		if n == 1 {
			spAcceptanceFlush(w, spAcceptanceRate)
			if rateRead.wait(r.Context()) {
				spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
			}
			return
		}
		spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_scope"))
	})
	t.Cleanup(rateRead.open)
	scope = database.APIKeyScopeLimit{ScopeType: database.APIKeyScopeTypeAccount, ScopeID: h.account.ID(), MaxConcurrency: 1}
	scopeGate = newScopeBudgetGate(keyID, []database.APIKeyScopeLimit{scope})
	t.Cleanup(scopeGate.releaseLeases)
	foreign := apiKeyScopeConcurrency.acquire(scopeSkipKey{apiKeyID: otherKeyID, scopeType: scope.ScopeType, scopeID: scope.ScopeID})
	t.Cleanup(func() { apiKeyScopeConcurrency.release(foreign) })
	spSharedEdgesAdmission(t, h, func(c *gin.Context) {
		c.Set(contextAPIKeyID, keyID)
		c.Set(contextScopeBudgetGate, scopeGate)
	})
	_, reader, _ := h.request(t)
	first := spAcceptanceReadEnvelope(t, reader)
	if first != spAcceptanceRate {
		t.Fatal("scope fixture did not reach real downstream rate barrier")
	}
	rateRead.open()
	wire := first + spAcceptanceReadRest(t, reader)
	h.wait(t)
	h.provePath(t, 2)
	h.assertStableIdentity(t)
	spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_scope")
	var leaseIDs []uint64
	for i := 0; i < 2; i++ {
		select {
		case s := <-observed:
			if s.own != 1 || s.other != 1 || s.leases != 1 || s.leaseID == 0 {
				t.Error("each actual attempt must own exactly one independent scope lease")
			}
			leaseIDs = append(leaseIDs, s.leaseID)
		default:
			t.Fatal("retry did not reach upstream with a reacquired scope lease")
		}
	}
	if leaseIDs[0] == leaseIDs[1] {
		t.Error("retry retained its old scope lease instead of releasing before selection")
	}
	if APIKeyScopeInflight(keyID, scope.ScopeType, scope.ScopeID) != 0 || APIKeyScopeInflight(otherKeyID, scope.ScopeType, scope.ScopeID) != 1 {
		t.Error("request leaked its own scope lease or released another API key's lease")
	}
}

func TestSafePreflightSharedEdgesSharedTerminalSemantics(t *testing.T) {
	const empty = `{"type":"response.incomplete","response":{"id":"resp_synthetic_A","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[],"usage":{"input_tokens":3,"output_tokens":0,"total_tokens":3,"output_tokens_details":{"reasoning_tokens":0}}}}`
	const truncated = `{"type":"response.completed","response":{"id":"resp_synthetic_truncated","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"synthetic-half"}]}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}`
	for _, tc := range []struct {
		name, frames      string
		budget, calls     int
		terminal, id      string
		privateA, healthy bool
	}{
		{"done_success", strings.Replace(spAcceptanceCompleted("resp_synthetic_done"), `"type":"response.completed"`, `"type":"response.done"`, 1), 1, 1, "response.completed", "resp_synthetic_done", false, false},
		{"done_failed_retries", spAcceptanceFrame(strings.Replace(spAcceptanceFailed, `"type":"response.failed"`, `"type":"response.done"`, 1)), 1, 2, "response.completed", "resp_synthetic_B", true, false},
		{"completed_with_failure_retries", spAcceptanceFrame(strings.Replace(spAcceptanceFailed, `"type":"response.failed"`, `"type":"response.completed"`, 1)), 1, 2, "response.completed", "resp_synthetic_B", true, false},
		{"completed_with_incomplete_status", spAcceptanceFrame(truncated), 2, 1, "response.incomplete", "resp_synthetic_truncated", false, false},
		{"empty_incomplete_retries", spAcceptanceFrame(empty), 1, 2, "response.completed", "resp_synthetic_B", true, false},
		{"empty_incomplete_zero_budget", spAcceptanceFrame(empty), 0, 1, "response.failed", "", false, true},
		{"reasoning_usage_closes_replay", spAcceptanceFrame(strings.Replace(empty, `"reasoning_tokens":0`, `"reasoning_tokens":1`, 1)), 2, 1, "response.failed", "", false, true},
		{"delta_then_empty_incomplete_is_real_truncation", spAcceptanceFrame(`{"type":"response.output_text.delta","delta":"synthetic-half"}`) + spAcceptanceFrame(empty), 2, 1, "response.incomplete", "resp_synthetic_A", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := spAcceptanceNewHarness(t, tc.budget, nil, func(w http.ResponseWriter, _ *http.Request, n int) {
				if n == 1 {
					spAcceptanceFlush(w, spAcceptanceRate+tc.frames)
					return
				}
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_B"))
			})
			before := spAcceptanceHealthOf(h.account)
			resp, reader, _ := h.request(t)
			wire := spAcceptanceReadRest(t, reader)
			result := h.wait(t)
			h.provePath(t, tc.calls)
			if resp.StatusCode != http.StatusOK {
				t.Error("terminal handling changed already-committed HTTP status")
			}
			spAcceptanceTerminal(t, wire, tc.terminal, tc.id)
			if strings.Contains(wire, `"type":"response.done"`) {
				t.Error("shared terminal normalization was bypassed")
			}
			if tc.privateA {
				spAcceptanceNoPrivateA(t, wire)
				h.assertStableIdentity(t)
				if strings.Contains(wire, "empty_incomplete_response") || strings.Contains(wire, `"type":"response.incomplete"`) {
					t.Error("discarded empty-incomplete attempt escaped the successful retry")
				}
			}
			if tc.healthy && (spAcceptanceHealthOf(h.account) != before || result.logicalStatus != http.StatusBadGateway) {
				t.Error("empty-incomplete lost its shared 502/non-account-fault semantics")
			}
		})
	}
}

// Quota storage is a fresh, synthetic SQLite database, separate from Handler
// usage logging. Only the real quota consumer receives it; no global DB state,
// auth middleware, production account store, or existing DB file is accessed.
func spSharedEdgesQuota(t *testing.T) (string, *database.DB, *apiKeyModelRequestQuota) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic-shared-quota.db")
	db, err := database.New("sqlite", path)
	if err != nil {
		t.Fatal("could not create isolated edge quota database")
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetUsageLogConfig("off", 100, 60)
	id, err := db.InsertAPIKeyWithOptions(context.Background(), database.APIKeyInput{
		Name: "synthetic-shared-edges", Key: "synthetic-not-an-authentication-key",
		Limits: database.APIKeyLimits{ModelRequestLimits: []database.APIKeyModelRequestLimit{{ID: "synthetic-initial", Model: "gpt-5.5", MaxRequests: 10, Timezone: "UTC"}}},
	})
	if err != nil {
		t.Fatal("could not create synthetic quota row")
	}
	row, err := db.GetAPIKeyByID(context.Background(), id)
	if err != nil {
		t.Fatal("could not read synthetic quota configuration")
	}
	return path, db, &apiKeyModelRequestQuota{db: db, keyID: id, requestID: "synthetic-shared-logical-request", rules: row.Limits.ModelRequestLimits}
}

func spSharedEdgesAttachQuota(t *testing.T, h *spAcceptanceHarness, quota *apiKeyModelRequestQuota) {
	t.Helper()
	spSharedEdgesAdmission(t, h, func(c *gin.Context) {
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), apiKeyModelRequestContextKey{}, quota))
	})
}

func TestSafePreflightSharedEdgesDedicatedQuotaErrorAfterRate(t *testing.T) {
	_, db, quota := spSharedEdgesQuota(t)
	gate := spAcceptanceNewGate()
	h := spAcceptanceNewHarness(t, 2, nil, func(w http.ResponseWriter, r *http.Request, _ int) {
		spAcceptanceFlush(w, spAcceptanceRate)
		if gate.wait(r.Context()) {
			spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
		}
	})
	t.Cleanup(gate.open)
	spSharedEdgesAttachQuota(t, h, quota)
	before := spAcceptanceHealthOf(h.account)
	resp, reader, _ := h.request(t)
	first := spAcceptanceReadEnvelope(t, reader)
	if first != spAcceptanceRate {
		t.Fatal("quota change did not follow genuine downstream rate delivery")
	}
	// A newly installed rule is authoritative even for the old authentication
	// snapshot. Exhaust it with another synthetic logical request. Do not forge
	// a quota error or change the current logical request's durable charge ID.
	rules := []database.APIKeyModelRequestLimit{{ID: "synthetic-new-rule", Model: "gpt-5.5", MaxRequests: 1, Timezone: "UTC"}}
	if err := db.UpdateAPIKeyLimits(context.Background(), quota.keyID, database.APIKeyLimits{ModelRequestLimits: rules}); err != nil {
		t.Fatal("could not install synthetic replacement quota rule")
	}
	if hit, err := db.ConsumeAPIKeyModelRequest(context.Background(), quota.keyID, "synthetic-other-logical-request", "gpt-5.5", rules, time.Now()); err != nil || hit != nil {
		t.Fatal("could not exhaust synthetic replacement quota rule")
	}
	gate.open()
	wire := first + spAcceptanceReadRest(t, reader)
	result := h.wait(t)
	if result.logicalStatus != http.StatusTooManyRequests {
		t.Errorf("quota rejection logical status=%d, want 429 even after wire 200", result.logicalStatus)
	}
	state := h.provePath(t, 1)
	if state == nil {
		t.Fatal("quota case never entered the shared Handler path")
	}
	state.mu.Lock()
	attempts, sent := state.attempts, state.sent
	state.mu.Unlock()
	if attempts != 2 || sent || resp.StatusCode != http.StatusOK {
		t.Error("quota rejection did not stop the second attempt before send while preserving HTTP 200")
	}
	spAcceptanceTerminal(t, wire, "error", "")
	var quotaErrors int
	for _, line := range strings.Split(wire, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := gjson.Parse(strings.TrimSpace(strings.TrimPrefix(line, "data: ")))
		if payload.Get("type").String() == "error" {
			quotaErrors++
			if payload.Get("error.code").String() != "rate_limit_reached" || payload.Get("error.details.used").Int() != 1 || payload.Get("error.details.limit").Int() != 1 || payload.Get("error.details.rule_id").String() != "synthetic-new-rule" {
				t.Error("shared Handler replaced dedicated quota details with a generic upstream/local failure")
			}
		}
	}
	if quotaErrors != 1 || spAcceptanceHealthOf(h.account) != before {
		t.Error("quota failure duplicated its terminal or penalized the upstream account")
	}
}

// A real SQLite write transaction holds the quota DB, not a transport hook or
// a fake quota callback. The consumer's mutex plus DB InUse proves it has
// entered ConsumeAPIKeyModelRequestQuota and is waiting with its pre-send
// request already constructed. This transaction is always released on cleanup.
func spSharedEdgesHoldSQLiteWriter(t *testing.T, path string) func() {
	t.Helper()
	lockDB, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_txlock=immediate&_pragma=busy_timeout%283000%29")
	if err != nil {
		t.Fatal("could not open synthetic quota lock connection")
	}
	lockDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = lockDB.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	tx, err := lockDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal("could not hold synthetic quota writer transaction")
	}
	var once sync.Once
	release := func() { once.Do(func() { _ = tx.Rollback() }) }
	t.Cleanup(release)
	return release
}

func spSharedEdgesWaitQuota(t *testing.T, h *spAcceptanceHarness, state *safePreflightState, quota *apiKeyModelRequestQuota, db *database.DB) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		state.mu.Lock()
		secondUnsent := state.attempts == 2 && !state.sent
		state.mu.Unlock()
		if secondUnsent {
			locked := quota.mu.TryLock()
			if locked {
				quota.mu.Unlock()
			} else if db.Stats().InUse > 0 {
				if h.calls.Load() != 1 {
					t.Fatal("second upstream request escaped before quota admission completed")
				}
				return
			}
		}
		select {
		case <-tick.C:
		case <-h.done:
			t.Fatal("Handler ended instead of reaching the real quota wait")
		case <-deadline.C:
			t.Fatal("could not prove second attempt blocked in real quota storage")
		}
	}
}

func TestSafePreflightSharedEdgesFinalGuardAfterQuotaWait(t *testing.T) {
	for _, kind := range []string{"unchanged_positive_control", "downstream_cancel", "retry_window_expired", "credential_generation_changed", "resolved_proxy_changed"} {
		t.Run(kind, func(t *testing.T) {
			path, db, quota := spSharedEdgesQuota(t)
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					spAcceptanceFlush(w, spAcceptanceRate)
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
					}
					return
				}
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_quota_wait"))
			})
			t.Cleanup(gate.open)
			spSharedEdgesAttachQuota(t, h, quota)
			var changedProxyCalls atomic.Int32
			var changedURL *url.URL
			var changedTransport *http.Transport
			if kind == "resolved_proxy_changed" {
				changed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					changedProxyCalls.Add(1)
					http.Error(w, "synthetic forbidden changed proxy", http.StatusBadGateway)
				}))
				t.Cleanup(changed.Close)
				var err error
				changedURL, err = url.Parse(changed.URL)
				if err != nil {
					t.Fatal("invalid synthetic changed proxy URL")
				}
				entry, ok := clientPool.Load("account-bound|" + clientPoolKey(h.account, h.proxyURL, codexTransportModeStandard))
				if !ok {
					t.Fatal("missing synthetic pooled standard transport")
				}
				changedTransport = entry.(*poolEntry).client.Transport.(*http.Transport)
				originalDial := changedTransport.DialContext
				// Install before identity is established. Both allowed sockets
				// are loopback; a missing production proxy guard must reach the
				// changed recorder, not be masked by the fixture's dial denylist.
				changedTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
					if addr == changedURL.Host {
						return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, addr)
					}
					return originalDial(ctx, network, addr)
				}
			}
			before := spAcceptanceHealthOf(h.account)
			resp, reader, cancel := h.request(t)
			first := spAcceptanceReadEnvelope(t, reader)
			if first != spAcceptanceRate {
				t.Fatal("quota-wait fixture did not pin through genuine rate publication")
			}
			state := h.provePath(t, 1)
			if state == nil {
				t.Fatal("quota-wait fixture has no actual shared-path marker")
			}
			connectsBefore := h.connects.Load()
			release := spSharedEdgesHoldSQLiteWriter(t, path)
			gate.open()
			spSharedEdgesWaitQuota(t, h, state, quota, db)

			switch kind {
			case "downstream_cancel":
				cancel()
				resp.Body.Close()
				select {
				case <-h.canceled:
				case <-time.After(time.Second):
					t.Fatal("Handler did not observe real downstream cancellation during quota wait")
				}
			case "retry_window_expired":
				// Explicitly age admission only; do not pretend this is a 60s
				// generation or set a new deadline on the upstream context.
				state.mu.Lock()
				state.started = time.Now().Add(-61 * time.Second)
				state.mu.Unlock()
			case "credential_generation_changed":
				h.account.Mu().Lock()
				h.account.AccessToken = "synthetic-rotated-during-quota-wait"
				h.account.CredentialGeneration++
				h.account.Mu().Unlock()
			case "resolved_proxy_changed":
				// The second executor is blocked inside quota: no transport
				// config read overlaps this mutation. Its final clone must check
				// actual proxy resolution before sending even a CONNECT packet.
				changedTransport.Proxy = func(*http.Request) (*url.URL, error) { return changedURL, nil }
			}
			release()
			wire := first
			if kind != "downstream_cancel" {
				wire += spAcceptanceReadRest(t, reader)
			}
			result := h.wait(t)
			wantCalls := 1
			if kind == "unchanged_positive_control" {
				wantCalls = 2
			}
			h.provePath(t, wantCalls)
			state.mu.Lock()
			attempts, sent := state.attempts, state.sent
			state.mu.Unlock()
			if attempts != 2 {
				t.Error("quota barrier did not exercise the second attempt's final send guard")
			}
			if kind == "unchanged_positive_control" {
				h.assertStableIdentity(t)
				spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_quota_wait")
				usage, err := db.GetAPIKeyModelRequestUsage(context.Background(), quota.keyID, quota.rules, time.Now())
				if err != nil || len(usage) != 1 || usage[0].Used != 1 {
					t.Error("retry did not retain one durable logical-request quota charge")
				}
				return
			}
			if h.connects.Load() != connectsBefore || changedProxyCalls.Load() != 0 {
				t.Error("guard permitted an additional CONNECT packet after quota wait")
			}
			if kind != "resolved_proxy_changed" && sent {
				t.Error("final send permit was consumed despite cancellation/expiry/identity rejection")
			}
			if spAcceptanceHealthOf(h.account) != before {
				t.Error("local post-quota rejection penalized upstream account")
			}
			if kind == "downstream_cancel" && result.logicalStatus != logStatusClientClosed {
				t.Errorf("cancelled pre-send request logical status=%d, want 499", result.logicalStatus)
			}
			if kind != "downstream_cancel" {
				spAcceptanceTerminal(t, wire, "response.failed", "")
				if result.logicalStatus != http.StatusBadGateway {
					t.Error("quota wait failed for another reason instead of the local final guard")
				}
			}
		})
	}
}

func spSharedEdgesReadRest(t *testing.T, reader io.Reader, limit int64) string {
	t.Helper()
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(body)) > limit {
		t.Fatal("synthetic edge stream failed to finish within its downstream fixture bound")
	}
	return string(body)
}

func TestSafePreflightSharedEdgesThirdCommittedAttemptKeepsLargeEvents(t *testing.T) {
	gate := spAcceptanceNewGate()
	largeText := strings.Repeat("x", (1<<20)+4096)
	largeFrame := spAcceptanceFrame(`{"type":"response.output_text.delta","delta":"` + largeText + `"}`)
	h := spAcceptanceNewHarness(t, 9, nil, func(w http.ResponseWriter, r *http.Request, n int) {
		if n < 3 {
			spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spAcceptanceFailed))
			return
		}
		spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceCreated("resp_synthetic_large")+spAcceptanceFrame(`{"type":"response.output_text.delta","delta":"synthetic-commit-barrier"}`))
		if gate.wait(r.Context()) {
			spAcceptanceFlush(w, largeFrame+spAcceptanceCompleted("resp_synthetic_large"))
		}
	})
	t.Cleanup(gate.open)
	_, reader, _ := h.request(t)
	var prefix strings.Builder
	for !strings.Contains(prefix.String(), "synthetic-commit-barrier") {
		prefix.WriteString(spAcceptanceReadEnvelope(t, reader))
	}
	state := h.provePath(t, 3)
	if state == nil {
		t.Fatal("large-event fixture did not enter shared path")
	}
	state.mu.Lock()
	committed, attempts := state.protocolCommitted, state.attempts
	state.mu.Unlock()
	if !committed || attempts != 3 {
		t.Fatal("large event was not ordered after the third attempt's actual protocol commit")
	}
	gate.open()
	wire := prefix.String() + spSharedEdgesReadRest(t, reader, 2<<20)
	h.wait(t)
	h.provePath(t, 3)
	h.assertStableIdentity(t)
	spAcceptanceNoPrivateA(t, wire)
	spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_large")
	var largeDeltas int
	for _, line := range strings.Split(wire, "\n") {
		if strings.HasPrefix(line, "data: ") {
			var event struct {
				Type  string `json:"type"`
				Delta string `json:"delta"`
			}
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event) == nil && event.Type == "response.output_text.delta" && event.Delta == largeText {
				largeDeltas++
			}
		}
	}
	if largeDeltas != 1 {
		t.Error("already-committed event over 1 MiB was truncated, duplicated, or rejected")
	}
}

func TestSafePreflightSharedEdgesCommittedGenerationOutlivesRetryWindow(t *testing.T) {
	readBarrier, abort := spAcceptanceNewGate(), spAcceptanceNewGate()
	h := spAcceptanceNewHarness(t, 9, nil, func(w http.ResponseWriter, r *http.Request, _ int) {
		spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(`{"type":"response.output_text.delta","delta":"synthetic-long-generation-commit"}`))
		if !readBarrier.wait(r.Context()) {
			return
		}
		// Real elapsed time, not state.started manipulation. A new experiment
		// timeout imposed at ingress, send, or commit would cut this stream.
		timer := time.NewTimer(61 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_long_generation"))
		case <-r.Context().Done():
		case <-abort.ch:
		}
	})
	t.Cleanup(abort.open)
	t.Cleanup(readBarrier.open)
	started := time.Now()
	resp, reader, _ := spSharedEdgesRequest(t, h, spAcceptanceBody, 75*time.Second)
	var prefix strings.Builder
	for !strings.Contains(prefix.String(), "synthetic-long-generation-commit") {
		prefix.WriteString(spAcceptanceReadEnvelope(t, reader))
	}
	state := h.provePath(t, 1)
	if state == nil {
		t.Fatal("long-generation fixture did not enter shared path")
	}
	state.mu.Lock()
	committed := state.protocolCommitted
	state.mu.Unlock()
	if !committed {
		t.Fatal("long generation was not selected before the real read barrier")
	}
	readBarrier.open()
	wire := prefix.String() + spAcceptanceReadRest(t, reader)
	h.wait(t)
	h.provePath(t, 1)
	if time.Since(started) <= 60*time.Second || resp.StatusCode != http.StatusOK {
		t.Error("fixture did not preserve HTTP success beyond the real retry-start window")
	}
	spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_long_generation")
}
