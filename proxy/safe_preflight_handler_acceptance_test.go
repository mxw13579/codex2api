package proxy

// Contract acceptance, not an implementation model. These tests deliberately
// enter Handler.Responses, and never call serveSafePreflight or its classifier
// helpers to produce an expected answer. The only white-box path observation is
// read-only inspection of the final outbound request's safe-preflight context.
//
// All credentials, response IDs and policy evidence are synthetic. Networking
// is restricted to loopback: a real standard Transport uses one explicit CONNECT
// proxy, which tunnels only to a TLS httptest server with certificate verification.
// No executor/RoundTripper replacement, Resin, attestation, remote authentication,
// tool execution, or continuation request is used. No test runs in parallel.

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	// An ordinary unstructured gpt-5.5 request receives an implicit hosted image
	// tool in the existing translator, and is correctly INELIGIBLE for V1.
	// JSON mode is a normal supported request shape that suppresses that tool;
	// do not delete tools or relax the product's eligibility just for a fixture.
	spAcceptanceBody = `{"model":"gpt-5.5","input":[{"role":"user","content":"synthetic-input"}],"stream":true,"text":{"format":{"type":"json_object"}}}`
	// Deliberately non-normalized JSON and CRLF: equality checks the original
	// upstream envelope, not a reserialized schema-helper result.
	spAcceptanceRate   = "event: codex.rate_limits\r\ndata: { \"type\":\"codex.rate_limits\", \"rate_limits\":{\"primary\":{\"used_percent\":10,\"window_minutes\":300}} }\r\n\r\n"
	spAcceptanceError  = `{"type":"error","error":{"type":"server_error","code":"server_is_overloaded","status_code":503,"message":"synthetic-private-overload"}}`
	spAcceptanceFailed = `{"type":"response.failed","response":{"id":"resp_synthetic_A","status":"failed","error":{"type":"server_error","code":"server_is_overloaded","status_code":503,"message":"synthetic-private-overload"}}}`
	spAcceptanceUA     = "codex_cli_rs/0.160.0 (Linux 6.8.0; x86_64) synthetic-acceptance"
)

func spAcceptanceFrame(data string) string { return "data: " + data + "\n\n" }

func spAcceptanceCreated(id string) string {
	return spAcceptanceFrame(`{"type":"response.created","response":{"id":"` + id + `","status":"in_progress","output":[]}}`)
}

func spAcceptanceCompleted(id string) string {
	return spAcceptanceFrame(`{"type":"response.completed","response":{"id":"` + id + `","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"synthetic-success"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
}

func spAcceptanceFlush(w http.ResponseWriter, wire string) {
	_, _ = io.WriteString(w, wire)
	w.(http.Flusher).Flush()
}

// A gate is released by the real downstream reader, not by a timer or an
// upstream observation. Cleanup also opens it so a failed assertion cannot
// strand httptest.Server.Close.
type spAcceptanceGate struct {
	ch   chan struct{}
	once sync.Once
}

func spAcceptanceNewGate() *spAcceptanceGate { return &spAcceptanceGate{ch: make(chan struct{})} }
func (g *spAcceptanceGate) open()            { g.once.Do(func() { close(g.ch) }) }
func (g *spAcceptanceGate) wait(ctx context.Context) bool {
	select {
	case <-g.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

type spAcceptanceSend struct {
	state    *safePreflightState
	attempt  int
	affinity string
	proxy    string
}

type spAcceptanceWireRequest struct {
	headers http.Header
	body    []byte
	host    string
	uri     string
}

type spAcceptanceResult struct {
	httpStatus    int
	logicalStatus int
}

type spAcceptanceHarness struct {
	store          *auth.Store
	account        *auth.Account
	handler        *Handler
	router         *gin.Engine
	downstream     *httptest.Server
	client         *http.Client
	proxyURL       string
	affinitySignal string
	calls          atomic.Int32
	connects       atomic.Int32
	badRoute       atomic.Bool
	mu             sync.Mutex
	sends          []spAcceptanceSend
	wire           []spAcceptanceWireRequest
	done           chan spAcceptanceResult
	canceled       chan struct{}
	cancelOnce     sync.Once
}

func spAcceptanceSettings(t *testing.T) {
	t.Helper()
	previous := CurrentRuntimeSettings()
	previousRules := CurrentPayloadRules()
	previousResin, previousResinEgress := resinCfg.Load(), auth.ResinEgressEnabled()
	previousMode := gin.Mode()
	previousKeepalive := continuousRetryKeepaliveInterval
	t.Cleanup(func() {
		ApplyRuntimeSettings(previous)
		currentPayloadRuleSet.Store(previousRules)
		resinCfg.Store(previousResin)
		auth.SetResinEgressEnabled(previousResinEgress)
		gin.SetMode(previousMode)
		continuousRetryKeepaliveInterval = previousKeepalive
	})
	// Setenv registers restoration with t.Cleanup; never inspect host env files.
	t.Setenv("CODEX_TRANSPORT_MODE", "standard")
	t.Setenv("CODEX_FINGERPRINT_DEBUG", "0")
	t.Setenv("CODEX_DISABLE_CAPACITY_SHED_HANDLING", "0")
	t.Setenv("CODEX_SESSION_HEADER_MODE", "native")
	t.Setenv("CODEX_SESSION_HEADER_ALIGN_CONVERGED", "0")
	t.Setenv(codexMultiAgentV2CompatEnv, "0")
	gin.SetMode(gin.TestMode)
	resinCfg.Store(nil)
	auth.SetResinEgressEnabled(false)
	continuousRetryKeepaliveInterval = time.Hour // enabled explicitly in heartbeat case
	if err := SetPayloadRulesJSON("{}"); err != nil {
		t.Fatal("could not install empty synthetic payload rules")
	}
	settings := DefaultRuntimeSettings()
	settings.CodexSafePreflightRetry = true
	settings.CodexForceWebsocket = false
	settings.CodexPreflightSSEPassthrough = false
	settings.CodexTelemetryEnabled = false
	settings.CodexTelemetryTimingDebug = false
	settings.CodexUpstreamCookieLogEnabled = false
	settings.CodexContinueThinking = false
	settings.OverflowAutoCompact = false
	settings.CompactViaResponses = false
	settings.CodexRequestCompression = false
	settings.CodexCLIVersionSyncEnabled = false
	settings.CodexOverloadPauseEnabled = false
	settings.FirstTokenTimeoutSec = 0
	settings.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	settings.RequestIsolationMode = RequestIsolationModeIsolated
	ApplyRuntimeSettings(settings)
}

func spAcceptanceNewHarness(t *testing.T, retries int, db *database.DB, script func(http.ResponseWriter, *http.Request, int)) *spAcceptanceHarness {
	t.Helper()
	spAcceptanceSettings(t)
	h := &spAcceptanceHarness{
		done: make(chan spAcceptanceResult, 8), canceled: make(chan struct{}),
		affinitySignal: "synthetic-acceptance-" + t.Name(),
	}
	// A local-only affinity signal gives provenance a nonempty, test-specific
	// key without supplying an upstream session or masking UUID instability.
	affinityHeaders := make(http.Header)
	affinityHeaders.Set(downstreamAffinityHeader, h.affinitySignal)
	affinityKey := resolveDownstreamAffinityID(affinityHeaders)
	oldOrigin, hadOrigin := codexTurnStateOrigins.Load(affinityKey)
	codexTurnStateOrigins.Delete(affinityKey)
	t.Cleanup(func() {
		codexTurnStateOrigins.Delete(affinityKey)
		if hadOrigin {
			codexTurnStateOrigins.Store(affinityKey, oldOrigin)
		}
	})
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Host != "chatgpt.com" || r.URL.Path != "/backend-api/codex/responses" || r.Header.Get("Upgrade") != "" {
			h.badRoute.Store(true)
			http.Error(w, "synthetic route rejected", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			h.badRoute.Store(true)
			http.Error(w, "synthetic body read failed", http.StatusBadRequest)
			return
		}
		h.mu.Lock()
		h.wire = append(h.wire, spAcceptanceWireRequest{r.Header.Clone(), body, r.Host, r.URL.RequestURI()})
		h.mu.Unlock()
		n := int(h.calls.Add(1))
		w.Header().Set("Content-Type", "text/event-stream")
		script(w, r, n)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal("invalid synthetic TLS URL")
	}

	// CONNECT never dials the requested public hostname. Its only destination
	// is the TLS fixture's loopback socket. TLS is still verified end-to-end.
	var tunnels sync.Map
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != "chatgpt.com:443" {
			h.badRoute.Store(true)
			http.Error(w, "synthetic proxy rejected route", http.StatusForbidden)
			return
		}
		target, err := net.DialTimeout("tcp", upstreamURL.Host, 2*time.Second)
		if err != nil {
			http.Error(w, "synthetic proxy unavailable", http.StatusBadGateway)
			return
		}
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			target.Close()
			return
		}
		tunnels.Store(client, target)
		defer func() {
			client.Close()
			target.Close()
			tunnels.Delete(client)
		}()
		h.connects.Add(1)
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		if buffered.Flush() != nil {
			return
		}
		copied := make(chan struct{})
		go func() {
			_, _ = io.Copy(target, buffered)
			target.Close()
			close(copied)
		}()
		_, _ = io.Copy(client, target)
		client.Close()
		<-copied
	}))
	t.Cleanup(func() {
		tunnels.Range(func(k, v any) bool {
			k.(net.Conn).Close()
			v.(net.Conn).Close()
			return true
		})
		proxyServer.Close()
	})
	h.proxyURL = proxyServer.URL
	proxyURL, err := url.Parse(proxyServer.URL)
	if err != nil {
		t.Fatal("invalid synthetic proxy URL")
	}
	h.account = &auth.Account{
		DBID: 971031, AccessToken: "synthetic-access-only", AccountID: "synthetic-account-only",
		PlanType: "pro", CredentialGeneration: 1, ProxyURL: proxyServer.URL,
		CustomHeaders: map[string]string{"User-Agent": spAcceptanceUA},
	}
	h.store = auth.NewStore(nil, nil, &database.SystemSettings{
		MaxConcurrency: 1, TestConcurrency: 1, MaxRetries: retries,
		MaxRateLimitRetries: 0, RetryIntervalMS: 0,
	})
	t.Cleanup(h.store.Stop)
	// NewStore interprets a zero persisted retry setting as its legacy default.
	// Exercise the explicit runtime zero budget rather than that bootstrap path.
	h.store.SetMaxRetries(retries)
	h.store.SetMaxRateLimitRetries(0)
	h.store.AddAccount(h.account)

	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	transport := &http.Transport{
		// The httptest certificate is valid for example.com. Using its name
		// and its private root is fixture routing, NOT InsecureSkipVerify.
		TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS12},
		Proxy: func(r *http.Request) (*url.URL, error) {
			state := safePreflightFromContext(r.Context())
			attempt := 0
			if state != nil {
				state.mu.Lock()
				attempt = state.attempts
				state.mu.Unlock()
			}
			h.mu.Lock()
			h.sends = append(h.sends, spAcceptanceSend{state, attempt, CodexTurnStateAffinityKeyFromContext(r.Context()), proxyServer.URL})
			h.mu.Unlock()
			return proxyURL, nil
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if addr != proxyURL.Host {
				return nil, errors.New("synthetic transport denied non-loopback route")
			}
			return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, addr)
		},
	}
	// Seed ONLY this synthetic account + explicit proxy's two pool keys. Both
	// routes use the same real standard transport, so falling back to the old
	// path cannot pass just because the fake upstream happened to succeed.
	for _, prefix := range []string{"", "account-bound|"} {
		key := prefix + clientPoolKey(h.account, proxyServer.URL, codexTransportModeStandard)
		previous, existed := clientPool.Load(key)
		entry := &poolEntry{createdAt: time.Now().UnixNano(), client: &http.Client{Transport: transport}}
		entry.touch()
		clientPool.Store(key, entry)
		t.Cleanup(func() {
			clientPool.Delete(key)
			if existed {
				clientPool.Store(key, previous)
			}
		})
	}
	t.Cleanup(transport.CloseIdleConnections)
	h.handler = NewHandler(h.store, db, &config.Config{AllowAnonymousV1: true}, nil)
	h.router = gin.New()
	h.router.Use(func(c *gin.Context) {
		stop := context.AfterFunc(c.Request.Context(), func() { h.cancelOnce.Do(func() { close(h.canceled) }) })
		defer stop()
		c.Next()
		h.done <- spAcceptanceResult{c.Writer.Status(), c.GetInt(AccessLogStatusContextKey)}
	})
	h.router.POST("/v1/responses", h.handler.Responses)
	h.downstream = httptest.NewServer(h.router)
	t.Cleanup(h.downstream.Close)
	downstreamTransport := &http.Transport{Proxy: nil}
	t.Cleanup(downstreamTransport.CloseIdleConnections)
	h.client = &http.Client{Transport: downstreamTransport, Timeout: 10 * time.Second}
	return h
}

func (h *spAcceptanceHarness) request(t *testing.T) (*http.Response, *bufio.Reader, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.downstream.URL+"/v1/responses", strings.NewReader(spAcceptanceBody))
	if err != nil {
		t.Fatal("could not construct synthetic downstream request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(downstreamAffinityHeader, h.affinitySignal)
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatal("loopback Handler request failed (details deliberately redacted)")
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp, bufio.NewReader(resp.Body), cancel
}

func spAcceptanceReadEnvelope(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	var wire strings.Builder
	for wire.Len() <= 1<<20 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal("downstream ended before a complete SSE read barrier")
		}
		wire.WriteString(line)
		if line == "\n" || line == "\r\n" {
			return wire.String()
		}
	}
	t.Fatal("downstream SSE envelope exceeds fixture bound")
	return ""
}

func spAcceptanceReadRest(t *testing.T, reader io.Reader) string {
	t.Helper()
	body, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil {
		t.Fatal("downstream failed to finish its SSE stream")
	}
	return string(body)
}

func (h *spAcceptanceHarness) wait(t *testing.T) spAcceptanceResult {
	t.Helper()
	select {
	case result := <-h.done:
		if h.account.GetActiveRequests() != 0 || h.account.GetOccupiedRequests() != 0 {
			t.Error("Handler leaked account active/occupied lease")
		}
		return result
	case <-time.After(8 * time.Second):
		t.Fatal("Handler did not finish within bounded local fixture time")
		return spAcceptanceResult{}
	}
}

func (h *spAcceptanceHarness) provePath(t *testing.T, wantCalls int) *safePreflightState {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if got := int(h.calls.Load()); got != wantCalls {
		t.Errorf("actual upstream calls=%d, want %d", got, wantCalls)
	}
	if h.badRoute.Load() || h.connects.Load() == 0 {
		t.Error("request did not exclusively traverse the explicit loopback CONNECT proxy")
	}
	if len(h.sends) == 0 {
		t.Fatal("no final-send observation: cannot claim safe-preflight coverage")
	}
	first := h.sends[0].state
	for i, send := range h.sends {
		if send.state == nil || send.state != first || send.attempt != i+1 || send.proxy != h.proxyURL {
			t.Error("actual outbound request did not enter the shared safe-preflight path")
		}
	}
	if len(h.sends) != int(h.calls.Load()) {
		t.Error("final sends and actual TLS upstream requests disagree")
	}
	for _, request := range h.wire {
		if request.headers.Get("X-Oai-Attestation") != "" || request.headers.Get("Cookie") != "" || request.headers.Get("Upgrade") != "" {
			t.Error("unexpected attestation, cookie, or WebSocket traffic in isolated fixture")
		}
	}
	return first
}

// Parse downstream SSE independently of readSafePreflightSSE and all candidate
// terminal classifiers. A trailing JSON object, empty 200, [DONE]-only stream,
// duplicate terminal, or incomplete envelope must not count as a result.
func spAcceptanceTerminal(t *testing.T, wire, wantType, wantID string) {
	t.Helper()
	wire = strings.ReplaceAll(wire, "\r\n", "\n")
	if !strings.HasSuffix(wire, "\n\n") {
		t.Error("stream did not end with a complete SSE envelope")
	}
	terminalCount := 0
	for _, envelope := range strings.Split(strings.TrimSuffix(wire, "\n\n"), "\n\n") {
		if envelope == "" {
			continue
		}
		var data []string
		var event string
		for _, line := range strings.Split(envelope, "\n") {
			switch {
			case strings.HasPrefix(line, ":"):
			case strings.HasPrefix(line, "event:"):
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			default:
				t.Error("non-SSE bytes or appended JSON in downstream stream")
			}
		}
		if len(data) == 0 {
			continue
		}
		if terminalCount != 0 {
			t.Error("downstream event appeared after the selected terminal")
		}
		var payload struct {
			Type     string `json:"type"`
			Response struct {
				ID     string `json:"id"`
				Status string `json:"status"`
				Error  struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			} `json:"response"`
		}
		if json.Unmarshal([]byte(strings.Join(data, "\n")), &payload) != nil || payload.Type == "" {
			t.Error("downstream contains an invalid or non-event JSON payload")
			continue
		}
		if event != "" && event != payload.Type {
			t.Error("SSE event name differs from payload type")
		}
		switch payload.Type {
		case "response.completed", "response.incomplete", "response.failed", "error":
			terminalCount++
			if payload.Type != wantType || (wantID != "" && payload.Response.ID != wantID) {
				t.Error("wrong downstream terminal type or response identity")
			}
			if payload.Type == "response.failed" && (payload.Response.Status != "failed" || payload.Response.Error.Code == "" || payload.Response.Error.Message == "") {
				t.Error("failure terminal lacks explicit failed status and structured error")
			}
		}
	}
	if terminalCount != 1 {
		t.Errorf("visible terminal count=%d, want exactly one", terminalCount)
	}
}

func spAcceptanceNoPrivateA(t *testing.T, wire string) {
	t.Helper()
	if strings.Contains(wire, "resp_synthetic_A") || strings.Contains(wire, "synthetic-private-overload") || strings.Contains(wire, "server_is_overloaded") {
		t.Error("failed attempt identity/error escaped to downstream")
	}
}

func (h *spAcceptanceHarness) assertStableIdentity(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.wire) < 2 {
		t.Error("cannot prove outbound identity stability without two actual sends")
		return
	}
	first := h.wire[0]
	if first.headers.Get("Authorization") != "Bearer synthetic-access-only" || first.headers.Get("Chatgpt-Account-Id") != "synthetic-account-only" {
		t.Error("actual outbound authorization/account identity is missing or incorrect")
	}
	if first.headers.Get("Session-Id") == "" || gjson.GetBytes(first.body, "prompt_cache_key").String() == "" {
		t.Error("actual outbound per-request session/cache identity is missing")
	}
	for _, next := range h.wire[1:] {
		// Compare actual wire values, including model/body/session/thread. Never
		// format credentials, complete headers, payloads, or their digests.
		if !reflect.DeepEqual(first.headers, next.headers) || !bytes.Equal(first.body, next.body) || first.host != next.host || first.uri != next.uri {
			t.Error("actual outbound authorization/account/body/session/route identity changed")
		}
	}
}

type spAcceptanceHealth struct {
	streak int
	kind   string
	last   time.Time
	cool   time.Time
	status auth.AccountStatus
}

func spAcceptanceHealthOf(a *auth.Account) spAcceptanceHealth {
	a.Mu().RLock()
	defer a.Mu().RUnlock()
	return spAcceptanceHealth{a.FailureStreak, a.LastFailureKind, a.LastFailureAt, a.CooldownUtil, a.Status}
}

func TestSafePreflightHandlerAcceptanceRateReadBarrierAndIdentity(t *testing.T) {
	gate := spAcceptanceNewGate()
	var readBarrier, secondBeforeRead atomic.Bool
	h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			spAcceptanceFlush(w, spAcceptanceCreated("resp_synthetic_A")+spAcceptanceRate)
			if gate.wait(r.Context()) {
				spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
			}
			return
		}
		if !readBarrier.Load() {
			secondBeforeRead.Store(true)
		}
		spAcceptanceFlush(w, spAcceptanceCreated("resp_synthetic_B")+spAcceptanceCompleted("resp_synthetic_B"))
	})
	t.Cleanup(gate.open)
	resp, reader, _ := h.request(t)
	first := spAcceptanceReadEnvelope(t, reader)
	if first != spAcceptanceRate {
		t.Error("first real downstream frame is not the original upstream rate envelope")
	}
	if h.calls.Load() != 1 {
		t.Error("upstream retry happened before downstream rate-frame read barrier")
	}
	readBarrier.Store(true)
	gate.open()
	wire := first + spAcceptanceReadRest(t, reader)
	h.wait(t)
	h.provePath(t, 2)
	h.assertStableIdentity(t)
	if secondBeforeRead.Load() || resp.StatusCode != http.StatusOK {
		t.Error("rate read ordering or committed HTTP status violated")
	}
	spAcceptanceNoPrivateA(t, wire)
	spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_B")
	if strings.Count(wire, "codex.rate_limits") != 2 { // one event field + one JSON type
		t.Error("rate frame was synthesized, replayed, or lost")
	}
}

func TestSafePreflightHandlerAcceptanceFiniteBudgets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		budget int
		calls  int
	}{{"zero_means_no_retry", 0, 1}, {"one_means_one_retry", 1, 2}, {"large_finite_budget_still_max_three", 9, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			h := spAcceptanceNewHarness(t, tc.budget, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceCreated("resp_synthetic_A")+spAcceptanceRate+spAcceptanceFrame(spAcceptanceFailed))
			})
			resp, reader, _ := h.request(t)
			wire := spAcceptanceReadRest(t, reader)
			h.wait(t)
			h.provePath(t, tc.calls)
			spAcceptanceTerminal(t, wire, "response.failed", "")
			if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
				t.Error("already-published rate stream changed HTTP framing")
			}
			if strings.Count(wire, "event: codex.rate_limits") != tc.calls {
				t.Error("genuine attempt rate envelopes lost or replayed")
			}
		})
	}
}

func TestSafePreflightHandlerAcceptanceTerminalOrderAndCommitBoundary(t *testing.T) {
	text := spAcceptanceFrame(`{"type":"response.output_text.delta","delta":"synthetic-public-text"}`)
	tool := spAcceptanceFrame(`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_synthetic","call_id":"call_synthetic","name":"synthetic_client_function","arguments":""}}`)
	for _, tc := range []struct {
		name     string
		first    string
		budget   int
		calls    int
		terminal string
		id       string
		visible  string
		privateA bool
	}{
		{"error_then_failed", spAcceptanceFrame(spAcceptanceError) + spAcceptanceFrame(spAcceptanceFailed), 1, 2, "response.completed", "resp_synthetic_B", "", true},
		{"error_then_EOF", spAcceptanceFrame(spAcceptanceError), 1, 2, "response.completed", "resp_synthetic_B", "", true},
		{"error_then_completed", spAcceptanceFrame(spAcceptanceError) + spAcceptanceCompleted("resp_synthetic_A"), 1, 1, "response.completed", "resp_synthetic_A", "", false},
		{"text_then_failed_no_replay", text + spAcceptanceFrame(spAcceptanceFailed), 2, 1, "response.failed", "", "synthetic-public-text", false},
		{"tool_identity_then_failed_no_replay", tool + spAcceptanceFrame(spAcceptanceFailed), 2, 1, "response.failed", "", "call_synthetic", false},
		{"text_then_EOF_is_failure", text, 2, 1, "response.failed", "", "synthetic-public-text", false},
		{"rate_then_EOF_is_not_empty_200", "", 0, 1, "response.failed", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, tc.budget, nil, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					// Also cover rate -> created, not just created -> rate. For
					// text/tools, do not fail/EOF until the real client reads them.
					prefix, tail := tc.first, ""
					if tc.visible != "" {
						failure := spAcceptanceFrame(spAcceptanceFailed)
						if strings.HasSuffix(prefix, failure) {
							prefix, tail = strings.TrimSuffix(prefix, failure), failure
						}
					}
					spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceCreated("resp_synthetic_A")+prefix)
					if tc.visible != "" && gate.wait(r.Context()) && tail != "" {
						spAcceptanceFlush(w, tail)
					}
					return
				}
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_B"))
			})
			t.Cleanup(gate.open)
			_, reader, _ := h.request(t)
			wire := ""
			if tc.visible != "" {
				for !strings.Contains(wire, tc.visible) {
					wire += spAcceptanceReadEnvelope(t, reader)
				}
				if h.calls.Load() != 1 {
					t.Error("retry began before the downstream content/tool read barrier")
				}
				gate.open()
			}
			wire += spAcceptanceReadRest(t, reader)
			h.wait(t)
			h.provePath(t, tc.calls)
			spAcceptanceTerminal(t, wire, tc.terminal, tc.id)
			if tc.privateA {
				spAcceptanceNoPrivateA(t, wire)
			}
			if tc.terminal == "response.completed" && strings.Contains(wire, "synthetic-private-overload") {
				t.Error("superseded error candidate escaped successful terminal")
			}
			if tc.visible != "" && !strings.Contains(wire, tc.visible) {
				t.Error("already-selected attempt's text/tool identity was discarded")
			}
		})
	}
}

func TestSafePreflightHandlerAcceptanceEarlyHTTPCommitOmitsTurnState(t *testing.T) {
	for _, heartbeat := range []bool{false, true} {
		name := "rate"
		if heartbeat {
			name = "heartbeat"
		}
		t.Run(name, func(t *testing.T) {
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					w.Header().Set(codexTurnStateHeader, "synthetic-turn-A")
					wire := spAcceptanceCreated("resp_synthetic_A")
					if !heartbeat {
						wire += spAcceptanceRate
					}
					spAcceptanceFlush(w, wire)
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
					}
					return
				}
				w.Header().Set(codexTurnStateHeader, "synthetic-turn-B")
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_B"))
			})
			t.Cleanup(gate.open)
			if heartbeat {
				continuousRetryKeepaliveInterval = 20 * time.Millisecond
			}
			resp, reader, _ := h.request(t)
			first := spAcceptanceReadEnvelope(t, reader)
			if heartbeat {
				if !strings.HasPrefix(first, ":") {
					t.Error("HTTP was not committed by a real downstream heartbeat")
				}
			} else if first != spAcceptanceRate {
				t.Error("HTTP was not committed by the original rate envelope")
			}
			if resp.Header.Get(codexTurnStateHeader) != "" {
				t.Error("attempt-bound turn-state leaked into actual early HTTP headers")
			}
			gate.open()
			wire := first + spAcceptanceReadRest(t, reader)
			h.wait(t)
			h.provePath(t, 2)
			spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_B")
			spAcceptanceNoPrivateA(t, wire)
			if resp.Header.Get(codexTurnStateHeader) != "" || resp.Trailer.Get(codexTurnStateHeader) != "" || strings.Contains(wire, "synthetic-turn-") {
				t.Error("turn-state was retained or invented as an SSE/trailer substitute")
			}
			h.mu.Lock()
			sends := append([]spAcceptanceSend(nil), h.sends...)
			h.mu.Unlock()
			for _, send := range sends {
				if send.affinity == "" {
					t.Error("missing affinity key makes provenance assertion vacuous")
					continue
				}
				if _, claimed := codexTurnStateOrigins.Load(send.affinity); claimed {
					t.Error("source provenance claims a turn-state header never delivered to the client")
				}
				key := send.affinity
				t.Cleanup(func() { ClearCodexTurnStateProvenance(key) })
			}
		})
	}
}

// A real Handler writer boundary, not safePreflightState.write in isolation.
// The first non-empty write delivers a prefix, then reports either a short
// write without an error or an explicit partial-write error.
type spAcceptanceFaultWriter struct {
	header http.Header
	writes int
	bytes  int
	fail   error
}

func (w *spAcceptanceFaultWriter) Header() http.Header { return w.header }
func (w *spAcceptanceFaultWriter) WriteHeader(int)     {}
func (w *spAcceptanceFaultWriter) Flush()              {}
func (w *spAcceptanceFaultWriter) Write(p []byte) (int, error) {
	w.writes++
	n := len(p) / 2
	w.bytes += n
	return n, w.fail
}

func TestSafePreflightHandlerAcceptanceCancellationAndFailedWrites(t *testing.T) {
	t.Run("cancel_after_real_rate_read", func(t *testing.T) {
		gate := spAcceptanceNewGate()
		h := spAcceptanceNewHarness(t, 2, nil, func(w http.ResponseWriter, r *http.Request, _ int) {
			spAcceptanceFlush(w, spAcceptanceRate)
			if gate.wait(r.Context()) {
				spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
			}
		})
		t.Cleanup(gate.open)
		before := spAcceptanceHealthOf(h.account)
		resp, reader, cancel := h.request(t)
		if spAcceptanceReadEnvelope(t, reader) != spAcceptanceRate {
			t.Fatal("cancellation fixture did not read the real rate frame")
		}
		cancel()
		resp.Body.Close()
		select {
		case <-h.canceled:
		case <-time.After(3 * time.Second):
			t.Fatal("server did not observe real downstream cancellation")
		}
		gate.open() // allow normal bounded usage drain, not another attempt
		h.wait(t)
		h.provePath(t, 1)
		if spAcceptanceHealthOf(h.account) != before {
			t.Error("downstream cancellation penalized upstream account")
		}
	})
	for _, short := range []bool{true, false} {
		name := "partial_write_error"
		if short {
			name = "short_write_without_error"
		}
		t.Run(name, func(t *testing.T) {
			h := spAcceptanceNewHarness(t, 2, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spAcceptanceFailed))
			})
			before := spAcceptanceHealthOf(h.account)
			writer := &spAcceptanceFaultWriter{header: make(http.Header)}
			if !short {
				writer.fail = io.ErrClosedPipe
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(spAcceptanceBody)).WithContext(ctx)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(downstreamAffinityHeader, h.affinitySignal)
			h.router.ServeHTTP(writer, req)
			h.wait(t)
			h.provePath(t, 1)
			if writer.writes != 1 || writer.bytes == 0 {
				t.Error("failed/short downstream write was ignored or followed by another write")
			}
			if spAcceptanceHealthOf(h.account) != before {
				t.Error("local downstream write failure penalized upstream account")
			}
		})
	}
}

func TestSafePreflightHandlerAcceptancePinnedIdentityMutationBlocksSend(t *testing.T) {
	for _, kind := range []string{"actual_authorization_header", "actual_body_payload_rule", "resolved_proxy", "pinned_account_unavailable"} {
		t.Run(kind, func(t *testing.T) {
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, r *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceRate)
				if gate.wait(r.Context()) {
					spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
				}
			})
			t.Cleanup(gate.open)
			before := spAcceptanceHealthOf(h.account)
			var changedProxyCalls atomic.Int32
			changedProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				changedProxyCalls.Add(1)
				http.Error(w, "synthetic changed route must not be used", http.StatusBadGateway)
			}))
			t.Cleanup(changedProxy.Close)
			_, reader, _ := h.request(t)
			first := spAcceptanceReadEnvelope(t, reader)
			if first != spAcceptanceRate {
				t.Fatal("identity mutation was not ordered after downstream rate publication")
			}
			switch kind {
			case "actual_authorization_header":
				// Leave the token getter AND credential generation unchanged.
				// Only a guard of actual final request values detects this.
				h.account.Mu().Lock()
				h.account.CustomHeaders["Authorization"] = "Bearer synthetic-rotated-header-only"
				h.account.Mu().Unlock()
			case "actual_body_payload_rule":
				if err := SetPayloadRulesJSON(`{"override":[{"params":{"instructions":"synthetic-body-changed-after-pin"}}]}`); err != nil {
					t.Fatal("could not install synthetic body mutation")
				}
			case "resolved_proxy":
				h.account.Mu().Lock()
				h.account.ProxyURL = changedProxy.URL
				h.account.Mu().Unlock()
				// A rejected attempt may still allocate its standard client.
				key := "account-bound|" + clientPoolKey(h.account, changedProxy.URL, codexTransportModeStandard)
				old, existed := clientPool.Load(key)
				t.Cleanup(func() {
					if v, ok := clientPool.LoadAndDelete(key); ok {
						v.(*poolEntry).client.CloseIdleConnections()
					}
					if existed {
						clientPool.Store(key, old)
					}
				})
			case "pinned_account_unavailable":
				// Removal after publication must fail closed, not switch back
				// to ordinary selection or produce an empty committed 200.
				h.store.RemoveAccount(h.account.ID())
			}
			gate.open()
			wire := first + spAcceptanceReadRest(t, reader)
			h.wait(t)
			state := h.provePath(t, 1)
			if state != nil {
				state.mu.Lock()
				attempts, sent := state.attempts, state.sent
				state.mu.Unlock()
				if kind == "pinned_account_unavailable" {
					if attempts != 1 {
						t.Error("new attempt started without reacquiring the pinned account")
					}
				} else if attempts != 2 || sent {
					t.Error("test did not reach a second attempt rejected before actual send")
				}
			}
			if changedProxyCalls.Load() != 0 {
				t.Error("changed proxy received traffic before identity rejection")
			}
			spAcceptanceTerminal(t, wire, "response.failed", "")
			if spAcceptanceHealthOf(h.account) != before {
				t.Error("local pinned-identity rejection penalized upstream account")
			}
		})
	}
}

// Swap only cache data (never its live mutex); preserve every old field and
// shared-backend reference without reading any old cache contents. Test data is
// memory-only, including the completed function identity used by the existing
// cache contract. No function is executed and no tool-result request is sent.
func spAcceptanceIsolateCache(t *testing.T) {
	t.Helper()
	isolated := &responseCacheState{
		store: make(map[string]*responseCacheEntry), sharedItems: make(map[[32]byte]*sharedResponseContextItem),
		lru: list.New(), markers: make(map[string]*responseCacheMarker), markerLRU: list.New(),
		chainOwners: make(map[string]*responseCacheChainOwner), chainLRU: list.New(),
		config: responseCacheConfig{maxBytes: 1 << 20, maxEntryBytes: 1 << 18, reconstructMaxBytes: 1 << 20,
			maxEntries: 32, ttl: time.Minute, maxItems: 100, writePolicy: database.ResponseCacheWritePolicyAlways},
	}
	swap := func() {
		respCache.mu.Lock()
		defer respCache.mu.Unlock()
		respCache.store, isolated.store = isolated.store, respCache.store
		respCache.sharedItems, isolated.sharedItems = isolated.sharedItems, respCache.sharedItems
		respCache.lru, isolated.lru = isolated.lru, respCache.lru
		respCache.markers, isolated.markers = isolated.markers, respCache.markers
		respCache.markerLRU, isolated.markerLRU = isolated.markerLRU, respCache.markerLRU
		respCache.chainOwners, isolated.chainOwners = isolated.chainOwners, respCache.chainOwners
		respCache.chainLRU, isolated.chainLRU = isolated.chainLRU, respCache.chainLRU
		respCache.config, isolated.config = isolated.config, respCache.config
		respCache.generation, isolated.generation = isolated.generation, respCache.generation
		respCache.stats, isolated.stats = isolated.stats, respCache.stats
		respCache.entrySerial, isolated.entrySerial = isolated.entrySerial, respCache.entrySerial
		respCache.runtimeCache, isolated.runtimeCache = isolated.runtimeCache, respCache.runtimeCache
		respCache.lastSyncAt, isolated.lastSyncAt = isolated.lastSyncAt, respCache.lastSyncAt
		respCache.lastSyncErr, isolated.lastSyncErr = isolated.lastSyncErr, respCache.lastSyncErr
	}
	swap()
	t.Cleanup(swap)
}

func TestSafePreflightHandlerAcceptancePreservesExistingCompletionAndGovernance(t *testing.T) {
	t.Run("completed_response_caches_continuation_state", func(t *testing.T) {
		spAcceptanceIsolateCache(t)
		const completed = `{"type":"response.completed","response":{"id":"resp_synthetic_cache","status":"completed","output":[{"type":"function_call","id":"fc_synthetic_cache","call_id":"call_synthetic_cache","name":"synthetic_client_function","arguments":"{}"}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`
		h := spAcceptanceNewHarness(t, 0, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
			spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(completed))
		})
		_, reader, _ := h.request(t)
		wire := spAcceptanceReadRest(t, reader)
		h.wait(t)
		h.provePath(t, 1)
		spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_cache")
		items := getResponseCache(responseCacheOwner(0), "resp_synthetic_cache")
		var input, call bool
		for _, item := range items {
			parsed := gjson.ParseBytes(item)
			input = input || (parsed.Get("role").String() == "user" && strings.Contains(string(item), "synthetic-input"))
			call = call || (parsed.Get("type").String() == "function_call" && parsed.Get("call_id").String() == "call_synthetic_cache")
		}
		if !input || !call {
			t.Error("successful new Handler path lost ordinary response.completed input/call continuation cache")
		}
		if len(getResponseCache(responseCacheOwner(971032), "resp_synthetic_cache")) != 0 {
			t.Error("completed response cache crossed API-key owner boundary")
		}
	})
	t.Run("committed_HTTP_200_keeps_failure_access_status", func(t *testing.T) {
		h := spAcceptanceNewHarness(t, 0, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
			spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spAcceptanceFailed))
		})
		resp, reader, _ := h.request(t)
		wire := spAcceptanceReadRest(t, reader)
		result := h.wait(t)
		h.provePath(t, 1)
		spAcceptanceTerminal(t, wire, "response.failed", "")
		if resp.StatusCode != 200 || result.httpStatus != 200 || result.logicalStatus != 503 {
			t.Errorf("access outcome lost: HTTP=%d writer=%d logical=%d; want 200/200/503", resp.StatusCode, result.httpStatus, result.logicalStatus)
		}
	})
	t.Run("cyber_policy_still_records_governance_incident", func(t *testing.T) {
		db, err := database.New("sqlite", filepath.Join(t.TempDir(), "synthetic-preflight-policy.db"))
		if err != nil {
			t.Fatal("could not create isolated synthetic policy database")
		}
		t.Cleanup(func() { _ = db.Close() })
		h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, _ *http.Request, _ int) {
			spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(`{"type":"response.failed","response":{"status":"failed","error":{"code":"cyber_policy","status_code":400,"message":"synthetic policy refusal"}}}`))
		})
		_, reader, _ := h.request(t)
		wire := spAcceptanceReadRest(t, reader)
		h.wait(t)
		h.provePath(t, 1)
		spAcceptanceTerminal(t, wire, "response.failed", "")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if !db.WaitPromptFilterAuditIdle(ctx) {
			t.Fatal("synthetic policy audit queue did not drain")
		}
		incidents, total, err := db.ListPromptPolicyIncidentsPage(ctx, database.PromptPolicyIncidentQuery{Page: 1, PageSize: 10, Endpoint: "/v1/responses"})
		if err != nil {
			t.Fatal("could not read isolated synthetic policy evidence")
		}
		if total != 1 || len(incidents) != 1 {
			t.Error("new path bypassed existing cyber-policy governance evidence callback")
			return
		}
		incident := incidents[0]
		if incident.UpstreamErrorCode != "cyber_policy" || incident.Transport != "sse" || incident.StatusCode != 400 || incident.AccountID != h.account.ID() || incident.AttemptIndex != 1 {
			t.Error("cyber-policy incident lost actual attempt/account/transport provenance")
		}
	})
}

func TestSafePreflightHandlerAcceptanceFirstTokenGuardStillApplies(t *testing.T) {
	abort := spAcceptanceNewGate()
	upstreamCanceled := make(chan struct{}, 1)
	h := spAcceptanceNewHarness(t, 0, nil, func(w http.ResponseWriter, r *http.Request, _ int) {
		// No rate/output frame: the existing first-token guard must remain
		// armed for lifecycle-only progress. A client deadline is NOT a pass.
		spAcceptanceFlush(w, spAcceptanceCreated("resp_synthetic_A"))
		select {
		case <-r.Context().Done():
			upstreamCanceled <- struct{}{}
		case <-abort.ch:
		}
	})
	t.Cleanup(abort.open)
	settings := CurrentRuntimeSettings()
	settings.FirstTokenTimeoutSec = 1
	ApplyRuntimeSettings(settings)
	started := time.Now()
	_, reader, _ := h.request(t)
	wire := spAcceptanceReadRest(t, reader)
	h.wait(t)
	h.provePath(t, 1)
	if elapsed := time.Since(started); elapsed < 700*time.Millisecond || elapsed > 4*time.Second {
		t.Errorf("first-token guard timing=%s, expected configured ~1s guard rather than client deadline", elapsed.Round(time.Millisecond))
	}
	select {
	case <-upstreamCanceled:
	case <-time.After(time.Second):
		t.Error("first-token guard did not cancel real upstream request")
	}
	spAcceptanceTerminal(t, wire, "response.failed", "")
	spAcceptanceNoPrivateA(t, wire)
}
