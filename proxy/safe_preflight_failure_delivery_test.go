package proxy

// Phase-1/3/4/5 review fixtures only. The existing Handler harness owns the real,
// certificate-verified loopback TLS + explicit CONNECT path. The wrapper below
// is OUTSIDE Gin and delegates real writes/flushes; it is not an upstream mock.
// A downstream reader must consume a complete terminal envelope before cancel.
// Holding Flush's return fixes that ordering, but does NOT invent a client ACK
// from http.Flusher (which has no error result). No test runs in parallel.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/api"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

const (
	spReviewFixDeliveryID = "resp_synthetic_review_delivery"
	spReviewFixContent    = `{"type":"response.output_text.delta","delta":"synthetic-review-visible-content"}`
	spReviewFixFailed     = `{"type":"response.failed","response":{"id":"resp_synthetic_review_delivery","status":"failed","output":[{"type":"function_call","id":"fc_synthetic_review","call_id":"call_synthetic_review","name":"synthetic_client_function","arguments":"{}"}],"error":{"type":"server_error","code":"server_error","status_code":502,"message":"synthetic-review-terminal-failure"},"usage":{"input_tokens":7,"output_tokens":11,"total_tokens":18,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":3}}}}`
	spReviewFixCompleted  = `{"type":"response.completed","response":{"id":"resp_synthetic_review_delivery","status":"completed","output":[{"type":"function_call","id":"fc_synthetic_review","call_id":"call_synthetic_review","name":"synthetic_client_function","arguments":"{}"}],"usage":{"input_tokens":7,"output_tokens":11,"total_tokens":18,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":3}}}}`
)

// A genuinely private failure: no output/tools/reasoning evidence. In the
// phase-3 rate-only fixtures the configured ZERO retry budget, not content or
// malformed control fields, selects the shared final renderer outside forward.
const spReviewFixRateOnlyFailed = `{"type":"response.failed","response":{"id":"resp_synthetic_review_delivery","status":"failed","output":[],"error":{"type":"server_error","code":"server_error","status_code":502,"message":"synthetic-review-rate-only-failure"},"usage":{"input_tokens":7,"output_tokens":0,"total_tokens":7,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":0}}}}`

type spReviewFixDeliveryProbe struct {
	fault           string
	terminalWire    atomic.Value // Synthetic ATTEMPTED bytes, never proof of delivery.
	blockFlush      bool
	flushEntered    *spAcceptanceGate
	canceled        <-chan struct{}
	abort           *spAcceptanceGate
	writes          atomic.Int32
	terminalWrites  atomic.Int32
	terminalBytes   atomic.Int64
	terminalLength  atomic.Int64
	terminalFlushes atomic.Int32
	flushSawCancel  atomic.Bool
}

type spReviewFixDeliveryWriter struct {
	http.ResponseWriter
	ctx             context.Context
	probe           *spReviewFixDeliveryProbe
	terminalPending bool
}

func (w *spReviewFixDeliveryWriter) Write(p []byte) (int, error) {
	probe := w.probe
	probe.writes.Add(1)
	if !bytes.Contains(p, []byte(`"type":"response.failed"`)) && !bytes.Contains(p, []byte(`"type":"response.completed"`)) &&
		!bytes.Contains(p, []byte(`"type":"error"`)) {
		return w.ResponseWriter.Write(p)
	}
	probe.terminalWrites.Add(1)
	probe.terminalLength.Store(int64(len(p)))
	probe.terminalWire.Store(string(p))
	wire := p
	switch probe.fault {
	case "short_without_error", "partial_with_error":
		wire = p[:len(p)/2]
	case "write_failure", "zero_without_error":
		wire = nil
	}
	var n int
	var err error
	if len(wire) != 0 {
		n, err = w.ResponseWriter.Write(wire)
	}
	probe.terminalBytes.Store(int64(n))
	if err == nil && probe.fault != "" && probe.fault != "short_without_error" && probe.fault != "zero_without_error" {
		err = io.ErrClosedPipe // Synthetic LOCAL write failure, not upstream evidence.
	}
	if n == len(p) && err == nil {
		w.terminalPending = true
	}
	return n, err
}

func (w *spReviewFixDeliveryWriter) Flush() {
	// Gin still owns its normal header/status bookkeeping. Do not unwrap it.
	w.ResponseWriter.(http.Flusher).Flush()
	if !w.terminalPending {
		return
	}
	w.terminalPending = false
	w.probe.terminalFlushes.Add(1)
	if w.probe.blockFlush {
		w.probe.flushEntered.open()
		select {
		case <-w.probe.canceled:
			w.probe.flushSawCancel.Store(w.ctx.Err() != nil)
		case <-w.probe.abort.ch:
			// Cleanup escape only; assertions require actual server cancellation.
		}
	}
}

func spReviewFixDeliveryWrap(t *testing.T, h *spAcceptanceHarness, fault string, block bool) *spReviewFixDeliveryProbe {
	t.Helper()
	probe := &spReviewFixDeliveryProbe{
		fault: fault, blockFlush: block, canceled: h.canceled,
		flushEntered: spAcceptanceNewGate(), abort: spAcceptanceNewGate(),
	}
	h.downstream.Close() // Replace an unused server, never mutate a live Handler.
	h.downstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.router.ServeHTTP(&spReviewFixDeliveryWriter{ResponseWriter: w, ctx: r.Context(), probe: probe}, r)
	}))
	t.Cleanup(h.downstream.Close)
	t.Cleanup(probe.abort.open)
	return probe
}

func spReviewFixFlushPolicy(policy string) {
	settings := CurrentRuntimeSettings()
	settings.StreamFlushPolicy = policy
	// In CONTENT fixtures the barrier establishes a previous stream-writer
	// flush, so coalesce can retain the terminal until its explicit flush.
	// RATE-ONLY final renderers bypass streamWriter: both settings below cover
	// that actual shared renderer, NOT coalesced terminal-buffer delivery.
	// Correctness does not depend on a sleep or winning an interval race.
	settings.StreamFlushIntervalMS = 1000
	ApplyRuntimeSettings(settings) // Restored by spAcceptanceNewHarness cleanup.
}

type spReviewFixUsageWant struct {
	status, input, output, total, cached, reasoning int
	retry                                           bool
	kind                                            string
}

// Shared with the private-status fixtures. Include 499 explicitly, sort by the
// actual attempt index, and compare every synthetic usage counter, not estimates
// or aggregate request counts. Query only the newly created temporary database.
func spReviewFixUsage(t *testing.T, db *database.DB, h *spAcceptanceHarness, wants ...spReviewFixUsageWant) {
	t.Helper()
	db.FlushUsageLogs()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.ListUsageLogsByFilter(ctx, database.UsageLogFilter{
		Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour), IncludeCanceled: true,
	})
	if err != nil || len(rows) != len(wants) {
		t.Fatalf("isolated usage row count=%d, want %d (query details redacted)", len(rows), len(wants))
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].AttemptIndex < rows[j].AttemptIndex })
	for i, want := range wants {
		row := rows[i]
		if row.StatusCode != want.status || row.AttemptIndex != i+1 || row.IsRetryAttempt != want.retry ||
			row.AccountID != h.account.ID() || row.Endpoint != "/v1/responses" || !row.Stream || row.ViaWebsocket {
			t.Errorf("usage attempt %d lost its HTTP provenance, retry flag, or logical outcome (status=%d, want %d)", i+1, row.StatusCode, want.status)
		}
		if row.InputTokens != want.input || row.OutputTokens != want.output || row.TotalTokens != want.total ||
			row.PromptTokens != want.input || row.CompletionTokens != want.output ||
			row.CachedTokens != want.cached || row.ReasoningTokens != want.reasoning {
			t.Errorf("usage attempt %d did not preserve exact synthetic input/output/total/cache/reasoning counters and aliases", i+1)
		}
		if row.UpstreamErrorKind != want.kind || (want.status == http.StatusOK) != (row.ErrorMessage == "") {
			t.Errorf("usage attempt %d has incorrect success/failure metadata", i+1)
		}
	}
}

func spReviewFixDeliveryUsage(t *testing.T, db *database.DB, h *spAcceptanceHarness, status int) {
	t.Helper()
	kind := ""
	if status == http.StatusBadGateway {
		kind = "server"
	}
	spReviewFixUsage(t, db, h, spReviewFixUsageWant{
		status: status, input: 7, output: 11, total: 18, cached: 2, reasoning: 3, kind: kind,
	})
}

func spReviewFixNoSuccess(t *testing.T, h *spAcceptanceHarness) {
	t.Helper()
	h.account.Mu().RLock()
	successes, lastSuccess := h.account.SuccessStreak, h.account.LastSuccessAt
	h.account.Mu().RUnlock()
	if successes != 0 || !lastSuccess.IsZero() {
		t.Error("failed/canceled terminal incorrectly reported account health success")
	}
	if len(getResponseCache(responseCacheOwner(0), spReviewFixDeliveryID)) != 0 {
		t.Error("failed/canceled terminal published successful continuation state")
	}
	if _, active := h.account.ModelCooldownFor("gpt-5.5"); !active {
		t.Error("failed/canceled terminal incorrectly cleared the in-flight model cooldown sentinel")
	}
}

func spReviewFixCooldownSentinel(h *spAcceptanceHarness) {
	// Set only AFTER the actual send/content read. This is not an admission or
	// authorization mutation; it detects accidental success cleanup in-flight.
	h.account.SetModelCooldownUntil("gpt-5.5", "synthetic-review-sentinel", time.Now().Add(time.Hour))
}

func TestSafePreflightReviewFixFailedTerminalDelivery(t *testing.T) {
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, lateCancel := range []bool{false, true} {
			name := policy + "/no_cancel_positive_control"
			if lateCancel {
				name = policy + "/cancel_after_terminal_read"
			}
			t.Run(name, func(t *testing.T) {
				spAcceptanceIsolateCache(t)
				db := spTerminalDeliveryDB(t)
				gate := spAcceptanceNewGate()
				h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
					spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spReviewFixContent))
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixFailed))
					}
				})
				t.Cleanup(gate.open)
				spReviewFixFlushPolicy(policy)
				probe := spReviewFixDeliveryWrap(t, h, "", lateCancel)
				resp, reader, cancel := h.request(t)
				first, content := spAcceptanceReadEnvelope(t, reader), spAcceptanceReadEnvelope(t, reader)
				if first != spAcceptanceRate || !strings.Contains(content, "synthetic-review-visible-content") {
					t.Fatal("missing genuine rate/content read barriers")
				}
				state := h.provePath(t, 1)
				if !state.committed() {
					t.Fatal("failure fixture did not commit content before terminal")
				}
				spReviewFixCooldownSentinel(h)
				gate.open()
				terminal := spAcceptanceReadEnvelope(t, reader)
				spAcceptanceTerminal(t, first+content+terminal, "response.failed", spReviewFixDeliveryID)
				if lateCancel {
					select {
					case <-probe.flushEntered.ch:
					case <-time.After(5 * time.Second):
						t.Fatal("terminal Flush did not reach its controlled return barrier")
					}
					select {
					case <-h.done:
						t.Fatal("Handler finalized while terminal Flush was held")
					default:
					}
					cancel() // Only AFTER the real client read the complete failed terminal.
					_ = resp.Body.Close()
					spTerminalDeliveryWaitCancel(t, h)
				} else if spAcceptanceReadRest(t, reader) != "" {
					t.Error("another event followed the selected failure terminal")
				}
				result := h.wait(t)
				h.provePath(t, 1)
				if probe.terminalWrites.Load() != 1 || probe.terminalFlushes.Load() != 1 || probe.terminalLength.Load() == 0 ||
					probe.terminalBytes.Load() != probe.terminalLength.Load() || lateCancel && !probe.flushSawCancel.Load() {
					t.Fatal("fixture did not establish full terminal write/flush and requested cancel ordering")
				}
				if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || result.logicalStatus != http.StatusBadGateway {
					t.Errorf("delivered failed terminal: HTTP=%d logical=%d, want 200/502 (neither 499 nor success)", result.httpStatus, result.logicalStatus)
				}
				spReviewFixDeliveryUsage(t, db, h, http.StatusBadGateway)
				spReviewFixNoSuccess(t, h)
				health := spAcceptanceHealthOf(h.account)
				if health.streak != 1 || health.kind != "server" || health.last.IsZero() {
					t.Error("delivered server failure did not retain exactly one semantic health failure")
				}
			})
		}
	}
}

func TestSafePreflightReviewFixCompletedTerminalLateCancel(t *testing.T) {
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		t.Run(policy, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			db := spTerminalDeliveryDB(t)
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spReviewFixContent))
				if gate.wait(r.Context()) {
					spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixCompleted))
				}
			})
			t.Cleanup(gate.open)
			spReviewFixFlushPolicy(policy)
			probe := spReviewFixDeliveryWrap(t, h, "", true)
			resp, reader, cancel := h.request(t)
			first, content := spAcceptanceReadEnvelope(t, reader), spAcceptanceReadEnvelope(t, reader)
			if first != spAcceptanceRate || !strings.Contains(content, "synthetic-review-visible-content") {
				t.Fatal("missing genuine rate/content read barriers")
			}
			if !h.provePath(t, 1).committed() {
				t.Fatal("success control did not enter the same committed Handler path")
			}
			spReviewFixCooldownSentinel(h)
			gate.open()
			spAcceptanceTerminal(t, first+content+spAcceptanceReadEnvelope(t, reader), "response.completed", spReviewFixDeliveryID)
			select {
			case <-probe.flushEntered.ch:
			case <-time.After(5 * time.Second):
				t.Fatal("successful terminal Flush did not reach the return barrier")
			}
			cancel()
			_ = resp.Body.Close()
			spTerminalDeliveryWaitCancel(t, h)
			result := h.wait(t)
			h.provePath(t, 1)
			if !probe.flushSawCancel.Load() || probe.terminalWrites.Load() != 1 || probe.terminalFlushes.Load() != 1 ||
				probe.terminalBytes.Load() != probe.terminalLength.Load() || probe.terminalLength.Load() == 0 {
				t.Fatal("success control did not exercise full delivery followed by cancel")
			}
			if result.httpStatus != http.StatusOK || result.logicalStatus != 0 && result.logicalStatus != http.StatusOK {
				t.Error("previously supported successful-terminal late cancel regressed")
			}
			spReviewFixDeliveryUsage(t, db, h, http.StatusOK)
			h.account.Mu().RLock()
			successes, lastSuccess := h.account.SuccessStreak, h.account.LastSuccessAt
			h.account.Mu().RUnlock()
			if successes != 1 || lastSuccess.IsZero() || spAcceptanceHealthOf(h.account).streak != 0 {
				t.Error("successful delivered terminal lost health success finalization")
			}
			if _, active := h.account.ModelCooldownFor("gpt-5.5"); active {
				t.Error("successful terminal did not perform ordinary model cooldown cleanup")
			}
			items := getResponseCache(responseCacheOwner(0), spReviewFixDeliveryID)
			var input, call bool
			for _, item := range items {
				input = input || bytes.Contains(item, []byte("synthetic-input"))
				call = call || bytes.Contains(item, []byte("call_synthetic_review"))
			}
			if !input || !call || len(getResponseCache(responseCacheOwner(971032), spReviewFixDeliveryID)) != 0 {
				t.Error("successful late-cancel control lost owner-isolated continuation cache")
			}
		})
	}
}

func TestSafePreflightReviewFixFailedTerminalDrainOnly(t *testing.T) {
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, committed := range []bool{false, true} {
			name := policy + "/rate_only"
			if committed {
				name = policy + "/content_committed"
			}
			t.Run(name, func(t *testing.T) {
				spAcceptanceIsolateCache(t)
				db := spTerminalDeliveryDB(t)
				gate := spAcceptanceNewGate()
				h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
					prefix := spAcceptanceRate
					if committed {
						prefix += spAcceptanceFrame(spReviewFixContent)
					}
					spAcceptanceFlush(w, prefix)
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixFailed))
					}
				})
				t.Cleanup(gate.open)
				spReviewFixFlushPolicy(policy)
				probe := spReviewFixDeliveryWrap(t, h, "", false)
				resp, reader, cancel := h.request(t)
				if spAcceptanceReadEnvelope(t, reader) != spAcceptanceRate {
					t.Fatal("missing genuine rate read barrier")
				}
				if committed && !strings.Contains(spAcceptanceReadEnvelope(t, reader), "synthetic-review-visible-content") {
					t.Fatal("missing genuine content read barrier")
				}
				state := h.provePath(t, 1)
				if state.committed() != committed {
					t.Fatal("fixture did not establish the requested commit boundary")
				}
				spReviewFixCooldownSentinel(h)
				before := spAcceptanceHealthOf(h.account)
				writes := probe.writes.Load()
				cancel()
				_ = resp.Body.Close()
				spTerminalDeliveryWaitCancel(t, h)
				gate.open() // The terminal now exists ONLY in bounded upstream usage drain.
				result := h.wait(t)
				h.provePath(t, 1)
				if result.logicalStatus != logStatusClientClosed {
					t.Errorf("drain-only failure status=%d, want 499", result.logicalStatus)
				}
				if probe.writes.Load() != writes || probe.terminalWrites.Load() != 0 || probe.terminalFlushes.Load() != 0 {
					t.Error("drain-only failure attempted downstream delivery after cancellation")
				}
				spReviewFixDeliveryUsage(t, db, h, logStatusClientClosed)
				spReviewFixNoSuccess(t, h)
				if spAcceptanceHealthOf(h.account) != before {
					t.Error("drain-only observation incorrectly applied upstream health failure")
				}
			})
		}
	}
}

func TestSafePreflightReviewFixFailedTerminalWriteErrors(t *testing.T) {
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, fault := range []string{"short_without_error", "partial_with_error", "write_failure", "full_with_error"} {
			t.Run(policy+"/"+fault, func(t *testing.T) {
				spAcceptanceIsolateCache(t)
				db := spTerminalDeliveryDB(t)
				gate := spAcceptanceNewGate()
				h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
					spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spReviewFixContent))
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixFailed))
					}
				})
				t.Cleanup(gate.open)
				spReviewFixFlushPolicy(policy)
				probe := spReviewFixDeliveryWrap(t, h, fault, false)
				_, reader, _ := h.request(t)
				if spAcceptanceReadEnvelope(t, reader) != spAcceptanceRate ||
					!strings.Contains(spAcceptanceReadEnvelope(t, reader), "synthetic-review-visible-content") {
					t.Fatal("missing real rate/content read barriers before local write fault")
				}
				state := h.provePath(t, 1)
				if !state.committed() {
					t.Fatal("write-fault control did not commit content")
				}
				spReviewFixCooldownSentinel(h)
				before := spAcceptanceHealthOf(h.account)
				gate.open()
				tail := spAcceptanceReadRest(t, reader)
				result := h.wait(t)
				h.provePath(t, 1)
				if result.logicalStatus != logStatusClientClosed || probe.terminalWrites.Load() != 1 || probe.terminalFlushes.Load() != 0 {
					t.Error("terminal write fault was ignored, retried, or mistaken for complete delivery")
				}
				written, length := probe.terminalBytes.Load(), probe.terminalLength.Load()
				switch fault {
				case "short_without_error", "partial_with_error":
					if written <= 0 || written >= length || strings.HasSuffix(tail, "\n\n") {
						t.Error("fixture did not produce a genuinely partial terminal envelope")
					}
				case "write_failure":
					if written != 0 || tail != "" {
						t.Error("zero-byte terminal write failure unexpectedly delivered bytes")
					}
				case "full_with_error":
					if written != length || length == 0 {
						t.Error("fixture did not return an error alongside a full terminal write")
					}
				}
				state.mu.Lock()
				failed := state.writeFailed
				state.mu.Unlock()
				if !failed || state.canStart(context.Background()) {
					t.Error("local terminal write failure left replay available")
				}
				spReviewFixDeliveryUsage(t, db, h, logStatusClientClosed)
				spReviewFixNoSuccess(t, h)
				if spAcceptanceHealthOf(h.account) != before {
					t.Error("downstream write fault incorrectly penalized upstream health")
				}
			})
		}
	}
}

// Helper-only result propagation: no Handler, real quota storage or HTTP status
// delivery is exercised here. Channels, not heartbeat timing, force abandonment
// and permit revocation BEFORE the worker returns its synthetic final result.
func TestSafePreflightReviewFixJoinedQuotaError(t *testing.T) {
	originalInterval := continuousRetryKeepaliveInterval
	continuousRetryKeepaliveInterval = time.Hour // Positive: never take the direct-call path.
	t.Cleanup(func() { continuousRetryKeepaliveInterval = originalInterval })

	quotaErr := &apiKeyModelRequestQuotaError{
		status: http.StatusServiceUnavailable,
		apiErr: api.NewAPIError(api.ErrCodeServiceUnavailable, "synthetic quota storage unavailable", api.ErrorTypeServer),
	}
	for _, tc := range []struct {
		name               string
		workerErr, wantErr error
	}{
		{"quota_storage_503", quotaErr, quotaErr},
		{"other_worker_error", errSafePreflightLocal, context.Canceled},
		{"late_success", nil, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			downstream := context.Background()
			keepalive := &requestContinuousRetryKeepalive{ctx: downstream}
			keepalive.Activate()
			upstreamCtx, cancelUpstream := context.WithCancel(context.WithValue(downstream,
				continuousRetryKeepaliveContextKey{}, continuousRetryKeepalive(keepalive)))
			if continuousRetryKeepaliveForContext(upstreamCtx) != keepalive || !keepalive.Active() || continuousRetryKeepaliveInterval <= 0 {
				cancelUpstream()
				t.Fatal("fixture did not enable the real keepalive wrapper path")
			}
			state := &safePreflightState{started: time.Now(), downstream: downstream}
			state.begin()
			workerStarted := make(chan struct{})
			workerRelease := make(chan struct{})
			releaseWorker := sync.OnceFunc(func() { close(workerRelease) })
			wrapperReturned := make(chan struct{})
			finished := make(chan struct{})
			result := make(chan continuousRetryHTTPResult, 1)
			// Timeouts are deadlock guards only; every ordering assertion uses a
			// channel. Cleanup also releases and joins a worker on assertion failure.
			wait := func(ch <-chan struct{}, failure string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatal(failure)
				}
			}
			t.Cleanup(func() {
				cancelUpstream()
				releaseWorker()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("helper worker did not finish during cleanup")
				}
			})
			go func() {
				defer close(finished)
				response, err := state.execute(upstreamCtx, func() {
					// execute calls this only after the wrapper returns an error
					// and revokes an unsent permit. This is the abandon barrier.
					cancelUpstream()
					close(wrapperReturned)
				}, func() (*http.Response, error) {
					close(workerStarted)
					<-workerRelease
					return nil, tc.workerErr
				})
				result <- continuousRetryHTTPResult{response: response, err: err}
			}()

			wait(workerStarted, "helper worker did not start")
			cancelUpstream() // Downstream remains live; only the wrapper's ctx is canceled.
			wait(wrapperReturned, "keepalive wrapper did not abandon the blocked worker")
			state.mu.Lock()
			revoked := state.revoked
			state.mu.Unlock()
			if !revoked || state.wasSent() || upstreamCtx.Err() != context.Canceled || downstream.Err() != nil {
				t.Fatal("fixture did not establish canceled upstream and revoked, unsent permit")
			}
			select {
			case <-finished:
				t.Fatal("execute returned before joining its blocked worker")
			default:
			}

			releaseWorker()
			wait(finished, "helper did not join the released worker")
			got := <-result
			if got.response != nil || got.err != tc.wantErr {
				t.Fatal("helper lost the joined quota error or replaced a non-quota wrapper result")
			}
			if tc.wantErr == quotaErr {
				if gotQuota := apiKeyModelRequestError(got.err); gotQuota != quotaErr || gotQuota.status != http.StatusServiceUnavailable {
					t.Fatal("helper did not preserve the dedicated quota storage 503 error")
				}
			}
		})
	}
}

func spReviewFixRateOnlyHarness(t *testing.T, policy, fault string, blockFlush bool) (*database.DB, *spAcceptanceHarness, *spAcceptanceGate, *spReviewFixDeliveryProbe) {
	t.Helper()
	spAcceptanceIsolateCache(t)
	db := spTerminalDeliveryDB(t)
	gate := spAcceptanceNewGate()
	h := spAcceptanceNewHarness(t, 0, db, func(w http.ResponseWriter, r *http.Request, _ int) {
		spAcceptanceFlush(w, spAcceptanceRate)
		if gate.wait(r.Context()) {
			spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixRateOnlyFailed))
		}
	})
	t.Cleanup(gate.open)
	spReviewFixFlushPolicy(policy)
	probe := spReviewFixDeliveryWrap(t, h, fault, blockFlush)
	return db, h, gate, probe
}

func spReviewFixRateOnlyBarrier(t *testing.T, h *spAcceptanceHarness, first string) *safePreflightState {
	t.Helper()
	if first != spAcceptanceRate || h.calls.Load() != 1 || h.store.GetMaxRetries() != 0 {
		t.Fatal("rate-only fixture needs a real rate read and explicit zero general-retry budget")
	}
	state := h.provePath(t, 1)
	if state.committed() || state.retryUnsafe() || !state.canStart(context.Background()) {
		t.Fatal("rate-only fixture already committed content or lost replay before the failure")
	}
	spReviewFixRateOnlyObservation(t, h)
	return state
}

func spReviewFixRateOnlyObservation(t *testing.T, h *spAcceptanceHarness) {
	t.Helper()
	if percent, known := h.account.GetUsagePercent5h(); !known || percent != 10 {
		t.Error("downstream delivery arbitration lost the genuine upstream rate observation")
	}
}

func spReviewFixRateOnlyUsage(t *testing.T, db *database.DB, h *spAcceptanceHarness, status int) {
	t.Helper()
	kind := ""
	if status == http.StatusBadGateway {
		kind = "server"
	}
	spReviewFixUsage(t, db, h, spReviewFixUsageWant{status: status, input: 7, total: 7, cached: 2, kind: kind})
}

// Check the ATTEMPTED serialization independently of client delivery. A shared
// final render has its generic code and no upstream response ID/usage. Forward
// would contain the upstream server_error, ID and usage instead. This remains
// path evidence even when the real downstream receives zero terminal bytes.
func spReviewFixFinalRendererAttempt(t *testing.T, probe *spReviewFixDeliveryProbe) string {
	t.Helper()
	wire, ok := probe.terminalWire.Load().(string)
	if !ok || wire == "" || probe.terminalLength.Load() != int64(len(wire)) {
		t.Fatal("missing attempted final-renderer serialization")
	}
	if probe.terminalWrites.Load() != 1 || !strings.Contains(wire, `"code":"upstream_error"`) ||
		strings.Contains(wire, spReviewFixDeliveryID) || strings.Contains(wire, `"usage"`) {
		t.Error("fixture did not attempt exactly one shared final render outside forward")
	}
	spAcceptanceTerminal(t, wire, "response.failed", "") // Serialization only, NOT a read/ACK.
	return wire
}

func TestSafePreflightReviewFixRateOnlyFinalRendererWriteErrors(t *testing.T) {
	// No content barrier, streamWriter terminal write, or coalesced terminal
	// drain is claimed here. Both policies reach writeCommittedResponsesRetryError.
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, fault := range []string{"short_without_error", "partial_with_error", "full_with_error", "write_failure", "zero_without_error"} {
			t.Run(policy+"/shared_final_renderer/"+fault, func(t *testing.T) {
				db, h, gate, probe := spReviewFixRateOnlyHarness(t, policy, fault, false)
				resp, reader, _ := h.request(t)
				first := spAcceptanceReadEnvelope(t, reader)
				state := spReviewFixRateOnlyBarrier(t, h, first)
				spReviewFixCooldownSentinel(h)
				before := spAcceptanceHealthOf(h.account)
				writes := probe.writes.Load()
				gate.open()
				tail := spAcceptanceReadRest(t, reader)
				result := h.wait(t)
				h.provePath(t, 1)
				attempted := spReviewFixFinalRendererAttempt(t, probe)
				if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || result.logicalStatus != logStatusClientClosed {
					t.Errorf("rate-only final-render write fault: HTTP=%d logical=%d, want 200/499", result.httpStatus, result.logicalStatus)
				}
				// terminalFlushes requires a preceding full, error-free Write.
				// The legacy renderer may still call void Flush after a failed
				// Write; that is deliberately NOT counted as delivery evidence.
				if probe.writes.Load() != writes+1 || probe.terminalFlushes.Load() != 0 {
					t.Error("failed final render attempted more output or was mistaken for full error-free delivery")
				}
				written, length := probe.terminalBytes.Load(), probe.terminalLength.Load()
				if int64(len(tail)) != written || !strings.HasPrefix(attempted, tail) {
					t.Error("real downstream bytes differ from the selected render or contain a second event")
				}
				switch fault {
				case "short_without_error", "partial_with_error":
					if written <= 0 || written >= length || strings.HasSuffix(tail, "\n\n") {
						t.Error("final-render fixture did not produce a genuinely partial envelope")
					}
				case "write_failure", "zero_without_error":
					if written != 0 || tail != "" {
						t.Error("zero-write final-render fixture unexpectedly delivered terminal bytes")
					}
				case "full_with_error":
					if written != length || length == 0 || tail != attempted {
						t.Error("final-render fixture did not return error alongside the full envelope")
					}
					spAcceptanceTerminal(t, first+tail, "response.failed", "")
				}
				state.mu.Lock()
				failed, failureDelivered, successDelivered := state.writeFailed, state.failedTerminalWrittenAndFlushed, state.successTerminalWrittenAndFlushed
				state.mu.Unlock()
				if !failed || failureDelivered || successDelivered || state.canStart(context.Background()) {
					t.Error("unchecked shared final-render write retained replay or certified terminal delivery")
				}
				spReviewFixRateOnlyUsage(t, db, h, logStatusClientClosed)
				spReviewFixNoSuccess(t, h)
				spReviewFixRateOnlyObservation(t, h)
				if spAcceptanceHealthOf(h.account) != before {
					t.Error("shared final-render write fault incorrectly applied an upstream health failure")
				}
			})
		}
	}
}

func TestSafePreflightReviewFixRateOnlyFinalRendererDelivery(t *testing.T) {
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, lateCancel := range []bool{false, true} {
			name := policy + "/shared_final_renderer/no_cancel_positive_control"
			if lateCancel {
				name = policy + "/shared_final_renderer/cancel_after_terminal_read"
			}
			t.Run(name, func(t *testing.T) {
				db, h, gate, probe := spReviewFixRateOnlyHarness(t, policy, "", lateCancel)
				resp, reader, cancel := h.request(t)
				first := spAcceptanceReadEnvelope(t, reader)
				spReviewFixRateOnlyBarrier(t, h, first)
				spReviewFixCooldownSentinel(h)
				before := spAcceptanceHealthOf(h.account)
				writes := probe.writes.Load()
				gate.open()
				terminal := spAcceptanceReadEnvelope(t, reader)
				spAcceptanceTerminal(t, first+terminal, "response.failed", "")
				if lateCancel {
					select {
					case <-probe.flushEntered.ch:
					case <-time.After(5 * time.Second):
						t.Fatal("shared final-render Flush did not reach the return barrier")
					}
					select {
					case <-h.done:
						t.Fatal("Handler finalized while shared final-render Flush was held")
					default:
					}
					// Freeze the side-effect boundary too: upstream quota is
					// already observed, but final usage/health must wait for the
					// renderer's delivery result, not its semantic 502 selection.
					spReviewFixUsage(t, db, h) // No finalized attempt while Flush is held.
					if spAcceptanceHealthOf(h.account) != before {
						t.Error("semantic health failure was applied before final-render delivery arbitration")
					}
					spReviewFixRateOnlyObservation(t, h)
					cancel() // Complete real terminal READ precedes server cancellation.
					_ = resp.Body.Close()
					spTerminalDeliveryWaitCancel(t, h)
				} else if spAcceptanceReadRest(t, reader) != "" {
					t.Error("another event followed the shared final-rendered failure")
				}
				result := h.wait(t)
				h.provePath(t, 1)
				attempted := spReviewFixFinalRendererAttempt(t, probe)
				if terminal != attempted || probe.writes.Load() != writes+1 || probe.terminalFlushes.Load() != 1 ||
					probe.terminalBytes.Load() != probe.terminalLength.Load() || lateCancel && !probe.flushSawCancel.Load() {
					t.Fatal("shared final-render fixture did not establish full delivery and requested cancellation ordering")
				}
				if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || result.logicalStatus != http.StatusBadGateway {
					t.Errorf("delivered shared failure: HTTP=%d logical=%d, want 200/502", result.httpStatus, result.logicalStatus)
				}
				spReviewFixRateOnlyUsage(t, db, h, http.StatusBadGateway)
				spReviewFixNoSuccess(t, h)
				spReviewFixRateOnlyObservation(t, h)
				health := spAcceptanceHealthOf(h.account)
				if health.streak != 1 || health.kind != "server" || health.last.IsZero() {
					t.Error("fully delivered shared failure lost its one semantic health failure")
				}
			})
		}
	}
}

// The same final-render block also handles a genuine EOF without a terminal
// and a local strict-SSE rejection. Preserve their existing logical/wire mapping:
// stream break = logical 598 / generic upstream_error, local = logical 502 /
// internal_error. Neither has terminal usage, and neither can become success.
func TestSafePreflightReviewFixFinalRendererBreakAndLocal(t *testing.T) {
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, selected := range []struct {
			name, tail, code, kind string
			status                 int
		}{
			{"stream_break", "", "upstream_error", "transport", logStatusUpstreamStreamBreak},
			{"local_failure", "data: [DONE]\n\n", ErrorCodeInternalError, "local", http.StatusBadGateway},
		} {
			for _, delivery := range []string{"normal", "late_cancel", "prewrite_cancel", "short_without_error", "partial_with_error", "full_with_error", "write_failure", "zero_without_error"} {
				t.Run(policy+"/"+selected.name+"/"+delivery, func(t *testing.T) {
					spAcceptanceIsolateCache(t)
					db := spTerminalDeliveryDB(t)
					gate := spAcceptanceNewGate()
					h := spAcceptanceNewHarness(t, 0, db, func(w http.ResponseWriter, r *http.Request, _ int) {
						spAcceptanceFlush(w, spAcceptanceRate)
						if gate.wait(r.Context()) && selected.tail != "" {
							spAcceptanceFlush(w, selected.tail)
						}
					})
					t.Cleanup(gate.open)
					spReviewFixFlushPolicy(policy)
					fault := delivery
					if delivery == "normal" || delivery == "late_cancel" || delivery == "prewrite_cancel" {
						fault = ""
					}
					probe := spReviewFixDeliveryWrap(t, h, fault, delivery == "late_cancel")
					resp, reader, cancel := h.request(t)
					first := spAcceptanceReadEnvelope(t, reader)
					state := spReviewFixRateOnlyBarrier(t, h, first)
					spReviewFixCooldownSentinel(h)
					before := spAcceptanceHealthOf(h.account)
					writes := probe.writes.Load()
					if delivery == "prewrite_cancel" {
						cancel()
						_ = resp.Body.Close()
						spTerminalDeliveryWaitCancel(t, h)
					}
					gate.open()
					var tail string
					if delivery == "late_cancel" {
						tail = spAcceptanceReadEnvelope(t, reader)
						spAcceptanceTerminal(t, first+tail, "response.failed", "")
						select {
						case <-probe.flushEntered.ch:
						case <-time.After(5 * time.Second):
							t.Fatal("selected final render did not reach its Flush return barrier")
						}
						select {
						case <-h.done:
							t.Fatal("selected final render finalized before Flush returned")
						default:
						}
						spReviewFixUsage(t, db, h)
						if spAcceptanceHealthOf(h.account) != before {
							t.Error("ordinary stream health changed before final delivery arbitration")
						}
						spReviewFixRateOnlyObservation(t, h)
						cancel() // Real terminal read, not a synthesized delivery ACK.
						_ = resp.Body.Close()
						spTerminalDeliveryWaitCancel(t, h)
					} else if delivery != "prewrite_cancel" {
						tail = spAcceptanceReadRest(t, reader)
					}
					result := h.wait(t)
					h.provePath(t, 1)
					wantStatus, wantKind := selected.status, selected.kind
					if fault != "" || delivery == "prewrite_cancel" {
						wantStatus, wantKind = logStatusClientClosed, ""
					}
					if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || result.logicalStatus != wantStatus {
						t.Errorf("selected final render logical=%d, want %d with published HTTP 200", result.logicalStatus, wantStatus)
					}
					if delivery == "prewrite_cancel" {
						if probe.writes.Load() != writes || probe.terminalWrites.Load() != 0 || probe.terminalFlushes.Load() != 0 {
							t.Error("selected final render wrote after prewrite cancellation")
						}
					} else {
						attempted, ok := probe.terminalWire.Load().(string)
						if !ok || attempted == "" || probe.terminalWrites.Load() != 1 || probe.writes.Load() != writes+1 ||
							probe.terminalLength.Load() != int64(len(attempted)) || !strings.Contains(attempted, `"code":"`+selected.code+`"`) ||
							strings.Contains(attempted, `"usage"`) || strings.Contains(attempted, spReviewFixDeliveryID) {
							t.Fatal("selected final renderer changed its existing mapping or attempted multiple terminals")
						}
						spAcceptanceTerminal(t, attempted, "response.failed", "") // Serialization only.
						if !strings.HasPrefix(attempted, tail) || int64(len(tail)) != probe.terminalBytes.Load() {
							t.Error("downstream bytes differ from the selected render or contain another event")
						}
						switch fault {
						case "":
							if tail != attempted || probe.terminalFlushes.Load() != 1 || delivery == "late_cancel" && !probe.flushSawCancel.Load() {
								t.Error("normal/late-cancel control lacks a complete local write and returned Flush")
							}
						case "short_without_error", "partial_with_error":
							if len(tail) == 0 || len(tail) >= len(attempted) || strings.HasSuffix(tail, "\n\n") {
								t.Error("selected final-render fixture did not actually write a partial envelope")
							}
						case "full_with_error":
							if tail != attempted {
								t.Error("full-with-error fixture lost its full attempted bytes")
							}
						case "write_failure", "zero_without_error":
							if tail != "" {
								t.Error("zero-write fixture unexpectedly delivered terminal bytes")
							}
						}
						if fault != "" && probe.terminalFlushes.Load() != 0 {
							t.Error("failed selected final render was mistaken for flushed delivery")
						}
					}
					state.mu.Lock()
					failed, delivered, success := state.writeFailed, state.failedTerminalWrittenAndFlushed, state.successTerminalWrittenAndFlushed
					state.mu.Unlock()
					if failed != (fault != "") || delivered != (wantStatus != logStatusClientClosed) || success || state.canStart(context.Background()) {
						t.Error("selected final-render evidence or closed replay boundary is incorrect")
					}
					spReviewFixUsage(t, db, h, spReviewFixUsageWant{status: wantStatus, kind: wantKind})
					spReviewFixNoSuccess(t, h)
					spReviewFixRateOnlyObservation(t, h)
					health := spAcceptanceHealthOf(h.account)
					if wantStatus == logStatusUpstreamStreamBreak {
						if health.streak != 1 || health.kind != "transport" || health.last.IsZero() {
							t.Error("fully rendered stream break lost its one ordinary semantic health failure")
						}
					} else if health != before {
						t.Error("local failure/downstream delivery failure changed ordinary upstream health")
					}
				})
			}
		}
	}
}

// There is NO upstream terminal or terminal usage in the post-content EOF
// fixture below. Only freeze the existing delta-character estimate and its
// aliases, not purported exact upstream token counters. This deliberately does
// not reuse the exact-terminal-usage assertion used by the failed-frame cases.
func spReviewFixPostContentEstimatedUsage(t *testing.T, db *database.DB, h *spAcceptanceHarness, status int) {
	t.Helper()
	db.FlushUsageLogs()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.ListUsageLogsByFilter(ctx, database.UsageLogFilter{
		Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour), IncludeCanceled: true,
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("post-content EOF usage rows=%d, want 1; queryFailed=%t (query details redacted)", len(rows), err != nil)
	}
	row := rows[0]
	if row.StatusCode != status || row.AttemptIndex != 1 || row.IsRetryAttempt ||
		row.AccountID != h.account.ID() || row.Endpoint != "/v1/responses" || !row.Stream || row.ViaWebsocket {
		t.Errorf("post-content EOF usage: status=%d (want %d), attempt=%d (want 1), retry=%t, stream=%t, websocket=%t; require the sole real HTTP attempt", row.StatusCode, status, row.AttemptIndex, row.IsRetryAttempt, row.Stream, row.ViaWebsocket)
	}
	// spReviewFixContent has this one synthetic ASCII delta. Legacy estimates
	// floor(len(delta)/3), at least one token; they know no input/cache/reasoning.
	estimate := len("synthetic-review-visible-content") / 3
	if estimate < 1 {
		estimate = 1
	}
	if row.OutputTokens != estimate || row.CompletionTokens != estimate || row.TotalTokens != estimate ||
		row.InputTokens != 0 || row.PromptTokens != 0 || row.CachedTokens != 0 || row.ReasoningTokens != 0 {
		t.Errorf("post-content EOF estimated usage: input/prompt=%d/%d, output/completion/total=%d/%d/%d (want estimate %d each), cache/reasoning=%d/%d; no exact upstream usage exists", row.InputTokens, row.PromptTokens, row.OutputTokens, row.CompletionTokens, row.TotalTokens, estimate, row.CachedTokens, row.ReasoningTokens)
	}
	kind := ""
	if status == logStatusUpstreamStreamBreak {
		kind = "transport"
	}
	if row.UpstreamErrorKind != kind || row.ErrorMessage == "" {
		t.Errorf("post-content EOF usage failure metadata: kind=%q (want %q), hasError=%t", row.UpstreamErrorKind, kind, row.ErrorMessage != "")
	}
}

// Phase 5 / independent P2: a real upstream sends content then returns EOF,
// NEVER response.failed/completed or usage. This reaches the in-stream
// writeResponsesStreamBreakEvent(streamWriter), not the RATE-ONLY shared final
// renderer tested above. The attempted upstream_stream_break payload identifies
// that path. All bytes still pass through real HTTP + verified TLS + CONNECT.
//
// The downstream reads the COMPLETE synthetic terminal before canceling while
// the actual Flush return is held. Both complete-delivery cases must retain the
// selected transport 598, not a false 200 or a late-cancel 499. Conversely every
// short/error write and prewrite cancel stays 499. A synthetic delivery is NOT
// an upstream terminal: do not fix this by inventing gotTerminal, terminal usage,
// or failurePayload, or by remapping all readErr/localFailure outcomes to 598.
// TestSafePreflightReviewFixFinalRendererBreakAndLocal keeps that local mapping.
func TestSafePreflightReviewFixPostContentStreamBreakDelivery(t *testing.T) {
	const created = `{"type":"response.created","response":{"id":"resp_synthetic_review_delivery","status":"in_progress"}}`
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, delivery := range []string{"normal", "late_cancel", "prewrite_cancel", "short_without_error", "partial_with_error", "full_with_error", "write_failure", "zero_without_error"} {
			t.Run(policy+"/post_content_eof/"+delivery, func(t *testing.T) {
				spAcceptanceIsolateCache(t)
				db := spTerminalDeliveryDB(t)
				gate := spAcceptanceNewGate()
				h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
					// Give cache checks a genuinely observed response ID, but no
					// successful terminal/output publication or fabricated usage.
					spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(created)+spAcceptanceFrame(spReviewFixContent))
					gate.wait(r.Context())
					// Normal HTTP body EOF after the read barrier; no injected
					// read error, terminal, sender result, or delivery evidence.
				})
				t.Cleanup(gate.open)
				spReviewFixFlushPolicy(policy)
				fault := delivery
				if delivery == "normal" || delivery == "late_cancel" || delivery == "prewrite_cancel" {
					fault = ""
				}
				probe := spReviewFixDeliveryWrap(t, h, fault, delivery == "late_cancel")
				resp, reader, cancel := h.request(t)
				first := spAcceptanceReadEnvelope(t, reader)
				identity, content := spAcceptanceReadEnvelope(t, reader), spAcceptanceReadEnvelope(t, reader)
				if first != spAcceptanceRate || identity != spAcceptanceFrame(created) || content != spAcceptanceFrame(spReviewFixContent) {
					t.Fatal("post-content EOF fixture lacks genuine downstream rate/identity/content read barriers")
				}
				state := h.provePath(t, 1)
				if !state.committed() || state.canStart(context.Background()) || !state.wasSent() || h.store.GetMaxRetries() != 2 {
					t.Fatal("post-content EOF fixture must commit the real first send with unused retry budget")
				}
				spReviewFixCooldownSentinel(h)
				before := spAcceptanceHealthOf(h.account)
				writes := probe.writes.Load()
				if probe.terminalWrites.Load() != 0 || probe.terminalFlushes.Load() != 0 {
					t.Fatal("post-content EOF fixture already observed an unexpected terminal")
				}
				if delivery == "prewrite_cancel" {
					cancel()
					_ = resp.Body.Close()
					spTerminalDeliveryWaitCancel(t, h)
				}
				gate.open() // Only now may the actual upstream body end without a terminal.
				var tail string
				if delivery == "normal" || delivery == "late_cancel" {
					tail = spAcceptanceReadEnvelope(t, reader)
					spAcceptanceTerminal(t, first+identity+content+tail, "response.failed", "")
					if delivery == "late_cancel" {
						select {
						case <-probe.flushEntered.ch:
						case <-time.After(5 * time.Second):
							t.Fatal("post-content synthetic terminal Flush did not reach its return barrier")
						}
						select {
						case <-h.done:
							t.Fatal("post-content EOF finalized while synthetic terminal Flush was held")
						default:
						}
						spReviewFixUsage(t, db, h) // No finalized usage while delivery is unresolved.
						if spAcceptanceHealthOf(h.account) != before {
							t.Error("post-content EOF applied ordinary health before delivery arbitration")
						}
						spReviewFixRateOnlyObservation(t, h) // Already observed quota must not be rolled back.
						// Complete real downstream envelope read precedes cancellation.
						cancel()
						_ = resp.Body.Close()
						spTerminalDeliveryWaitCancel(t, h)
					} else if rest := spAcceptanceReadRest(t, reader); rest != "" {
						t.Errorf("post-content EOF wrote %d extra downstream bytes after its sole terminal", len(rest))
					}
				} else if delivery != "prewrite_cancel" {
					tail = spAcceptanceReadRest(t, reader)
				}
				result := h.wait(t)
				wantStatus := logStatusUpstreamStreamBreak
				if fault != "" || delivery == "prewrite_cancel" {
					wantStatus = logStatusClientClosed
				}
				if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || result.logicalStatus != wantStatus {
					t.Errorf("post-content EOF delivery=%s: client HTTP=%d, Handler HTTP=%d, logical=%d; want 200/200/%d", delivery, resp.StatusCode, result.httpStatus, result.logicalStatus, wantStatus)
				}
				leases, calls := h.account.GetTotalRequests(), h.calls.Load()
				if leases != 1 || calls != 1 {
					t.Errorf("post-content EOF: scheduling leases=%d, actual upstream calls=%d; want 1 each, no new send or refund", leases, calls)
				}
				h.provePath(t, 1) // Independently retain the exact real TLS + CONNECT count.
				if delivery == "prewrite_cancel" {
					if probe.writes.Load() != writes || probe.terminalWrites.Load() != 0 || probe.terminalFlushes.Load() != 0 {
						t.Errorf("post-content EOF wrote after cancellation: writes=%d (before %d), terminals=%d, terminalFlushes=%d", probe.writes.Load(), writes, probe.terminalWrites.Load(), probe.terminalFlushes.Load())
					}
				} else {
					attempted, ok := probe.terminalWire.Load().(string)
					if !ok || attempted == "" || probe.terminalWrites.Load() != 1 || probe.writes.Load() != writes+1 ||
						probe.terminalLength.Load() != int64(len(attempted)) || !strings.Contains(attempted, `"code":"upstream_stream_break"`) ||
						!strings.Contains(attempted, `"created_at":`) || strings.Contains(attempted, `"usage"`) || strings.Contains(attempted, spReviewFixDeliveryID) {
						t.Fatalf("post-content EOF did not select exactly one legacy in-stream synthetic break: writes=%d (before %d), terminals=%d, attemptedLength=%d", probe.writes.Load(), writes, probe.terminalWrites.Load(), probe.terminalLength.Load())
					}
					spAcceptanceTerminal(t, attempted, "response.failed", "") // Attempted bytes only, not delivery.
					if !strings.HasPrefix(attempted, tail) || int64(len(tail)) != probe.terminalBytes.Load() {
						t.Errorf("post-content EOF downstream bytes=%d, reported write bytes=%d, attempted bytes=%d; changed payload or duplicate event", len(tail), probe.terminalBytes.Load(), len(attempted))
					}
					switch fault {
					case "":
						if tail != attempted || probe.terminalFlushes.Load() != 1 || delivery == "late_cancel" && !probe.flushSawCancel.Load() {
							t.Errorf("post-content EOF lacks full write/Flush/read/cancel ordering: read=%d, attempted=%d, terminalFlushes=%d, FlushSawCancel=%t", len(tail), len(attempted), probe.terminalFlushes.Load(), probe.flushSawCancel.Load())
						}
					case "short_without_error", "partial_with_error":
						if len(tail) == 0 || len(tail) >= len(attempted) || strings.HasSuffix(tail, "\n\n") {
							t.Error("post-content EOF short-write control did not produce a partial terminal envelope")
						}
					case "full_with_error":
						if tail != attempted {
							t.Error("post-content EOF full-with-error control lost the attempted bytes")
						}
					case "write_failure", "zero_without_error":
						if tail != "" {
							t.Errorf("post-content EOF zero-write control delivered %d bytes", len(tail))
						}
					}
					if fault != "" && probe.terminalFlushes.Load() != 0 {
						t.Errorf("post-content EOF write fault certified %d complete terminal flushes", probe.terminalFlushes.Load())
					}
				}
				state.mu.Lock()
				attempts, failed := state.attempts, state.writeFailed
				delivered, success := state.failedTerminalWrittenAndFlushed, state.successTerminalWrittenAndFlushed
				state.mu.Unlock()
				sent, canStart := state.wasSent(), state.canStart(context.Background())
				if attempts != 1 || !sent || canStart {
					t.Errorf("post-content EOF created/lost an attempt or reopened replay: attempts=%d, wasSent=%t, canStart=%t; want 1/true/false", attempts, sent, canStart)
				}
				if failed != (fault != "") || delivered != (wantStatus == logStatusUpstreamStreamBreak) || success {
					t.Errorf("post-content EOF evidence: writeFailed=%t (want %t), failureDelivered=%t (want %t), successDelivered=%t (want false)", failed, fault != "", delivered, wantStatus == logStatusUpstreamStreamBreak, success)
				}
				spReviewFixPostContentEstimatedUsage(t, db, h, wantStatus)
				spReviewFixNoSuccess(t, h)
				spReviewFixRateOnlyObservation(t, h)
				health := spAcceptanceHealthOf(h.account)
				if wantStatus == logStatusUpstreamStreamBreak {
					if health.streak != 1 || health.kind != "transport" || health.last.IsZero() {
						t.Errorf("delivered post-content EOF health: streak=%d, kind=%q, hasFailureTime=%t; want one transport failure", health.streak, health.kind, !health.last.IsZero())
					}
				} else if health != before {
					t.Error("post-content EOF downstream failure/cancel penalized ordinary upstream health")
				}
			})
		}
	}
}

// Freeze the pre-existing payload bytes independently of the shared builder.
// The dynamic timestamp is supplied explicitly; no upstream terminal is forged.
func TestSafePreflightReviewFixStreamBreakPayloadCompatibility(t *testing.T) {
	want := `{"type":"response.failed","response":{"created_at":1700000000,"status":"failed","error":{"code":"upstream_stream_break","message":"Upstream stream ended prematurely; safe to retry"}}}`
	if got := responsesStreamBreakPayload(1700000000); string(got) != want {
		t.Error("local stream-break payload changed the legacy byte structure")
	}
}

// Arbitration-only coverage, not HTTP delivery evidence: the delivery flags
// below are explicit table inputs. The real Handler/write/Flush/client-read
// ordering remains covered by the unchanged post-content and final-render
// fixtures above. No case invents gotTerminal or an upstream failurePayload.
func TestSafePreflightReviewFixLocalTerminalSemanticArbitration(t *testing.T) {
	for _, selected := range []struct {
		name    string
		readErr error
		want    streamOutcome
	}{
		{"eof", nil, streamOutcome{logStatusCode: logStatusUpstreamStreamBreak, failureKind: "transport", failureMessage: "上游流提前结束，未收到终止事件", penalize: true}},
		{"read_error", io.ErrUnexpectedEOF, classifyStreamOutcome(nil, io.ErrUnexpectedEOF, nil, false)},
		{"replay_local", errContinuousRetryReplayStorage, streamOutcome{logStatusCode: http.StatusInternalServerError, failureKind: "local", failureMessage: continuousRetryLocalFailureMessage, terminalLocal: true}},
		{"safe_local", errSafePreflightLocal, streamOutcome{logStatusCode: http.StatusBadGateway, failureKind: "local", failureMessage: errSafePreflightLocal.Error(), terminalLocal: true}},
	} {
		for _, delivery := range []struct {
			name              string
			delivered, failed bool
			writeErr          error
		}{
			{"delivered_late_cancel", true, false, nil},
			{"undelivered_cancel", false, false, nil},
			{"write_error_priority", true, false, io.ErrShortWrite},
			{"latched_write_error_priority", true, true, nil},
		} {
			t.Run(selected.name+"/"+delivery.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
				state := &safePreflightState{
					failedTerminalWrittenAndFlushed: delivery.delivered,
					writeFailed:                     delivery.failed,
				}
				cancel()
				// Match safe Handler ordering: classify real read semantics before
				// arbitrating the already-canceled downstream context.
				semantic := classifyStreamOutcome(nil, selected.readErr, nil, false)
				got := safePreflightOverlay(state, c, semantic, selected.readErr, delivery.writeErr)
				if len(semantic.failurePayload) != 0 || len(got.failurePayload) != 0 || state.successTerminalWrittenAndFlushed {
					t.Fatal("local arbitration manufactured upstream payload or successful delivery")
				}
				if !delivery.delivered || delivery.failed || delivery.writeErr != nil {
					if got.logStatusCode != logStatusClientClosed || got.failureKind != "" || got.penalize || got.terminalLocal {
						t.Error("undelivered cancellation/write error lost highest-priority 499")
					}
					return
				}
				want := selected.want
				if got.logStatusCode != want.logStatusCode || got.failureKind != want.failureKind || got.failureMessage != want.failureMessage ||
					got.penalize != want.penalize || got.terminalLocal != want.terminalLocal {
					t.Error("delivered local terminal lost its original EOF/read/local semantics to late cancel or blanket 598")
				}
			})
		}
	}
}

// Helper-only coverage for BOTH Gin writer entry points. Handler integration
// above exercises the unchanged WriteString renderer; this independently keeps
// Write from silently escaping the same safe-only short/error checks.
func TestSafePreflightReviewFixFinalRendererWriterMethods(t *testing.T) {
	for _, method := range []string{"Write", "WriteString"} {
		for _, fault := range []string{"", "short_without_error", "partial_with_error", "full_with_error", "write_failure", "zero_without_error"} {
			t.Run(method+"/"+fault, func(t *testing.T) {
				probe := &spReviewFixDeliveryProbe{fault: fault}
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(&spReviewFixDeliveryWriter{ResponseWriter: recorder, probe: probe})
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				original := c.Writer
				state := &safePreflightState{started: time.Now()}
				writer := state.captureFinalFailure(c)
				wire := spAcceptanceFrame(spReviewFixRateOnlyFailed)
				var n int
				var err error
				if method == "Write" {
					n, err = c.Writer.Write([]byte(wire))
				} else {
					n, err = c.Writer.WriteString(wire)
				}
				c.Writer.Flush() // As in the shared renderer, even if Write failed.
				outcome := writer.finish(streamOutcome{logStatusCode: logStatusUpstreamStreamBreak, failureKind: "transport", penalize: true})
				if c.Writer != original || probe.terminalWrites.Load() != 1 || state.canStart(context.Background()) {
					t.Error("capture failed to restore Gin writer or close replay after its single render")
				}
				if fault == "" {
					if n != len(wire) || err != nil || outcome.logStatusCode != logStatusUpstreamStreamBreak || !outcome.penalize ||
						!state.failedTerminalWrittenAndFlushed || state.writeFailed || probe.terminalFlushes.Load() != 1 {
						t.Error("full checked write/flush changed the selected semantic failure")
					}
				} else if err == nil || outcome.logStatusCode != logStatusClientClosed || outcome.penalize ||
					!state.writeFailed || state.failedTerminalWrittenAndFlushed || probe.terminalFlushes.Load() != 0 {
					t.Error("checked writer ignored a short/error result or certified failed delivery")
				}
			})
		}
	}
}

// Phase 4: attempt A really retries after its private failed terminal; attempt B
// returns an ordinary HTTP 502, NOT a response.failed in an HTTP 200 body. This
// selects the earlier non-200 Handler exit, outside the post-read finalizer.
// Keep A's exact usage and ordinary health failure when B's LOCAL write fails;
// neither refund an actual send nor charge B's client-gone outcome to health.
func TestSafePreflightReviewFixRetriedHTTPErrorFinalDelivery(t *testing.T) {
	const httpFailure = `{"error":{"type":"server_error","code":"server_error","message":"synthetic-phase4-http-502"}}`
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		for _, delivery := range []string{"normal", "late_cancel", "short_without_error", "partial_with_error", "full_with_error", "write_failure", "zero_without_error"} {
			t.Run(policy+"/"+delivery, func(t *testing.T) {
				spAcceptanceIsolateCache(t)
				db := spTerminalDeliveryDB(t)
				firstRelease, secondEntered, secondRelease := spAcceptanceNewGate(), spAcceptanceNewGate(), spAcceptanceNewGate()
				h := spAcceptanceNewHarness(t, 1, db, func(w http.ResponseWriter, r *http.Request, n int) {
					if n == 1 {
						spAcceptanceFlush(w, spAcceptanceRate)
						if firstRelease.wait(r.Context()) {
							spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixRateOnlyFailed))
						}
						return
					}
					secondEntered.open() // Real second TLS request, before its HTTP headers.
					if secondRelease.wait(r.Context()) {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusBadGateway)
						_, _ = io.WriteString(w, httpFailure)
					}
				})
				spReviewFixFlushPolicy(policy)
				fault := delivery
				if delivery == "normal" || delivery == "late_cancel" {
					fault = ""
				}
				probe := spReviewFixDeliveryWrap(t, h, fault, delivery == "late_cancel")
				t.Cleanup(firstRelease.open)
				t.Cleanup(secondRelease.open)
				resp, reader, cancel := h.request(t)
				first := spAcceptanceReadEnvelope(t, reader)
				state := h.provePath(t, 1)
				if first != spAcceptanceRate || h.store.GetMaxRetries() != 1 || h.account.GetTotalRequests() != 1 ||
					state.committed() || state.retryUnsafe() || !state.canStart(context.Background()) {
					t.Fatal("HTTP-exit fixture lacks its genuine rate read and one available retry")
				}
				firstRelease.open()
				select {
				case <-secondEntered.ch:
				case <-time.After(5 * time.Second):
					t.Fatal("private failed attempt did not reach the second real TLS request")
				}
				h.provePath(t, 2)
				h.assertStableIdentity(t)
				firstUsage := spReviewFixUsageWant{status: http.StatusBadGateway, input: 7, total: 7, cached: 2, kind: "server", retry: true}
				spReviewFixUsage(t, db, h, firstUsage)
				before := spAcceptanceHealthOf(h.account)
				if before.streak != 1 || before.kind != "server" || before.last.IsZero() || h.account.GetTotalRequests() != 2 ||
					probe.writes.Load() != 1 || probe.terminalWrites.Load() != 0 || state.committed() || state.retryUnsafe() {
					t.Fatal("second HTTP attempt lacks the finalized first-attempt facts or already published a terminal")
				}
				// Only after B's real send: this sentinel must not block retry admission.
				spReviewFixCooldownSentinel(h)
				secondRelease.open()
				var tail string
				if delivery == "late_cancel" {
					tail = spAcceptanceReadEnvelope(t, reader)
					spAcceptanceTerminal(t, first+tail, "response.failed", "")
					select {
					case <-probe.flushEntered.ch:
					case <-time.After(5 * time.Second):
						t.Fatal("HTTP-exit renderer did not reach its terminal Flush return barrier")
					}
					select {
					case <-h.done:
						t.Fatal("HTTP-exit Handler finalized before terminal Flush returned")
					default:
					}
					// A remains finalized, but B must wait for this final write's
					// arbitration. Retain the cancellation control even on failure.
					spReviewFixUsage(t, db, h, firstUsage)
					if spAcceptanceHealthOf(h.account) != before {
						t.Error("HTTP-exit applied the second attempt's ordinary health before final Flush returned")
					}
					spReviewFixRateOnlyObservation(t, h)
					cancel() // Complete real terminal read precedes server cancellation.
					_ = resp.Body.Close()
					spTerminalDeliveryWaitCancel(t, h)
				} else {
					tail = spAcceptanceReadRest(t, reader)
				}
				result := h.wait(t)
				h.provePath(t, 2)
				attempted := spReviewFixFinalRendererAttempt(t, probe)
				wantStatus, wantKind := http.StatusBadGateway, "server"
				if fault != "" {
					wantStatus, wantKind = logStatusClientClosed, ""
				}
				if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || result.logicalStatus != wantStatus {
					t.Errorf("HTTP-exit final render logical=%d, want %d with published HTTP 200", result.logicalStatus, wantStatus)
				}
				if probe.writes.Load() != 2 || !strings.HasPrefix(attempted, tail) || int64(len(tail)) != probe.terminalBytes.Load() ||
					!strings.Contains(attempted, "synthetic-phase4-http-502") || strings.Contains(attempted, "synthetic-review-rate-only-failure") {
					t.Error("HTTP-exit renderer lost the selected second failure, duplicated output, or leaked the discarded first failure")
				}
				switch fault {
				case "":
					spAcceptanceTerminal(t, first+tail, "response.failed", "")
					if tail != attempted || probe.terminalFlushes.Load() != 1 || delivery == "late_cancel" && !probe.flushSawCancel.Load() {
						t.Error("HTTP-exit normal/late-cancel control lacks full write/flush and requested cancellation ordering")
					}
				case "short_without_error", "partial_with_error":
					if len(tail) == 0 || len(tail) >= len(attempted) || strings.HasSuffix(tail, "\n\n") {
						t.Error("HTTP-exit write fault did not actually truncate its terminal envelope")
					}
				case "full_with_error":
					if tail != attempted {
						t.Error("HTTP-exit full-with-error control did not deliver the attempted bytes")
					}
				case "write_failure", "zero_without_error":
					if tail != "" {
						t.Error("HTTP-exit zero-write control unexpectedly delivered terminal bytes")
					}
				}
				if fault != "" && probe.terminalFlushes.Load() != 0 {
					t.Error("HTTP-exit failed write was certified as complete delivery")
				}
				state.mu.Lock()
				attempts, failed := state.attempts, state.writeFailed
				delivered, success := state.failedTerminalWrittenAndFlushed, state.successTerminalWrittenAndFlushed
				state.mu.Unlock()
				if attempts != 2 || h.account.GetTotalRequests() != 2 || failed != (fault != "") || delivered != (fault == "") ||
					success || state.canStart(context.Background()) {
					t.Error("HTTP-exit finalization lost a real attempt, retained replay, or ignored the local delivery result")
				}
				spReviewFixUsage(t, db, h, firstUsage, spReviewFixUsageWant{status: wantStatus, kind: wantKind})
				spReviewFixRateOnlyObservation(t, h)
				spReviewFixNoSuccess(t, h)
				health := spAcceptanceHealthOf(h.account)
				if fault != "" {
					if health != before {
						t.Error("HTTP-exit client-gone outcome penalized health or erased the first attempt's real failure")
					}
				} else if health.streak != 2 || health.kind != "server" || health.last.IsZero() {
					t.Error("fully delivered HTTP failure did not retain both semantic attempt failures")
				}
			})
		}
	}
}

// Other early exits after a genuinely sent/private failed attempt. Only the
// transport case sends B; local guard, quota and no-account refusals must keep
// A's usage/health without inventing B usage. GetTotalRequests counts acquired
// scheduling leases, NOT sends: local_guard/quota acquire B's lease before its
// unsent rejection; no_account never acquires B. The phase-5 fixture correction
// does not change product accounting or supersede the retained fixed3 failure.
// The controls change synthetic admission inputs, never a sender result or a
// production test hook.
func TestSafePreflightReviewFixEarlyExitFinalDelivery(t *testing.T) {
	for _, exit := range []string{"transport", "local_guard", "quota", "no_account"} {
		for _, delivery := range []string{"normal", "late_cancel", "short_without_error", "partial_with_error", "full_with_error", "write_failure", "zero_without_error"} {
			t.Run(exit+"/"+delivery, func(t *testing.T) {
				spAcceptanceIsolateCache(t)
				db := spTerminalDeliveryDB(t)
				gate := spAcceptanceNewGate()
				var dropped atomic.Bool
				h := spAcceptanceNewHarness(t, 1, db, func(w http.ResponseWriter, r *http.Request, n int) {
					if n == 1 {
						spAcceptanceFlush(w, spAcceptanceRate)
						if gate.wait(r.Context()) {
							spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixRateOnlyFailed))
						}
						return
					}
					// The real TLS peer has read B's POST before dropping it
					// without HTTP response headers: this is a sent attempt.
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						http.Error(w, "synthetic hijack unavailable", http.StatusInternalServerError)
						return
					}
					dropped.Store(true)
					_ = conn.Close()
				})
				t.Cleanup(gate.open)
				var quotaDB *database.DB
				var quota *apiKeyModelRequestQuota
				if exit == "quota" {
					_, quotaDB, quota = spSharedEdgesQuota(t)
					spSharedEdgesAttachQuota(t, h, quota)
				}
				fault := delivery
				if delivery == "normal" || delivery == "late_cancel" {
					fault = ""
				}
				probe := spReviewFixDeliveryWrap(t, h, fault, delivery == "late_cancel")
				resp, reader, cancel := h.request(t)
				first := spAcceptanceReadEnvelope(t, reader)
				state := h.provePath(t, 1)
				if first != spAcceptanceRate || state.committed() || state.retryUnsafe() || !state.canStart(context.Background()) {
					t.Fatal("early-exit fixture lacks a real rate read and open private retry boundary")
				}
				switch exit {
				case "local_guard":
					// A pinned request cannot retry with a changed final header.
					h.account.Mu().Lock()
					h.account.CustomHeaders["User-Agent"] = "synthetic-phase4-changed-agent"
					h.account.Mu().Unlock()
				case "quota":
					// A fresh authoritative rule is exhausted by another synthetic
					// logical request. A's existing durable charge is untouched.
					rules := []database.APIKeyModelRequestLimit{{ID: "synthetic-phase4-new-rule", Model: "gpt-5.5", MaxRequests: 1, Timezone: "UTC"}}
					if err := quotaDB.UpdateAPIKeyLimits(context.Background(), quota.keyID, database.APIKeyLimits{ModelRequestLimits: rules}); err != nil {
						t.Fatal("could not install synthetic quota rule")
					}
					if hit, err := quotaDB.ConsumeAPIKeyModelRequest(context.Background(), quota.keyID, "synthetic-phase4-other-request", "gpt-5.5", rules, time.Now()); err != nil || hit != nil {
						t.Fatal("could not exhaust synthetic quota rule")
					}
				case "no_account":
					spReviewFixCooldownSentinel(h) // Exclude the sole pinned account before selection B.
				}
				firstUsage := spReviewFixUsageWant{status: http.StatusBadGateway, input: 7, total: 7, cached: 2, kind: "server", retry: true}
				gate.open()
				var tail string
				if delivery == "late_cancel" {
					tail = spAcceptanceReadEnvelope(t, reader)
					select {
					case <-probe.flushEntered.ch:
					case <-time.After(5 * time.Second):
						t.Fatal("early-exit renderer did not reach its Flush return barrier")
					}
					select {
					case <-h.done:
						t.Fatal("early-exit Handler finalized before Flush returned")
					default:
					}
					spReviewFixUsage(t, db, h, firstUsage)
					if health := spAcceptanceHealthOf(h.account); health.streak != 1 || health.kind != "server" || health.last.IsZero() {
						t.Error("early-exit health did not retain only A while final Flush was held")
					}
					cancel() // The real client has consumed the complete selected terminal.
					_ = resp.Body.Close()
					spTerminalDeliveryWaitCancel(t, h)
				} else {
					tail = spAcceptanceReadRest(t, reader)
				}
				result := h.wait(t)
				wantCalls, wantAttempts, wantLeases, wantStatus, wantType := 1, 2, 2, http.StatusBadGateway, "response.failed"
				if exit == "transport" {
					wantCalls, wantStatus = 2, logStatusUpstreamStreamBreak
				} else if exit == "quota" {
					wantStatus, wantType = http.StatusTooManyRequests, "error"
				} else if exit == "no_account" {
					wantAttempts, wantLeases = 1, 1
				}
				if fault != "" {
					wantStatus = logStatusClientClosed
				}
				leases, calls := h.account.GetTotalRequests(), h.calls.Load()
				t.Logf("early exit %s: scheduling leases=%d (want %d), actual upstream calls=%d (want %d); CONNECT checked independently by provePath", exit, leases, wantLeases, calls, wantCalls)
				if leases != int64(wantLeases) {
					t.Errorf("early-exit scheduling lease count=%d, want %d; this is not the actual-send counter", leases, wantLeases)
				}
				if int(calls) != wantCalls {
					t.Errorf("early-exit actual upstream calls=%d, want %d; acquired unsent leases must not send, and sent A must not be refunded", calls, wantCalls)
				}
				h.provePath(t, wantCalls) // Retain the independent real TLS + CONNECT count assertion.
				if wasDropped := dropped.Load(); wasDropped != (exit == "transport") {
					t.Errorf("early-exit real second TLS POST dropped=%t, want %t", wasDropped, exit == "transport")
				}
				if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || result.logicalStatus != wantStatus {
					t.Errorf("early-exit client HTTP=%d, Handler HTTP=%d, logical=%d; want 200/200/%d", resp.StatusCode, result.httpStatus, result.logicalStatus, wantStatus)
				}
				attempted, ok := probe.terminalWire.Load().(string)
				if !ok || attempted == "" || probe.terminalWrites.Load() != 1 || probe.writes.Load() != 2 ||
					int64(len(tail)) != probe.terminalBytes.Load() || !strings.HasPrefix(attempted, tail) {
					t.Fatal("early exit did not attempt exactly one checked terminal")
				}
				spAcceptanceTerminal(t, attempted, wantType, "") // Attempted serialization only.
				if exit == "local_guard" && !strings.Contains(attempted, errSafePreflightLocal.Error()) ||
					exit == "quota" && (!strings.Contains(attempted, `"code":"rate_limit_reached"`) || !strings.Contains(attempted, `"rule_id":"synthetic-phase4-new-rule"`)) {
					t.Error("early-exit fixture did not reach the selected real guard/quota rejection")
				}
				if fault == "" {
					if tail != attempted || probe.terminalFlushes.Load() != 1 || delivery == "late_cancel" && !probe.flushSawCancel.Load() {
						t.Error("early-exit delivery lacks full write/flush and requested cancel ordering")
					}
				} else if probe.terminalFlushes.Load() != 0 {
					t.Error("early-exit write error was certified as flushed delivery")
				}
				switch fault {
				case "short_without_error", "partial_with_error":
					if len(tail) == 0 || len(tail) >= len(attempted) || strings.HasSuffix(tail, "\n\n") {
						t.Error("early-exit fixture did not truncate its terminal")
					}
				case "full_with_error":
					if tail != attempted {
						t.Error("early-exit full-with-error fixture lost attempted bytes")
					}
				case "write_failure", "zero_without_error":
					if tail != "" {
						t.Error("early-exit zero-write fixture delivered terminal bytes")
					}
				}
				state.mu.Lock()
				attempts, failed := state.attempts, state.writeFailed
				delivered, success := state.failedTerminalWrittenAndFlushed, state.successTerminalWrittenAndFlushed
				state.mu.Unlock()
				// no_account still describes sent A; local_guard/quota describe
				// the newly begun but UNSENT B, despite its acquired second lease.
				sent, wantSent := state.wasSent(), exit == "transport" || exit == "no_account"
				if attempts != wantAttempts || sent != wantSent {
					t.Errorf("early-exit attempts=%d (want %d), wasSent=%t (want %t), leases=%d, actual calls=%d", attempts, wantAttempts, sent, wantSent, leases, calls)
				}
				canStart := state.canStart(context.Background())
				if failed != (fault != "") || delivered != (fault == "") || success || canStart {
					t.Errorf("early-exit evidence: writeFailed=%t (want %t), failureDelivered=%t (want %t), successDelivered=%t, canStart=%t", failed, fault != "", delivered, fault == "", success, canStart)
				}
				wants := []spReviewFixUsageWant{firstUsage}
				wantHealth, wantKind := 1, "server"
				if exit == "transport" {
					wants = append(wants, spReviewFixUsageWant{status: wantStatus})
					if fault == "" {
						wantHealth, wantKind = 2, "transport"
					}
				}
				spReviewFixUsage(t, db, h, wants...)
				spReviewFixRateOnlyObservation(t, h)
				if health := spAcceptanceHealthOf(h.account); health.streak != wantHealth || health.kind != wantKind || health.last.IsZero() {
					t.Error("early exit penalized a local/delivery rejection or erased A's genuine health failure")
				}
				h.account.Mu().RLock()
				successes, lastSuccess := h.account.SuccessStreak, h.account.LastSuccessAt
				h.account.Mu().RUnlock()
				if successes != 0 || !lastSuccess.IsZero() || len(getResponseCache(responseCacheOwner(0), spReviewFixDeliveryID)) != 0 {
					t.Error("early failure published health success or continuation state")
				}
			})
		}
	}
}

// Transparent test-only context: all values, deadlines and cancellation still
// come from the real server context. runtime.CallersFrames expands inline
// frames; match the direct post-read Handler -> pending-deadline lookup, NOT a
// line number, source-text assertion, forward callback, or transport callback.
// A missing/changed stage fails the fixture's reached/hit assertions below.
// No production test hook, sender replacement, or deadline result is injected.
type spReviewFixPostReadContext struct {
	context.Context
	postRead func(context.Context, any)
}

func (c *spReviewFixPostReadContext) Value(key any) any {
	value := c.Context.Value(key)
	if _, ok := key.(continuousRetryDeadlineContextKey); ok && spReviewFixPostReadDeadlineStack() {
		c.postRead(c.Context, value)
	}
	return value
}

func spReviewFixPostReadDeadlineStack() bool {
	const prefix = "github.com/codex2api/proxy."
	var pcs [64]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		frame, more := frames.Next()
		if frame.Function == prefix+"continuousRetryDeadlineForContext" {
			if !more {
				return false
			}
			pending, more := frames.Next()
			if !more || pending.Function != prefix+"withContinuousRetryDeadlinePending" {
				return false
			}
			handler, _ := frames.Next()
			return handler.Function == prefix+"(*Handler).Responses"
		}
		if !more {
			return false
		}
	}
}

func TestSafePreflightReviewFixSuppressedFailureRetryWindowCloses(t *testing.T) {
	for _, expire := range []bool{true, false} {
		name := "expires_after_suppression_before_final_retry"
		if !expire {
			name = "window_open_retry_positive_control"
		}
		t.Run(name, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			db := spTerminalDeliveryDB(t)
			firstRelease, failureFlushed, stageRelease := spAcceptanceNewGate(), spAcceptanceNewGate(), spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 1, db, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					spAcceptanceFlush(w, spAcceptanceRate)
					if firstRelease.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixRateOnlyFailed))
						failureFlushed.open()
					}
					return
				}
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_phase4_window_B"))
			})
			spReviewFixFlushPolicy(StreamFlushPolicyImmediate)
			probe := spReviewFixDeliveryWrap(t, h, "", false)
			var stateRef atomic.Pointer[safePreflightState]
			var rateRead atomic.Bool
			var hookCalls atomic.Int32
			reached := make(chan bool, 1)
			wrapped := h.downstream.Config.Handler
			h.downstream.Close() // Reconfigure only an unused loopback listener.
			h.downstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := &spReviewFixPostReadContext{Context: r.Context(), postRead: func(parent context.Context, value any) {
					if hookCalls.Add(1) != 1 {
						return
					}
					flushed := false
					select {
					case <-failureFlushed.ch:
						flushed = true
					case <-time.After(5 * time.Second): // Deadlock guard, never a clock trigger.
					}
					state := stateRef.Load()
					valid := flushed && rateRead.Load() && value == nil && parent.Err() == nil && state != nil &&
						h.calls.Load() == 1 && h.account.GetTotalRequests() == 1 && h.store.GetMaxRetries() == 1 &&
						probe.writes.Load() == 1 && probe.terminalWrites.Load() == 0 && safePreflightRetryWindow == 60*time.Second
					if state != nil {
						state.mu.Lock()
						valid = valid && state.attempts == 1 && state.staged && state.sent && state.pinned &&
							!state.replayUnsafe && !state.protocolCommitted && !state.writeFailed && !state.localFailure &&
							!state.failedTerminalWrittenAndFlushed && !state.successTerminalWrittenAndFlushed &&
							time.Since(state.started) < safePreflightRetryWindow
						if valid && expire {
							// Synthetic CLOCK-PHASE control, not an actual 60-second wait.
							// Only age this request after the real read pump returned with
							// a private failed terminal; never widen the production window.
							state.started = time.Now().Add(-safePreflightRetryWindow - time.Second)
						}
						state.mu.Unlock()
					}
					reached <- valid
					stageRelease.wait(parent)
				}}
				wrapped.ServeHTTP(w, r.WithContext(ctx))
			}))
			t.Cleanup(h.downstream.Close)
			t.Cleanup(firstRelease.open)
			t.Cleanup(stageRelease.open)
			resp, reader, _ := h.request(t)
			first := spAcceptanceReadEnvelope(t, reader)
			state := h.provePath(t, 1)
			if first != spAcceptanceRate || state.committed() || state.retryUnsafe() || !state.canStart(context.Background()) {
				t.Fatal("window fixture lacks its real downstream rate read and initially open retry window")
			}
			stateRef.Store(state)
			rateRead.Store(true)
			before := spAcceptanceHealthOf(h.account)
			firstRelease.open()
			select {
			case valid := <-reached:
				if !valid {
					t.Fatal("clock stage was not strictly after real rate+failure reads with a still-private, retryable attempt")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("post-read/pre-final-retry runtime stage did not fire; no clock-phase coverage established")
			}
			// The direct post-read pending call requires a parsed failure payload.
			// With one available retry, no content/write/cancel and the still-open
			// pre-aging state above, that callback took suppression, not forwarding
			// or the zero-budget HTTP-error branch. Do not manufacture its result.
			h.provePath(t, 1)
			if hookCalls.Load() != 1 || state.canStart(context.Background()) == expire || state.retryUnsafe() ||
				probe.terminalWrites.Load() != 0 || spAcceptanceHealthOf(h.account) != before {
				t.Fatal("clock hook missed the suppression/final-retry boundary or changed something other than clock age")
			}
			spReviewFixUsage(t, db, h) // No finalized attempt at the held post-read boundary.
			spReviewFixRateOnlyObservation(t, h)
			if expire {
				spReviewFixCooldownSentinel(h) // No retry permitted after clock closure.
			}
			stageRelease.open()
			tail := spAcceptanceReadRest(t, reader)
			result := h.wait(t)
			wantCalls, wantStatus, wantType, wantID := 1, http.StatusBadGateway, "response.failed", ""
			wants := []spReviewFixUsageWant{{status: http.StatusBadGateway, input: 7, total: 7, cached: 2, kind: "server", retry: !expire}}
			if !expire {
				wantCalls, wantStatus, wantType, wantID = 2, http.StatusOK, "response.completed", "resp_synthetic_phase4_window_B"
				wants = append(wants, spReviewFixUsageWant{status: http.StatusOK, input: 1, output: 1, total: 2})
				h.assertStableIdentity(t)
			}
			h.provePath(t, wantCalls)
			logical := result.logicalStatus
			if logical == 0 {
				logical = result.httpStatus
			}
			if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || logical != wantStatus {
				t.Errorf("post-suppression final logical status=%d, want %d with published HTTP 200", logical, wantStatus)
			}
			state.mu.Lock()
			attempts, failed, local := state.attempts, state.writeFailed, state.localFailure
			delivered, success := state.failedTerminalWrittenAndFlushed, state.successTerminalWrittenAndFlushed
			state.mu.Unlock()
			if hookCalls.Load() != 1 || attempts != wantCalls || h.account.GetTotalRequests() != int64(wantCalls) || failed || local ||
				delivered != expire || success == expire || state.canStart(context.Background()) {
				t.Error("window closure created another attempt/local failure, or lost the selected terminal's delivery evidence")
			}
			spReviewFixUsage(t, db, h, wants...)
			spReviewFixRateOnlyObservation(t, h)
			spAcceptanceTerminal(t, first+tail, wantType, wantID)
			if probe.writes.Load() != 2 || probe.terminalWrites.Load() != 1 || probe.terminalFlushes.Load() != 1 ||
				probe.terminalLength.Load() == 0 || int64(len(tail)) != probe.terminalLength.Load() ||
				probe.terminalBytes.Load() != probe.terminalLength.Load() || strings.Contains(tail, spReviewFixDeliveryID) {
				t.Error("suppression left rate+EOF, duplicated a terminal, or leaked the private failed response identity")
			}
			if expire {
				spReviewFixNoSuccess(t, h)
				health := spAcceptanceHealthOf(h.account)
				if health.streak != 1 || health.kind != "server" || health.last.IsZero() {
					t.Error("clock-expired final failure lost its one semantic health outcome")
				}
				if attempted := spReviewFixFinalRendererAttempt(t, probe); tail != attempted {
					t.Error("clock-expired failure did not reach the real downstream exactly once")
				}
			} else {
				if strings.Contains(tail, "synthetic-review-rate-only-failure") || spAcceptanceHealthOf(h.account).streak != 0 {
					t.Error("open-window success control leaked the discarded failure or lost health success")
				}
			}
		})
	}
}

func TestSafePreflightReviewFixRateOnlyFinalRendererPrewriteCancel(t *testing.T) {
	for _, policy := range []string{StreamFlushPolicyImmediate, StreamFlushPolicyCoalesce} {
		t.Run(policy+"/shared_final_renderer/drain_only", func(t *testing.T) {
			db, h, gate, probe := spReviewFixRateOnlyHarness(t, policy, "", false)
			resp, reader, cancel := h.request(t)
			spReviewFixRateOnlyBarrier(t, h, spAcceptanceReadEnvelope(t, reader))
			spReviewFixCooldownSentinel(h)
			before := spAcceptanceHealthOf(h.account)
			writes := probe.writes.Load()
			cancel()
			_ = resp.Body.Close()
			spTerminalDeliveryWaitCancel(t, h)
			gate.open() // Zero-budget failure is observed ONLY in real upstream drain.
			result := h.wait(t)
			h.provePath(t, 1)
			if result.httpStatus != http.StatusOK || result.logicalStatus != logStatusClientClosed {
				t.Error("cancel before final rendering did not retain published HTTP 200 and logical 499")
			}
			if probe.writes.Load() != writes || probe.terminalWrites.Load() != 0 || probe.terminalFlushes.Load() != 0 {
				t.Error("zero-budget final renderer attempted output after observed cancellation")
			}
			spReviewFixRateOnlyUsage(t, db, h, logStatusClientClosed)
			spReviewFixNoSuccess(t, h)
			spReviewFixRateOnlyObservation(t, h)
			if spAcceptanceHealthOf(h.account) != before {
				t.Error("drain-only failure incorrectly changed upstream health")
			}
		})
	}
}
