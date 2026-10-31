package proxy

// Local delivery evidence, not production/client acceptance: all traffic uses
// the existing synthetic TLS + explicit loopback CONNECT Handler fixture. The
// downstream really reads a complete SSE envelope before canceling. A wrapper
// OUTSIDE Gin delegates Write/Flush through the normal writer/headers and holds
// the terminal Flush until the server has observed that cancellation. This
// fixes the race without sleeps, fake upstream executors, or invented usage.
// http.Flusher has no error result; its return is not a client parsing receipt.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/database"
	"github.com/tidwall/gjson"
)

const (
	spTerminalDeliveryID       = "resp_synthetic_terminal_delivery"
	spTerminalDeliveryComplete = `{"type":"response.completed","response":{"id":"resp_synthetic_terminal_delivery","status":"completed","output":[{"type":"function_call","id":"fc_synthetic_delivery","call_id":"call_synthetic_delivery","name":"synthetic_client_function","arguments":"{}"}],"usage":{"input_tokens":7,"output_tokens":11,"total_tokens":18}}}`
)

type spTerminalDeliveryProbe struct {
	fault           string
	blockFlush      bool
	flushEntered    chan struct{}
	canceled        <-chan struct{}
	abort           *spAcceptanceGate
	writes          atomic.Int32
	terminalWrites  atomic.Int32
	terminalBytes   atomic.Int64
	terminalLength  atomic.Int64
	terminalFlushes atomic.Int32
	flushSawCancel  atomic.Bool
}

type spTerminalDeliveryWriter struct {
	http.ResponseWriter
	ctx             context.Context
	probe           *spTerminalDeliveryProbe
	terminalPending bool
}

func (w *spTerminalDeliveryWriter) Write(p []byte) (int, error) {
	probe := w.probe
	probe.writes.Add(1)
	terminal := bytes.Contains(p, []byte(`"type":"response.completed"`))
	if !terminal {
		return w.ResponseWriter.Write(p)
	}
	probe.terminalWrites.Add(1)
	probe.terminalLength.Store(int64(len(p)))
	wire := p
	if probe.fault == "short_without_error" || probe.fault == "partial_with_error" {
		wire = p[:len(p)/2]
	} else if probe.fault == "write_failure" {
		wire = nil
	}
	var n int
	var err error
	if len(wire) > 0 {
		n, err = w.ResponseWriter.Write(wire)
	}
	probe.terminalBytes.Store(int64(n))
	if err == nil && probe.fault != "" && probe.fault != "short_without_error" {
		err = io.ErrClosedPipe // synthetic local failure, never upstream evidence
	}
	if n == len(p) && err == nil {
		w.terminalPending = true
	}
	return n, err
}

func (w *spTerminalDeliveryWriter) Flush() {
	// Do not unwrap Gin or use ResponseController to bypass its header/write
	// bookkeeping. Gin calls this wrapper, which flushes the actual transport.
	w.ResponseWriter.(http.Flusher).Flush()
	if !w.terminalPending {
		return
	}
	w.terminalPending = false
	w.probe.terminalFlushes.Add(1)
	if w.probe.blockFlush {
		close(w.probe.flushEntered)
		select {
		case <-w.probe.canceled:
			w.probe.flushSawCancel.Store(w.ctx.Err() != nil)
		case <-w.probe.abort.ch:
			// Cleanup escape only. The success assertion requires real cancel.
		}
	}
}

func spTerminalDeliveryWrap(t *testing.T, h *spAcceptanceHarness, fault string, blockFlush bool) *spTerminalDeliveryProbe {
	t.Helper()
	probe := &spTerminalDeliveryProbe{
		fault: fault, blockFlush: blockFlush, flushEntered: make(chan struct{}),
		canceled: h.canceled, abort: spAcceptanceNewGate(),
	}
	// Configure a new unused listener, never mutate a running server's Handler.
	h.downstream.Close()
	h.downstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.router.ServeHTTP(&spTerminalDeliveryWriter{ResponseWriter: w, ctx: r.Context(), probe: probe}, r)
	}))
	t.Cleanup(h.downstream.Close)
	t.Cleanup(probe.abort.open)
	return probe
}

func spTerminalDeliveryDB(t *testing.T) *database.DB {
	t.Helper()
	db, err := database.New("sqlite", filepath.Join(t.TempDir(), "synthetic-terminal-delivery.db"))
	if err != nil {
		t.Fatal("could not create isolated synthetic usage database")
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetUsageLogConfig("full", 100, 60)
	return db
}

func spTerminalDeliveryUsage(t *testing.T, db *database.DB, h *spAcceptanceHarness, status int) {
	t.Helper()
	db.FlushUsageLogs()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Default usage queries hide 499; explicitly include it so absence cannot
	// masquerade as a correct canceled-attempt record.
	logs, err := db.ListUsageLogsByFilter(ctx, database.UsageLogFilter{
		Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour), IncludeCanceled: true,
	})
	if err != nil || len(logs) != 1 {
		t.Fatal("expected exactly one isolated usage row, including canceled requests")
	}
	row := logs[0]
	if row.StatusCode != status || row.AccountID != h.account.ID() || row.Endpoint != "/v1/responses" ||
		!row.Stream || row.ViaWebsocket || row.IsRetryAttempt || row.AttemptIndex != 1 {
		t.Error("usage lost the actual single HTTP attempt or its logical outcome")
	}
	if row.InputTokens != 7 || row.OutputTokens != 11 || row.TotalTokens != 18 {
		t.Error("usage did not retain the synthetic terminal's exact counters")
	}
	if status == http.StatusOK && (row.ErrorMessage != "" || row.UpstreamErrorKind != "") {
		t.Error("successful local terminal delivery retained failure usage metadata")
	}
}

func spTerminalDeliveryEvidence(t *testing.T, state *safePreflightState, want bool) {
	t.Helper()
	state.mu.Lock()
	got := state.successTerminalWrittenAndFlushed
	state.mu.Unlock()
	if got != want {
		t.Errorf("local complete terminal write/flush evidence=%v, want %v", got, want)
	}
}

func spTerminalDeliveryWaitCancel(t *testing.T, h *spAcceptanceHarness) {
	t.Helper()
	select {
	case <-h.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe real downstream cancellation")
	}
}

func TestSafePreflightTerminalDeliveryLateCancelPreservesSuccess(t *testing.T) {
	spAcceptanceIsolateCache(t)
	db := spTerminalDeliveryDB(t)
	gate := spAcceptanceNewGate()
	h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
		spAcceptanceFlush(w, spAcceptanceRate)
		if gate.wait(r.Context()) {
			spAcceptanceFlush(w, spAcceptanceFrame(spTerminalDeliveryComplete))
		}
	})
	t.Cleanup(gate.open)
	probe := spTerminalDeliveryWrap(t, h, "", true)
	resp, reader, cancel := h.request(t)
	first := spAcceptanceReadEnvelope(t, reader)
	if first != spAcceptanceRate || resp.StatusCode != http.StatusOK {
		t.Fatal("missing genuine downstream rate/header barrier")
	}
	gate.open()
	terminal := spAcceptanceReadEnvelope(t, reader)
	spAcceptanceTerminal(t, first+terminal, "response.completed", spTerminalDeliveryID)
	select {
	case <-probe.flushEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal transport flush did not reach the controlled return barrier")
	}
	state := h.provePath(t, 1)
	spTerminalDeliveryEvidence(t, state, false) // Flush has not returned yet.
	select {
	case <-h.done:
		t.Fatal("Handler finalized before the controlled terminal Flush returned")
	default:
	}
	// The complete envelope was parsed above by the actual downstream reader.
	// Only now close SSE; the server-side Flush cannot return before cancel.
	cancel()
	_ = resp.Body.Close()
	spTerminalDeliveryWaitCancel(t, h)
	result := h.wait(t)
	h.provePath(t, 1)
	if !probe.flushSawCancel.Load() || probe.terminalFlushes.Load() != 1 || probe.terminalWrites.Load() != 1 ||
		probe.terminalBytes.Load() != probe.terminalLength.Load() {
		t.Fatal("fixture did not fix the complete-write/flush-return/cancel race")
	}
	if result.httpStatus != http.StatusOK || result.logicalStatus != 0 && result.logicalStatus != http.StatusOK {
		t.Errorf("delivered success became failure: HTTP=%d logical=%d", result.httpStatus, result.logicalStatus)
	}
	spTerminalDeliveryEvidence(t, state, true)
	spTerminalDeliveryUsage(t, db, h, http.StatusOK)
	items := getResponseCache(responseCacheOwner(0), spTerminalDeliveryID)
	var input, call bool
	for _, item := range items {
		parsed := gjson.ParseBytes(item)
		input = input || parsed.Get("role").String() == "user" && strings.Contains(string(item), "synthetic-input")
		call = call || parsed.Get("type").String() == "function_call" && parsed.Get("call_id").String() == "call_synthetic_delivery"
	}
	if !input || !call {
		t.Error("late cancellation discarded shared completed-response continuation cache")
	}
	if len(getResponseCache(responseCacheOwner(971032), spTerminalDeliveryID)) != 0 {
		t.Error("successful cache escaped its owner boundary")
	}
}

func TestSafePreflightTerminalDeliveryDrainObservationIsNotSuccess(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "rate_only"
		if committed {
			name = "content_already_committed"
		}
		t.Run(name, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			db := spTerminalDeliveryDB(t)
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
				prefix := spAcceptanceRate
				if committed {
					prefix += spAcceptanceFrame(`{"type":"response.output_text.delta","delta":"synthetic-before-cancel"}`)
				}
				spAcceptanceFlush(w, prefix)
				if gate.wait(r.Context()) {
					spAcceptanceFlush(w, spAcceptanceFrame(spTerminalDeliveryComplete))
				}
			})
			t.Cleanup(gate.open)
			probe := spTerminalDeliveryWrap(t, h, "", false)
			resp, reader, cancel := h.request(t)
			if spAcceptanceReadEnvelope(t, reader) != spAcceptanceRate {
				t.Fatal("missing genuine downstream rate barrier")
			}
			if committed && !strings.Contains(spAcceptanceReadEnvelope(t, reader), "synthetic-before-cancel") {
				t.Fatal("missing genuine downstream content-commit barrier")
			}
			state := h.provePath(t, 1)
			if state.committed() != committed {
				t.Fatal("fixture did not establish the requested protocol-commit control")
			}
			writesBeforeCancel := probe.writes.Load()
			cancel()
			_ = resp.Body.Close()
			spTerminalDeliveryWaitCancel(t, h)
			gate.open() // Only the ordinary bounded usage drain can see completed.
			result := h.wait(t)
			h.provePath(t, 1)
			if result.logicalStatus != logStatusClientClosed {
				t.Errorf("drain-only terminal logical status=%d, want 499", result.logicalStatus)
			}
			if probe.writes.Load() != writesBeforeCancel || probe.terminalWrites.Load() != 0 || probe.terminalFlushes.Load() != 0 {
				t.Error("usage drain attempted downstream delivery after cancellation")
			}
			spTerminalDeliveryEvidence(t, state, false)
			// Exact usage proves terminal observation, explicitly NOT delivery.
			spTerminalDeliveryUsage(t, db, h, logStatusClientClosed)
			if len(getResponseCache(responseCacheOwner(0), spTerminalDeliveryID)) != 0 {
				t.Error("drain-only completed response was cached")
			}
		})
	}
}

func TestSafePreflightTerminalDeliveryWriteFailuresAreNotSuccess(t *testing.T) {
	for _, fault := range []string{"short_without_error", "partial_with_error", "write_failure", "full_with_error"} {
		t.Run(fault, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			db := spTerminalDeliveryDB(t)
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, r *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceRate)
				if gate.wait(r.Context()) {
					spAcceptanceFlush(w, spAcceptanceFrame(spTerminalDeliveryComplete))
				}
			})
			t.Cleanup(gate.open)
			probe := spTerminalDeliveryWrap(t, h, fault, false)
			_, reader, _ := h.request(t)
			if spAcceptanceReadEnvelope(t, reader) != spAcceptanceRate {
				t.Fatal("missing genuine downstream read barrier before terminal write fault")
			}
			gate.open()
			tail := spAcceptanceReadRest(t, reader)
			result := h.wait(t)
			state := h.provePath(t, 1)
			if result.logicalStatus != logStatusClientClosed || probe.terminalWrites.Load() != 1 || probe.terminalFlushes.Load() != 0 {
				t.Error("terminal write fault was ignored, retried, or counted as flushed success")
			}
			written, length := probe.terminalBytes.Load(), probe.terminalLength.Load()
			switch fault {
			case "short_without_error", "partial_with_error":
				if written <= 0 || written >= length || strings.HasSuffix(tail, "\n\n") {
					t.Error("fixture did not produce a genuinely partial terminal envelope")
				}
			case "write_failure":
				if written != 0 || tail != "" {
					t.Error("failed terminal write unexpectedly delivered bytes")
				}
			case "full_with_error":
				if written != length || length == 0 {
					t.Error("fixture did not return an error alongside a full terminal write")
				}
			}
			state.mu.Lock()
			failed := state.writeFailed
			state.mu.Unlock()
			if !failed {
				t.Error("terminal write fault did not close the replay window")
			}
			spTerminalDeliveryEvidence(t, state, false)
			spTerminalDeliveryUsage(t, db, h, logStatusClientClosed)
			if len(getResponseCache(responseCacheOwner(0), spTerminalDeliveryID)) != 0 {
				t.Error("partial/failed terminal delivery published a success cache entry")
			}
		})
	}
}
