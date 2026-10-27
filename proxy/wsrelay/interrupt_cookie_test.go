package wsrelay

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

const (
	interruptCookieResponseID = "resp_synthetic_interrupt"
	interruptCookieAttemptID  = "synthetic-interrupt-attempt"
	interruptCookieMetadata   = `{"type":"codex.response.metadata","headers":{"cf-ray":"synthetic-turn-ray","x-request-id":"synthetic-turn-request","ignored":"synthetic-metadata-not-for-log"}}`
	interruptCookieDelta      = `{"type":"response.output_text.delta","delta":"synthetic-output-not-for-log"}`
	interruptCookieTerminal   = `{"type":"response.incomplete","sequence_number":9007199254740993,"response":{"id":"resp_synthetic_interrupt","status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[{"text":"synthetic-output-not-for-log"}],"usage":{"input_tokens":11,"output_tokens":2}}}`
	interruptCookieAck        = `{"type":"response.interrupt.accepted","response_id":"resp_synthetic_interrupt"}`
)

func newInterruptCookieTrace(ctx context.Context) *proxy.UpstreamCookieTrace {
	return proxy.NewUpstreamCookieTrace(ctx, &auth.Account{DBID: 7}, "websocket", "synthetic-cookie-connection", http.Header{
		"Cookie":        {"__cflb=synthetic-request-cookie"},
		"Authorization": {"Bearer synthetic-authorization-not-for-log"},
	}, &http.Response{StatusCode: http.StatusSwitchingProtocols, Header: http.Header{
		"Set-Cookie":   {"__oailb=" + websocketCookieToken() + "; Path=/; HttpOnly", "__cflb=synthetic-response-cookie"},
		"Cf-Ray":       {"synthetic-handshake-ray"},
		"X-Request-Id": {"synthetic-handshake-request"},
	}}).BeginTurn(ctx, interruptCookieAttemptID, "synthetic-model")
}

// Use the real lease queue and ReadStream dispatcher, but no sockets, workers,
// authentication or upstream requests. A final synthetic cancellation makes an
// unexpected read past the supplied frames fail immediately rather than hang.
func interruptCookieReadFixture(t *testing.T, trace *proxy.UpstreamCookieTrace, interrupted bool, frames []string) *WsResponse {
	t.Helper()
	conn := NewWsConnection(nil, NewSession(7, &Manager{}), "ws://127.0.0.1/unused")
	conn.SetState(StateConnected)
	conn.readPumpOnce.Do(func() {}) // The in-memory queue is the only frame source.
	if err := conn.ensureReadLeaseForResponse(interruptCookieAttemptID); err != nil {
		t.Fatal(err)
	}
	r := &WsResponse{conn: conn, pendingReq: &PendingRequest{RequestID: interruptCookieAttemptID}, cookieTrace: trace}
	state := conn.ensureReadState()
	if interrupted {
		r.interruptResponseID = interruptCookieResponseID
		state.mu.Lock()
		state.interruptResponseID = interruptCookieResponseID
		state.mu.Unlock()
	}
	for _, frame := range frames {
		if err := conn.enqueueBusinessFrameForLease(websocket.TextMessage, []byte(frame), interruptCookieAttemptID); err != nil {
			t.Fatal(err)
		}
	}
	state.mu.Lock()
	state.appendQueueItemLocked(readPumpItem{err: context.Canceled, leaseID: interruptCookieAttemptID})
	state.mu.Unlock()
	return r
}

func assertInterruptCookieEntries(t *testing.T, ctx context.Context, terminalType string, status int, errorCode string) {
	t.Helper()
	entries := websocketCookieEntries(t, ctx)
	want := 2
	if terminalType != "" {
		want++
	}
	if len(entries) != want {
		t.Fatalf("cookie entries = %d, want %d (one handshake, one turn, at most one terminal)", len(entries), want)
	}
	if entries[0].Get("event").String() != "websocket_handshake" || entries[1].Get("event").String() != "websocket_turn" {
		t.Fatal("handshake or turn-start entry lost")
	}
	for i, entry := range entries {
		for path, expected := range map[string]string{
			"connection_id":        "synthetic-cookie-connection",
			"cookie.0":             "__cflb=synthetic-request-cookie",
			"handshake_cf_ray":     "synthetic-handshake-ray",
			"handshake_request_id": "synthetic-handshake-request",
			"unified_host":         "chat.gateway.unified-183.api.openai.com",
		} {
			if entry.Get(path).String() != expected {
				t.Errorf("entry %d lost %s", i, path)
			}
		}
		if entry.Get("http_status").Int() != http.StatusSwitchingProtocols || entry.Get("account_id").Int() != 7 || len(entry.Get("set_cookie").Array()) != 2 {
			t.Errorf("entry %d lost handshake status, account or cookies", i)
		}
		if i > 0 && (entry.Get("attempt_id").String() != interruptCookieAttemptID || entry.Get("model").String() != "synthetic-model") {
			t.Errorf("entry %d lost turn identity", i)
		}
		if i < 2 && (entry.Get("cf_ray").Exists() || entry.Get("upstream_request_id").Exists()) {
			t.Errorf("entry %d mixed handshake and turn metadata", i)
		}
	}
	if terminalType != "" {
		terminal := entries[2]
		if terminal.Get("event").String() != terminalType || terminal.Get("status").Int() != int64(status) || terminal.Get("error_code").String() != errorCode {
			t.Errorf("terminal classification = (%s, %d, %s), want (%s, %d, %s)", terminal.Get("event").String(), terminal.Get("status").Int(), terminal.Get("error_code").String(), terminalType, status, errorCode)
		}
		if terminal.Get("cf_ray").String() != "synthetic-turn-ray" || terminal.Get("upstream_request_id").String() != "synthetic-turn-request" {
			t.Error("terminal lost per-turn metadata")
		}
	}
	raw := proxy.SnapshotUpstreamCookieLog(ctx)
	for _, excluded := range []string{"synthetic-output-not-for-log", "synthetic-metadata-not-for-log", "synthetic-authorization-not-for-log", "synthetic-error-message-not-for-log", "sequence_number", "input_tokens"} {
		if strings.Contains(raw, excluded) {
			t.Error("cookie diagnostics retained frame content or unrelated headers")
		}
	}
}

func TestInterruptCookieReadStreamOrdering(t *testing.T) {
	for _, terminal := range []struct {
		name, frame, event string
	}{
		{"incomplete", interruptCookieTerminal, "response.incomplete"},
		{"done_incomplete", strings.Replace(interruptCookieTerminal, `"type":"response.incomplete"`, `"type":"response.done"`, 1), "response.incomplete"},
		{"completed", `{"type":"response.completed","response":{"id":"resp_synthetic_interrupt","status":"completed","output":[{"text":"synthetic-output-not-for-log"}]}}`, "response.completed"},
		{"done_completed", `{"type":"response.done","response":{"id":"resp_synthetic_interrupt","status":"completed","output":[{"text":"synthetic-output-not-for-log"}]}}`, "response.completed"},
	} {
		for _, ackType := range []string{"response.interrupt.accepted", "response.interrupt.failed"} {
			for _, order := range []string{"terminal_before_ack", "ack_before_terminal", "duplicate_terminal_before_ack"} {
				t.Run(terminal.name+"/"+ackType+"/"+order, func(t *testing.T) {
					ctx := websocketCookieLogSetup(t)
					trace := newInterruptCookieTrace(ctx)
					ack := strings.Replace(interruptCookieAck, "response.interrupt.accepted", ackType, 1)
					normalized := strings.Replace(terminal.frame, `"type":"response.done"`, `"type":"`+terminal.event+`"`, 1)
					frames := []string{interruptCookieMetadata, interruptCookieDelta, terminal.frame, ack}
					want := []string{interruptCookieMetadata, interruptCookieDelta, normalized, ack}
					if order == "ack_before_terminal" {
						frames[2], frames[3] = ack, terminal.frame
						want[2], want[3] = ack, normalized
					} else if order == "duplicate_terminal_before_ack" {
						frames = []string{interruptCookieMetadata, interruptCookieDelta, terminal.frame, terminal.frame, ack}
					}
					r := interruptCookieReadFixture(t, trace, true, frames)
					calls := 0
					err := r.ReadStream(func(data []byte) bool {
						if calls >= len(want) || string(data) != want[calls] {
							t.Error("delivered frame content or order changed")
						}
						calls++
						if gjson.GetBytes(data, "type").String() == terminal.event {
							assertInterruptCookieEntries(t, ctx, terminal.event, http.StatusOK, "")
							if r.streamCompleted != (order == "ack_before_terminal") {
								t.Error("interrupt completion boundary changed before callback")
							}
						}
						return true
					})
					if err != nil || calls != len(want) || !r.streamCompleted || r.connBroken || r.interruptResponseID != "" {
						t.Fatalf("ReadStream: err=%v calls=%d completed=%v broken=%v pendingInterrupt=%v", err, calls, r.streamCompleted, r.connBroken, r.interruptResponseID != "")
					}
					assertInterruptCookieEntries(t, ctx, terminal.event, http.StatusOK, "")
					if websocketCookieEntries(t, ctx)[2].Get("response_id").String() != interruptCookieResponseID {
						t.Error("terminal lost response ID")
					}
				})
			}
		}
	}
}

func TestInterruptCookieReadStreamFailures(t *testing.T) {
	for _, tc := range []struct {
		name, frame, code string
		status            int
	}{
		{"failed", `{"type":"response.failed","response":{"id":"resp_synthetic_interrupt","status":"failed","status_code":503,"error":{"code":"server_error","message":"synthetic-error-message-not-for-log"}}}`, "server_error", 503},
		{"done_failed", `{"type":"response.done","response":{"id":"resp_synthetic_interrupt","status":"failed","status_code":503,"error":{"code":"server_error"}}}`, "server_error", 503},
		{"completed_failed", `{"type":"response.completed","response":{"id":"resp_synthetic_interrupt","status":"failed","status_code":503,"error":{"code":"server_error"}}}`, "server_error", 503},
		{"error_status", `{"type":"error","status":429,"message":"synthetic-error-message-not-for-log"}`, "429", 429},
		{"error_status_code", `{"type":"error","status_code":429,"error":{"code":"turn_rate_limit_exceeded","type":"rate_limit_error","message":"synthetic-error-message-not-for-log"}}`, "turn_rate_limit_exceeded", 429},
		{"error_status_precedence", `{"type":"error","status":401,"status_code":429,"message":"synthetic-error-message-not-for-log"}`, "401", 401},
	} {
		for _, mode := range []string{"normal", "pending_interrupt", "acknowledged_interrupt"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				ctx := websocketCookieLogSetup(t)
				trace := newInterruptCookieTrace(ctx)
				frames := []string{interruptCookieMetadata, interruptCookieDelta}
				if mode == "acknowledged_interrupt" {
					frames = append(frames, interruptCookieAck)
				}
				frames = append(frames, tc.frame)
				r := interruptCookieReadFixture(t, trace, mode != "normal", frames)
				calls := 0
				err := r.ReadStream(func(data []byte) bool {
					calls++
					if gjson.GetBytes(data, "type").String() == "response.failed" {
						assertInterruptCookieEntries(t, ctx, "response.failed", tc.status, tc.code)
						if gjson.GetBytes(data, "response.error.code").String() != tc.code {
							t.Error("downstream error classification changed")
						}
					}
					return true
				})
				if err != nil || calls != len(frames) || !r.streamCompleted || r.connBroken {
					t.Fatalf("failure delivery changed: err=%v calls=%d completed=%v broken=%v", err, calls, r.streamCompleted, r.connBroken)
				}
				assertInterruptCookieEntries(t, ctx, "response.failed", tc.status, tc.code)
				// Pending failure/ack lifecycle is intentionally outside this fix.
				if (r.interruptResponseID != "") != (mode == "pending_interrupt") {
					t.Error("failure changed pending interrupt semantics")
				}
			})
		}
	}
}

func TestInterruptCookieReadStreamDisabled(t *testing.T) {
	for _, disableAfterStart := range []bool{false, true} {
		for _, ackFirst := range []bool{false, true} {
			name := "disabled_at_start"
			if disableAfterStart {
				name = "disabled_before_frames"
			}
			if ackFirst {
				name += "/ack_first"
			} else {
				name += "/terminal_first"
			}
			t.Run(name, func(t *testing.T) {
				ctx := websocketCookieLogSetup(t)
				var trace *proxy.UpstreamCookieTrace
				if disableAfterStart {
					trace = newInterruptCookieTrace(ctx)
				}
				proxy.UpdateRuntimeSettings(func(s proxy.RuntimeSettings) proxy.RuntimeSettings {
					s.CodexUpstreamCookieLogEnabled = false
					return s
				})
				if !disableAfterStart {
					trace = newInterruptCookieTrace(ctx)
					if trace != nil {
						t.Fatal("disabled collection created a trace")
					}
				}
				before := proxy.SnapshotUpstreamCookieLog(ctx)
				frames := []string{interruptCookieMetadata, interruptCookieTerminal, interruptCookieAck}
				if ackFirst {
					frames[1], frames[2] = frames[2], frames[1]
				}
				r := interruptCookieReadFixture(t, trace, true, frames)
				calls := 0
				if err := r.ReadStream(func([]byte) bool { calls++; return true }); err != nil || calls != len(frames) {
					t.Fatalf("disabled collection changed delivery: err=%v calls=%d", err, calls)
				}
				if proxy.SnapshotUpstreamCookieLog(ctx) != before {
					t.Error("collection bypassed the runtime switch")
				}
			})
		}
	}
}

func TestInterruptCookieReadStreamCallbackStop(t *testing.T) {
	for _, tc := range []struct {
		name         string
		ackFirst     bool
		stopType     string
		wantTerminal bool
		wantBroken   bool
	}{
		{"delta", false, "response.output_text.delta", false, true},
		{"terminal_before_ack", false, "response.incomplete", true, true},
		{"ack_before_terminal", true, "response.interrupt.accepted", false, true},
		{"terminal_after_ack", true, "response.incomplete", true, false},
		{"ack_after_terminal", false, "response.interrupt.accepted", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := websocketCookieLogSetup(t)
			frames := []string{interruptCookieMetadata, interruptCookieDelta, interruptCookieTerminal, interruptCookieAck}
			if tc.ackFirst {
				frames[2], frames[3] = frames[3], frames[2]
			}
			r := interruptCookieReadFixture(t, newInterruptCookieTrace(ctx), true, frames)
			calls, stopped := 0, false
			err := r.ReadStream(func(data []byte) bool {
				if stopped || calls >= len(frames) || string(data) != frames[calls] {
					t.Error("delivered content changed or callback continued after stop")
				}
				calls++
				stopped = gjson.GetBytes(data, "type").String() == tc.stopType
				return !stopped
			})
			if err != nil || !stopped || !r.streamCompleted || r.connBroken != tc.wantBroken {
				t.Fatalf("callback stop semantics changed: err=%v stopped=%v completed=%v broken=%v", err, stopped, r.streamCompleted, r.connBroken)
			}
			terminal := ""
			if tc.wantTerminal {
				terminal = "response.incomplete"
			}
			assertInterruptCookieEntries(t, ctx, terminal, http.StatusOK, "")
		})
	}
}

func TestInterruptCookieReadStreamReadCancellation(t *testing.T) {
	ctx := websocketCookieLogSetup(t)
	frames := []string{interruptCookieMetadata, interruptCookieDelta}
	r := interruptCookieReadFixture(t, newInterruptCookieTrace(ctx), true, frames)
	calls := 0
	err := r.ReadStream(func(data []byte) bool {
		if calls >= len(frames) || string(data) != frames[calls] {
			t.Error("already delivered content changed")
		}
		calls++
		return true
	})
	if !errors.Is(err, context.Canceled) || calls != len(frames) || !r.connBroken || r.streamCompleted {
		t.Fatalf("read cancellation semantics changed: err=%v calls=%d broken=%v completed=%v", err, calls, r.connBroken, r.streamCompleted)
	}
	assertInterruptCookieEntries(t, ctx, "", 0, "")
}
