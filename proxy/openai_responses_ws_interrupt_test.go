package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

func TestOpenAIResponsesWSInterruptPreservesTerminalClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		payload  string
		wantType string
		finished bool
	}{
		{"failed_alias", `{"type":"response.done","response":{"status":"failed","error":{"code":"server_error"}}}`, "response.failed", true},
		{"incomplete_alias", `{"type":"response.done","response":{"status":"incomplete","incomplete_details":{"reason":"interrupted"}}}`, "response.incomplete", true},
		{"error_event", `{"type":"error","error":{"code":"server_error"}}`, "error", true},
		{"unknown_status", `{"type":"response.done","response":{"status":"provider_pending_review"}}`, "response.done", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := &openAIResponsesWSInterrupt{}
			frame, err := control.accept([]byte(tc.payload))
			if err != nil {
				t.Fatalf("accept: %v", err)
			}
			if got := gjson.GetBytes(frame, "type").String(); got != tc.wantType {
				t.Errorf("event type = %q, want %q", got, tc.wantType)
			}
			if control.done() != tc.finished {
				t.Errorf("finished = %v, want %v", control.done(), tc.finished)
			}
			for _, path := range []string{"response", "error"} {
				if gjson.GetBytes(frame, path).Raw != gjson.Get(tc.payload, path).Raw {
					t.Errorf("metadata changed at %s", path)
				}
			}
		})
	}
}

func TestOpenAIResponsesWSInterruptFailureStopsAckTimer(t *testing.T) {
	for _, frame := range []string{
		`{"type":"response.failed","response":{"status":"failed"}}`,
		`{"type":"response.done","response":{"status":"failed"}}`,
		`{"type":"error","error":{"code":"fixture_error"}}`,
	} {
		for _, ackFirst := range []bool{false, true} {
			control := &openAIResponsesWSInterrupt{
				responseID: "resp_fixture", timer: time.NewTimer(time.Hour),
			}
			defer control.stop()
			if ackFirst {
				if _, err := control.accept([]byte(`{"type":"response.interrupt.accepted","response_id":"resp_fixture"}`)); err != nil {
					t.Fatal(err)
				}
				if control.done() || control.reusable() {
					t.Fatal("ack alone finished or released the lease")
				}
			}
			if _, err := control.accept([]byte(frame)); err != nil {
				t.Fatal(err)
			}
			if !control.done() || control.reusable() != ackFirst {
				t.Errorf("failure lease: done=%v reusable=%v, ackFirst=%v", control.done(), control.reusable(), ackFirst)
			}
			// Stop returns false only if accept already stopped this hour-long timer.
			if control.timer.Stop() {
				t.Error("failure left the ack timer running")
			}
			if err := control.send(context.Background(), nil, nil); !errors.Is(err, ErrResponsesWSInterruptFinished) {
				t.Errorf("late callback error = %v, want finished", err)
			}
		}
	}
}

func TestOpenAIResponsesWSInterruptStopSealsCallback(t *testing.T) {
	control := &openAIResponsesWSInterrupt{timer: time.NewTimer(time.Hour)}
	control.stop()
	if control.timer.Stop() {
		t.Error("stop left the ack timer running")
	}
	// Early EOF/downstream failure must seal a retained callback too, before
	// it can touch a retired socket or write into a later lease.
	if err := control.send(context.Background(), nil, nil); !errors.Is(err, ErrResponsesWSInterruptFinished) {
		t.Errorf("stopped callback error = %v, want finished", err)
	}
}

// Exercise the merge boundary between terminal normalization and interrupt
// acknowledgements using only loopback sockets and synthetic protocol frames.
func TestOpenAIResponsesWSStreamInterruptTerminalAliases(t *testing.T) {
	const responseID = "resp_interrupt_fixture"
	const ack = `{"type":"response.interrupt.accepted","response_id":"resp_interrupt_fixture","sequence_number":9007199254740993}`
	for _, tc := range []struct {
		name        string
		terminal    string
		wantType    string
		ackFirst    bool
		wantBinding bool
	}{
		{"done_completed_terminal_first", `{"type":"response.done","response":{"id":"resp_interrupt_fixture","status":"completed"}}`, "response.completed", false, true},
		{"done_incomplete_terminal_first", `{"type":"response.done","response":{"id":"resp_interrupt_fixture","status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[]}}`, "response.incomplete", false, true},
		{"done_incomplete_ack_first", `{"type":"response.done","response":{"id":"resp_interrupt_fixture","status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[]}}`, "response.incomplete", true, true},
		{"done_failed_ack_first", `{"type":"response.done","response":{"id":"resp_interrupt_fixture","status":"failed","error":{"code":"server_error","message":"synthetic failure"}}}`, "response.failed", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			registry := newResponsesWSInterruptRegistry()
			ctx, state := registry.begin(ctx, "")
			defer registry.finish(state)
			continueFrames := make(chan struct{})
			serverDone := make(chan struct{})
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(serverDone)
				socket, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Errorf("loopback upgrade: %v", err)
					return
				}
				defer socket.Close()
				_ = socket.SetReadDeadline(time.Now().Add(3 * time.Second))
				_, payload, err := socket.ReadMessage()
				if err != nil || gjson.GetBytes(payload, "type").String() != "response.interrupt" {
					t.Errorf("expected interrupt control frame: %v", err)
					return
				}
				frames := []string{tc.terminal, ack}
				if tc.ackFirst {
					frames = []string{ack, tc.terminal}
				}
				for i, frame := range frames {
					if i == 1 {
						select {
						case <-continueFrames:
						case <-ctx.Done():
							return
						}
					}
					if err := socket.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
						t.Errorf("loopback write: %v", err)
						return
					}
				}
				// A consumed terminal and ack, not peer closure, must release the lease.
				_, _, _ = socket.ReadMessage()
			}))
			defer server.Close()
			dialer := websocket.Dialer{} // Never use an environment proxy.
			socket, _, err := dialer.DialContext(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatalf("loopback dial: %v", err)
			}
			defer func() {
				cancel()
				_ = socket.Close()
				<-serverDone
			}()
			pool := newOpenAIResponsesWSPool()
			conn := &openAIResponsesWSConn{
				spec: openAIResponsesWSSpec{accountID: 7}, conn: socket,
				created: time.Now(), busy: true, response: map[string]struct{}{},
			}
			pool.conns = append(pool.conns, conn)
			resp := openAIResponsesWSStream(ctx, pool, conn)
			defer resp.Body.Close()
			state.mu.Lock()
			send := state.send
			state.mu.Unlock()
			if send == nil {
				t.Fatal("interrupt callback was not registered")
			}
			if err := send([]byte(`{"type":"response.interrupt","response_id":"resp_interrupt_fixture","mode":"discard_partial_items"}`)); err != nil {
				t.Fatalf("send interrupt: %v", err)
			}
			reader := bufio.NewReader(resp.Body)
			first, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("read first event: %v", err)
			}
			pool.mu.Lock()
			busy, dead := conn.busy, conn.dead
			pool.mu.Unlock()
			if !busy || dead {
				t.Errorf("lease released before both terminal and ack: busy=%v dead=%v", busy, dead)
			}
			close(continueFrames)
			rest, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read remaining events: %v", err)
			}
			events := strings.Split(strings.TrimSuffix(first+string(rest), "\n\n"), "\n\n")
			if len(events) != 2 {
				t.Fatalf("event count = %d, want 2", len(events))
			}
			terminalIndex, ackIndex := 0, 1
			if tc.ackFirst {
				terminalIndex, ackIndex = 1, 0
			}
			terminal := strings.TrimPrefix(events[terminalIndex], "data: ")
			if gjson.Get(terminal, "type").String() != tc.wantType || gjson.Get(terminal, "response").Raw != gjson.Get(tc.terminal, "response").Raw {
				t.Errorf("terminal classification or metadata changed: %s", terminal)
			}
			if events[ackIndex] != "data: "+ack {
				t.Error("acknowledgement metadata or event order changed")
			}
			pool.mu.Lock()
			bound := pool.bindings[openAIResponsesWSBindingKey(7, responseID)] == conn
			busy, dead = conn.busy, conn.dead
			pool.mu.Unlock()
			if busy || dead || bound != tc.wantBinding {
				t.Errorf("final lease: busy=%v dead=%v bound=%v; want released, live, bound=%v", busy, dead, bound, tc.wantBinding)
			}
		})
	}
}
