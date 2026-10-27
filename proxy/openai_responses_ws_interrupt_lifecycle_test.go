package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

func TestOpenAIResponsesWSInterruptAbortedLease(t *testing.T) {
	const completed = `{"type":"response.completed","response":{"id":"resp_fixture","status":"completed"}}`
	const failed = `{"type":"response.failed","response":{"id":"resp_fixture","status":"failed"}}`
	const ack = `{"type":"response.interrupt.accepted","response_id":"resp_fixture"}`
	for _, tc := range []struct {
		name, terminal           string
		interrupt, ackFirst      bool
		ackLast, cancelAfterRead bool
	}{
		{"cancel_before_terminal", "", true, false, false, true},
		{"cancel_waiting_for_ack", completed, true, false, false, true},
		{"downstream_failed_pending_ack", failed, true, false, false, false},
		{"downstream_failed_ack_first", failed, true, true, false, false},
		{"downstream_failed_uninterrupted", failed, false, false, false, false},
		{"downstream_completed_uninterrupted", completed, false, false, false, false},
		{"downstream_completed_pending_ack", completed, true, false, false, false},
		{"downstream_ack_after_terminal", completed, true, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			registry := newResponsesWSInterruptRegistry()
			ctx, state := registry.begin(ctx, "")
			defer registry.finish(state)
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
				deadline, _ := ctx.Deadline()
				_ = socket.SetReadDeadline(deadline)
				_ = socket.SetWriteDeadline(deadline)
				if tc.interrupt {
					_, payload, err := socket.ReadMessage()
					if err != nil || gjson.GetBytes(payload, "type").String() != "response.interrupt" {
						t.Errorf("expected synthetic interrupt: %v", err)
						return
					}
				}
				var frames []string
				if tc.ackFirst {
					frames = append(frames, ack)
				}
				if tc.terminal != "" {
					frames = append(frames, tc.terminal)
				}
				if tc.ackLast {
					frames = append(frames, ack)
				}
				for _, frame := range frames {
					if err := socket.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
						t.Errorf("loopback write: %v", err)
						return
					}
				}
				_, _, _ = socket.ReadMessage()
			}))
			defer server.Close()
			dialer := websocket.Dialer{} // Loopback only; no environment proxy.
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
			leaseEnded := make(chan struct{}, 1)
			conn := &openAIResponsesWSConn{
				spec: openAIResponsesWSSpec{accountID: 7}, conn: socket,
				created: time.Now(), busy: true, response: map[string]struct{}{},
				waiters: []chan struct{}{leaseEnded},
			}
			pool.conns = append(pool.conns, conn)
			resp := openAIResponsesWSStream(ctx, pool, conn)
			defer resp.Body.Close()
			if tc.interrupt {
				state.mu.Lock()
				send := state.send
				state.mu.Unlock()
				if send == nil {
					t.Fatal("missing interrupt callback")
				}
				if err := send([]byte(`{"type":"response.interrupt","response_id":"resp_fixture","mode":"discard_partial_items"}`)); err != nil {
					t.Fatalf("send interrupt: %v", err)
				}
			}
			readFrame := func(frame string) {
				t.Helper()
				want := "data: " + frame + "\n\n"
				buffer := make([]byte, len(want))
				if _, err := io.ReadFull(resp.Body, buffer); err != nil || string(buffer) != want {
					t.Fatalf("read complete fixture frame: %v", err)
				}
			}
			if tc.ackFirst {
				readFrame(ack)
			}
			if (tc.cancelAfterRead || tc.ackLast) && tc.terminal != "" {
				readFrame(tc.terminal)
			}
			if tc.cancelAfterRead {
				cancel()
				if _, err := io.ReadAll(resp.Body); err == nil {
					t.Error("cancelled incomplete lease ended without a read error")
				}
			} else {
				// Read only a prefix so the terminal/ack write is still blocked
				// when the downstream body closes. Merely seeing a terminal
				// must not permit release after this write fails.
				prefix := make([]byte, len("data: "))
				if _, err := io.ReadFull(resp.Body, prefix); err != nil {
					t.Fatalf("read SSE prefix: %v", err)
				}
				_ = resp.Body.Close()
			}
			select {
			case <-leaseEnded:
			case <-time.After(time.Second):
				t.Fatal("aborted lease was not promptly retired")
			}
			pool.mu.Lock()
			busy, dead, bound := conn.busy, conn.dead, len(pool.bindings)
			pool.mu.Unlock()
			if busy || !dead || bound != 0 {
				t.Errorf("aborted lease busy=%v dead=%v bindings=%d", busy, dead, bound)
			}
		})
	}
}

// A delayed ack must never become the first frame of the next response lease.
// All requests and protocol frames are synthetic and use loopback without a proxy.
func TestOpenAIResponsesWSInterruptLeaseLifecycle(t *testing.T) {
	for _, terminal := range []struct {
		name, frame, wantType string
		failure               bool
	}{
		{"failed", `{"type":"response.failed","response":{"id":"resp_first","status":"failed","error":{"code":"fixture_error"}},"sequence_number":9007199254740993}`, "response.failed", true},
		{"done_failed", `{"type":"response.done","response":{"id":"resp_first","status":"failed","error":{"code":"fixture_error"}},"sequence_number":9007199254740993}`, "response.failed", true},
		{"error", `{"type":"error","error":{"code":"fixture_error"},"sequence_number":9007199254740993}`, "error", true},
		{"done_completed", `{"type":"response.done","response":{"id":"resp_first","status":"completed"}}`, "response.completed", false},
		{"done_incomplete", `{"type":"response.done","response":{"id":"resp_first","status":"incomplete","incomplete_details":{"reason":"interrupted"}}}`, "response.incomplete", false},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			for _, order := range []struct {
				name, ackType string
				interrupt     bool
				ackFirst      bool
				queuedAck     bool
			}{
				{"uninterrupted", "", false, false, false},
				{"accepted_before_terminal", "response.interrupt.accepted", true, true, false},
				{"failed_before_terminal", "response.interrupt.failed", true, true, false},
				{"terminal_before_accepted", "response.interrupt.accepted", true, false, false},
				{"terminal_before_failed", "response.interrupt.failed", true, false, false},
				{"terminal_then_queued_accepted", "response.interrupt.accepted", true, false, true},
				{"terminal_then_queued_failed", "response.interrupt.failed", true, false, true},
				{"missing_ack", "", true, false, false},
			} {
				if !terminal.failure && order.interrupt && order.ackType == "" {
					continue // Successful terminals retain the existing bounded ack wait.
				}
				t.Run(order.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
					defer cancel()
					registry := newResponsesWSInterruptRegistry()
					firstCtx, state := registry.begin(ctx, "")
					defer registry.finish(state)
					pool := newOpenAIResponsesWSPool()
					var upgrades, firstRequests, secondRequests atomic.Int32
					var handlers sync.WaitGroup
					allowLateAck := make(chan struct{})
					lateAckAttempted := make(chan struct{})
					wantRetired := terminal.failure && order.interrupt && !order.ackFirst
					ack := fmt.Sprintf(`{"type":%q,"response_id":"resp_first","sequence_number":9007199254740994}`, order.ackType)
					const firstBody = `{"type":"response.create","input":"first fixture"}`
					const secondBody = `{"type":"response.create","input":"second fixture","previous_response_id":"resp_first"}`
					const created = `{"type":"response.created","response":{"id":"resp_first","status":"in_progress"}}`
					const secondTerminal = `{"type":"response.completed","response":{"id":"resp_second","status":"completed"}}`
					upgrader := websocket.Upgrader{}
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						handlers.Add(1)
						defer handlers.Done()
						socket, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Errorf("loopback upgrade: %v", err)
							return
						}
						defer socket.Close()
						upgrades.Add(1)
						deadline, _ := ctx.Deadline()
						_ = socket.SetReadDeadline(deadline)
						_ = socket.SetWriteDeadline(deadline)
						write := func(frame string) bool {
							if err := socket.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
								t.Errorf("loopback frame write: %v", err)
								return false
							}
							return true
						}
						for {
							_, body, err := socket.ReadMessage()
							if err != nil {
								return
							}
							switch string(body) {
							case firstBody:
								if firstRequests.Add(1) != 1 {
									t.Error("first request body was replayed")
									return
								}
								if order.interrupt {
									_, payload, err := socket.ReadMessage()
									if err != nil || gjson.GetBytes(payload, "type").String() != "response.interrupt" || gjson.GetBytes(payload, "response_id").String() != "resp_first" {
										t.Errorf("expected synthetic interrupt: %v", err)
										return
									}
								}
								if !write(created) {
									return
								}
								if order.ackFirst && !write(ack) {
									return
								}
								if !write(terminal.frame) {
									return
								}
								if order.interrupt && !order.ackFirst && order.ackType != "" {
									if wantRetired && !order.queuedAck {
										// Delay the ack until EOF. Queued cases instead write
										// while the reader is blocked on the created SSE.
										select {
										case <-allowLateAck:
										case <-ctx.Done():
											return
										}
										// Retirement may have already closed the peer.
										_ = socket.WriteMessage(websocket.TextMessage, []byte(ack))
									} else if !write(ack) {
										return
									}
									close(lateAckAttempted)
								}
							case secondBody:
								if secondRequests.Add(1) != 1 {
									t.Error("second request body was replayed")
									return
								}
								if !write(secondTerminal) {
									return
								}
							default:
								t.Error("unexpected or changed request body")
								return
							}
						}
					}))
					defer func() {
						cancel()
						pool.mu.Lock()
						conns := append([]*openAIResponsesWSConn(nil), pool.conns...)
						pool.mu.Unlock()
						for _, conn := range conns {
							pool.retire(conn)
						}
						server.Close()
						handlers.Wait()
					}()
					spec := openAIResponsesWSSpec{
						accountID: 7, accountBound: true, baseURL: server.URL,
						wsURL: "ws" + strings.TrimPrefix(server.URL, "http"),
					}
					resp, err := pool.roundTrip(firstCtx, spec, []byte(firstBody))
					if err != nil {
						t.Fatalf("first round: %v", err)
					}
					defer resp.Body.Close()
					pool.mu.Lock()
					firstConn := pool.conns[0]
					pool.mu.Unlock()
					state.mu.Lock()
					retainedSend := state.send
					state.mu.Unlock()
					if retainedSend == nil {
						t.Fatal("missing interrupt callback")
					}
					const interrupt = `{"type":"response.interrupt","response_id":"resp_first","mode":"discard_partial_items"}`
					if order.interrupt {
						if err := retainedSend([]byte(interrupt)); err != nil {
							t.Fatalf("send interrupt: %v", err)
						}
					}
					if order.queuedAck {
						// Do not drain the pipe yet: the old ack must already be on
						// the socket before the reader can accept the failed terminal.
						select {
						case <-lateAckAttempted:
						case <-ctx.Done():
							t.Fatal("ack was not queued before draining the first SSE body")
						}
					}
					first, err := io.ReadAll(resp.Body)
					if err != nil {
						t.Fatalf("first SSE body (must not wait for missing ack): %v", err)
					}
					if ctx.Err() != nil {
						t.Fatal("first lease only ended after context timeout")
					}
					wantFrames := []string{created}
					if order.ackFirst {
						wantFrames = append(wantFrames, ack)
					}
					wantTerminal := string(NormalizeResponsesTerminalEvent([]byte(terminal.frame)))
					if gjson.Get(wantTerminal, "type").String() != terminal.wantType {
						t.Fatal("unexpected terminal classification")
					}
					wantFrames = append(wantFrames, wantTerminal)
					if order.interrupt && !order.ackFirst && !wantRetired {
						wantFrames = append(wantFrames, ack)
					}
					wantFirst := "data: " + strings.Join(wantFrames, "\n\ndata: ") + "\n\n"
					if string(first) != wantFirst {
						t.Error("first SSE event order or metadata changed")
					}
					pool.mu.Lock()
					busy, dead := firstConn.busy, firstConn.dead
					bound := pool.bindings[openAIResponsesWSBindingKey(7, "resp_first")] == firstConn
					pool.mu.Unlock()
					if busy || dead != wantRetired || bound == wantRetired {
						t.Errorf("first lease busy=%v dead=%v bound=%v; want retired=%v", busy, dead, bound, wantRetired)
					}
					close(allowLateAck)
					if wantRetired && order.ackType != "" {
						select {
						case <-lateAckAttempted:
						case <-ctx.Done():
							t.Fatal("late ack was not attempted")
						}
					}
					second, err := pool.roundTrip(ctx, spec, []byte(secondBody))
					if err != nil {
						t.Fatalf("second round: %v", err)
					}
					defer second.Body.Close()
					// An old caller can retain the callback beyond unregistration.
					// It must not write to either a retired socket or the next lease.
					if err := retainedSend([]byte(interrupt)); !errors.Is(err, ErrResponsesWSInterruptFinished) {
						t.Errorf("prior lease callback error = %v, want finished", err)
					}
					body, err := io.ReadAll(second.Body)
					if err != nil || string(body) != "data: "+secondTerminal+"\n\n" {
						t.Errorf("next lease consumed stale data or failed: %v", err)
					}
					wantUpgrades := int32(1)
					if wantRetired {
						wantUpgrades = 2
					}
					if upgrades.Load() != wantUpgrades || firstRequests.Load() != 1 || secondRequests.Load() != 1 {
						t.Errorf("upgrades=%d first requests=%d second requests=%d; want %d/1/1 (no replay)", upgrades.Load(), firstRequests.Load(), secondRequests.Load(), wantUpgrades)
					}
				})
			}
		})
	}
}
