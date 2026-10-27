package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type openAIResponsesWSPoolAcquireResult struct {
	conn      *openAIResponsesWSConn
	handshake *http.Response
	err       error
}

// Wait for actual enrollment rather than assuming a sleeping goroutine has
// reached acquire. All callers supply either a loopback URL or an empty URL.
func startOpenAIResponsesWSPoolLifetimeWaiter(t *testing.T, ctx context.Context, pool *openAIResponsesWSPool, conn *openAIResponsesWSConn) (context.CancelFunc, <-chan openAIResponsesWSPoolAcquireResult) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	pool.mu.Lock()
	before := len(conn.waiters)
	pool.mu.Unlock()
	result := make(chan openAIResponsesWSPoolAcquireResult, 1)
	go func() {
		got, handshake, err := pool.acquire(ctx, conn.spec, "resp_parent_fixture")
		result <- openAIResponsesWSPoolAcquireResult{got, handshake, err}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		pool.mu.Lock()
		waiting := len(conn.waiters) > before && conn.busy && !conn.dead
		pool.mu.Unlock()
		if waiting {
			return cancel, result
		}
		select {
		case got := <-result:
			pool.retire(got.conn)
			if got.handshake != nil {
				_ = got.handshake.Body.Close()
			}
			t.Fatal("continuation returned instead of waiting for the busy lease")
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("continuation did not enroll on the busy lease")
		case <-ctx.Done():
			t.Fatal("fixture context ended before waiter enrollment")
		}
	}
}

func cancelOpenAIResponsesWSPoolLifetimeWaiter(t *testing.T, cancel context.CancelFunc, result <-chan openAIResponsesWSPoolAcquireResult) {
	t.Helper()
	cancel()
	select {
	case got := <-result:
		if !errors.Is(got.err, context.Canceled) || got.conn != nil || got.handshake != nil {
			t.Fatal("cancelled continuation did not return context.Canceled without a lease")
		}
	case <-time.After(time.Second):
		t.Fatal("continuation cancellation was blocked by the busy lease")
	}
}

func TestOpenAIResponsesWSPoolBusyExpiryWaitCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool := newOpenAIResponsesWSPool()
	// No socket or valid dial URL: this test is entirely in memory, including
	// the failure path if acquire incorrectly tries to replace the lease.
	conn := &openAIResponsesWSConn{
		spec:    openAIResponsesWSSpec{accountID: 7, accountBound: true},
		created: time.Now(), busy: true,
	}
	pool.conns = append(pool.conns, conn)
	pool.bind(conn, "resp_parent_fixture")
	pool.mu.Lock()
	conn.created = time.Now().Add(-openAIResponsesWSMaxAge - time.Second)
	pool.mu.Unlock()
	cancelWait, result := startOpenAIResponsesWSPoolLifetimeWaiter(t, ctx, pool, conn)
	cancelOpenAIResponsesWSPoolLifetimeWaiter(t, cancelWait, result)
	pool.mu.Lock()
	alive := conn.busy && !conn.dead && len(pool.conns) == 1 && pool.bindings[openAIResponsesWSBindingKey(7, "resp_parent_fixture")] == conn
	pool.mu.Unlock()
	if !alive {
		t.Fatal("cancelled waiter retired the aged busy lease or removed its binding")
	}
	pool.release(conn)
	pool.mu.Lock()
	retired := conn.dead && !conn.busy && len(conn.waiters) == 0 && len(pool.conns) == 0 && len(pool.bindings) == 0
	pool.mu.Unlock()
	if !retired {
		t.Fatal("completed aged lease retained pool state or waiters")
	}
}

func TestOpenAIResponsesWSPoolRetiredSocketStartup(t *testing.T) {
	pool := newOpenAIResponsesWSPool()
	conn := &openAIResponsesWSConn{spec: openAIResponsesWSSpec{accountID: 7}, busy: true}
	pool.conns = append(pool.conns, conn)
	pool.retire(conn)
	if err := writeOpenAIResponsesWS(context.Background(), nil, []byte(`{"type":"response.create"}`)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("missing first-write socket error = %v, want closed pipe", err)
	}
	registry := newResponsesWSInterruptRegistry()
	ctx, state := registry.begin(context.Background(), "")
	defer registry.finish(state)
	resp := openAIResponsesWSStream(ctx, pool, conn)
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("missing stream socket error = %v, want closed pipe", err)
	}
	state.mu.Lock()
	registered := state.send != nil
	state.mu.Unlock()
	if registered {
		t.Fatal("retired socket registered an unsafe interrupt callback")
	}
}

// The lease crosses the age boundary between acquire and its first write.
// Continuations must wait through both stream startup and any terminal/ack
// drain, then wake onto a fresh socket. No server closes the old socket first.
func TestOpenAIResponsesWSPoolBusyLeaseCrossesMaxAge(t *testing.T) {
	const request = `{"type":"response.create","input":"lifetime fixture","previous_response_id":"resp_parent_fixture"}`
	const completed = `{"type":"response.completed","response":{"id":"resp_lifetime_fixture","status":"completed"}}`
	const failed = `{"type":"response.failed","response":{"id":"resp_lifetime_fixture","status":"failed"}}`
	const interrupt = `{"type":"response.interrupt","response_id":"resp_lifetime_fixture","mode":"discard_partial_items"}`
	const ack = `{"type":"response.interrupt.accepted","response_id":"resp_lifetime_fixture"}`
	for _, tc := range []struct {
		name, terminal string
		interrupt, ack bool
	}{
		{"ordinary", completed, false, false},
		{"completed_pending_ack", completed, true, true},
		{"failed_missing_ack", failed, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			registry := newResponsesWSInterruptRegistry()
			streamCtx, state := registry.begin(ctx, "")
			defer registry.finish(state)
			pool := newOpenAIResponsesWSPool()
			allowTerminal, allowAck := make(chan struct{}), make(chan struct{})
			peerClosed := make(chan error, 1)
			var handlers sync.WaitGroup
			var upgrades atomic.Int32
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlers.Add(1)
				defer handlers.Done()
				// Count before the handshake becomes visible to the client.
				// DialContext can return before Upgrade's caller resumes.
				upgrade := upgrades.Add(1)
				socket, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Errorf("loopback upgrade: %v", err)
					return
				}
				defer socket.Close()
				deadline, _ := ctx.Deadline()
				_ = socket.SetReadDeadline(deadline)
				_ = socket.SetWriteDeadline(deadline)
				if upgrade != 1 {
					// The awakened continuation only acquires/releases its lease.
					_, _, _ = socket.ReadMessage()
					return
				}
				_, payload, err := socket.ReadMessage()
				if err != nil || string(payload) != request {
					t.Errorf("first write did not survive the age boundary: %v", err)
					return
				}
				if tc.interrupt {
					_, payload, err = socket.ReadMessage()
					if err != nil || string(payload) != interrupt {
						t.Errorf("synthetic interrupt write: %v", err)
						return
					}
				}
				select {
				case <-allowTerminal:
				case <-ctx.Done():
					return
				}
				if err := socket.WriteMessage(websocket.TextMessage, []byte(tc.terminal)); err != nil {
					t.Errorf("loopback terminal: %v", err)
					return
				}
				if tc.ack {
					select {
					case <-allowAck:
					case <-ctx.Done():
						return
					}
					if err := socket.WriteMessage(websocket.TextMessage, []byte(ack)); err != nil {
						t.Errorf("loopback acknowledgement: %v", err)
						return
					}
				}
				_, _, err = socket.ReadMessage()
				peerClosed <- err
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
			conn, handshake, err := pool.acquire(ctx, spec, "")
			if err != nil || handshake != nil || conn == nil {
				t.Fatalf("loopback lease: %v", err)
			}
			pool.bind(conn, "resp_parent_fixture")
			pool.mu.Lock()
			socket := conn.conn
			conn.created = time.Now().Add(-openAIResponsesWSMaxAge - time.Second)
			pool.mu.Unlock()
			// Always close the actual socket on test failure too; otherwise the
			// release leak under test could disappear from the pool's snapshot.
			defer socket.Close()
			assertBusy := func() {
				t.Helper()
				pool.mu.Lock()
				alive := conn.busy && !conn.dead && conn.conn == socket && conn.expired() && pool.bindings[openAIResponsesWSBindingKey(7, "resp_parent_fixture")] == conn
				pool.mu.Unlock()
				if !alive || upgrades.Load() != 1 {
					t.Fatal("aged busy socket was cleared, retired or replaced before the lease finished")
				}
			}
			cancelWait, result := startOpenAIResponsesWSPoolLifetimeWaiter(t, ctx, pool, conn)
			cancelOpenAIResponsesWSPoolLifetimeWaiter(t, cancelWait, result)
			_, next := startOpenAIResponsesWSPoolLifetimeWaiter(t, ctx, pool, conn)
			assertBusy()
			if err := writeOpenAIResponsesWS(streamCtx, socket, []byte(request)); err != nil {
				t.Fatalf("first write after waiting continuation: %v", err)
			}
			resp := openAIResponsesWSStream(streamCtx, pool, conn)
			defer resp.Body.Close()
			state.mu.Lock()
			send := state.send
			state.mu.Unlock()
			if send == nil {
				t.Fatal("missing interrupt callback on live socket")
			}
			if tc.interrupt {
				if err := send([]byte(interrupt)); err != nil {
					t.Fatalf("send synthetic interrupt: %v", err)
				}
			}
			close(allowTerminal)
			want := "data: " + tc.terminal + "\n\n"
			if tc.ack {
				terminal := make([]byte, len(want))
				if _, err := io.ReadFull(resp.Body, terminal); err != nil || string(terminal) != want {
					t.Fatalf("terminal before delayed ack: %v", err)
				}
				assertBusy()
				cancelAckWait, ackWait := startOpenAIResponsesWSPoolLifetimeWaiter(t, ctx, pool, conn)
				cancelOpenAIResponsesWSPoolLifetimeWaiter(t, cancelAckWait, ackWait)
				assertBusy()
				close(allowAck)
				want = "data: " + ack + "\n\n"
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil || string(body) != want || ctx.Err() != nil {
				t.Fatalf("aged lease did not finish with exactly the expected frames: %v", err)
			}
			select {
			case err := <-peerClosed:
				var netErr net.Error
				if err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
					t.Fatal("old socket was not explicitly closed by lease completion")
				}
			case <-time.After(time.Second):
				t.Fatal("completed expired socket leaked instead of closing")
			}
			var successor *openAIResponsesWSConn
			select {
			case got := <-next:
				if got.err != nil || got.handshake != nil || got.conn == nil || got.conn == conn {
					t.Fatalf("waiting continuation did not acquire a fresh lease: %v", got.err)
				}
				successor = got.conn
			case <-time.After(time.Second):
				t.Fatal("retirement did not wake the waiting continuation")
			}
			pool.mu.Lock()
			retired := conn.dead && !conn.busy && conn.conn == nil && len(conn.waiters) == 0 && len(pool.bindings) == 0 && len(pool.conns) == 1 && pool.conns[0] == successor
			pool.mu.Unlock()
			if !retired || upgrades.Load() != 2 {
				t.Fatal("expired lease kept pool state or was reused by its waiter")
			}
			if err := send([]byte(interrupt)); !errors.Is(err, ErrResponsesWSInterruptFinished) {
				t.Fatalf("retained callback after retirement = %v, want finished", err)
			}
			// A non-expired successor still releases/reuses normally: the age
			// fix must not turn every completion into retirement.
			pool.release(successor)
			reused, handshake, err := pool.acquire(ctx, spec, "")
			if err != nil || handshake != nil || reused != successor || upgrades.Load() != 2 {
				t.Fatalf("ordinary idle reuse changed: %v", err)
			}
		})
	}
}
