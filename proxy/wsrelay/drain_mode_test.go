package wsrelay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy"
	"github.com/gorilla/websocket"
)

func drainTestConn(t *testing.T, manager *Manager, accountID int64, sessionID string) *WsConnection {
	t.Helper()
	key := manager.poolKey(accountID, "wss://example.test/responses", sessionID, "")
	return newPooledConn(t, manager, accountID, sessionID, key, time.Now().Add(-time.Second))
}

func bindDownstream(manager *Manager, responseID string, wc *WsConnection, done <-chan struct{}) {
	manager.bindResponseConn(responseID, responseConnBinding{conn: wc, sessionKey: wc.session.ID,
		accountID: wc.session.AccountID, apiKey: "key-1", downstreamDone: done})
}

func TestEnterDrainModeClosesIdleKeepsBusyAndLiveDownstreamBound(t *testing.T) {
	manager := NewManager()
	defer manager.Stop()
	const accountID int64 = 7

	idle := drainTestConn(t, manager, accountID, "idle")
	httpBound := drainTestConn(t, manager, accountID, "http-bound")
	manager.BindResponseConn("resp-http", httpBound, "http-bound", accountID, "key-1")
	busy := drainTestConn(t, manager, accountID, "busy")
	busy.session.AddPendingRequest("busy")
	liveDownstream := make(chan struct{})
	wsBound := drainTestConn(t, manager, accountID, "ws-bound")
	bindDownstream(manager, "resp-ws", wsBound, liveDownstream)
	closedDownstream := make(chan struct{})
	close(closedDownstream)
	staleWSBound := drainTestConn(t, manager, accountID, "stale-ws-bound")
	bindDownstream(manager, "resp-stale", staleWSBound, closedDownstream)

	result, changed := manager.EnterDrainMode()
	if !changed || !manager.Draining() {
		t.Fatal("EnterDrainMode must switch into drain mode")
	}
	if result.Closed != 3 || result.KeptBusy != 1 || result.KeptDownstream != 1 {
		t.Fatalf("result=%+v, want closed=3 busy=1 downstream=1", result)
	}
	for name, wc := range map[string]*WsConnection{"idle": idle, "http-bound": httpBound, "stale-ws-bound": staleWSBound} {
		if wc.IsConnected() {
			t.Fatalf("%s connection must be closed", name)
		}
		if _, ok := manager.connections.Load(wc.PoolKey); ok {
			t.Fatalf("%s connection must leave the pool", name)
		}
	}
	if !busy.IsConnected() || !wsBound.IsConnected() {
		t.Fatal("busy and live-downstream-bound connections must stay open")
	}
	if got, _ := manager.lookupResponseConn("resp-http", accountID, "key-1"); got != nil {
		t.Fatal("binding of a closed connection must be cleared")
	}
	if got, _ := manager.lookupResponseConn("resp-ws", accountID, "key-1"); got != wsBound {
		t.Fatal("live downstream continuation binding must survive drain")
	}

	// 下游 WS 断开后，周期巡检关闭原来保留的连接。
	close(liveDownstream)
	manager.evictExpired()
	if wsBound.IsConnected() {
		t.Fatal("connection kept for a downstream session must close after that session ends")
	}
	if !busy.IsConnected() {
		t.Fatal("periodic drain sweep must not close a busy connection")
	}
}

func TestDrainModeClosesConnectionsOnReleaseAndExitRestoresRetention(t *testing.T) {
	manager := NewManager()
	defer manager.Stop()
	const accountID int64 = 8

	busy := drainTestConn(t, manager, accountID, "busy")
	pending := busy.session.AddPendingRequest("busy")
	manager.BindResponseConn("resp-busy", busy, "busy", accountID, "key-1")
	if _, changed := manager.EnterDrainMode(); !changed {
		t.Fatal("first enter must change state")
	}
	if !busy.IsConnected() {
		t.Fatal("in-flight connection must survive entering drain mode")
	}
	busy.session.RemovePendingRequest(pending.RequestID)
	manager.ReleaseConnection(busy)
	if busy.IsConnected() {
		t.Fatal("connection released during drain mode must be closed, not retained")
	}

	live := make(chan struct{})
	defer close(live)
	wsBound := drainTestConn(t, manager, accountID, "ws-bound")
	pending = wsBound.session.AddPendingRequest("ws-bound")
	bindDownstream(manager, "resp-ws", wsBound, live)
	wsBound.session.RemovePendingRequest(pending.RequestID)
	manager.ReleaseConnection(wsBound)
	if !wsBound.IsConnected() {
		t.Fatal("connection bound to a live downstream WS session must be retained on release")
	}

	if !manager.ExitDrainMode() || manager.Draining() {
		t.Fatal("ExitDrainMode must leave drain mode")
	}
	normal := drainTestConn(t, manager, accountID, "normal")
	pending = normal.session.AddPendingRequest("normal")
	normal.session.RemovePendingRequest(pending.RequestID)
	manager.ReleaseConnection(normal)
	if !normal.IsConnected() {
		t.Fatal("after exit, released connections must be retained again")
	}
	manager.evictExpired()
	if !normal.IsConnected() {
		t.Fatal("periodic sweep must not drain after exit")
	}
}

func TestDrainModeEnterExitIdempotent(t *testing.T) {
	manager := NewManager()
	defer manager.Stop()
	if manager.ExitDrainMode() {
		t.Fatal("exit while not draining must be a no-op")
	}
	if _, changed := manager.EnterDrainMode(); !changed {
		t.Fatal("first enter must change state")
	}
	later := drainTestConn(t, manager, 9, "later")
	result, changed := manager.EnterDrainMode()
	if changed || result != (DrainSweepResult{}) {
		t.Fatalf("repeated enter must be a no-op, got changed=%v result=%+v", changed, result)
	}
	if !later.IsConnected() {
		t.Fatal("repeated enter must not sweep")
	}
	if !manager.ExitDrainMode() || manager.ExitDrainMode() {
		t.Fatal("exit must change state exactly once")
	}
}

func TestDrainSweepSkipsConnectionWhosePoolKeyIsLocked(t *testing.T) {
	manager := NewManager()
	defer manager.Stop()
	// 新建连接入池后、占位前只持有 pool-key 锁：排空巡检必须跳过，不能关掉它。
	wc := drainTestConn(t, manager, 10, "dialing")
	lock, release := manager.keyLock(wc.PoolKey)
	lock.Lock()
	result, _ := manager.EnterDrainMode()
	lock.Unlock()
	release()
	if result.KeptBusy != 1 || result.Closed != 0 || !wc.IsConnected() {
		t.Fatalf("result=%+v connected=%v, want the locked connection kept", result, wc.IsConnected())
	}
	manager.evictExpired()
	if wc.IsConnected() {
		t.Fatal("next periodic sweep must close the now idle connection")
	}
}

func TestBindCompletedResponseCarriesDownstreamSessionSignal(t *testing.T) {
	manager := NewManager()
	defer manager.Stop()
	wc := drainTestConn(t, manager, 12, "ws-turn")
	done := make(chan struct{})
	resp := &WsResponse{conn: wc, manager: manager, sessionID: "ws-turn", apiKey: "key-1", downstreamDone: done}
	resp.bindCompletedResponse("resp-turn")
	manager.respConnMu.Lock()
	binding := manager.respConnBindings["resp-turn"]
	manager.respConnMu.Unlock()
	if binding.downstreamDone == nil || binding.conn != wc {
		t.Fatal("completed WS turn binding must keep the downstream session signal")
	}
}

func resetSyntheticReadLease(wc *WsConnection) {
	readState := wc.ensureReadState()
	readState.mu.Lock()
	readState.activeLease = ""
	readState.leasePhase = readLeaseIdle
	readState.leaseWrite = nil
	readState.leaseTerminalQueued = false
	readState.mu.Unlock()
}

func TestDrainModeConcurrentAcquireReleaseNeverClosesBusyConnection(t *testing.T) {
	setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) { s.CodexWSDownstreamKeepaliveSlots = 64 })
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)

	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(wc *WsConnection) bool { return wc.IsConnected() }
	account := &auth.Account{DBID: 11, DynamicConcurrencyLimit: 64}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	stop := make(chan struct{})
	var toggles sync.WaitGroup
	toggles.Add(1)
	go func() {
		defer toggles.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				manager.EnterDrainMode()
				manager.EnterDrainMode()
			} else {
				manager.sweepForDrainIfActive()
				manager.ExitDrainMode()
				manager.ExitDrainMode()
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var workers sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 8; w++ {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			for i := 0; i < 15; i++ {
				wc, pending, err := manager.AcquireConnection(context.Background(), account, wsURL, fmt.Sprintf("session-%d", w%4), http.Header{}, "")
				if err != nil {
					errs <- err
					return
				}
				time.Sleep(time.Millisecond)
				if !wc.IsConnected() {
					errs <- fmt.Errorf("drain closed a connection with an in-flight request")
					return
				}
				resetSyntheticReadLease(wc)
				wc.session.RemovePendingRequest(pending.RequestID)
				manager.ReleaseConnection(wc)
			}
		}(w)
	}
	workers.Wait()
	close(stop)
	toggles.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	manager.EnterDrainMode()
	manager.sweepForDrainIfActive()
	if got := manager.ConnectionCount(); got != 0 {
		t.Fatalf("idle connections after final drain sweep = %d, want 0", got)
	}
}
