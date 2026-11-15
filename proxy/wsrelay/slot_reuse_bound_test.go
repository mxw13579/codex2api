package wsrelay

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy"
	"github.com/gorilla/websocket"
)

// newCountingWSServer 返回一个真实的 WS 上游及其握手计数。
func newCountingWSServer(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	var handshakes atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		handshakes.Add(1)
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http"), &handshakes
}

// finishStatelessRequest 按执行器的收尾顺序完成一个请求：终态绑定响应，
// 归还读租约，移除在途请求，再归还连接。
func finishStatelessRequest(t *testing.T, manager *Manager, wc *WsConnection, pr *PendingRequest, slotKey, responseID, apiKey string) {
	t.Helper()
	if responseID != "" {
		manager.bindResponseConn(responseID, responseConnBinding{conn: wc, sessionKey: slotKey, accountID: wc.session.AccountID, apiKey: apiKey})
	}
	if !wc.cancelUnsentReadLease(pr.RequestID) {
		t.Fatalf("read lease for %s was not cancellable", pr.RequestID)
	}
	wc.session.RemovePendingRequest(pr.RequestID)
	manager.ReleaseConnection(wc)
}

func runSequentialStatelessRequests(t *testing.T, manager *Manager, account *auth.Account, wsURL string, slots, requests int) {
	t.Helper()
	for i := 0; i < requests; i++ {
		wc, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", fmt.Sprintf("stateless-%d", i), slots, http.Header{}, "")
		if err != nil {
			t.Fatalf("request %d: AcquireReusableConnection: %v", i, err)
		}
		finishStatelessRequest(t, manager, wc, pr, key, fmt.Sprintf("resp_%d", i), "key")
	}
}

func TestSlotReuseBoundKeepsHandshakesWithinSlots(t *testing.T) {
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 501, DynamicConcurrencyLimit: 4}

	runSequentialStatelessRequests(t, manager, account, wsURL, 2, 100)
	if got := handshakes.Load(); got > 2 {
		t.Fatalf("handshakes = %d, want at most 2 (one per slot)", got)
	}
	line := statsLineFor(t, manager.connectionStatsLines(time.Now()), account.ID())
	for _, want := range []string{"slot_fallback=0 ", "dial_kind=slot:2,oneshot:0,"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
	if !strings.Contains(line, "slot_bound:98,") {
		t.Fatalf("stats line %q: want 98 bound-slot reuses", line)
	}
}

func TestSlotReuseBoundDisabledRestoresOneHandshakePerRequest(t *testing.T) {
	t.Setenv("CODEX_WS_SLOT_REUSE_BOUND", "off")
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 502, DynamicConcurrencyLimit: 4}

	runSequentialStatelessRequests(t, manager, account, wsURL, 2, 100)
	// 旧行为：每个槽位用一次就被绑定占住，之后每个请求都新建一次性连接。
	if got := handshakes.Load(); got != 100 {
		t.Fatalf("handshakes = %d, want 100 with bound-slot reuse disabled", got)
	}
}

func TestSlotReuseBoundEnvSwitch(t *testing.T) {
	for value, want := range map[string]bool{"": true, "1": true, "on": true, "0": false, "false": false, "NO": false, " off ": false} {
		t.Setenv("CODEX_WS_SLOT_REUSE_BOUND", value)
		if got := slotReuseBoundEnabled(); got != want {
			t.Fatalf("slotReuseBoundEnabled(%q) = %v, want %v", value, got, want)
		}
	}
}

// newBoundSlotConnection 构造一条空闲、带有效续链绑定的槽位连接。
func newBoundSlotConnection(t *testing.T, manager *Manager, account *auth.Account, wsURL, slotSession, responseID string, lastUsed time.Time) *WsConnection {
	t.Helper()
	wc, session := newTestSlotConnection(manager, account, wsURL, slotSession)
	session.markUserContext()
	manager.BindResponseConn(responseID, wc, slotSession, account.ID(), "key")
	if !manager.hasLiveResponseBinding(wc) {
		t.Fatalf("slot %s has no live binding", slotSession)
	}
	wc.lastUsed.Store(lastUsed.UnixNano())
	return wc
}

func TestSlotReuseBoundTakesLeastRecentlyUsedSlot(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(*WsConnection) bool { return true }
	account := &auth.Account{DBID: 503}
	wsURL := "wss://example.test/responses"
	now := time.Now()
	newBoundSlotConnection(t, manager, account, wsURL, "cache#0", "resp_0", now.Add(-time.Minute))
	oldest := newBoundSlotConnection(t, manager, account, wsURL, "cache#1", "resp_1", now.Add(-3*time.Minute))
	newBoundSlotConnection(t, manager, account, wsURL, "cache#2", "resp_2", now.Add(-2*time.Minute))

	got, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", "stateless-x", 3, http.Header{}, "")
	if err != nil {
		t.Fatalf("AcquireReusableConnection: %v", err)
	}
	defer got.session.RemovePendingRequest(pr.RequestID)
	if got != oldest || key != "cache#1" {
		t.Fatalf("took %q, want least recently used cache#1", key)
	}
	for _, id := range []string{"resp_0", "resp_2"} {
		if wc, _ := manager.lookupResponseConn(id, account.ID(), "key"); wc == nil {
			t.Fatalf("binding %s of an untouched slot was dropped", id)
		}
	}
}

func TestSlotReuseBoundInvalidatesOldBinding(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(*WsConnection) bool { return true }
	account := &auth.Account{DBID: 7}
	wsURL := "wss://example.test/responses"
	slot := newBoundSlotConnection(t, manager, account, wsURL, "cache#0", "resp_old", time.Now())
	executor := NewExecutorWithManager(manager)
	old := websocketContinuation{responseID: "resp_old", accountID: account.ID(), apiKey: "key"}

	got, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", "stateless-x", 1, http.Header{}, "")
	if err != nil || got != slot {
		t.Fatalf("AcquireReusableConnection = (%p, %v), want bound slot", got, err)
	}
	if wc, _ := manager.lookupContinuationConn(old); wc != nil {
		t.Fatal("old binding still resolves after the slot was taken over")
	}
	_, _, _, err = executor.acquireContinuation(context.Background(), old)
	var lost *proxy.ResponsesContinuationLostError
	if !errors.As(err, &lost) {
		t.Fatalf("continuation with old response ID returned %v, want ResponsesContinuationLostError", err)
	}
	if got.responseBindingsDisabled {
		t.Fatal("taking over a bound slot must not disable future bindings")
	}

	finishStatelessRequest(t, manager, got, pr, key, "resp_new", "key")
	next, nextPending, _, err := executor.acquireContinuation(context.Background(), websocketContinuation{responseID: "resp_new", accountID: account.ID(), apiKey: "key"})
	if err != nil || next != slot {
		t.Fatalf("continuation with new response ID = (%p, %v), want the reused slot", next, err)
	}
	next.session.RemovePendingRequest(nextPending.RequestID)
}

func TestSlotReuseBoundSkipsBusySlots(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(*WsConnection) bool { return true }
	account := &auth.Account{DBID: 504}
	wsURL := "wss://example.test/responses"
	busy := newBoundSlotConnection(t, manager, account, wsURL, "cache#0", "resp_busy", time.Now().Add(-time.Minute))
	busyPending, err := manager.addPendingAndBeginReadLease(busy, "cache#0", capacityChat)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.session.RemovePendingRequest(busyPending.RequestID)
	idle := newBoundSlotConnection(t, manager, account, wsURL, "cache#1", "resp_idle", time.Now())

	got, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", "stateless-x", 2, http.Header{}, "")
	if err != nil {
		t.Fatalf("AcquireReusableConnection: %v", err)
	}
	defer got.session.RemovePendingRequest(pr.RequestID)
	if got != idle || key != "cache#1" {
		t.Fatalf("took %q, want idle bound slot cache#1 (busy slot must be skipped)", key)
	}
	if busy.session.PendingCount() != 1 {
		t.Fatalf("busy slot pending = %d, want 1", busy.session.PendingCount())
	}
	if wc, _ := manager.lookupResponseConn("resp_busy", account.ID(), "key"); wc != busy {
		t.Fatal("binding of the busy slot was dropped")
	}
}

func TestSlotReuseBoundFallsBackWhenAllSlotsBusy(t *testing.T) {
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 505, DynamicConcurrencyLimit: 4}

	var held []*WsConnection
	for i := 0; i < 2; i++ {
		wc, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", fmt.Sprintf("stateless-hold-%d", i), 2, http.Header{}, "")
		if err != nil || key != fmt.Sprintf("cache#%d", i) {
			t.Fatalf("slot %d: key=%q err=%v", i, key, err)
		}
		held = append(held, wc)
		defer wc.session.RemovePendingRequest(pr.RequestID)
	}
	wc, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", "stateless-fallback", 2, http.Header{}, "")
	if err != nil {
		t.Fatalf("fallback acquire: %v", err)
	}
	defer wc.session.RemovePendingRequest(pr.RequestID)
	if key != "stateless-fallback" || wc == held[0] || wc == held[1] {
		t.Fatalf("all-busy acquire used %q, want the one-shot fallback", key)
	}
	if got := handshakes.Load(); got != 3 {
		t.Fatalf("handshakes = %d, want 3", got)
	}
	line := statsLineFor(t, manager.connectionStatsLines(time.Now()), account.ID())
	for _, want := range []string{"slot_fallback=1 ", "slot_fallback_bound=0 ", "slot_fallback_busy=2 ", "dial_kind=slot:2,oneshot:1,"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
}

// TestSlotReuseBoundConcurrentWithContinuation 让无会话请求与续链请求并发争用同一批
// 槽位连接，验证任何时刻每条连接最多一个在途请求。需在 -race 下运行。
func TestSlotReuseBoundConcurrentWithContinuation(t *testing.T) {
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 506, DynamicConcurrencyLimit: 16}
	executor := NewExecutorWithManager(manager)
	const slots = 2

	var inflightMu sync.Mutex
	inflight := make(map[*WsConnection]int)
	var violations atomic.Int32
	enter := func(wc *WsConnection) {
		inflightMu.Lock()
		inflight[wc]++
		if inflight[wc] != 1 {
			violations.Add(1)
		}
		if wc.session.PendingCount() != 1 {
			violations.Add(1)
		}
		inflightMu.Unlock()
	}
	leave := func(wc *WsConnection) {
		inflightMu.Lock()
		inflight[wc]--
		inflightMu.Unlock()
	}
	var lastResponse atomic.Value
	lastResponse.Store("")
	var responseSeq, continuationHits atomic.Int32
	var destroyedWhileHeld atomic.Int32
	complete := func(wc *WsConnection, pr *PendingRequest, key string) {
		id := fmt.Sprintf("resp_%d", responseSeq.Add(1))
		time.Sleep(time.Duration(responseSeq.Load()%3) * 100 * time.Microsecond)
		leave(wc)
		manager.bindResponseConn(id, responseConnBinding{conn: wc, sessionKey: key, accountID: account.ID(), apiKey: "key"})
		lastResponse.Store(id)
		// 收尾在连接的 pool-key 锁内归还读租约和在途请求，使取连方不会看到"半归还"
		// 状态：本测试只考察取连与取连之间的互斥，收尾与取连复验之间的竞态另行处理。
		lock, releaseKeyLock := manager.keyLock(wc.PoolKey)
		lock.Lock()
		// 仍持有在途请求时连接就已断开，只可能是争用的输家销毁了赢家的连接。
		if !wc.IsConnected() {
			destroyedWhileHeld.Add(1)
		}
		wc.cancelUnsentReadLease(pr.RequestID)
		wc.session.RemovePendingRequest(pr.RequestID)
		lock.Unlock()
		releaseKeyLock()
		manager.ReleaseConnection(wc)
	}

	var wg, statelessWG sync.WaitGroup
	var statelessDone atomic.Bool
	errs := make(chan error, 64)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		statelessWG.Add(1)
		go func(g int) {
			defer wg.Done()
			defer statelessWG.Done()
			for i := 0; i < 60; i++ {
				wc, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", fmt.Sprintf("stateless-%d-%d", g, i), slots, http.Header{}, "")
				if err != nil {
					errs <- err
					return
				}
				enter(wc)
				complete(wc, pr, key)
				time.Sleep(200 * time.Microsecond)
			}
		}(g)
	}
	go func() {
		statelessWG.Wait()
		statelessDone.Store(true)
	}()
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !statelessDone.Load() {
				id, _ := lastResponse.Load().(string)
				if id == "" {
					time.Sleep(50 * time.Microsecond)
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				wc, pr, key, err := executor.acquireContinuation(ctx, websocketContinuation{responseID: id, accountID: account.ID(), apiKey: "key"})
				cancel()
				if err != nil {
					var lost *proxy.ResponsesContinuationLostError
					var busy *proxy.ResponsesContinuationBusyError
					if !errors.As(err, &lost) && !errors.As(err, &busy) && !errors.Is(err, context.DeadlineExceeded) {
						errs <- fmt.Errorf("continuation: %w", err)
						return
					}
					continue
				}
				continuationHits.Add(1)
				enter(wc)
				complete(wc, pr, key)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := violations.Load(); got != 0 {
		t.Fatalf("%d acquisitions shared a connection with another in-flight request", got)
	}
	if got := destroyedWhileHeld.Load(); got != 0 {
		t.Fatalf("%d connections were destroyed while a request still held them", got)
	}
	manager.connStats.mu.Lock()
	accountStats := manager.connStats.accounts[account.ID()]
	boundReuses := accountStats.reuses[reuseKindSlotBound]
	fallbacks := accountStats.slotFallback
	closes := make(map[string]int, len(accountStats.closes))
	for reason, n := range accountStats.closes {
		closes[reason] = n
	}
	manager.connStats.mu.Unlock()
	// 争用的输家若在赢家已占位的连接上继续占位，会以 unusable_connection 销毁赢家的连接。
	if n := closes[closeUnusable]; n != 0 {
		t.Fatalf("%d connections closed as %s (closes=%v); a losing acquirer destroyed a winner's connection", n, closeUnusable, closes)
	}
	// 每个槽位最多握手一次，其余握手只能来自一次性兜底；连接被误销毁会触发额外重拨。
	if got, limit := int(handshakes.Load()), slots+fallbacks; got > limit {
		t.Fatalf("handshakes = %d, want at most slots(%d) + one-shot fallbacks(%d) = %d (closes=%v)", got, slots, fallbacks, limit, closes)
	}
	if boundReuses == 0 {
		t.Fatal("no stateless request took over a bound slot; the third pass was not exercised")
	}
	if continuationHits.Load() == 0 {
		t.Fatal("no continuation request ever won a connection; the race was not exercised")
	}
	t.Logf("continuation hits: %d, bound-slot reuses: %d, responses: %d, handshakes: %d, fallbacks: %d, closes: %v",
		continuationHits.Load(), boundReuses, responseSeq.Load(), handshakes.Load(), fallbacks, closes)
}

// keyLockRefs 返回 pool-key 锁当前的引用数（持有者与等待者之和）。
func keyLockRefs(manager *Manager, key string) int {
	manager.keyLocks.mu.Lock()
	defer manager.keyLocks.mu.Unlock()
	if entry := manager.keyLocks.locks[key]; entry != nil {
		return entry.refs
	}
	return 0
}

// TestSlotReuseBoundProbeInterleavedWithContinuation 固定一种交错：第三遍在探活中
// 阻塞（持有 pool-key 锁），同时续链请求争用同一条连接，然后放行探活。正确实现下
// 两者互斥：恰好一方拿到连接，连接保持可用，续链方以 ResponsesContinuationLostError
// 结束，而不是拿到连接或销毁第三遍已占位的连接。
func TestSlotReuseBoundProbeInterleavedWithContinuation(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 507}
	wsURL := "wss://example.test/responses"
	wc := newBoundSlotConnection(t, manager, account, wsURL, "cache#0", "resp_old", time.Now())
	key := wc.PoolKey

	entered := make(chan struct{})
	release := make(chan struct{})
	laterProbe := make(chan struct{}, 8)
	var calls atomic.Int32
	// 第一遍遇到带绑定的槽位不探活，因此第一次探活来自第三遍。
	manager.probeFunc = func(*WsConnection) bool {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		} else {
			laterProbe <- struct{}{}
		}
		return true
	}
	type result struct {
		wc  *WsConnection
		pr  *PendingRequest
		err error
	}
	slotDone := make(chan result, 1)
	go func() {
		got, pr, _, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", "stateless-x", 1, http.Header{}, "")
		slotDone <- result{got, pr, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("third pass never probed the bound slot")
	}
	baseRefs := keyLockRefs(manager, key)

	executor := NewExecutorWithManager(manager)
	contDone := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, pr, _, err := executor.acquireContinuation(ctx, websocketContinuation{responseID: "resp_old", accountID: account.ID(), apiKey: "key"})
		contDone <- result{got, pr, err}
	}()
	// 正确实现下续链方随后取得同一把 pool-key 锁的引用并阻塞在该锁上，既不探活
	// 也不返回。若续链方绕过了互斥（探活或直接返回），先等它完成取连，再放行
	// 第三遍，使竞争输家必然出现。
	var cont result
	contFinished := false
	deadline := time.After(5 * time.Second)
	for !contFinished && keyLockRefs(manager, key) <= baseRefs {
		select {
		case <-laterProbe:
			cont, contFinished = <-contDone, true
		case cont = <-contDone:
			contFinished = true
		case <-deadline:
			t.Fatal("continuation never reached the pool-key lock")
		case <-time.After(100 * time.Microsecond):
		}
	}
	if !contFinished {
		select {
		case <-laterProbe:
			cont, contFinished = <-contDone, true
		case cont = <-contDone:
			contFinished = true
		case <-time.After(50 * time.Millisecond):
		}
	}
	close(release)
	slot := <-slotDone
	if !contFinished {
		cont = <-contDone
	}

	owners := 0
	if slot.err == nil && slot.wc == wc {
		owners++
	}
	if cont.err == nil && cont.wc == wc {
		owners++
	}
	t.Logf("slot err=%v same=%v; continuation err=%v same=%v; connected=%v pending=%d",
		slot.err, slot.wc == wc, cont.err, cont.wc == wc, wc.IsConnected(), wc.session.PendingCount())
	if owners > 1 {
		t.Fatal("third pass and continuation both own the same connection")
	}
	if !wc.IsConnected() {
		t.Fatal("connection was destroyed by the acquisition race")
	}
	if pending := wc.session.PendingCount(); pending > 1 {
		t.Fatalf("connection has %d in-flight requests, want at most 1", pending)
	}
	if slot.err != nil || slot.wc != wc {
		t.Fatalf("third pass = (%p, %v), want the bound slot", slot.wc, slot.err)
	}
	var lost *proxy.ResponsesContinuationLostError
	if !errors.As(cont.err, &lost) {
		t.Fatalf("continuation for the taken-over response = (%p, %v), want ResponsesContinuationLostError", cont.wc, cont.err)
	}
	wc.session.RemovePendingRequest(slot.pr.RequestID)
}

// TestSlotReuseBoundStopsOnCanceledContext 验证第三遍在候选之间检查 ctx：
// 已取消时返回 ctx.Err()，不接管槽位，也不落到一次性拨号。
func TestSlotReuseBoundStopsOnCanceledContext(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(*WsConnection) bool { return true }
	account := &auth.Account{DBID: 508}
	wsURL := "wss://example.test/responses"
	slot := newBoundSlotConnection(t, manager, account, wsURL, "cache#0", "resp_old", time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, _, _, err := manager.AcquireReusableConnection(ctx, account, wsURL, "cache", "stateless-x", 1, http.Header{}, "")
	if !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("AcquireReusableConnection = (%p, %v), want context.Canceled", got, err)
	}
	if slot.session.PendingCount() != 0 {
		t.Fatal("canceled request took over the bound slot")
	}
	if wc, _ := manager.lookupResponseConn("resp_old", account.ID(), "key"); wc != slot {
		t.Fatal("canceled request dropped the old binding")
	}
	manager.connStats.mu.Lock()
	fallbacks := manager.connStats.accounts[account.ID()]
	manager.connStats.mu.Unlock()
	if fallbacks != nil && fallbacks.slotFallback != 0 {
		t.Fatal("canceled request fell through to a one-shot dial")
	}
}
