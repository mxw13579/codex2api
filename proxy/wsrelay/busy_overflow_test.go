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

// setBusyRuntimeSettings 临时修改 busy 策略相关运行时设置，测试结束自动还原。
func setBusyRuntimeSettings(t *testing.T, mutate func(*proxy.RuntimeSettings)) {
	t.Helper()
	prev := proxy.CurrentRuntimeSettings()
	next := prev
	mutate(&next)
	proxy.ApplyRuntimeSettings(next)
	t.Cleanup(func() { proxy.ApplyRuntimeSettings(prev) })
}

// newBusyTestConnection 构造一条"已连接且被在途请求占用"的 fake 连接并入池。
func newBusyTestConnection(t *testing.T, manager *Manager, account *auth.Account, wsURL, sessionKey string) (*WsConnection, *PendingRequest) {
	t.Helper()
	key := manager.poolKey(account.ID(), wsURL, sessionKey, "")
	session := NewSession(account.ID(), manager)
	session.SetConnected(true)
	blocking := session.AddPendingRequest(sessionKey)
	conn := &WsConnection{
		session:  session,
		URL:      wsURL,
		PoolKey:  key,
		httpResp: &http.Response{StatusCode: http.StatusSwitchingProtocols},
	}
	conn.SetState(StateConnected)
	conn.Touch()
	manager.connections.Store(key, conn)
	manager.sessions.Store(key, session)
	return conn, blocking
}

func TestBusyAcquireMaxWaitConfigurable(t *testing.T) {
	setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) { s.CodexWSBusyMaxWaitSec = 1 })

	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(wc *WsConnection) bool { return true }

	account := &auth.Account{DBID: 42}
	wsURL := "wss://example.test/responses"
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })

	start := time.Now()
	_, _, err := manager.AcquireConnection(context.Background(), account, wsURL, "session-1", http.Header{}, "")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected busy acquire to time out")
	}
	if !strings.Contains(err.Error(), "waiting for busy session") {
		t.Fatalf("err = %v, want busy session timeout", err)
	}
	if elapsed < 900*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("elapsed = %s, want ~1s configured max wait", elapsed)
	}
}

func TestBusyOverflowReusesIdleSiblingSlot(t *testing.T) {
	setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) {
		s.CodexWSBusyOverflow = true
		s.CodexWSBusyPatienceSec = 0
	})

	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(wc *WsConnection) bool { return true }

	account := &auth.Account{DBID: 42, DynamicConcurrencyLimit: 4}
	wsURL := "wss://example.test/responses"
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })

	// 预置一条空闲的 overflow 兄弟连接
	siblingKey := manager.poolKey(account.ID(), wsURL, "session-1#ovf-1", "")
	siblingSession := NewSession(account.ID(), manager)
	siblingSession.SetConnected(true)
	sibling := &WsConnection{
		session:  siblingSession,
		URL:      wsURL,
		PoolKey:  siblingKey,
		httpResp: &http.Response{StatusCode: http.StatusSwitchingProtocols},
	}
	sibling.SetState(StateConnected)
	sibling.Touch()
	manager.connections.Store(siblingKey, sibling)
	manager.sessions.Store(siblingKey, siblingSession)

	got, pr, err := manager.AcquireConnection(context.Background(), account, wsURL, "session-1", http.Header{}, "")
	if err != nil {
		t.Fatalf("AcquireConnection() error = %v", err)
	}
	if got != sibling {
		t.Fatal("expected idle overflow sibling connection to be reused")
	}
	if pr == nil {
		t.Fatal("expected pending request reservation on sibling")
	}
	siblingSession.RemovePendingRequest(pr.RequestID)
}

func TestBusyOverflowCreatesSiblingConnection(t *testing.T) {
	setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) {
		s.CodexWSBusyOverflow = true
		s.CodexWSBusyPatienceSec = 0
	})

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
	account := &auth.Account{DBID: 42, DynamicConcurrencyLimit: 4}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, pr, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil {
		t.Fatalf("AcquireConnection() error = %v", err)
	}
	if got == busy {
		t.Fatal("expected a new overflow sibling connection, got the busy one")
	}
	if pr == nil {
		t.Fatal("expected pending request reservation")
	}
	if !strings.Contains(got.session.ID, busyOverflowKeyInfix) {
		t.Fatalf("sibling session ID = %q, want overflow slot key", got.session.ID)
	}
	got.session.RemovePendingRequest(pr.RequestID)
	manager.DiscardConnection(got)
}

func TestBusyOverflowDisabledKeepsWaiting(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(wc *WsConnection) bool { return true }

	account := &auth.Account{DBID: 42, DynamicConcurrencyLimit: 4}
	wsURL := "wss://example.test/responses"
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err == nil {
		t.Fatal("expected acquire to keep waiting (ctx timeout) when overflow is disabled")
	}
	// 未开启 overflow 时不应出现兄弟槽位连接
	overflowKey := manager.poolKey(account.ID(), wsURL, "session-1#ovf-1", "")
	if _, ok := manager.connections.Load(overflowKey); ok {
		t.Fatal("overflow sibling must not be created when disabled")
	}
}

func TestBusyOverflowKeyNotRecursive(t *testing.T) {
	if !isBusyOverflowSessionKey("session-1#ovf-1") {
		t.Fatal("overflow slot key should be recognized")
	}
	if isBusyOverflowSessionKey("session-1") {
		t.Fatal("plain session key must not be treated as overflow slot")
	}
}

// enableBusyOverflowForTest 打开 busy overflow（patience=0），并设置兄弟槽位数与等待上限。
func enableBusyOverflowForTest(t *testing.T, slots, maxWaitSec int) {
	t.Helper()
	setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) {
		s.CodexWSBusyOverflow = true
		s.CodexWSBusyPatienceSec = 0
		s.CodexWSBusyOverflowSlots = slots
		s.CodexWSBusyMaxWaitSec = maxWaitSec
	})
}

// overflowSiblingCount 返回连接池中某会话的 overflow 兄弟连接数。
func overflowSiblingCount(manager *Manager, sessionKey string) int {
	n := 0
	manager.connections.Range(func(_, value any) bool {
		if wc := value.(*WsConnection); wc.session != nil && strings.HasPrefix(wc.session.ID, sessionKey+busyOverflowKeyInfix) {
			n++
		}
		return true
	})
	return n
}

func TestBusyOverflowSlotsFollowRuntimeSetting(t *testing.T) {
	for _, tt := range []struct{ set, want int }{{0, DefaultBusyOverflowSlots}, {-3, DefaultBusyOverflowSlots}, {1, 1}, {7, 7}, {16, 16}, {17, MaxBusyOverflowSlots}} {
		setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) { s.CodexWSBusyOverflowSlots = tt.set })
		if got := busyOverflowSlots(); got != tt.want {
			t.Fatalf("busyOverflowSlots() with setting %d = %d, want %d", tt.set, got, tt.want)
		}
	}
	if DefaultBusyOverflowSlots != 4 || MaxBusyOverflowSlots != 16 {
		t.Fatalf("default/max = %d/%d, want 4/16", DefaultBusyOverflowSlots, MaxBusyOverflowSlots)
	}
}

// TestBusyOverflowReusesBoundSibling：没有空槽位时，兄弟连接完成一次响应并带有效绑定后，
// 下一次溢出复用它而不是新建；旧绑定被丢弃，带旧 previous_response_id 的续链得到
// ResponsesContinuationLostError。
func TestBusyOverflowReusesBoundSibling(t *testing.T) {
	enableBusyOverflowForTest(t, 1, 5)
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 601, DynamicConcurrencyLimit: 8}
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })
	ctx := context.Background()

	first, firstPending, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil {
		t.Fatalf("first overflow acquire: %v", err)
	}
	if first == busy || first.session.ID != "session-1#ovf-1" {
		t.Fatalf("first overflow took %q, want session-1#ovf-1", first.session.ID)
	}
	finishStatelessRequest(t, manager, first, firstPending, first.session.ID, "resp_ovf_old", "key")
	if !manager.hasLiveResponseBinding(first) {
		t.Fatal("finished sibling has no live binding")
	}

	second, secondPending, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil {
		t.Fatalf("second overflow acquire: %v", err)
	}
	defer second.session.RemovePendingRequest(secondPending.RequestID)
	if second != first {
		t.Fatalf("second overflow took %q, want the bound sibling to be reused", second.session.ID)
	}
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("handshakes = %d, want 1 (bound sibling reused, not redialed)", got)
	}
	if got := overflowSiblingCount(manager, "session-1"); got != 1 {
		t.Fatalf("overflow siblings = %d, want 1", got)
	}
	old := websocketContinuation{responseID: "resp_ovf_old", accountID: account.ID(), apiKey: "key"}
	if wc, _ := manager.lookupContinuationConn(old); wc != nil {
		t.Fatal("old binding still resolves after the sibling was taken over")
	}
	_, _, _, err = NewExecutorWithManager(manager).acquireContinuation(ctx, old)
	var lost *proxy.ResponsesContinuationLostError
	if !errors.As(err, &lost) {
		t.Fatalf("continuation with old response ID returned %v, want ResponsesContinuationLostError", err)
	}
	line := statsLineFor(t, manager.connectionStatsLines(time.Now()), account.ID())
	for _, want := range []string{"overflow:1 ", "slot_bound:0,"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
}

// TestBusyOverflowBoundReuseDisabledKeepsSkipping：CODEX_WS_SLOT_REUSE_BOUND=0 时保持旧行为，
// 带绑定的兄弟连接被跳过；没有空槽位时继续等待原连接直到超时，不接管、不新建。
func TestBusyOverflowBoundReuseDisabledKeepsSkipping(t *testing.T) {
	t.Setenv("CODEX_WS_SLOT_REUSE_BOUND", "0")
	enableBusyOverflowForTest(t, 1, 1)
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 602, DynamicConcurrencyLimit: 8}
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })
	ctx := context.Background()

	first, firstPending, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil {
		t.Fatalf("first overflow acquire: %v", err)
	}
	finishStatelessRequest(t, manager, first, firstPending, first.session.ID, "resp_ovf_old", "key")

	_, _, err = manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err == nil || !strings.Contains(err.Error(), "waiting for busy session") {
		t.Fatalf("second acquire err = %v, want busy session timeout with bound reuse disabled", err)
	}
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("handshakes = %d, want 1 (no free slot to dial)", got)
	}
	if first.session.PendingCount() != 0 {
		t.Fatal("bound sibling was taken over with bound reuse disabled")
	}
	if wc, _ := manager.lookupResponseConn("resp_ovf_old", account.ID(), "key"); wc != first {
		t.Fatal("binding of the skipped sibling was dropped")
	}
}

// TestBusyOverflowCreatesBeforeTakingOverBound：有空槽位且允许新建时，新建兄弟连接而不
// 丢弃已有兄弟连接的旧绑定；不允许新建时（取连内后续醒来）才接管带绑定的兄弟连接。
func TestBusyOverflowCreatesBeforeTakingOverBound(t *testing.T) {
	enableBusyOverflowForTest(t, 3, 5)
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 608, DynamicConcurrencyLimit: 8}
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })
	ctx := context.Background()
	executor := NewExecutorWithManager(manager)

	first, firstPending, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil {
		t.Fatalf("first overflow acquire: %v", err)
	}
	finishStatelessRequest(t, manager, first, firstPending, first.session.ID, "resp_keep", "key")

	second, secondPending, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil {
		t.Fatalf("second overflow acquire: %v", err)
	}
	defer second.session.RemovePendingRequest(secondPending.RequestID)
	if second == first || second.session.ID != "session-1#ovf-2" {
		t.Fatalf("second overflow took %q, want a new sibling in the free slot ovf-2", second.session.ID)
	}
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("handshakes = %d, want 2", got)
	}
	keep := websocketContinuation{responseID: "resp_keep", accountID: account.ID(), apiKey: "key"}
	if wc, _ := manager.lookupContinuationConn(keep); wc != first {
		t.Fatal("binding of the idle sibling was dropped although a free slot existed")
	}

	// 不允许新建（同一次取连的后续醒来）：空槽位 ovf-3 不用，接管带绑定的 ovf-1。
	third, thirdPending, ok := manager.tryAcquireBusyOverflow(ctx, account, wsURL, "session-1", http.Header{}, "", false)
	if !ok || third != first {
		t.Fatalf("reuse-only overflow = (%v, %v), want the bound sibling ovf-1", third, ok)
	}
	defer third.session.RemovePendingRequest(thirdPending.RequestID)
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("handshakes = %d after reuse-only overflow, want 2", got)
	}
	_, _, _, err = executor.acquireContinuation(ctx, keep)
	var lost *proxy.ResponsesContinuationLostError
	if !errors.As(err, &lost) {
		t.Fatalf("continuation after takeover returned %v, want ResponsesContinuationLostError", err)
	}
}

// TestBusyOverflowPrefersUnboundThenLeastRecentlyUsedBound：槽位全被占用时，先用无绑定的
// 空闲兄弟连接，再按最久未使用接管带绑定的；正在处理请求的兄弟连接不被取走。
func TestBusyOverflowPrefersUnboundThenLeastRecentlyUsedBound(t *testing.T) {
	enableBusyOverflowForTest(t, 4, 5)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(*WsConnection) bool { return true }
	account := &auth.Account{DBID: 603, DynamicConcurrencyLimit: 8}
	wsURL := "wss://example.test/responses"
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })
	now := time.Now()
	// ovf-1：在途且带绑定；ovf-2/ovf-3：空闲带绑定（ovf-3 更旧）；ovf-4：空闲无绑定。
	busySibling := newBoundSlotConnection(t, manager, account, wsURL, "session-1#ovf-1", "resp_busy", now.Add(-5*time.Minute))
	busySiblingPending, err := manager.addPendingAndBeginReadLease(busySibling, "session-1#ovf-1", capacityChat)
	if err != nil {
		t.Fatal(err)
	}
	defer busySibling.session.RemovePendingRequest(busySiblingPending.RequestID)
	newBoundSlotConnection(t, manager, account, wsURL, "session-1#ovf-2", "resp_2", now.Add(-time.Minute))
	oldestBound := newBoundSlotConnection(t, manager, account, wsURL, "session-1#ovf-3", "resp_3", now.Add(-2*time.Minute))
	unbound, _ := newTestSlotConnection(manager, account, wsURL, "session-1#ovf-4")

	ctx := context.Background()
	got, pr, err := manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil || got != unbound {
		t.Fatalf("first acquire = (%v, %v), want the unbound idle sibling ovf-4", got, err)
	}
	defer got.session.RemovePendingRequest(pr.RequestID)

	got, pr, err = manager.AcquireConnection(ctx, account, wsURL, "session-1", http.Header{}, "")
	if err != nil || got != oldestBound {
		t.Fatalf("second acquire = (%v, %v), want least recently used bound sibling ovf-3", got, err)
	}
	defer got.session.RemovePendingRequest(pr.RequestID)
	if wc, _ := manager.lookupResponseConn("resp_3", account.ID(), "key"); wc != nil {
		t.Fatal("binding of the taken-over sibling still resolves")
	}
	if wc, _ := manager.lookupResponseConn("resp_2", account.ID(), "key"); wc == nil {
		t.Fatal("binding of an untouched sibling was dropped")
	}
	if busySibling.session.PendingCount() != 1 {
		t.Fatalf("busy sibling pending = %d, want 1", busySibling.session.PendingCount())
	}
	if wc, _ := manager.lookupResponseConn("resp_busy", account.ID(), "key"); wc != busySibling {
		t.Fatal("binding of the busy sibling was dropped")
	}
}

// TestBusyOverflowSlotLimitBoundsSiblings：槽位数设为 N 时同会话最多 N 条兄弟连接；
// 主连接占用 1 个请求，其余 N+1 个并发请求里多出的一个只等待、不新建，
// 兄弟连接空闲后由它复用。
func TestBusyOverflowSlotLimitBoundsSiblings(t *testing.T) {
	const slots = 3
	enableBusyOverflowForTest(t, slots, 10)
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 604, DynamicConcurrencyLimit: 16}
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })

	type acquired struct {
		wc  *WsConnection
		pr  *PendingRequest
		err error
	}
	results := make(chan acquired, slots+1)
	for i := 0; i < slots+1; i++ {
		go func() {
			wc, pr, err := manager.AcquireConnection(context.Background(), account, wsURL, "session-1", http.Header{}, "")
			results <- acquired{wc, pr, err}
		}()
	}
	var held []acquired
	for len(held) < slots {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("overflow acquire: %v", r.err)
			}
			held = append(held, r)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d siblings acquired", len(held), slots)
		}
	}
	// 多出的请求应持续等待：既不返回，也不新建兄弟连接。
	select {
	case r := <-results:
		t.Fatalf("extra request returned early: wc=%v err=%v", r.wc, r.err)
	case <-time.After(300 * time.Millisecond):
	}
	if got := handshakes.Load(); got != slots {
		t.Fatalf("handshakes = %d, want %d (one per sibling slot)", got, slots)
	}
	if got := overflowSiblingCount(manager, "session-1"); got != slots {
		t.Fatalf("overflow siblings = %d, want %d", got, slots)
	}
	finishStatelessRequest(t, manager, held[0].wc, held[0].pr, held[0].wc.session.ID, "resp_done", "key")
	select {
	case r := <-results:
		if r.err != nil || r.wc != held[0].wc {
			t.Fatalf("waiter got (%v, %v), want the freed sibling", r.wc, r.err)
		}
		r.wc.session.RemovePendingRequest(r.pr.RequestID)
	case <-time.After(3 * time.Second):
		t.Fatal("waiter did not reuse the freed sibling")
	}
	for _, h := range held[1:] {
		h.wc.session.RemovePendingRequest(h.pr.RequestID)
	}
	if got := handshakes.Load(); got != slots {
		t.Fatalf("handshakes = %d after reuse, want %d", got, slots)
	}
}

// TestBusyOverflowWaiterReusesSiblingFreedDuringWait：等待期间兄弟连接空闲后，等待者被唤醒
// 并复用它，不等满上限。
func TestBusyOverflowWaiterReusesSiblingFreedDuringWait(t *testing.T) {
	enableBusyOverflowForTest(t, 1, 10)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	manager.probeFunc = func(*WsConnection) bool { return true }
	account := &auth.Account{DBID: 605, DynamicConcurrencyLimit: 8}
	wsURL := "wss://example.test/responses"
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })
	sibling, siblingBlocking := newBusyTestConnection(t, manager, account, wsURL, "session-1#ovf-1")

	type acquired struct {
		wc  *WsConnection
		pr  *PendingRequest
		err error
		at  time.Time
	}
	done := make(chan acquired, 1)
	go func() {
		wc, pr, err := manager.AcquireConnection(context.Background(), account, wsURL, "session-1", http.Header{}, "")
		done <- acquired{wc, pr, err, time.Now()}
	}()
	time.Sleep(500 * time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("acquire returned before any connection was free: %v %v", r.wc, r.err)
	default:
	}
	freedAt := time.Now()
	sibling.session.RemovePendingRequest(siblingBlocking.RequestID)
	select {
	case r := <-done:
		if r.err != nil || r.wc != sibling {
			t.Fatalf("acquire = (%v, %v), want the freed sibling", r.wc, r.err)
		}
		if lag := r.at.Sub(freedAt); lag > time.Second {
			t.Fatalf("waiter took %s to reuse the freed sibling", lag)
		}
		r.wc.session.RemovePendingRequest(r.pr.RequestID)
	case <-time.After(3 * time.Second):
		t.Fatal("waiter did not reuse the sibling freed during the wait")
	}
}

// TestBusyOverflowCreatesAtMostOncePerAcquire：允许新建的溢出每次取连最多一次，之后醒来
// 只尝试复用。
func TestBusyOverflowCreatesAtMostOncePerAcquire(t *testing.T) {
	enableBusyOverflowForTest(t, 2, 1)
	var dials atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 606, DynamicConcurrencyLimit: 8}
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				manager.notifyAccountWaiters(account.ID())
			}
		}
	}()
	_, _, err := manager.AcquireConnection(context.Background(), account, wsURL, "session-1", http.Header{}, "")
	if err == nil || !strings.Contains(err.Error(), "waiting for busy session") {
		t.Fatalf("err = %v, want busy session timeout", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("overflow dials = %d, want exactly 1 per acquire", got)
	}
}

// TestBusyOverflowConcurrentWithContinuation 让同会话的并发请求与续链请求争用同一批兄弟
// 连接，验证任何时刻每条连接最多一个在途请求、正在处理请求的连接不被取走。需在 -race 下运行。
func TestBusyOverflowConcurrentWithContinuation(t *testing.T) {
	const slots = 2
	enableBusyOverflowForTest(t, slots, 30)
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 607, DynamicConcurrencyLimit: 16}
	busy, blocking := newBusyTestConnection(t, manager, account, wsURL, "session-1")
	t.Cleanup(func() { busy.session.RemovePendingRequest(blocking.RequestID) })
	executor := NewExecutorWithManager(manager)

	var inflightMu sync.Mutex
	inflight := make(map[*WsConnection]int)
	var violations, destroyedWhileHeld, responseSeq, continuationHits atomic.Int32
	enter := func(wc *WsConnection) {
		inflightMu.Lock()
		inflight[wc]++
		if inflight[wc] != 1 || wc.session.PendingCount() != 1 {
			violations.Add(1)
		}
		inflightMu.Unlock()
	}
	var lastResponse atomic.Value
	lastResponse.Store("")
	complete := func(wc *WsConnection, pr *PendingRequest, key string) {
		id := fmt.Sprintf("resp_%d", responseSeq.Add(1))
		time.Sleep(time.Duration(responseSeq.Load()%3) * 200 * time.Microsecond)
		inflightMu.Lock()
		inflight[wc]--
		inflightMu.Unlock()
		manager.bindResponseConn(id, responseConnBinding{conn: wc, sessionKey: key, accountID: account.ID(), apiKey: "key"})
		lastResponse.Store(id)
		lock, releaseKeyLock := manager.keyLock(wc.PoolKey)
		lock.Lock()
		if !wc.IsConnected() {
			destroyedWhileHeld.Add(1)
		}
		wc.cancelUnsentReadLease(pr.RequestID)
		wc.session.RemovePendingRequest(pr.RequestID)
		lock.Unlock()
		releaseKeyLock()
		manager.ReleaseConnection(wc)
	}

	var wg, sessionWG sync.WaitGroup
	var sessionDone atomic.Bool
	errs := make(chan error, 64)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		sessionWG.Add(1)
		go func() {
			defer wg.Done()
			defer sessionWG.Done()
			for i := 0; i < 40; i++ {
				wc, pr, err := manager.AcquireConnection(context.Background(), account, wsURL, "session-1", http.Header{}, "")
				if err != nil {
					errs <- err
					return
				}
				enter(wc)
				complete(wc, pr, wc.session.ID)
				time.Sleep(200 * time.Microsecond)
			}
		}()
	}
	go func() {
		sessionWG.Wait()
		sessionDone.Store(true)
	}()
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !sessionDone.Load() {
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
					var busyErr *proxy.ResponsesContinuationBusyError
					if !errors.As(err, &lost) && !errors.As(err, &busyErr) && !errors.Is(err, context.DeadlineExceeded) {
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
	if got := handshakes.Load(); got > slots {
		t.Fatalf("handshakes = %d, want at most %d sibling slots", got, slots)
	}
	if got := overflowSiblingCount(manager, "session-1"); got > slots {
		t.Fatalf("overflow siblings = %d, want at most %d", got, slots)
	}
	t.Logf("responses=%d continuation hits=%d handshakes=%d", responseSeq.Load(), continuationHits.Load(), handshakes.Load())
}
