package wsrelay

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy"
)

func acquireStatelessForTest(t *testing.T, manager *Manager, account *auth.Account, wsURL string, slots int, fallback string) (*WsConnection, *PendingRequest, string) {
	t.Helper()
	wc, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", fallback, slots, http.Header{}, "")
	if err != nil {
		t.Fatalf("AcquireReusableConnection(%s): %v", fallback, err)
	}
	return wc, pr, key
}

func accountStatsSnapshot(manager *Manager, accountID int64) (dials [dialKindCount]int, reuses [reuseKindCount]int, fallbacks int, closes map[string]int) {
	manager.connStats.mu.Lock()
	defer manager.connStats.mu.Unlock()
	closes = make(map[string]int)
	stats := manager.connStats.accounts[accountID]
	if stats == nil {
		return dials, reuses, 0, closes
	}
	for reason, n := range stats.closes {
		closes[reason] = n
	}
	return stats.dials, stats.reuses, stats.slotFallback, closes
}

// 存在空槽位且存在带绑定的空闲槽位时，无会话请求先接管后者：不新握手，旧绑定被丢弃。
func TestSlotReusePrefersBoundIdleSlotOverFreeSlot(t *testing.T) {
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 601, DynamicConcurrencyLimit: 8}

	first, firstPending, firstKey := acquireStatelessForTest(t, manager, account, wsURL, 4, "stateless-0")
	finishStatelessRequest(t, manager, first, firstPending, firstKey, "resp_0", "key")
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("handshakes after first request = %d, want 1", got)
	}

	second, secondPending, secondKey := acquireStatelessForTest(t, manager, account, wsURL, 4, "stateless-1")
	defer second.session.RemovePendingRequest(secondPending.RequestID)
	if second != first || secondKey != firstKey {
		t.Fatalf("second request used %q, want the bound idle slot %q", secondKey, firstKey)
	}
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("handshakes = %d, want 1 (no dial while a bound slot is idle)", got)
	}
	if wc, _ := manager.lookupResponseConn("resp_0", account.ID(), "key"); wc != nil {
		t.Fatal("old binding still resolves after the slot was taken over")
	}
	dials, reuses, fallbacks, _ := accountStatsSnapshot(manager, account.ID())
	if dials[dialKindSlot] != 1 || reuses[reuseKindSlotBound] != 1 || fallbacks != 0 {
		t.Fatalf("dial slot=%d slot_bound=%d fallbacks=%d, want 1/1/0", dials[dialKindSlot], reuses[reuseKindSlotBound], fallbacks)
	}
}

// 已有槽位全部在忙且还有空槽位时，新建一条槽位连接。
func TestSlotReuseDialsFreeSlotWhenExistingSlotsBusy(t *testing.T) {
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 602, DynamicConcurrencyLimit: 8}

	// cache#0 带绑定且空闲后又被占住；cache#1 无连接。
	held, heldPending, heldKey := acquireStatelessForTest(t, manager, account, wsURL, 4, "stateless-0")
	finishStatelessRequest(t, manager, held, heldPending, heldKey, "resp_0", "key")
	held, heldPending, heldKey = acquireStatelessForTest(t, manager, account, wsURL, 4, "stateless-1")
	defer held.session.RemovePendingRequest(heldPending.RequestID)
	if heldKey != "cache#0" || handshakes.Load() != 1 {
		t.Fatalf("setup: key=%q handshakes=%d, want cache#0 and 1", heldKey, handshakes.Load())
	}

	wc, pr, key := acquireStatelessForTest(t, manager, account, wsURL, 4, "stateless-2")
	defer wc.session.RemovePendingRequest(pr.RequestID)
	if wc == held || key != "cache#1" {
		t.Fatalf("busy slots acquire used %q, want a new connection in free slot cache#1", key)
	}
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("handshakes = %d, want 2", got)
	}
	if held.session.PendingCount() != 1 {
		t.Fatalf("busy slot pending = %d, want 1", held.session.PendingCount())
	}
}

// CODEX_WS_SLOT_REUSE_BOUND=0 时保持旧顺序：有空槽位就新建，不接管带绑定的槽位。
func TestSlotReuseBoundDisabledDialsFreeSlot(t *testing.T) {
	t.Setenv("CODEX_WS_SLOT_REUSE_BOUND", "0")
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 603, DynamicConcurrencyLimit: 8}

	first, firstPending, firstKey := acquireStatelessForTest(t, manager, account, wsURL, 4, "stateless-0")
	finishStatelessRequest(t, manager, first, firstPending, firstKey, "resp_0", "key")

	second, secondPending, secondKey := acquireStatelessForTest(t, manager, account, wsURL, 4, "stateless-1")
	defer second.session.RemovePendingRequest(secondPending.RequestID)
	if second == first || secondKey != "cache#1" {
		t.Fatalf("second request used %q, want a new connection in free slot cache#1", secondKey)
	}
	if got := handshakes.Load(); got != 2 {
		t.Fatalf("handshakes = %d, want 2", got)
	}
	if wc, _ := manager.lookupResponseConn("resp_0", account.ID(), "key"); wc != first {
		t.Fatal("binding of the untouched bound slot was dropped")
	}
}

// 回归：空闲保留上限小于总连接数，串行无会话请求夹杂触发 LRU 回收的对话连接。
// 旧顺序下每个请求都在空槽位新建，新建又让其他槽位被回收，握手随请求数增长；
// 先接管后新建时槽位连接始终是最近使用的，握手次数保持为 1。
func TestSlotReuseBeforeDialBreaksLRURebuildLoop(t *testing.T) {
	const slots, retention, requests = 4, 3, 40
	setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) {
		s.CodexWSDownstreamKeepaliveSlots = retention
		s.CodexWSStatelessSlots = slots
	})
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 604, DynamicConcurrencyLimit: 64}

	for i := 0; i < requests; i++ {
		wc, pr, key := acquireStatelessForTest(t, manager, account, wsURL, slots, fmt.Sprintf("stateless-%d", i))
		finishStatelessRequest(t, manager, wc, pr, key, fmt.Sprintf("resp_%d", i), "key")
		// 对话连接收尾时按 LRU 收缩空闲保留池。
		chatID := fmt.Sprintf("chat-%d", i)
		chat := newPooledConn(t, manager, account.ID(), chatID, manager.poolKey(account.ID(), wsURL, chatID, ""), time.Now())
		manager.ReleaseConnection(chat)
	}
	dials, reuses, fallbacks, closes := accountStatsSnapshot(manager, account.ID())
	if closes[closeIdleChatLRU] == 0 {
		t.Fatalf("no %s evictions (closes=%v); the retention limit was not exercised", closeIdleChatLRU, closes)
	}
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("handshakes = %d after %d requests, want 1 (dial slot=%d slot_bound=%d fallbacks=%d closes=%v)",
			got, requests, dials[dialKindSlot], reuses[reuseKindSlotBound], fallbacks, closes)
	}
	if reuses[reuseKindSlotBound] != requests-1 || fallbacks != 0 {
		t.Fatalf("slot_bound=%d fallbacks=%d, want %d/0", reuses[reuseKindSlotBound], fallbacks, requests-1)
	}
}

// 并发取槽位：正在处理请求的槽位不会被接管，每条连接同一时刻最多一个在途请求。需在 -race 下运行。
func TestSlotReuseBeforeDialConcurrentNeverTakesBusySlot(t *testing.T) {
	const slots, workers, perWorker = 3, 6, 40
	wsURL, handshakes := newCountingWSServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 605, DynamicConcurrencyLimit: 32}

	var mu sync.Mutex
	inflight := make(map[*WsConnection]int)
	var violations, seq atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				wc, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "cache", fmt.Sprintf("stateless-%d-%d", g, i), slots, http.Header{}, "")
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				inflight[wc]++
				if inflight[wc] != 1 || wc.session.PendingCount() != 1 {
					violations.Add(1)
				}
				mu.Unlock()
				time.Sleep(time.Duration(seq.Add(1)%3) * 100 * time.Microsecond)
				mu.Lock()
				inflight[wc]--
				mu.Unlock()
				id := fmt.Sprintf("resp_%d", seq.Add(1))
				manager.bindResponseConn(id, responseConnBinding{conn: wc, sessionKey: key, accountID: account.ID(), apiKey: "key"})
				lock, releaseKeyLock := manager.keyLock(wc.PoolKey)
				lock.Lock()
				wc.cancelUnsentReadLease(pr.RequestID)
				wc.session.RemovePendingRequest(pr.RequestID)
				lock.Unlock()
				releaseKeyLock()
				manager.ReleaseConnection(wc)
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := violations.Load(); got != 0 {
		t.Fatalf("%d acquisitions shared a connection with another in-flight request", got)
	}
	_, reuses, fallbacks, closes := accountStatsSnapshot(manager, account.ID())
	if n := closes[closeUnusable]; n != 0 {
		t.Fatalf("%d connections closed as %s (closes=%v)", n, closeUnusable, closes)
	}
	if got, limit := int(handshakes.Load()), slots+fallbacks; got > limit {
		t.Fatalf("handshakes = %d, want at most slots(%d) + fallbacks(%d)", got, slots, fallbacks)
	}
	if reuses[reuseKindSlotBound] == 0 {
		t.Fatal("no bound-slot takeover happened; the reuse-before-dial path was not exercised")
	}
	t.Logf("handshakes=%d slot_bound=%d fallbacks=%d closes=%v", handshakes.Load(), reuses[reuseKindSlotBound], fallbacks, closes)
}
