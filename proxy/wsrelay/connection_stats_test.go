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

func newStatsTestServer(t *testing.T, forbidden bool) string {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if forbidden {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
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
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func statsLineFor(t *testing.T, lines []string, accountID int64) string {
	t.Helper()
	prefix := fmt.Sprintf("[WS-Stats] account=%d ", accountID)
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	t.Fatalf("no stats line for account %d in %q", accountID, lines)
	return ""
}

func TestConnectionStatsCountsHandshakeOutcomesPerAccount(t *testing.T) {
	okURL := newStatsTestServer(t, false)
	forbiddenURL := newStatsTestServer(t, true)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 42, DynamicConcurrencyLimit: 4}

	for i := 0; i < 2; i++ {
		wc, pending, err := manager.AcquireConnection(context.Background(), account, okURL, fmt.Sprintf("ok-%d", i), http.Header{}, "")
		if err != nil {
			t.Fatalf("AcquireConnection(ok-%d): %v", i, err)
		}
		wc.session.RemovePendingRequest(pending.RequestID)
		manager.ReleaseConnection(wc)
	}
	if _, _, err := manager.AcquireConnection(context.Background(), account, forbiddenURL, "blocked", http.Header{}, ""); err == nil {
		t.Fatal("expected 403 handshake to fail")
	}
	start := time.Now()
	manager.connStats.startWindow(start)
	lines := manager.connectionStatsLines(start.Add(90 * time.Second))
	line := statsLineFor(t, lines, account.ID())
	for _, want := range []string{"window_s=90", "handshake_ok=2", "handshake_403=1", "handshake_other_fail=0", "handshake_canceled=0", "dial_kind=slot:0,oneshot:0,session:3,overflow:0", "connections=2", "busy=0"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
	// The window is consumed: an idle minute produces no lines.
	if again := manager.connectionStatsLines(time.Now()); len(again) != 0 {
		t.Fatalf("stats window was not reset: %q", again)
	}
}

func TestConnectionStatsRecordsCloseReasons(t *testing.T) {
	setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) { s.CodexWSDownstreamKeepaliveSlots = 1 })
	wsURL := newStatsTestServer(t, false)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 7, DynamicConcurrencyLimit: 1}

	for i := 0; i < 3; i++ {
		wc, pending, err := manager.AcquireConnection(context.Background(), account, wsURL, fmt.Sprintf("session-%d", i), http.Header{}, "")
		if err != nil {
			t.Fatalf("AcquireConnection(%d): %v", i, err)
		}
		wc.session.RemovePendingRequest(pending.RequestID)
		manager.ReleaseConnection(wc)
		time.Sleep(2 * time.Millisecond)
	}

	line := statsLineFor(t, manager.connectionStatsLines(time.Now()), account.ID())
	for _, want := range []string{"handshake_ok=3", "closes=idle_chat_lru:2", "connections=1", "chat_idle_limit=1"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
}

func TestConnectionStatsRecorderIgnoresEmptyWindowsAndFormatsCloses(t *testing.T) {
	var recorder connectionStatsRecorder
	if got := recorder.swap(time.Now()); got.accounts != nil || got.elapsed != 0 {
		t.Fatalf("zero recorder swap = %+v, want empty", got)
	}
	recorder.recordClose(1, "idle_timeout")
	recorder.recordClose(1, "idle_chat_lru")
	recorder.recordClose(1, "idle_chat_lru")
	recorder.recordClose(1, " ")
	stats := recorder.swap(time.Now()).accounts[1]
	if got := formatConnectionCloses(stats.closes); got != "idle_chat_lru:2,idle_timeout:1,unknown:1" {
		t.Fatalf("closes = %q", got)
	}
	if formatConnectionCloses(nil) != "-" {
		t.Fatal("empty closes must render as -")
	}
	if !(&accountConnectionStats{}).empty() {
		t.Fatal("zero stats must be empty")
	}
}

func TestConnectionStatsClassifiesHandshakesWithoutOverlap(t *testing.T) {
	var recorder connectionStatsRecorder
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	bg := context.Background()
	rejected429 := &http.Response{StatusCode: http.StatusTooManyRequests}
	forbidden := &http.Response{StatusCode: http.StatusForbidden}
	switching := &http.Response{StatusCode: http.StatusSwitchingProtocols}
	// ok x2
	recorder.recordHandshake(bg, 9, dialKindSlot, 30*time.Millisecond, switching, nil)
	recorder.recordHandshake(bg, 9, dialKindSlot, 10*time.Millisecond, switching, nil)
	// 403 x2 (one arrives after the caller canceled: still 403)
	recorder.recordHandshake(bg, 9, dialKindSession, 50*time.Millisecond, forbidden, websocket.ErrBadHandshake)
	recorder.recordHandshake(canceled, 9, dialKindSession, 150*time.Millisecond, forbidden, websocket.ErrBadHandshake)
	// other x5: 429, 429 after cancel, transport error, handshake timeout, caller deadline
	recorder.recordHandshake(bg, 9, dialKindOneShot, 5*time.Millisecond, rejected429, websocket.ErrBadHandshake)
	recorder.recordHandshake(canceled, 9, dialKindOneShot, 5*time.Millisecond, rejected429, websocket.ErrBadHandshake)
	recorder.recordHandshake(bg, 9, dialKindOverflow, 5*time.Millisecond, nil, fmt.Errorf("dial tcp: connection refused"))
	recorder.recordHandshake(bg, 9, dialKindOverflow, 5*time.Millisecond, nil, context.DeadlineExceeded)
	recorder.recordHandshake(expired, 9, dialKindOverflow, 5*time.Millisecond, nil, context.DeadlineExceeded)
	// canceled x1: no upstream response and caller ctx canceled
	recorder.recordHandshake(canceled, 9, dialKindSession, 5*time.Millisecond, nil, context.Canceled)
	got := recorder.swap(time.Now()).accounts[9]
	if got.handshakeOK != 2 || got.handshake403 != 2 || got.handshakeOtherFail != 5 || got.handshakeCanceled != 1 {
		t.Fatalf("stats = %+v", got)
	}
	if got.okMsTotal != 40 || got.okMsMax != 30 || got.forbiddenMsTotal != 200 || got.forbiddenMsMax != 150 {
		t.Fatalf("latency ok=%d/%d 403=%d/%d", got.okMsTotal, got.okMsMax, got.forbiddenMsTotal, got.forbiddenMsMax)
	}
	if want := "slot:2,oneshot:2,session:3,overflow:3"; formatDialKinds(got.dials) != want {
		t.Fatalf("dial kinds = %q, want %q", formatDialKinds(got.dials), want)
	}
}

func newStatusTestServer(t *testing.T, status int) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(status), status)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

// newStallTestServer 收到握手请求后不响应，直到测试结束。
func newStallTestServer(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	arrived := make(chan struct{}, 8)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	return "ws" + strings.TrimPrefix(server.URL, "http"), arrived
}

func TestConnectionStatsClassifiesRealHandshakeFailures(t *testing.T) {
	forbiddenURL := newStatusTestServer(t, http.StatusForbidden)
	tooManyURL := newStatusTestServer(t, http.StatusTooManyRequests)
	stallURL, arrived := newStallTestServer(t)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 61, DynamicConcurrencyLimit: 4}

	if _, _, err := manager.AcquireConnection(context.Background(), account, forbiddenURL, "s-403", http.Header{}, ""); err == nil {
		t.Fatal("403 handshake succeeded")
	}
	if _, _, err := manager.AcquireConnection(context.Background(), account, tooManyURL, "s-429", http.Header{}, ""); err == nil {
		t.Fatal("429 handshake succeeded")
	}
	// 握手超时：调用方 ctx 未取消，属于 other_fail。
	manager.dialer.HandshakeTimeout = 50 * time.Millisecond
	if _, _, err := manager.AcquireConnection(context.Background(), account, stallURL, "s-timeout", http.Header{}, ""); err == nil {
		t.Fatal("stalled handshake succeeded")
	}
	<-arrived
	// 调用方真实取消拨号：上游没有响应，计为 canceled。gorilla v1.5.3 在 Upgrade
	// 阶段只按 deadline 中断，取消后由较短的握手超时结束拨号；此时调用方 ctx 已是
	// Canceled，仍归入 canceled。
	manager.dialer.HandshakeTimeout = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-arrived
		cancel()
	}()
	if _, _, err := manager.AcquireConnection(ctx, account, stallURL, "s-cancel", http.Header{}, ""); err == nil {
		t.Fatal("canceled handshake succeeded")
	}
	line := statsLineFor(t, manager.connectionStatsLines(time.Now()), account.ID())
	for _, want := range []string{"handshake_ok=0 ", "handshake_403=1 ", "handshake_other_fail=2 ", "handshake_canceled=1 ", "dial_kind=slot:0,oneshot:0,session:4,overflow:0"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
}

func TestConnectionStatsCountsDialAndReuseKinds(t *testing.T) {
	wsURL := newStatsTestServer(t, false)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	executor := NewExecutorWithManager(manager)
	account := &auth.Account{DBID: 62, DynamicConcurrencyLimit: 4}
	ctx := context.Background()
	slot := func(fallback, responseID string) string {
		t.Helper()
		wc, pr, key, err := manager.AcquireReusableConnection(ctx, account, wsURL, "c", fallback, 1, http.Header{}, "")
		if err != nil {
			t.Fatalf("AcquireReusableConnection(%s): %v", fallback, err)
		}
		finishStatelessRequest(t, manager, wc, pr, key, responseID, "key")
		return key
	}
	if key := slot("stateless-a", ""); key != "c#0" { // slot dial
		t.Fatalf("first slot key = %q", key)
	}
	slot("stateless-b", "r1") // slot reuse, leaves a binding
	slot("stateless-c", "r2") // bound slot taken over
	t.Setenv("CODEX_WS_SLOT_REUSE_BOUND", "0")
	if key := slot("stateless-d", ""); key != "stateless-d" { // one-shot dial
		t.Fatalf("fallback key = %q, want stateless-d", key)
	}

	session := func(responseID string) {
		t.Helper()
		wc, pr, err := manager.AcquireConnection(ctx, account, wsURL, "sess", http.Header{}, "")
		if err != nil {
			t.Fatalf("AcquireConnection: %v", err)
		}
		finishStatelessRequest(t, manager, wc, pr, "sess", responseID, "key")
	}
	session("r3") // session dial
	wc, pr, key, err := executor.acquireContinuation(ctx, websocketContinuation{responseID: "r3", accountID: account.ID(), apiKey: "key"})
	if err != nil {
		t.Fatalf("acquireContinuation: %v", err)
	}
	finishStatelessRequest(t, manager, wc, pr, key, "", "key") // continuation reuse
	session("")                                                // session reuse
	for i := 0; i < 2; i++ {                                   // overflow dial, then overflow reuse
		owc, opr, ok := manager.tryAcquireBusyOverflow(ctx, account, wsURL, "sess", http.Header{}, "", true)
		if !ok {
			t.Fatalf("tryAcquireBusyOverflow #%d failed", i)
		}
		finishStatelessRequest(t, manager, owc, opr, owc.session.ID, "", "key")
	}

	line := statsLineFor(t, manager.connectionStatsLines(time.Now()), account.ID())
	for _, want := range []string{
		"dial_kind=slot:1,oneshot:1,session:1,overflow:1 ",
		"reuse_kind=slot:1,slot_bound:1,session:1,continuation:1,overflow:1 ",
		"slot_fallback=1 slot_fallback_bound=1 slot_fallback_busy=0 ",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
}

func TestConnectionStatsSlotFallbackReasonsWhenBoundReuseDisabled(t *testing.T) {
	t.Setenv("CODEX_WS_SLOT_REUSE_BOUND", "false")
	wsURL := newStatsTestServer(t, false)
	manager := NewManager()
	t.Cleanup(manager.Stop)
	account := &auth.Account{DBID: 63, DynamicConcurrencyLimit: 4}
	acquire := func(fallback string) (*WsConnection, *PendingRequest, string) {
		t.Helper()
		wc, pr, key, err := manager.AcquireReusableConnection(context.Background(), account, wsURL, "c", fallback, 2, http.Header{}, "")
		if err != nil {
			t.Fatalf("AcquireReusableConnection(%s): %v", fallback, err)
		}
		return wc, pr, key
	}
	a, apr, akey := acquire("stateless-a")
	b, bpr, _ := acquire("stateless-b")
	f, fpr, fkey := acquire("stateless-f1") // both slots busy
	if fkey != "stateless-f1" {
		t.Fatalf("key = %q, want fallback", fkey)
	}
	finishStatelessRequest(t, manager, f, fpr, fkey, "", "key")
	finishStatelessRequest(t, manager, a, apr, akey, "ra", "key") // slot 0 idle but bound
	f, fpr, fkey = acquire("stateless-f2")                        // slot 0 bound, slot 1 busy
	if fkey != "stateless-f2" {
		t.Fatalf("key = %q, want fallback", fkey)
	}
	finishStatelessRequest(t, manager, f, fpr, fkey, "", "key")
	defer b.session.RemovePendingRequest(bpr.RequestID)

	line := statsLineFor(t, manager.connectionStatsLines(time.Now()), account.ID())
	for _, want := range []string{"slot_fallback=2 ", "slot_fallback_bound=1 ", "slot_fallback_busy=3 ", "slot_bound:0,"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stats line %q missing %q", line, want)
		}
	}
}

func TestConnectionStatsCountsRejectedBindings(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.Stop)
	full := newBoundTestConn(t, manager, 64, "full")
	target := newBoundTestConn(t, manager, 64, "target")
	expires := time.Now().Add(time.Hour)
	manager.respConnMu.Lock()
	manager.respConnBindings = make(map[string]responseConnBinding, responseConnBindingMaxEntries)
	for i := 0; i < responseConnBindingMaxEntries; i++ {
		manager.respConnBindings[fmt.Sprintf("filler_%d", i)] = responseConnBinding{conn: full, accountID: 64, expiresAt: expires}
	}
	manager.respConnMu.Unlock()

	manager.BindResponseConn("rejected", target, "target", 64, "key")
	if wc, _ := manager.lookupResponseConn("rejected", 64, "key"); wc != nil {
		t.Fatal("binding beyond the table limit was stored")
	}
	lines := manager.connectionStatsLines(time.Now())
	want := fmt.Sprintf("bindings=%d binding_limit=16384 binding_rejected=1 connections=2", responseConnBindingMaxEntries)
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "[WS-Stats] global ") || !strings.Contains(lines[0], want) {
		t.Fatalf("lines = %q, want a single global line containing %q", lines, want)
	}
}

type statsCapture struct {
	mu    sync.Mutex
	lines []string
	added chan struct{}
}

func newStatsCapture(manager *Manager) *statsCapture {
	c := &statsCapture{added: make(chan struct{}, 64)}
	manager.setConnectionStatsOutput(func(line string) {
		c.mu.Lock()
		c.lines = append(c.lines, line)
		c.mu.Unlock()
		select {
		case c.added <- struct{}{}:
		default:
		}
	})
	return c
}

func (c *statsCapture) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func TestConnectionStatsTickerEmitsWindows(t *testing.T) {
	prev := connectionStatsInterval
	connectionStatsInterval = 20 * time.Millisecond
	manager := NewManager()
	connectionStatsInterval = prev
	t.Cleanup(manager.Stop)
	capture := newStatsCapture(manager)

	manager.connStats.recordClose(65, "idle_timeout")
	deadline := time.After(2 * time.Second)
	for {
		lines := capture.snapshot()
		if len(lines) >= 2 {
			if !strings.HasPrefix(lines[0], "[WS-Stats] account=65 window_s=0 ") || !strings.HasPrefix(lines[1], "[WS-Stats] global window_s=0 ") {
				t.Fatalf("ticker output = %q", lines)
			}
			return
		}
		select {
		case <-capture.added:
		case <-deadline:
			t.Fatalf("ticker did not emit stats, got %q", lines)
		}
	}
}

func TestConnectionStatsStopEmitsFinalWindow(t *testing.T) {
	wsURL := newStatsTestServer(t, false)
	manager := NewManager() // 默认 1 分钟周期，测试期间不会触发定时输出
	capture := newStatsCapture(manager)
	account := &auth.Account{DBID: 66, DynamicConcurrencyLimit: 4}
	wc, pr, err := manager.AcquireConnection(context.Background(), account, wsURL, "s", http.Header{}, "")
	if err != nil {
		t.Fatalf("AcquireConnection: %v", err)
	}
	finishStatelessRequest(t, manager, wc, pr, "s", "", "key")
	manager.Stop()

	lines := capture.snapshot()
	line := statsLineFor(t, lines, account.ID())
	for _, want := range []string{"handshake_ok=1 ", "closes=manager_shutdown:1 "} {
		if !strings.Contains(line, want) {
			t.Fatalf("final window %q missing %q", line, want)
		}
	}
	if !strings.HasPrefix(lines[len(lines)-1], "[WS-Stats] global ") {
		t.Fatalf("final window lacks the global line: %q", lines)
	}
	manager.Stop() // idempotent: no second window
	if got := capture.snapshot(); len(got) != len(lines) {
		t.Fatalf("second Stop emitted more stats: %q", got)
	}
}
