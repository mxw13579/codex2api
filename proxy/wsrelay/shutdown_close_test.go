package wsrelay

import (
	"context"
	"crypto/tls"
	"fmt"
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

// stallConn 包在 TLS 之下的传输连接：stalled 后所有写都阻塞（忽略写期限，模拟比
// 内核发送缓冲写满更坏的情况），直到本连接被 Close。用来复现对端半死时
// tls.Conn.Close 写 close_notify 卡住的场景。
type stallConn struct {
	net.Conn
	stalled      atomic.Bool
	closed       chan struct{}
	closeOnce    sync.Once
	activeWrites atomic.Int32
}

func newStallConn(c net.Conn) *stallConn {
	return &stallConn{Conn: c, closed: make(chan struct{})}
}

func (c *stallConn) Write(p []byte) (int, error) {
	if c.stalled.Load() {
		c.activeWrites.Add(1)
		defer c.activeWrites.Add(-1)
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}

func (c *stallConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *stallConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

type tlsTestConn struct {
	wc        *WsConnection
	transport *stallConn
}

// dialTLSTestConnections 建立 n 条真实 wss 连接（httptest TLS 服务端），
// 以空闲连接身份放进 manager 的连接池。
func dialTLSTestConnections(t *testing.T, m *Manager, n int) []tlsTestConn {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")

	var mu sync.Mutex
	var transports []*stallConn
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			sc := newStallConn(c)
			mu.Lock()
			transports = append(transports, sc)
			mu.Unlock()
			return sc, nil
		},
	}
	out := make([]tlsTestConn, 0, n)
	for i := 0; i < n; i++ {
		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		mu.Lock()
		transport := transports[len(transports)-1]
		mu.Unlock()
		session := NewSession(int64(i+1), m)
		session.SetConnected(true)
		wc := NewWsConnection(conn, session, wsURL)
		wc.PoolKey = m.poolKey(int64(i+1), wsURL, fmt.Sprintf("s%d", i), "")
		m.connections.Store(wc.PoolKey, wc)
		m.sessions.Store(wc.PoolKey, session)
		out = append(out, tlsTestConn{wc: wc, transport: transport})
	}
	return out
}

func waitUntil(t *testing.T, limit time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 若干条连接的 TLS close_notify 写阻塞时，Manager.Stop 仍在期限内返回，
// 阻塞的连接被强制关闭，其余连接正常关闭，被阻塞的写全部返回。
func TestManagerStopBoundedWhenTLSCloseWritesBlock(t *testing.T) {
	m := NewManager()
	conns := dialTLSTestConnections(t, m, 40)
	const stalled = 6
	for i := 0; i < stalled; i++ {
		conns[i].transport.stalled.Store(true)
	}

	start := time.Now()
	m.Stop()
	elapsed := time.Since(start)
	// 串行时为 6 条 × 无限阻塞；有界后约为 boundedCloseForceAfter。
	if limit := boundedCloseForceAfter + time.Second; elapsed > limit {
		t.Fatalf("Manager.Stop took %s, want <= %s", elapsed, limit)
	}
	for i, c := range conns {
		if c.wc.IsConnected() {
			t.Fatalf("conn %d still connected after Stop", i)
		}
		if !c.transport.isClosed() {
			t.Fatalf("conn %d transport not closed after Stop", i)
		}
		if _, ok := m.connections.Load(c.wc.PoolKey); ok {
			t.Fatalf("conn %d still in pool after Stop", i)
		}
	}
	waitUntil(t, time.Second, func() bool {
		for _, c := range conns {
			if c.transport.activeWrites.Load() != 0 {
				return false
			}
		}
		return true
	}, "blocked writes to return")
}

// 没有阻塞时 Stop 不需要等到强制关闭期限。
func TestManagerStopFastWhenNothingBlocks(t *testing.T) {
	m := NewManager()
	conns := dialTLSTestConnections(t, m, 20)
	start := time.Now()
	m.Stop()
	if elapsed := time.Since(start); elapsed >= boundedCloseForceAfter {
		t.Fatalf("Manager.Stop took %s with no blocked writes", elapsed)
	}
	for i, c := range conns {
		if c.wc.IsConnected() || !c.transport.isClosed() {
			t.Fatalf("conn %d not closed", i)
		}
	}
}

// 整体等待上限：即使关闭一直不返回，closeAllConnectionsWithin 也按 wait 返回。
func TestCloseAllConnectionsWithinReturnsAtOverallLimit(t *testing.T) {
	m := NewManager()
	t.Cleanup(m.Stop)
	conns := dialTLSTestConnections(t, m, 3)
	conns[0].transport.stalled.Store(true)
	start := time.Now()
	// forceAfter 远大于 wait：强制关闭尚未触发时整体等待先到期。
	if ok := m.closeAllConnectionsWithin(closeManagerShutdown, 2*time.Second, 200*time.Millisecond); ok {
		t.Fatal("closeAllConnectionsWithin reported completion while a close was blocked")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("closeAllConnectionsWithin took %s, want about 200ms", elapsed)
	}
	// 后台关闭在强制期限到达后完成，不泄漏。
	waitUntil(t, 3*time.Second, func() bool {
		return conns[0].transport.isClosed() && conns[0].transport.activeWrites.Load() == 0 && !conns[0].wc.IsConnected()
	}, "background close to finish")
}

// 排空模式持锁关闭空闲连接：单条 close_notify 阻塞不应无限占住账号锁。
func TestDrainSweepBoundedWhenTLSCloseWritesBlock(t *testing.T) {
	m := NewManager()
	t.Cleanup(m.Stop)
	conns := dialTLSTestConnections(t, m, 8)
	conns[0].transport.stalled.Store(true)
	start := time.Now()
	result, changed := m.EnterDrainMode()
	elapsed := time.Since(start)
	if !changed || result.Closed != len(conns) {
		t.Fatalf("EnterDrainMode = %+v changed=%t, want all %d closed", result, changed, len(conns))
	}
	if limit := boundedCloseForceAfter + time.Second; elapsed > limit {
		t.Fatalf("drain sweep took %s, want <= %s", elapsed, limit)
	}
	for i, c := range conns {
		if c.wc.IsConnected() || !c.transport.isClosed() {
			t.Fatalf("conn %d not closed by drain", i)
		}
	}
}

func setKeepaliveTickForTest(t *testing.T, d time.Duration) {
	t.Helper()
	old := keepaliveTickGranularity
	keepaliveTickGranularity = d
	t.Cleanup(func() { keepaliveTickGranularity = old })
}

// 保活一轮进行中且某条 Ping 写阻塞时，Stop 在期限内返回，且不再给剩余空闲连接发 Ping；
// 阻塞解除后后台 goroutine 退出。
func TestKeepaliveStopBoundedWhenPingBlocks(t *testing.T) {
	setKeepaliveTickForTest(t, 10*time.Millisecond)
	m := NewManager()
	t.Cleanup(m.Stop)
	for i := 0; i < 20; i++ {
		addConnectedConn(t, m, int64(i+1), fmt.Sprintf("s%d", i))
	}
	var calls atomic.Int32
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	release := func() { unblockOnce.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	m.keepalivePingFunc = func(wc *WsConnection) error {
		if calls.Add(1) == 1 {
			close(blocked)
			<-unblock
		}
		return nil
	}
	task := NewKeepaliveTask(m, func() bool { return true }, func() int { return 1 })
	task.Start()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive round did not start")
	}

	start := time.Now()
	exited := task.Stop()
	elapsed := time.Since(start)
	if exited {
		t.Fatal("Stop reported exit while a ping was still blocked")
	}
	if limit := keepaliveStopWait + 500*time.Millisecond; elapsed > limit {
		t.Fatalf("Stop took %s, want <= %s", elapsed, limit)
	}

	release()
	done := make(chan struct{})
	go func() { task.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keepalive goroutine did not exit after blocked ping returned")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("ping calls = %d, want 1 (no new pings after Stop)", got)
	}
}

// 没有阻塞时 Stop 立即返回并确认 goroutine 已退出。
func TestKeepaliveStopReturnsPromptlyWhenIdle(t *testing.T) {
	setKeepaliveTickForTest(t, 10*time.Millisecond)
	m := NewManager()
	t.Cleanup(m.Stop)
	addConnectedConn(t, m, 1, "s1")
	var calls atomic.Int32
	m.keepalivePingFunc = func(*WsConnection) error { calls.Add(1); return nil }
	task := NewKeepaliveTask(m, func() bool { return true }, func() int { return 1 })
	task.Start()
	waitUntil(t, 2*time.Second, func() bool { return calls.Load() > 0 }, "first keepalive round")
	start := time.Now()
	if !task.Stop() {
		t.Fatal("Stop did not observe goroutine exit")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Stop took %s", elapsed)
	}
}

// 已取消的 ctx 下一轮不发任何 Ping；正常运行期的 PingIdleConnections 不受影响。
func TestPingIdleConnectionsStopsOnCanceledContext(t *testing.T) {
	m := NewManager()
	t.Cleanup(m.Stop)
	for i := 0; i < 5; i++ {
		addConnectedConn(t, m, int64(i+1), fmt.Sprintf("s%d", i))
	}
	var calls atomic.Int32
	m.keepalivePingFunc = func(*WsConnection) error { calls.Add(1); return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if pinged, failed := m.pingIdleConnections(ctx); pinged != 0 || failed != 0 || calls.Load() != 0 {
		t.Fatalf("canceled round pinged=%d failed=%d calls=%d, want 0", pinged, failed, calls.Load())
	}
	if pinged, _ := m.PingIdleConnections(); pinged != 5 {
		t.Fatalf("PingIdleConnections pinged=%d, want 5", pinged)
	}
}
