package wsrelay

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/codex2api/proxy"
	"github.com/gorilla/websocket"
)

const (
	closeCapacityReclaim  = "capacity_reclaim"
	closeIdleChatLRU      = "idle_chat_lru"
	closeIdleTimeout      = "idle_timeout"
	closeLifetimeRotation = "lifetime_rotation"
	closeDisconnected     = "disconnected"
	closeManagerShutdown  = "manager_shutdown"
	closeProbeFailed      = "probe_failed"
	closePoolRemoved      = "pool_removed"
	closeUnusable         = "unusable_connection"
	closeUnconsumed       = "unconsumed_response"
	closeOneShotComplete  = "unbound_oneshot_complete"
)

type connectionCloseCause struct {
	source string
	reason string
	code   int
	bound  bool
}

func (wc *WsConnection) recordCloseCause(cause connectionCloseCause) {
	wc.closeCause.CompareAndSwap(nil, &cause)
}

func (wc *WsConnection) recordDirectClose() {
	if wc.closeCause.Load() != nil {
		return
	}
	cause := connectionCloseCause{source: "local", reason: "explicit_close"}
	if slot := wc.capacitySlot.Load(); slot != nil {
		cause.bound = slot.manager.hasLiveResponseBinding(wc)
	}
	wc.recordCloseCause(cause)
}

func (m *Manager) discardConnectionFor(wc *WsConnection, reason string) {
	if wc == nil {
		return
	}
	wc.recordCloseCause(connectionCloseCause{source: "local", reason: reason, bound: m.hasLiveResponseBinding(wc)})
	m.DiscardConnection(wc)
}

const (
	// boundedCloseForceAfter 是单条连接关闭的期限。gorilla 的 Close 不发 WS 关闭帧，
	// 但 wss 底层 tls.Conn.Close 会先写 TLS close_notify（自带 5 秒写超时）；对端半死、
	// 发送缓冲写满时这一步会阻塞。超过期限就直接关闭 TLS 之下的传输连接打断写阻塞。
	boundedCloseForceAfter = time.Second
	// shutdownCloseWait 是进程停止时并发关闭全部连接的整体等待上限。
	shutdownCloseWait = 2 * time.Second
)

// forceCloseTransport 关闭 TLS 之下的传输连接（无 TLS 时即底层连接本身），
// 让阻塞在 close_notify 或其他写上的调用立即返回。可与 Close 并发调用。
func (wc *WsConnection) forceCloseTransport() {
	if wc == nil || wc.conn == nil {
		return
	}
	transport := wc.conn.NetConn()
	if inner, ok := transport.(interface{ NetConn() net.Conn }); ok && inner.NetConn() != nil {
		transport = inner.NetConn()
	}
	if transport != nil {
		_ = transport.Close()
	}
}

// discardConnectionBounded 与 discardConnectionFor 相同，但关闭超过 forceAfter
// 仍未返回时强制关闭传输连接，保证单条关闭耗时有界。
func (m *Manager) discardConnectionBounded(wc *WsConnection, reason string, forceAfter time.Duration) {
	if wc == nil {
		return
	}
	timer := time.AfterFunc(forceAfter, wc.forceCloseTransport)
	defer timer.Stop()
	m.discardConnectionFor(wc, reason)
}

// closeAllConnectionsWithin 并发关闭容量槽位和连接池中的全部连接，
// 单条关闭超过 forceAfter 强制关闭传输连接，整体最多等待 wait。
// 返回 false 表示等待超时（剩余关闭仍在后台进行）。
func (m *Manager) closeAllConnectionsWithin(reason string, forceAfter, wait time.Duration) bool {
	seen := make(map[*WsConnection]struct{})
	var connections []*WsConnection
	add := func(wc *WsConnection) {
		if wc == nil {
			return
		}
		if _, ok := seen[wc]; ok {
			return
		}
		seen[wc] = struct{}{}
		connections = append(connections, wc)
	}
	m.capacityMu.Lock()
	for _, slots := range m.connectionSlots {
		for _, wc := range slots {
			add(wc)
		}
	}
	m.capacityMu.Unlock()
	m.connections.Range(func(_, value any) bool {
		if wc, ok := value.(*WsConnection); ok {
			add(wc)
		}
		return true
	})
	if len(connections) == 0 {
		return true
	}

	var wg sync.WaitGroup
	wg.Add(len(connections))
	for _, wc := range connections {
		go func(wc *WsConnection) {
			defer wg.Done()
			m.discardConnectionBounded(wc, reason, forceAfter)
		}(wc)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		log.Printf("[WS] 停止时关闭 %d 条上游连接超过 %s，不再等待", len(connections), wait)
		return false
	}
}

func connectionExpiryReason(wc *WsConnection) string {
	if !wc.IsConnected() {
		return closeDisconnected
	}
	if wc.IsExpired() {
		return closeIdleTimeout
	}
	return closeLifetimeRotation
}

func (wc *WsConnection) recordReadClose(err error) {
	cause := connectionCloseCause{source: "network", reason: "read_failure"}
	var peer *websocket.CloseError
	if errors.As(err, &peer) {
		cause.code = peer.Code
		if peer.Code != websocket.CloseAbnormalClosure && peer.Code != websocket.CloseNoStatusReceived && peer.Code != websocket.CloseTLSHandshake {
			cause.source, cause.reason = "upstream", "peer_close"
		}
	}
	if slot := wc.capacitySlot.Load(); slot != nil {
		cause.bound = slot.manager.hasLiveResponseBinding(wc)
	}
	wc.recordCloseCause(cause)
}

func (wc *WsConnection) logClose() {
	cause := wc.closeCause.Load()
	if cause == nil {
		cause = &connectionCloseCause{source: "local", reason: "explicit_close"}
	}
	accountID, pending, remaining := int64(0), 0, 0
	if wc.session != nil {
		accountID, pending = wc.session.AccountID, wc.session.PendingCount()
	}
	if slot := wc.capacitySlot.Load(); slot != nil {
		remaining = slot.manager.accountConnectionCount(accountID)
		slot.manager.connStats.recordClose(accountID, cause.reason)
	}
	age, idle := time.Duration(0), time.Duration(0)
	if wc.createdAt > 0 {
		age = time.Since(time.Unix(0, wc.createdAt))
	}
	if used := wc.lastUsed.Load(); used > 0 {
		idle = time.Since(time.Unix(0, used))
	}
	log.Printf("[WS] 连接关闭 account=%d connection=%p source=%s reason=%s code=%d bound=%t pending=%d age_ms=%d idle_ms=%d connections=%d chat_idle_limit=%d blank_limit=%d",
		accountID, wc, cause.source, cause.reason, cause.code, cause.bound, pending, age.Milliseconds(), idle.Milliseconds(), remaining, connectionLimit(capacityChat), connectionLimit(capacityBlank))
}

func (m *Manager) connectionCapacityError(accountID int64, kind connectionCapacityKind) error {
	select {
	case <-m.stopCleanup:
		return context.Canceled
	default:
	}
	limit := connectionLimit(kind)
	log.Printf("[WS] 新建连接容量拒绝 account=%d budget=%s connections=%d limit=%d", accountID, kind.String(), m.budgetConnectionCount(connectionCapacityRequest{accountID: accountID, kind: kind}), limit)
	return &proxy.UpstreamWSConnectionCapacityError{Limit: limit}
}
