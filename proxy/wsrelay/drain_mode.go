package wsrelay

import (
	"time"
)

// ==================== 排空模式（蓝绿发布旧槽） ====================
//
// 旧槽被 HAProxy 设为 DRAIN 后不再接新连接，但仍握着大量空闲上游 WS 连接；
// 出口代理按账号限制并发连接数，旧槽的空闲连接会挤占新槽的握手名额。
// 排空模式下：
//   - 进入时关闭所有空闲上游连接（无在途请求），在途请求不动；
//   - 之后每个请求结束时连接不再放回池里保留，直接关闭；
//   - 例外：仍存活的下游 WS 会话最近一轮产出的响应绑定在某条空闲连接上时保留该连接，
//     否则该会话下一轮 previous_response_id 续链必然得到"上下文不可用"。
//     下游 WS 断开后，cleanupLoop 的周期巡检会把这些连接一并关闭。
//
// 加锁顺序与取连路径一致：pool-key 锁 → 账号锁 → respConnMu。

const closeDrainMode = "drain_mode"

// DrainSweepResult 一次排空巡检的结果。
type DrainSweepResult struct {
	// Closed 本次关闭的空闲连接数。
	Closed int
	// KeptBusy 有在途请求（或正在被取连占用）而保留的连接数。
	KeptBusy int
	// KeptDownstream 被仍存活的下游 WS 会话续链绑定而保留的空闲连接数。
	KeptDownstream int
}

// EnterDrainMode 进入排空模式并立即关闭空闲连接。已在排空模式时只返回 changed=false，
// 不做任何事（幂等）。
func (m *Manager) EnterDrainMode() (result DrainSweepResult, changed bool) {
	if m == nil {
		return DrainSweepResult{}, false
	}
	if !m.draining.CompareAndSwap(false, true) {
		return DrainSweepResult{}, false
	}
	return m.sweepIdleConnectionsForDrain(), true
}

// ExitDrainMode 退出排空模式，恢复正常的连接保留。已关闭的连接不重建。
func (m *Manager) ExitDrainMode() (changed bool) {
	if m == nil {
		return false
	}
	return m.draining.CompareAndSwap(true, false)
}

// Draining 是否处于排空模式。
func (m *Manager) Draining() bool {
	return m != nil && m.draining.Load()
}

// sweepIdleConnectionsForDrain 关闭所有空闲且未被存活下游 WS 会话绑定的连接。
func (m *Manager) sweepIdleConnectionsForDrain() DrainSweepResult {
	var result DrainSweepResult
	var candidates []*WsConnection
	m.connections.Range(func(_, value any) bool {
		if wc, ok := value.(*WsConnection); ok && wc != nil {
			candidates = append(candidates, wc)
		}
		return true
	})
	for _, wc := range candidates {
		switch m.drainIdleConnection(wc) {
		case drainOutcomeClosed:
			result.Closed++
		case drainOutcomeBusy:
			result.KeptBusy++
		case drainOutcomeDownstream:
			result.KeptDownstream++
		}
	}
	return result
}

type drainOutcome int

const (
	drainOutcomeGone drainOutcome = iota
	drainOutcomeClosed
	drainOutcomeBusy
	drainOutcomeDownstream
)

func (m *Manager) drainIdleConnection(wc *WsConnection) drainOutcome {
	if wc.session == nil || wc.PoolKey == "" {
		return drainOutcomeGone
	}
	// 新建连接从入池到占位 pending 之间只持有 pool-key 锁、不持有账号锁；
	// 持有 pool-key 锁才能排除这个窗口。拿不到说明正有请求在取这条连接（探活/拨号），
	// 不阻塞等待，记为在用，留给请求结束或下一轮周期巡检处理。
	lock, releaseKeyLock := m.keyLock(wc.PoolKey)
	defer releaseKeyLock()
	if !lock.TryLock() {
		return drainOutcomeBusy
	}
	defer lock.Unlock()
	accountLock, releaseAccountLock := m.accountLock(wc.session.AccountID)
	defer releaseAccountLock()
	accountLock.Lock()
	defer accountLock.Unlock()
	return m.drainIdleConnectionLocked(wc)
}

// drainIdleConnectionLocked 要求调用方持有账号锁（取连占位都在账号锁下进行）。
func (m *Manager) drainIdleConnectionLocked(wc *WsConnection) drainOutcome {
	current, exists := m.connections.Load(wc.PoolKey)
	if !exists || current != wc {
		return drainOutcomeGone
	}
	if wc.session.PendingCount() != 0 {
		return drainOutcomeBusy
	}
	m.respConnMu.Lock()
	if m.heldByLiveDownstreamSessionLocked(wc, time.Now()) {
		m.respConnMu.Unlock()
		return drainOutcomeDownstream
	}
	// 与绑定检查原子完成：禁止晚到的绑定把连接重新留住。
	wc.responseBindingsDisabled = true
	m.respConnMu.Unlock()
	m.discardConnectionFor(wc, closeDrainMode)
	return drainOutcomeClosed
}

// heldByLiveDownstreamSessionLocked 要求持有 respConnMu：连接上仍有未过期的续链绑定，
// 且产出该响应的下游 WS 会话尚未断开。
func (m *Manager) heldByLiveDownstreamSessionLocked(wc *WsConnection, now time.Time) bool {
	for _, binding := range m.respConnBindings {
		if binding.conn != wc || binding.downstreamDone == nil || !now.Before(binding.expiresAt) {
			continue
		}
		select {
		case <-binding.downstreamDone:
		default:
			return true
		}
	}
	return false
}

// releaseForDrainLocked 在排空模式下处理请求结束归还的连接，调用方持有账号锁。
// 返回 true 表示连接已关闭，调用方不再保留。
func (m *Manager) releaseForDrainLocked(wc *WsConnection) bool {
	if !m.Draining() {
		return false
	}
	// 释放与下一次占位之间可能已被同会话的新请求取走：留给那个请求结束时处理。
	if wc.session.PendingCount() != 0 {
		return false
	}
	return m.drainIdleConnectionLocked(wc) == drainOutcomeClosed
}

// sweepForDrainIfActive 由周期巡检调用：排空模式下关闭新空闲的连接，以及下游 WS
// 已断开后不再需要保留的连接。
func (m *Manager) sweepForDrainIfActive() {
	if !m.Draining() {
		return
	}
	m.sweepIdleConnectionsForDrain()
}

// EnterDrainMode 对全局管理器进入排空模式。
func EnterDrainMode() (DrainSweepResult, bool) {
	return GetManager().EnterDrainMode()
}

// ExitDrainMode 对全局管理器退出排空模式。
func ExitDrainMode() bool {
	return GetManager().ExitDrainMode()
}
