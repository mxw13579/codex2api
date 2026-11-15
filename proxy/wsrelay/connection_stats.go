package wsrelay

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// connectionStatsInterval 是每账号握手/关闭汇总日志的周期。变量形式只为测试缩短；
// NewManager 创建时读取一次，之后修改不影响已启动的管理器。
var connectionStatsInterval = time.Minute

// connectionDialKind 标记一次上游握手由哪条取连路径发起，由调用点显式填写。
type connectionDialKind int

const (
	dialKindUnknown connectionDialKind = iota
	dialKindSlot
	dialKindOneShot
	dialKindSession
	dialKindOverflow
	dialKindCount
)

var dialKindNames = [dialKindCount]string{"unknown", "slot", "oneshot", "session", "overflow"}

// connectionReuseKind 标记一次连接复用来自哪条取连路径。
type connectionReuseKind int

const (
	reuseKindSlot connectionReuseKind = iota
	reuseKindSlotBound
	reuseKindSession
	reuseKindContinuation
	reuseKindOverflow
	reuseKindCount
)

var reuseKindNames = [reuseKindCount]string{"slot", "slot_bound", "session", "continuation", "overflow"}

// accountConnectionStats 汇总一个账号在一个统计窗口内的上游 WS 握手、复用与关闭。
// 用于观察握手频率与 Cloudflare 403 拒绝的关系。四类握手结果互不重叠。
type accountConnectionStats struct {
	handshakeOK        int
	handshake403       int
	handshakeOtherFail int
	handshakeCanceled  int
	okMsTotal          int64
	okMsMax            int64
	forbiddenMsTotal   int64
	forbiddenMsMax     int64
	dials              [dialKindCount]int
	reuses             [reuseKindCount]int
	slotFallback       int
	slotFallbackBound  int
	slotFallbackBusy   int
	closes             map[string]int
}

func (s *accountConnectionStats) empty() bool {
	if s.handshakeOK != 0 || s.handshake403 != 0 || s.handshakeOtherFail != 0 || s.handshakeCanceled != 0 {
		return false
	}
	for _, n := range s.reuses {
		if n != 0 {
			return false
		}
	}
	return s.slotFallback == 0 && len(s.closes) == 0
}

// connectionStatsRecorder 的零值可直接使用。mu 是叶子锁：持锁期间只修改本结构，
// 不调用任何其他代码，因此可以在 respConnMu、账号锁等任意锁内调用记录方法。
type connectionStatsRecorder struct {
	mu              sync.Mutex
	accounts        map[int64]*accountConnectionStats
	windowStart     time.Time
	bindingRejected int
}

// connectionStatsWindow 是一次 swap 取走的窗口。
type connectionStatsWindow struct {
	accounts        map[int64]*accountConnectionStats
	bindingRejected int
	elapsed         time.Duration
}

func (r *connectionStatsRecorder) accountLocked(accountID int64) *accountConnectionStats {
	if r.accounts == nil {
		r.accounts = make(map[int64]*accountConnectionStats)
	}
	stats := r.accounts[accountID]
	if stats == nil {
		stats = &accountConnectionStats{}
		r.accounts[accountID] = stats
	}
	return stats
}

func (r *connectionStatsRecorder) startWindow(now time.Time) {
	r.mu.Lock()
	r.windowStart = now
	r.mu.Unlock()
}

// classifyHandshake 把一次拨号结果归入四类之一：
//   - ok：成功；
//   - 403：上游返回 403；
//   - canceled：上游没有返回响应，且调用方 ctx 为 context.Canceled；
//   - other：其余全部（上游其他状态码、握手超时、传输错误、调用方 ctx 到期）。
func classifyHandshake(ctx context.Context, resp *http.Response, err error) string {
	switch {
	case err == nil:
		return "ok"
	case resp != nil && resp.StatusCode == http.StatusForbidden:
		return "403"
	case resp == nil && ctx != nil && errors.Is(ctx.Err(), context.Canceled):
		return "canceled"
	default:
		return "other"
	}
}

// recordHandshake 记录一次上游 WS 拨号（含代理建连、TLS 与 Upgrade）的结果、耗时与发起路径。
func (r *connectionStatsRecorder) recordHandshake(ctx context.Context, accountID int64, kind connectionDialKind, elapsed time.Duration, resp *http.Response, err error) {
	if r == nil {
		return
	}
	outcome := classifyHandshake(ctx, resp, err)
	ms := elapsed.Milliseconds()
	if kind < 0 || kind >= dialKindCount {
		kind = dialKindUnknown
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.accountLocked(accountID)
	stats.dials[kind]++
	switch outcome {
	case "ok":
		stats.handshakeOK++
		stats.okMsTotal += ms
		if ms > stats.okMsMax {
			stats.okMsMax = ms
		}
	case "403":
		stats.handshake403++
		stats.forbiddenMsTotal += ms
		if ms > stats.forbiddenMsMax {
			stats.forbiddenMsMax = ms
		}
	case "canceled":
		stats.handshakeCanceled++
	default:
		stats.handshakeOtherFail++
	}
}

func (r *connectionStatsRecorder) recordReuse(accountID int64, kind connectionReuseKind) {
	if r == nil || kind < 0 || kind >= reuseKindCount {
		return
	}
	r.mu.Lock()
	r.accountLocked(accountID).reuses[kind]++
	r.mu.Unlock()
}

// recordSlotFallback 记录一次无会话请求落到一次性连接兜底，以及当时槽位的状态。
func (r *connectionStatsRecorder) recordSlotFallback(accountID int64, bound, busy int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	stats := r.accountLocked(accountID)
	stats.slotFallback++
	stats.slotFallbackBound += bound
	stats.slotFallbackBusy += busy
	r.mu.Unlock()
}

func (r *connectionStatsRecorder) recordBindingRejected() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.bindingRejected++
	r.mu.Unlock()
}

func (r *connectionStatsRecorder) recordClose(accountID int64, reason string) {
	if r == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "unknown"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.accountLocked(accountID)
	if stats.closes == nil {
		stats.closes = make(map[string]int)
	}
	stats.closes[reason]++
}

// swap 取走当前窗口并从 now 开始新窗口。elapsed 为本窗口实际经过的时间；
// 未调用过 startWindow 的零值记录器返回 0。
func (r *connectionStatsRecorder) swap(now time.Time) connectionStatsWindow {
	r.mu.Lock()
	defer r.mu.Unlock()
	window := connectionStatsWindow{accounts: r.accounts, bindingRejected: r.bindingRejected}
	if !r.windowStart.IsZero() && now.After(r.windowStart) {
		window.elapsed = now.Sub(r.windowStart)
	}
	r.accounts = nil
	r.bindingRejected = 0
	r.windowStart = now
	return window
}

func formatConnectionCloses(closes map[string]int) string {
	if len(closes) == 0 {
		return "-"
	}
	reasons := make([]string, 0, len(closes))
	for reason := range closes {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, fmt.Sprintf("%s:%d", reason, closes[reason]))
	}
	return strings.Join(parts, ",")
}

func formatDialKinds(dials [dialKindCount]int) string {
	parts := make([]string, 0, dialKindCount)
	for kind := dialKindSlot; kind < dialKindCount; kind++ {
		parts = append(parts, fmt.Sprintf("%s:%d", dialKindNames[kind], dials[kind]))
	}
	if dials[dialKindUnknown] > 0 {
		parts = append(parts, fmt.Sprintf("%s:%d", dialKindNames[dialKindUnknown], dials[dialKindUnknown]))
	}
	return strings.Join(parts, ",")
}

func formatReuseKinds(reuses [reuseKindCount]int) string {
	parts := make([]string, 0, reuseKindCount)
	for kind := reuseKindSlot; kind < reuseKindCount; kind++ {
		parts = append(parts, fmt.Sprintf("%s:%d", reuseKindNames[kind], reuses[kind]))
	}
	return strings.Join(parts, ",")
}

func averageMs(total int64, count int) int64 {
	if count <= 0 {
		return 0
	}
	return total / int64(count)
}

func windowSeconds(elapsed time.Duration) int {
	return int(elapsed.Round(time.Second) / time.Second)
}

// connectionStatsLines 生成本窗口每个有活动账号的一行汇总（附带当前连接快照）和一行全局汇总。
// 只读取快照，不改变任何连接或保留状态。
func (m *Manager) connectionStatsLines(now time.Time) []string {
	window := m.connStats.swap(now)
	seconds := windowSeconds(window.elapsed)
	accounts := make([]int64, 0, len(window.accounts))
	for accountID, s := range window.accounts {
		if !s.empty() {
			accounts = append(accounts, accountID)
		}
	}
	if len(accounts) == 0 && window.bindingRejected == 0 {
		return nil
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i] < accounts[j] })
	// busy 只统计仍在池内的连接；已摘池但仍在收尾的连接不计入。
	busy := make(map[int64]int)
	totalConnections := 0
	m.connections.Range(func(_, value any) bool {
		totalConnections++
		if wc, ok := value.(*WsConnection); ok && wc != nil && wc.session != nil && wc.session.PendingCount() > 0 {
			busy[wc.session.AccountID]++
		}
		return true
	})
	lines := make([]string, 0, len(accounts)+1)
	for _, accountID := range accounts {
		s := window.accounts[accountID]
		lines = append(lines, fmt.Sprintf(
			"[WS-Stats] account=%d window_s=%d handshake_ok=%d handshake_403=%d handshake_other_fail=%d handshake_canceled=%d handshake_ok_ms_avg=%d handshake_ok_ms_max=%d handshake_403_ms_avg=%d handshake_403_ms_max=%d dial_kind=%s reuse_kind=%s slot_fallback=%d slot_fallback_bound=%d slot_fallback_busy=%d closes=%s connections=%d busy=%d chat_idle_limit=%d",
			accountID, seconds, s.handshakeOK, s.handshake403, s.handshakeOtherFail, s.handshakeCanceled,
			averageMs(s.okMsTotal, s.handshakeOK), s.okMsMax, averageMs(s.forbiddenMsTotal, s.handshake403), s.forbiddenMsMax,
			formatDialKinds(s.dials), formatReuseKinds(s.reuses), s.slotFallback, s.slotFallbackBound, s.slotFallbackBusy,
			formatConnectionCloses(s.closes), m.accountConnectionCount(accountID), busy[accountID], connectionLimit(capacityChat)))
	}
	m.respConnMu.Lock()
	bindings := len(m.respConnBindings)
	m.respConnMu.Unlock()
	lines = append(lines, fmt.Sprintf("[WS-Stats] global window_s=%d bindings=%d binding_limit=%d binding_rejected=%d connections=%d",
		seconds, bindings, responseConnBindingMaxEntries, window.bindingRejected, totalConnections))
	return lines
}

// setConnectionStatsOutput 替换统计行输出函数（测试用）；nil 恢复为 log.Print。
func (m *Manager) setConnectionStatsOutput(fn func(string)) {
	m.mu.Lock()
	m.statsOutput = fn
	m.mu.Unlock()
}

func (m *Manager) logConnectionStats() {
	lines := m.connectionStatsLines(time.Now())
	if len(lines) == 0 {
		return
	}
	m.mu.RLock()
	output := m.statsOutput
	m.mu.RUnlock()
	if output == nil {
		output = func(line string) { log.Print(line) }
	}
	for _, line := range lines {
		output(line)
	}
}
