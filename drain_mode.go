package main

import (
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/codex2api/proxy"
	"github.com/codex2api/proxy/wsrelay"
)

// ==================== 进程级排空模式（蓝绿发布旧槽） ====================
//
// 发布脚本在 HAProxy 把旧槽设为 DRAIN 之后发 SIGUSR1 进入排空模式，回滚时对重新
// 成为活动槽的一侧发 SIGUSR2 退出。排空模式只改变连接保留策略：
//   - 关闭空闲上游 WS 连接，之后请求结束时上游 WS 连接直接关闭不保留；
//   - 关闭 HTTP 上游连接池里的空闲连接；
//   - 下游 HTTP 服务关闭 keep-alive（空闲 keep-alive 连接关闭，在途响应带 Connection: close）。
//
// 在途请求、在途下游 WS 不被主动断开；调度、重试、计费和后台任务保持原样。
// Go 程序对未注册的 SIGUSR1/SIGUSR2 默认直接终止进程，所以 /health 通过
// drain_mode_supported 告知脚本本版本已注册处理函数。

// drainModeHooks 是排空模式要驱动的各个组件，测试时可替换。
type drainModeHooks struct {
	enterUpstreamWS   func() (wsrelay.DrainSweepResult, bool)
	exitUpstreamWS    func() bool
	closeIdleHTTP     func() int
	setHTTPKeepAlives func(enabled bool)
	logf              func(format string, args ...any)
}

func defaultDrainModeHooks() drainModeHooks {
	return drainModeHooks{
		enterUpstreamWS: wsrelay.EnterDrainMode,
		exitUpstreamWS:  wsrelay.ExitDrainMode,
		closeIdleHTTP:   proxy.CloseIdlePooledClientConnections,
		logf:            log.Printf,
	}
}

type drainController struct {
	mu       sync.Mutex
	hooks    drainModeHooks
	draining atomic.Bool
}

func newDrainController(hooks drainModeHooks) *drainController {
	if hooks.logf == nil {
		hooks.logf = log.Printf
	}
	return &drainController{hooks: hooks}
}

// SetHTTPKeepAlives 注册下游 HTTP 服务的 keep-alive 开关（http.Server 创建后调用）。
// 已在排空模式时立即关闭 keep-alive。
func (d *drainController) SetHTTPKeepAlives(fn func(enabled bool)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hooks.setHTTPKeepAlives = fn
	if fn != nil && d.draining.Load() {
		fn(false)
	}
}

// Draining 当前是否处于排空模式。
func (d *drainController) Draining() bool {
	return d != nil && d.draining.Load()
}

// Enter 进入排空模式；已在排空模式时不做任何事，返回 false。
func (d *drainController) Enter() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining.Load() {
		d.hooks.logf("[Drain] enter ignored: already draining")
		return false
	}
	d.draining.Store(true)
	if d.hooks.setHTTPKeepAlives != nil {
		d.hooks.setHTTPKeepAlives(false)
	}
	var ws wsrelay.DrainSweepResult
	if d.hooks.enterUpstreamWS != nil {
		ws, _ = d.hooks.enterUpstreamWS()
	}
	httpClients := 0
	if d.hooks.closeIdleHTTP != nil {
		httpClients = d.hooks.closeIdleHTTP()
	}
	d.hooks.logf("[Drain] entered closed_idle_upstream_ws=%d kept_busy_upstream_ws=%d kept_downstream_bound_upstream_ws=%d http_clients_idle_closed=%d",
		ws.Closed, ws.KeptBusy, ws.KeptDownstream, httpClients)
	return true
}

// Exit 退出排空模式；不在排空模式时不做任何事，返回 false。
func (d *drainController) Exit() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.draining.Load() {
		d.hooks.logf("[Drain] exit ignored: not draining")
		return false
	}
	if d.hooks.exitUpstreamWS != nil {
		d.hooks.exitUpstreamWS()
	}
	if d.hooks.setHTTPKeepAlives != nil {
		d.hooks.setHTTPKeepAlives(true)
	}
	d.draining.Store(false)
	d.hooks.logf("[Drain] exited")
	return true
}

// handleSignal 按信号进入/退出排空模式；不认识的信号忽略。
func (d *drainController) handleSignal(sig os.Signal) {
	switch {
	case isDrainEnterSignal(sig):
		d.Enter()
	case isDrainExitSignal(sig):
		d.Exit()
	}
}

// run 串行处理信号，直到 signals 关闭或 stop 关闭。
func (d *drainController) run(signals <-chan os.Signal, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case sig, ok := <-signals:
			if !ok {
				return
			}
			d.handleSignal(sig)
		}
	}
}
