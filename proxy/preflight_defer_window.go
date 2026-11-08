package proxy

import "time"

const maxPreflightPassthroughDeferWindow = 5 * time.Second

// preflightPassthroughDeferWindow 返回前置元数据透传开启时的短暂缓冲窗口。
//
// 透传开启时，codex.rate_limits 等前置元数据一到就写给下游，HTTP 200 随之提交，
// 之后紧跟的降载/限流就进不了首包前换号重试，确定性错误也只能塞进 200 流里。
// 窗口内这些帧按“透传关闭”的语义缓冲：窗口内出现可重试错误照常换号，出现确定性
// 错误按真实 HTTP 状态码返回，出现正文则随正文一起写出；窗口到期仍无后续帧时，
// 把已缓冲的前置帧写出，恢复透传的首响效果（之后语义与原透传一致）。
//
// CODEX_PREFLIGHT_PASSTHROUGH_DEFER_MS：毫秒，未设置或 0 表示不缓冲（原透传行为），
// 上限 5000。只在后台开启前置元数据透传时生效。
func preflightPassthroughDeferWindow() time.Duration {
	ms := envNonNegativeInt64("CODEX_PREFLIGHT_PASSTHROUGH_DEFER_MS", 0)
	if ms <= 0 {
		return 0
	}
	window := time.Duration(ms) * time.Millisecond
	if window > maxPreflightPassthroughDeferWindow {
		window = maxPreflightPassthroughDeferWindow
	}
	return window
}
