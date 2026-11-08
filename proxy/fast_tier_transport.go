package proxy

import "strings"

// fastTierRequiresHTTP 报告请求是否声明了加速档（priority / fast / ultrafast）。
//
// 上游 WebSocket 的 x-codex-routing-hint 只随握手发送、逐连接冻结；连接池按会话复用，
// 不区分档位。加速档请求若复用了按默认档建立的连接，握手 hint 与请求体 service_tier
// 不一致，上游可能不按加速档处理；反之默认档请求也可能复用加速档连接。加速档一律改走
// HTTP，每个请求都携带与自身一致的 routing hint，不必按档位拆分 WS 连接池。
// 只作用于 Codex 上游；中转账号自带的上游 WebSocket 由调用方保持原行为。
func fastTierRequiresHTTP(serviceTier string) bool {
	switch strings.ToLower(strings.TrimSpace(serviceTier)) {
	case "priority", "fast", "ultrafast":
		return true
	default:
		return false
	}
}
