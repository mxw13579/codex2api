package database

const (
	DefaultCodexWSDownstreamKeepaliveSlots = 8
	MaxCodexWSDownstreamKeepaliveSlots     = 512

	// DefaultCodexWSBusyOverflowSlots / MaxCodexWSBusyOverflowSlots 是每个 busy 会话
	// 在同账号上允许的 overflow 兄弟连接数的默认值与上限。
	DefaultCodexWSBusyOverflowSlots = 4
	MaxCodexWSBusyOverflowSlots     = 16
)

// NormalizeCodexWSDownstreamKeepaliveSlots 归一化空闲聊天连接保留上限，0 表示不保留。
func NormalizeCodexWSDownstreamKeepaliveSlots(slots int) int {
	if slots < 0 {
		return DefaultCodexWSDownstreamKeepaliveSlots
	}
	if slots > MaxCodexWSDownstreamKeepaliveSlots {
		return MaxCodexWSDownstreamKeepaliveSlots
	}
	return slots
}

// NormalizeCodexWSBusyOverflowSlots 把每个 busy 会话的 overflow 兄弟槽位数限制在 1-16，
// 0 或负值回落默认 4。
func NormalizeCodexWSBusyOverflowSlots(slots int) int {
	if slots <= 0 {
		return DefaultCodexWSBusyOverflowSlots
	}
	if slots > MaxCodexWSBusyOverflowSlots {
		return MaxCodexWSBusyOverflowSlots
	}
	return slots
}
