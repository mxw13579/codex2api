package auth

import "github.com/codex2api/database"

// SetCodexWSDownstreamKeepaliveSlots 设置每账号独立的空闲聊天连接保留上限。
func (s *Store) SetCodexWSDownstreamKeepaliveSlots(slots int) {
	if s != nil {
		s.codexWSDownstreamKeepaliveSlots.Store(int64(database.NormalizeCodexWSDownstreamKeepaliveSlots(slots)))
	}
}

// CodexWSDownstreamKeepaliveSlots 返回每账号独立的空闲聊天连接保留上限。
func (s *Store) CodexWSDownstreamKeepaliveSlots() int {
	if s == nil {
		return database.DefaultCodexWSDownstreamKeepaliveSlots
	}
	return database.NormalizeCodexWSDownstreamKeepaliveSlots(int(s.codexWSDownstreamKeepaliveSlots.Load()))
}

// SetCodexWSBusyOverflowSlots 设置每个 busy 会话在同账号上允许的 overflow 兄弟连接数。
func (s *Store) SetCodexWSBusyOverflowSlots(slots int) {
	if s != nil {
		s.codexWSBusyOverflowSlots.Store(int64(database.NormalizeCodexWSBusyOverflowSlots(slots)))
	}
}

// CodexWSBusyOverflowSlots 返回每个 busy 会话在同账号上允许的 overflow 兄弟连接数。
func (s *Store) CodexWSBusyOverflowSlots() int {
	if s == nil {
		return database.DefaultCodexWSBusyOverflowSlots
	}
	return database.NormalizeCodexWSBusyOverflowSlots(int(s.codexWSBusyOverflowSlots.Load()))
}
