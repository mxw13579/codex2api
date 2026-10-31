package wsrelay

import (
	"fmt"
	"testing"
	"time"

	"github.com/codex2api/proxy"
)

func TestChatRetentionAbove32PreservesActiveAndBlankConnections(t *testing.T) {
	for _, limit := range []int{32, 48, 64, 96, 128} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			setBusyRuntimeSettings(t, func(s *proxy.RuntimeSettings) {
				s.CodexWSDownstreamKeepaliveSlots = limit
				s.CodexWSStatelessSlots = 32
			})
			if connectionLimit(capacityChat) != limit || connectionLimit(capacityBlank) != 32 {
				t.Fatal("chat and blank limits must be independent")
			}
			manager := NewManager()
			defer manager.Stop()
			if manager.ConnectionCount() != 0 {
				t.Fatal("changing retention must not eagerly create connections")
			}
			const accountID int64 = 42
			const wsURL = "wss://example.test/responses"
			now := time.Now()
			newChat := func(account int64, sessionID string, age time.Duration) *WsConnection {
				key := manager.poolKey(account, wsURL, sessionID, "")
				return newPooledConn(t, manager, account, sessionID, key, now.Add(age))
			}
			idle := make([]*WsConnection, limit+11)
			for i := range idle {
				id := fmt.Sprintf("chat-%d", i)
				idle[i] = newChat(accountID, id, -time.Minute+time.Duration(i)*time.Millisecond)
				manager.BindResponseConn(id, idle[i], id, accountID, "key-1")
			}
			active := newChat(accountID, "active", -2*IdleTimeout)
			active.session.AddPendingRequest("active")
			other := newChat(accountID+1, "other-account", -time.Minute)

			// Register a true blank connection before Touch could mark it as chat.
			session := NewSession(accountID, manager)
			session.SetConnected(true)
			blank := NewWsConnection(nil, session, wsURL)
			blank.PoolKey = manager.poolKey(accountID, wsURL, "blank", "")
			manager.connections.Store(blank.PoolKey, blank)
			manager.sessions.Store(blank.PoolKey, session)
			manager.adoptConnectionCapacity(blank)
			if connectionBudgetKind(blank) != capacityBlank {
				t.Fatal("fixture must represent a blank connection")
			}

			manager.trimAccountIdleChatConnections(accountID)
			if got := manager.ConnectionCount(); got != limit+3 {
				t.Fatalf("connections=%d, want %d idle + active + blank + other account", got, limit)
			}
			for i, wc := range idle {
				wantConnected := i >= 11
				if wc.IsConnected() != wantConnected {
					t.Fatalf("LRU order incorrect at index %d", i)
				}
				id := fmt.Sprintf("chat-%d", i)
				bound, _ := manager.lookupResponseConn(id, accountID, "key-1")
				if (bound == wc) != wantConnected {
					t.Fatalf("binding lifetime incorrect at index %d", i)
				}
				if bound, _ := manager.lookupResponseConn(id, accountID, "key-2"); bound != nil {
					t.Fatal("cross-key continuation binding")
				}
				if bound, _ := manager.lookupResponseConn(id, accountID+1, "key-1"); bound != nil {
					t.Fatal("cross-account continuation binding")
				}
			}
			for _, lower := range []int{32, 0} {
				proxy.UpdateRuntimeSettings(func(s proxy.RuntimeSettings) proxy.RuntimeSettings {
					s.CodexWSDownstreamKeepaliveSlots = lower
					return s
				})
				manager.trimAccountIdleChatConnections(accountID)
				if got := manager.ConnectionCount(); got != lower+3 {
					t.Fatalf("hot decrease to %d: connections=%d, want %d", lower, got, lower+3)
				}
				if !active.IsConnected() || !blank.IsConnected() || !other.IsConnected() {
					t.Fatal("retention shrink closed an active, blank or other-account connection")
				}
			}
			for _, wc := range idle {
				if wc.IsConnected() {
					t.Fatal("zero retention kept an idle chat connection")
				}
			}
		})
	}
}
