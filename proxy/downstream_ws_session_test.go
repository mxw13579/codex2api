package proxy

import (
	"context"
	"testing"
)

func TestDownstreamWebsocketSessionDoneSurvivesDerivedContexts(t *testing.T) {
	if DownstreamWebsocketSessionDone(context.Background()) != nil {
		t.Fatal("plain context must not be a downstream WS session")
	}
	session, cancel := context.WithCancel(context.Background())
	marked := WithDownstreamWebsocketSession(session)
	// 上游请求上下文与会话取消解耦（newDrainableUpstreamContext），但仍要看到会话结束信号。
	derived, cancelDerived := context.WithCancel(context.WithoutCancel(marked))
	defer cancelDerived()
	done := DownstreamWebsocketSessionDone(derived)
	if done == nil {
		t.Fatal("derived context lost the downstream WS session marker")
	}
	select {
	case <-done:
		t.Fatal("session must still be live")
	default:
	}
	cancel()
	<-done
	if derived.Err() != nil {
		t.Fatal("fixture: derived context must not be cancelled by the session")
	}
}
