package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestUltrafastPayloadUpgradeRestoresWSContinuation(t *testing.T) {
	withPayloadRules(t, `{}`)
	fixture := newNativeWSFixture(t, func(_ context.Context, _ []byte) (*http.Response, error) {
		return nativeWSResponse("ultra_parent", `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"prior answer"}]}`), nil
	})
	fixture.account.Mu().Lock()
	fixture.account.PlanType = "promax"
	fixture.account.Mu().Unlock()
	oldResin := resinCfg.Load()
	t.Cleanup(func() { resinCfg.Store(oldResin) })
	bodies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodies <- readUpstreamRequestBody(r)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, wsContextTestSSE("ultra_child"))
	}))
	defer server.Close()
	SetResinConfig(&ResinConfig{BaseURL: server.URL, PlatformName: "test"})
	conn := fixture.connect(t)
	if event := nativeWSTurn(t, conn, `{"input":[{"role":"user","content":"original question"}]}`); gjson.GetBytes(event, "type").String() != "response.completed" {
		t.Fatalf("first turn: %s", event)
	}
	withPayloadRules(t, `{"override":[{"params":{"service_tier":"ultrafast"}}]}`)
	if event := nativeWSTurn(t, conn, `{"previous_response_id":"ultra_parent","input":[{"role":"user","content":"follow up"}]}`); gjson.GetBytes(event, "type").String() != "response.completed" {
		t.Fatalf("second turn: %s", event)
	}
	select {
	case body := <-bodies:
		if gjson.GetBytes(body, "previous_response_id").Exists() || gjson.GetBytes(body, "service_tier").String() != "ultrafast" {
			t.Fatalf("bad HTTP body: %s", body)
		}
		for _, text := range []string{"original question", "prior answer", "follow up"} {
			if !strings.Contains(gjson.GetBytes(body, "input").Raw, text) {
				t.Fatalf("lost %q: %s", text, body)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("missing HTTP upstream request")
	}
}

func TestUltrafastUnexpandedContinuationIsNeverDiscarded(t *testing.T) {
	f := newUltraFixture(t, "promax")
	withPayloadRules(t, `{"override":[{"params":{"service_tier":"ultrafast"}}]}`)
	resp, err := ExecuteRequest(context.Background(), f.store.FindByID(1), []byte(`{"model":"gpt-5.6-sol","input":"increment only","previous_response_id":"parent"}`), "", "", "", nil, nil, true)
	if resp != nil || serviceTierError(err) == nil || len(f.calls) != 0 {
		t.Fatalf("resp=%v err=%v calls=%v", resp, err, f.calls)
	}
}

func TestUltrafastRetryCannotUpgradeFailOpenContinuation(t *testing.T) {
	t.Setenv("CODEX_WS_CONTINUATION_FAIL_OPEN", "true")
	withPayloadRules(t, `{}`)
	var wsCalls, httpCalls atomic.Int32
	fixture := newNativeWSFixture(t, func(_ context.Context, _ []byte) (*http.Response, error) {
		wsCalls.Add(1)
		if err := SetPayloadRulesJSON(`{"override":[{"params":{"service_tier":"ultrafast"}}]}`); err != nil {
			t.Error(err)
		}
		return &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"previous_response_not_found","message":"Previous response not found"}}`))}, nil
	})
	fixture.account.Mu().Lock()
	fixture.account.PlanType = "promax"
	fixture.account.Mu().Unlock()
	oldResin := resinCfg.Load()
	t.Cleanup(func() { resinCfg.Store(oldResin) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, wsContextTestSSE("should_not_send"))
	}))
	defer server.Close()
	SetResinConfig(&ResinConfig{BaseURL: server.URL, PlatformName: "test"})
	conn := fixture.connect(t)
	event := nativeWSTurn(t, conn, `{"previous_response_id":"uncached_parent","input":[{"role":"user","content":"increment only"}]}`)
	if gjson.GetBytes(event, "type").String() != "error" || wsCalls.Load() != 1 || httpCalls.Load() != 0 {
		t.Fatalf("event=%s ws=%d http=%d", event, wsCalls.Load(), httpCalls.Load())
	}
}

func TestUltrafastLatePayloadUpgradeCannotUseLostContext(t *testing.T) {
	f := newUltraFixture(t, "promax")
	withPayloadRules(t, `{"override":[{"params":{"service_tier":"ultrafast"}}]}`)
	ctx := context.WithValue(context.Background(), ultrafastContextLostKey{}, true)
	// An earlier retry already removed the ID through the legacy fail-open
	// escape hatch, before a later account/rule upgraded the final tier.
	resp, err := ExecuteRequest(ctx, f.store.FindByID(1), []byte(`{"model":"gpt-5.6-sol","input":"increment only"}`), "", "", "", nil, nil, false)
	if resp != nil || serviceTierError(err) == nil || len(f.calls) != 0 {
		t.Fatalf("resp=%v err=%v calls=%v", resp, err, f.calls)
	}
}

func TestUltrafastUnavailableProMaxDoesNotBorrow(t *testing.T) {
	for _, state := range []string{"busy", "cooldown"} {
		for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
			t.Run(state+path, func(t *testing.T) {
				f := newUltraFixture(t, "promax", "team", "pro", "plus")
				a := f.store.FindByID(1)
				if state == "busy" {
					a.ActiveRequests.Store(8)
					a.OccupiedRequests.Store(8)
				} else {
					f.store.MarkCooldown(a, time.Hour, "rate_limited")
				}
				r := f.request(t, path, "ultrafast", 0)
				if r.Code < 400 || len(f.calls) != 0 {
					t.Fatalf("status=%d calls=%v body=%s", r.Code, f.calls, r.Body.String())
				}
			})
		}
	}
}
