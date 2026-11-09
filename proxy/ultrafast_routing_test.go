package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

func ultraContext(tier string) context.Context {
	return context.WithValue(context.Background(), ultrafastRequestKey{}, isUltrafastTier(tier))
}

func TestUltrafastPlanWhitelist(t *testing.T) {
	for _, plan := range []string{"promax", "ProMax", " pro_max ", "pro-max", "pro", "prolite", "plus", "team", "business", "enterprise", "free", "", "unknown", "promax-team"} {
		t.Run(plan, func(t *testing.T) {
			a := &auth.Account{PlanType: plan}
			want := plan == "promax" || plan == "ProMax" || plan == " pro_max " || plan == "pro-max"
			if got := ultrafastAccountFilter(ultraContext("ultrafast"), nil)(a); got != want {
				t.Fatalf("plan %q allowed=%v want=%v", plan, got, want)
			}
			for _, tier := range []string{"", "default", "fast", "priority"} {
				if f := ultrafastAccountFilter(ultraContext(tier), nil); f != nil {
					t.Fatalf("non-ultrafast tier %q changed scheduling", tier)
				}
			}
			if ultrafastAccountFilter(ultraContext("ultrafast"), func(*auth.Account) bool { return false })(a) {
				t.Fatal("tier filter bypassed existing eligibility")
			}
		})
	}
	if accountSupportsUltrafast(nil) || accountSupportsUltrafast(&auth.Account{UpstreamType: "openai_responses", PlanType: "promax", BaseURL: "http://relay.invalid", APIKey: "test"}) {
		t.Fatal("nil or relay account accepted")
	}
}

type ultraFixture struct {
	handler   *Handler
	store     *auth.Store
	mu        sync.Mutex
	calls     []string
	bodies    [][]byte
	failFirst bool
}

func newUltraFixture(t *testing.T, plans ...string) *ultraFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)
	f := &ultraFixture{}
	oldRuntime, oldResin, oldWait := CurrentRuntimeSettings(), resinCfg.Load(), dispatchAccountWaitTimeout
	runtime := oldRuntime
	runtime.CodexForceWebsocket = false
	runtime.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	ApplyRuntimeSettings(runtime)
	dispatchAccountWaitTimeout = 5 * time.Millisecond
	t.Cleanup(func() {
		ApplyRuntimeSettings(oldRuntime)
		resinCfg.Store(oldResin)
		dispatchAccountWaitTimeout = oldWait
	})
	withPayloadRules(t, `{}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readUpstreamRequestBody(r)
		f.mu.Lock()
		f.calls = append(f.calls, r.Header.Get("Authorization"))
		f.bodies = append(f.bodies, body)
		fail := f.failFirst && len(f.calls) == 1
		f.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"message":"fixture failure"}}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/compact") {
			_, _ = io.WriteString(w, `{"id":"compact_fixture","object":"response.compaction","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, fastTierCompletedSSE)
	}))
	t.Cleanup(server.Close)
	SetResinConfig(&ResinConfig{BaseURL: server.URL, PlatformName: "test"})
	f.store = auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 8, MaxRetries: 2, MaxRateLimitRetries: 1, RetryIntervalMS: 1})
	t.Cleanup(f.store.Stop)
	for i, plan := range plans {
		f.store.AddAccount(&auth.Account{DBID: int64(i + 1), AccessToken: fmt.Sprintf("%s-%d", plan, i+1), PlanType: plan, Status: auth.StatusReady})
	}
	f.handler = NewHandler(f.store, nil, &config.Config{AllowAnonymousV1: true}, nil)
	return f
}

func (f *ultraFixture) request(t *testing.T, path, tier string, key int64) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"model":"gpt-5.6-sol","stream":true,"input":"hi","messages":[{"role":"user","content":"hi"}],"max_tokens":100`
	if tier != "" {
		body += `,"service_tier":"` + tier + `"`
	}
	body += `}`
	baseline := make(map[int64][2]int64)
	for _, a := range f.store.Accounts() {
		baseline[a.ID()] = [2]int64{a.GetActiveRequests(), a.OccupiedRequests.Load()}
	}
	r := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(r)
	c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	c.Set(contextAPIKeyID, key)
	switch path {
	case "/v1/responses":
		f.handler.Responses(c)
	case "/v1/chat/completions":
		f.handler.ChatCompletions(c)
	case "/v1/messages":
		f.handler.Messages(c)
	case "/v1/responses/compact":
		f.handler.ResponsesCompact(c)
	default:
		t.Fatal(path)
	}
	for _, a := range f.store.Accounts() {
		if a.GetActiveRequests() != baseline[a.ID()][0] || a.OccupiedRequests.Load() != baseline[a.ID()][1] {
			t.Fatalf("account %d leaked lease: active=%d occupied=%d", a.ID(), a.GetActiveRequests(), a.OccupiedRequests.Load())
		}
	}
	return r
}

func TestUltrafastHTTPRoutesAndKeyGroups(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages", "/v1/responses/compact"} {
		for _, scenario := range []string{"mixed", "no_promax", "other_group", "allowed_group", "plan_denied"} {
			t.Run(path+"/"+scenario, func(t *testing.T) {
				plans := []string{"team", "pro", "plus", "prolite", "promax"}
				if scenario == "no_promax" {
					plans = plans[:4]
				}
				f := newUltraFixture(t, plans...)
				for _, a := range f.store.Accounts() {
					f.store.ApplyAccountGroups(a.ID(), []int64{a.ID()})
				}
				switch scenario {
				case "other_group":
					f.store.SetAPIKeyAllowedGroups(7, []int64{1, 2, 3, 4})
				case "allowed_group":
					f.store.SetAPIKeyAllowedGroups(7, []int64{1, 5})
				case "plan_denied":
					f.store.SetAPIKeyAllowedPlans(7, []string{"team", "pro"})
				}
				start := time.Now()
				r := f.request(t, path, "ultrafast", 7)
				wantSuccess := scenario == "mixed" || scenario == "allowed_group"
				if wantSuccess {
					if r.Code != 200 || len(f.calls) != 1 || f.calls[0] != "Bearer promax-5" {
						t.Fatalf("status=%d calls=%v body=%s", r.Code, f.calls, r.Body.String())
					}
					if gjson.GetBytes(f.bodies[0], "service_tier").String() != "ultrafast" {
						t.Fatalf("tier lost: %s", f.bodies[0])
					}
				} else {
					if r.Code != 503 || len(f.calls) != 0 {
						t.Fatalf("status=%d calls=%v body=%s", r.Code, f.calls, r.Body.String())
					}
					if time.Since(start) > time.Second {
						t.Fatal("absent plan entered scheduler wait")
					}
					if !strings.Contains(r.Body.String(), "ultrafast") {
						t.Fatal(r.Body.String())
					}
				}
			})
		}
	}
}

func TestUltrafastDoesNotReserveProMax(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		for _, tier := range []string{"", "fast", "priority", "ultrafast"} {
			t.Run(path+"/"+tier, func(t *testing.T) {
				f := newUltraFixture(t, "promax")
				r := f.request(t, path, tier, 0)
				if r.Code != 200 || len(f.calls) != 1 {
					t.Fatalf("status=%d calls=%v body=%s", r.Code, f.calls, r.Body.String())
				}
			})
		}
	}
}

func TestUltrafastRetriesStayOnProMax(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			f := newUltraFixture(t, "team", "pro", "promax", "promax")
			f.failFirst = true
			r := f.request(t, path, "ultrafast", 0)
			if r.Code != 200 || len(f.calls) != 2 {
				t.Fatalf("status=%d calls=%v body=%s", r.Code, f.calls, r.Body.String())
			}
			for _, call := range f.calls {
				if !strings.HasPrefix(call, "Bearer promax-") {
					t.Fatal(f.calls)
				}
			}
		})
	}
}

func TestUltrafastPayloadConflictsDoNotSendOrLeak(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		for _, mode := range []string{"downgrade", "upgrade_wrong_plan", "upgrade_promax"} {
			t.Run(path+"/"+mode, func(t *testing.T) {
				plan, inputTier, finalTier := "promax", "ultrafast", "priority"
				if mode != "downgrade" {
					inputTier, finalTier = "", "ultrafast"
				}
				if mode == "upgrade_wrong_plan" {
					plan = "team"
				}
				f := newUltraFixture(t, plan)
				withPayloadRules(t, `{"override":[{"params":{"service_tier":"`+finalTier+`"}}]}`)
				r := f.request(t, path, inputTier, 0)
				wantStatus, wantCalls := 400, 0
				if mode == "upgrade_promax" {
					wantStatus, wantCalls = 200, 1
				}
				if r.Code != wantStatus || len(f.calls) != wantCalls {
					t.Fatalf("status=%d calls=%v body=%s", r.Code, f.calls, r.Body.String())
				}
			})
		}
	}
}

func TestUltrafastPinnedAccountCannotBypass(t *testing.T) {
	h, up, accounts := newPinnedCodexHandler(t, nil, func(w http.ResponseWriter, _ *http.Request) { writePinnedSuccessSSE(w) })
	r := httptest.NewRecorder()
	body := strings.TrimSuffix(pinnedChatBody, "}") + `,"service_tier":"ultrafast"}`
	h.ChatCompletions(pinnedTestContext(r, "/v1/chat/completions", body, 7, 2))
	if r.Code == 200 || up.calls.Load() != 0 {
		t.Fatalf("status=%d calls=%d body=%s", r.Code, up.calls.Load(), r.Body.String())
	}
	// Existing external-scheduler cooldown/concurrency exemptions still apply.
	h.store.UpdateAccountPlanType(accounts[1], "promax")
	r = httptest.NewRecorder()
	h.ChatCompletions(pinnedTestContext(r, "/v1/chat/completions", body, 7, 2))
	if r.Code != 200 || up.calls.Load() != 1 {
		t.Fatalf("status=%d calls=%d body=%s", r.Code, up.calls.Load(), r.Body.String())
	}
}

func TestUltrafastRetainedLeaseAndPlanChange(t *testing.T) {
	f := newUltraFixture(t, "promax")
	a := f.store.NextExcludingWithDispatch(0, nil, nil, auth.DispatchPolicyStandard)
	if a == nil {
		t.Fatal("no lease")
	}
	var retained websocketHTTPFallbackState
	retained.Retain(a, "", 0, "test")
	f.store.UpdateAccountPlanType(a, "team")
	a, _, ok := retained.Take()
	if !ok {
		t.Fatal("retained lease missing")
	}
	resp, err := ExecuteRequest(ultraContext("ultrafast"), a, []byte(`{"model":"gpt-5.6-sol","service_tier":"ultrafast","input":"hi"}`), "", "", "", nil, nil, false)
	f.store.Release(a) // the attempt, not the executor, owns this lease
	retained.ReleaseRetained(f.store.Release)
	if resp != nil || serviceTierError(err) == nil || len(f.calls) != 0 || a.OccupiedRequests.Load() != 0 {
		t.Fatalf("resp=%v err=%v calls=%v occupied=%d", resp, err, f.calls, a.OccupiedRequests.Load())
	}
}

func TestUltrafastWSTurnIsLocalAndCanRecover(t *testing.T) {
	f := newUltraFixture(t, "team")
	router := gin.New()
	f.handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	turn := func(tier string) []byte {
		t.Helper()
		if err := conn.WriteJSON(map[string]any{"type": "response.create", "model": "gpt-5.6-sol", "input": "hi", "service_tier": tier}); err != nil {
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		return readResponsesWSTerminalEvent(t, conn)
	}
	if event := turn("ultrafast"); gjson.GetBytes(event, "error.code").String() != errorCodeTierPoolUnavailable {
		t.Fatalf("unexpected rejection: %s", event)
	}
	// A rejected turn must not poison the next fast turn on the same socket.
	if event := turn("fast"); gjson.GetBytes(event, "type").String() != "response.completed" {
		t.Fatalf("fast turn failed: %s", event)
	}
	f.store.UpdateAccountPlanType(f.store.FindByID(1), "promax")
	if event := turn("ultrafast"); gjson.GetBytes(event, "type").String() != "response.completed" {
		t.Fatalf("ultrafast turn failed: %s", event)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 2 {
		t.Fatal(f.calls)
	}
	if gjson.GetBytes(f.bodies[1], "service_tier").String() != "ultrafast" {
		t.Fatal(string(f.bodies[1]))
	}
}

func TestUltrafastIntentCanonicalization(t *testing.T) {
	for _, input := range []string{`{"serviceTier":" ULTRAFAST "}`, `{"service_tier":" ULTRAFAST "}`, `{"service_tier":"ultrafast","serviceTier":"ultrafast"}`} {
		out, ultra, err := normalizeUltrafastBody([]byte(input))
		if err != nil || !ultra || gjson.GetBytes(out, "service_tier").String() != "ultrafast" || gjson.GetBytes(out, "serviceTier").Exists() {
			t.Fatalf("out=%s ultra=%v err=%v", out, ultra, err)
		}
	}
	for _, input := range []string{`{"service_tier":"fast","serviceTier":"ultrafast"}`, `{"service_tier":"ultrafast","serviceTier":"default"}`} {
		if _, _, err := normalizeUltrafastBody([]byte(input)); err == nil {
			t.Fatal("conflicting tiers accepted")
		}
	}
}
