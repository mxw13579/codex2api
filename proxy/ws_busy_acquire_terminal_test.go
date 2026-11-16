package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// newWsBusyAcquireTerminalScenario 让每次 WS 执行都返回 err；号池只有一个账号，
// 第一次失败后该账号被排除，下一次选号拿不到账号，走「无可用账号」终态分支。
func newWsBusyAcquireTerminalScenario(t *testing.T, err error) (*Handler, *atomic.Int32) {
	t.Helper()
	handler, _, calls := newWsBusyAcquireTerminalPool(t, 1, func(int32) error { return err })
	return handler, calls
}

// newWsBusyAcquireTerminalPool 建一个含 accounts 个账号的号池，第 n 次（从 1 起）WS 执行
// 返回 errFor(n)。
func newWsBusyAcquireTerminalPool(t *testing.T, accounts int, errFor func(call int32) error, mutate ...func(*database.SystemSettings)) (*Handler, *auth.Store, *atomic.Int32) {
	t.Helper()
	previousSettings := CurrentRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previousSettings) })
	settings := previousSettings
	settings.CodexForceWebsocket = true
	settings.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	ApplyRuntimeSettings(settings)

	previousExecute := WebsocketExecuteFunc
	t.Cleanup(func() { WebsocketExecuteFunc = previousExecute })
	var calls atomic.Int32
	WebsocketExecuteFunc = func(context.Context, *auth.Account, []byte, string, string, string, *DeviceProfileConfig, http.Header, string) (*http.Response, error) {
		return nil, errFor(calls.Add(1))
	}

	systemSettings := &database.SystemSettings{
		MaxConcurrency: 2, TestConcurrency: 1, TestModel: "gpt-5.5", MaxRetries: 3, MaxRateLimitRetries: 0,
	}
	for _, fn := range mutate {
		fn(systemSettings)
	}
	store := auth.NewStore(nil, nil, systemSettings)
	t.Cleanup(store.Stop)
	store.SetRetryIntervalMS(0)
	for i := 1; i <= accounts; i++ {
		store.AddAccount(&auth.Account{DBID: int64(i), AccessToken: fmt.Sprintf("test-token-%d", i), PlanType: "pro", AccountID: fmt.Sprintf("test-account-%d", i)})
	}
	return NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil), store, &calls
}

func wsBusyAcquireTerminalCases() []continuousRetryLocalEndpointCase {
	return []continuousRetryLocalEndpointCase{
		{
			name: "responses", path: "/v1/responses",
			body:   `{"model":"gpt-5.5","input":"hello","stream":false}`,
			invoke: func(h *Handler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "chat completions", path: "/v1/chat/completions",
			body:   `{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],"stream":false}`,
			invoke: func(h *Handler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "anthropic messages", path: "/v1/messages",
			body:   `{"model":"claude-opus-4-6","max_tokens":128,"messages":[{"role":"user","content":"hello"}],"stream":false}`,
			invoke: func(h *Handler, c *gin.Context) { h.Messages(c) },
		},
	}
}

func TestWsBusyAcquireTimeoutReturnsQueueTimeout429(t *testing.T) {
	gin.SetMode(gin.TestMode)
	busyErr := errors.New("acquire websocket connection timed out after 20s waiting for busy session")
	for _, tc := range wsBusyAcquireTerminalCases() {
		t.Run(tc.name, func(t *testing.T) {
			handler, calls := newWsBusyAcquireTerminalScenario(t, busyErr)
			recorder, _ := continuousRetryLocalRequest(t, tc, handler)
			if calls.Load() == 0 {
				t.Fatal("upstream websocket executor was never called")
			}
			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429; body=%s", recorder.Code, recorder.Body.String())
			}
			body := recorder.Body.Bytes()
			message := gjson.GetBytes(body, "error.message").String()
			errType := gjson.GetBytes(body, "error.type").String()
			wantMessage := wsBusyAcquireQueueTimeoutMessageZH
			if tc.path == "/v1/messages" {
				wantMessage = wsBusyAcquireQueueTimeoutMessageEN
				if got := gjson.GetBytes(body, "type").String(); got != "error" {
					t.Fatalf("anthropic envelope type = %q, want error; body=%s", got, body)
				}
			}
			if message != wantMessage || errType != "rate_limit_error" {
				t.Fatalf("error = (%q, %q), want (%q, rate_limit_error); body=%s", message, errType, wantMessage, body)
			}
			if got := recorder.Header().Get("Retry-After"); got != "1" {
				t.Fatalf("Retry-After = %q, want 1", got)
			}
		})
	}
}

func TestNonBusyTransportFailureKeepsNoAvailableAccount503(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range wsBusyAcquireTerminalCases() {
		t.Run(tc.name, func(t *testing.T) {
			handler, calls := newWsBusyAcquireTerminalScenario(t, errors.New("connection reset by peer"))
			recorder, _ := continuousRetryLocalRequest(t, tc, handler)
			if calls.Load() == 0 {
				t.Fatal("upstream websocket executor was never called")
			}
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
			}
			message := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String()
			want := noAvailableAccountMessage("gpt-5.5")
			if tc.path == "/v1/messages" {
				want = noAvailableAnthropicAccountMessage("claude-opus-4-6")
			}
			if message != want || strings.Contains(message, "排队超时") {
				t.Fatalf("message = %q, want %q", message, want)
			}
		})
	}
}

// TestWsBusyAcquireTimeoutStreamingResponsesStatus 覆盖流式 /v1/responses 的 429 与 Retry-After。
// 注意：安全预检只对非 WS 尝试启用（safePreflightAttemptEligible），而 busy 超时只来自 WS
// 尝试，因此这里走的是普通流式出口，不经过安全预检先写响应头的分支。
func TestWsBusyAcquireTimeoutStreamingResponsesStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	busyErr := errors.New("acquire websocket connection timed out after 20s waiting for busy session")
	handler, _ := newWsBusyAcquireTerminalScenario(t, busyErr)
	recorder, _ := continuousRetryLocalRequest(t, continuousRetryLocalEndpointCase{
		path:   "/v1/responses",
		body:   `{"model":"gpt-5.5","input":"hello","stream":true}`,
		invoke: func(h *Handler, c *gin.Context) { h.Responses(c) },
	}, handler)
	if recorder.Code != http.StatusTooManyRequests || !strings.Contains(recorder.Body.String(), wsBusyAcquireQueueTimeoutMessageZH) {
		t.Fatalf("status = %d, body=%s; want 429 with queue timeout message", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}

// TestWsBusyAcquireTimeoutThenSchedulerQueueErrorKeepsQueueResponse：上一次尝试 busy 超时，
// 换号时调度队列已满，最终应是调度队列原有的 503 响应，状态码与正文一致。
func TestWsBusyAcquireTimeoutThenSchedulerQueueErrorKeepsQueueResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := append(wsBusyAcquireTerminalCases(), continuousRetryLocalEndpointCase{
		name: "streaming responses", path: "/v1/responses",
		body:   `{"model":"gpt-5.5","input":"hello","stream":true}`,
		invoke: func(h *Handler, c *gin.Context) { h.Responses(c) },
	})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var store *auth.Store
			saturate := func() {
				// 占满调度队列：一个永远等不到账号（排除全部账号）的等待者。
				store.SetSchedulerWaitLimits(1, 1)
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _, _, _ = store.WaitForDispatchAvailable(ctx, "", time.Minute, 0, map[int64]bool{1: true}, nil, false, auth.DispatchPolicyStandard)
				}()
				t.Cleanup(func() { cancel(); <-done })
				until := time.Now().Add(time.Second)
				for store.GetSchedulerMetrics().Waiters != 1 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				if store.GetSchedulerMetrics().Waiters != 1 {
					t.Error("scheduler queue did not fill")
				}
			}
			handler, s, calls := newWsBusyAcquireTerminalPool(t, 1, func(call int32) error {
				if call == 1 {
					saturate()
				}
				return errors.New("acquire websocket connection timed out after 20s waiting for busy session")
			}, func(settings *database.SystemSettings) {
				// 非 legacy 引擎在没有候选时也排队等待，调度队列满才会作为 selectionErr 返回。
				settings.FastSchedulerEnabled = true
			})
			store = s
			recorder, _ := continuousRetryLocalRequest(t, tc, handler)
			if calls.Load() != 1 {
				t.Fatalf("upstream calls = %d, want 1", calls.Load())
			}
			body := recorder.Body.String()
			if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" || !strings.Contains(body, schedulerQueueFullMessage) {
				t.Fatalf("response = %d retry-after=%q body=%s; want scheduler queue 503", recorder.Code, recorder.Header().Get("Retry-After"), body)
			}
			if strings.Contains(body, wsBusyAcquireQueueTimeoutMessageZH) || strings.Contains(body, wsBusyAcquireQueueTimeoutMessageEN) {
				t.Fatalf("scheduler queue response carries the busy queue-timeout message: %s", body)
			}
		})
	}
}

// TestBusyThenNonBusyTransportFailureIsNotBusy429：多账号号池里先 busy 超时、后一次是非 busy
// 的传输失败，最终按原逻辑返回「无可用账号」，而不是 busy 的 429。
func TestBusyThenNonBusyTransportFailureIsNotBusy429(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range wsBusyAcquireTerminalCases() {
		t.Run(tc.name, func(t *testing.T) {
			handler, _, calls := newWsBusyAcquireTerminalPool(t, 2, func(call int32) error {
				if call == 1 {
					return errors.New("acquire websocket connection timed out after 20s waiting for busy session")
				}
				return errors.New("connection reset by peer")
			})
			recorder, _ := continuousRetryLocalRequest(t, tc, handler)
			if calls.Load() != 2 {
				t.Fatalf("upstream calls = %d, want 2 (one per account)", calls.Load())
			}
			want := noAvailableAccountMessage("gpt-5.5")
			if tc.path == "/v1/messages" {
				want = noAvailableAnthropicAccountMessage("claude-opus-4-6")
			}
			message := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String()
			if recorder.Code != http.StatusServiceUnavailable || message != want {
				t.Fatalf("response = %d %q, want 503 %q; body=%s", recorder.Code, message, want, recorder.Body.String())
			}
		})
	}
}

// TestEmptyPoolKeepsOriginalNoAccountResponse：号池一开始就没有可用账号时保持原有响应。
func TestEmptyPoolKeepsOriginalNoAccountResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range wsBusyAcquireTerminalCases() {
		t.Run(tc.name, func(t *testing.T) {
			handler, _, calls := newWsBusyAcquireTerminalPool(t, 0, func(int32) error {
				return errors.New("acquire websocket connection timed out after 20s waiting for busy session")
			})
			recorder, _ := continuousRetryLocalRequest(t, tc, handler)
			if calls.Load() != 0 {
				t.Fatalf("upstream calls = %d, want 0", calls.Load())
			}
			want := noAvailableAccountMessage("gpt-5.5")
			if tc.path == "/v1/messages" {
				want = noAvailableAnthropicAccountMessage("claude-opus-4-6")
			}
			message := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String()
			if recorder.Code != http.StatusServiceUnavailable || message != want {
				t.Fatalf("response = %d %q, want 503 %q; body=%s", recorder.Code, message, want, recorder.Body.String())
			}
		})
	}
}
