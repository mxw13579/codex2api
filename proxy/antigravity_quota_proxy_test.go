package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

// quotaGateTestGeminiPath 是 native Gemini 入口路径（handler.go 的
// /v1beta/models/*action 路由），429 必须回落到 Gemini 原生错误信封。
const quotaGateTestGeminiPath = "/v1beta/models/gemini-3-pro:generateContent"

// quotaGateTestContext 构造指定请求路径的 Gin 测试上下文与录制器。
func quotaGateTestContext(t *testing.T, path string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, path, nil)
	return c, recorder
}

// quotaGateTestStore 构造可独立运行的 Store（无 DB），账号按需装载。
func quotaGateTestStore(t *testing.T, accounts ...*auth.Account) *auth.Store {
	t.Helper()
	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 2, FastSchedulerEnabled: true})
	t.Cleanup(store.Stop)
	for _, account := range accounts {
		store.AddAccount(account)
	}
	return store
}

// quotaGateTestSaturateWaitQueue 把调度等待队列占满（total=1、已有 1 个等待者），
// 这样「是否进入排队」就能从返回错误上直接区分：准入被拒 = 走了排队路径。
func quotaGateTestSaturateWaitQueue(t *testing.T, store *auth.Store) {
	t.Helper()
	store.SetSchedulerWaitLimits(1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _ = store.WaitForDispatchAvailable(ctx, "", time.Minute, 0, nil, nil, false, auth.DispatchPolicyStandard)
	}()
	t.Cleanup(func() { cancel(); <-done })
	until := time.Now().Add(2 * time.Second)
	for store.GetSchedulerMetrics().Waiters != 1 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if store.GetSchedulerMetrics().Waiters != 1 {
		t.Fatal("wait queue did not fill")
	}
}

// quotaGateTestBlockedAccount 构造被配额硬门禁阻断的 Antigravity 账号：weekly 桶
// remaining=0 + 可信 metadata + 1 小时后的可信 resetAt（Task 12 的谓词语义）。
func quotaGateTestBlockedAccount(t *testing.T, id int64) *auth.Account {
	t.Helper()
	now := time.Now().UTC()
	zero := 0.0
	return &auth.Account{
		DBID:                 id,
		UpstreamType:         auth.UpstreamAntigravity,
		AccessToken:          "at",
		RefreshToken:         "rt",
		AntigravityProjectID: "project-id",
		HealthTier:           auth.HealthTierHealthy,
		AntigravityQuota: auth.AntigravityQuotaSnapshot{
			SchemaVersion: 2,
			SourceMetadata: map[string]auth.SourceMeta{
				"groups": {AppliedSeq: 3, ObservedAt: now, PresenceVerified: true},
			},
			Groups: []auth.AntigravityQuotaGroup{{
				Buckets: []auth.AntigravityQuotaBucket{{
					Window:            "weekly",
					RemainingFraction: &zero,
					FractionPresent:   true,
					ResetTime:         now.Add(time.Hour).Format(time.RFC3339),
					ObservedAt:        now,
					ObservationSeq:    3,
					TTLSeconds:        3600,
				}},
			}},
		},
	}
}

// quotaGateTestUnhealthyAccount 构造因非配额原因（StatusError）不可调度的账号，
// 用于「混合原因不得当作配额阻断」的场景。
func quotaGateTestUnhealthyAccount(id int64) *auth.Account {
	return &auth.Account{
		DBID:                 id,
		UpstreamType:         auth.UpstreamAntigravity,
		AccessToken:          "at",
		RefreshToken:         "rt",
		AntigravityProjectID: "project-id",
		Status:               auth.StatusError,
		HealthTier:           auth.HealthTierHealthy,
	}
}

// TestProxy_QuotaGate429MappingAndSkipQueue 覆盖代理层对配额阻断的 429 映射、
// 跳过排队等待、续链绑定保留及 native countTokens 的受控阻断。
func TestProxy_QuotaGate429MappingAndSkipQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("pool exhaustion maps to 429 with Retry-After and reason", func(t *testing.T) {
		c, recorder := quotaGateTestContext(t, quotaGateTestGeminiPath)
		writeSchedulerQuotaExhaustedError(c, 120, "quota_gate")
		if recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("expected 429 Too Many Requests, got %d", recorder.Code)
		}
		if got := recorder.Header().Get("Retry-After"); got != "120" {
			t.Fatalf("expected Retry-After 120, got %q", got)
		}
		if !strings.Contains(recorder.Body.String(), "quota_gate") {
			t.Fatalf("expected reason quota_gate in body, got %s", recorder.Body.String())
		}
		// Gemini 原生入口保持原生错误信封（RESOURCE_EXHAUSTED），不与 OpenAI 信封混用。
		if !strings.Contains(recorder.Body.String(), "RESOURCE_EXHAUSTED") {
			t.Fatalf("expected Gemini native envelope, got %s", recorder.Body.String())
		}
	})

	t.Run("bound continuation maps to quota_gate_bound", func(t *testing.T) {
		c, recorder := quotaGateTestContext(t, quotaGateTestGeminiPath)
		writeSchedulerQuotaExhaustedError(c, 60, "quota_gate_bound")
		if recorder.Code != http.StatusTooManyRequests || !strings.Contains(recorder.Body.String(), "quota_gate_bound") {
			t.Fatalf("expected 429 with quota_gate_bound, got %d %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("Retry-After is clamped to [1,3600] and defaults to 60", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			input  int
			expect string
		}{
			{name: "zero falls back to default", input: 0, expect: "60"},
			{name: "negative falls back to default", input: -5, expect: "60"},
			{name: "capped at one hour", input: 999999, expect: "3600"},
			{name: "in range preserved", input: 1, expect: "1"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c, recorder := quotaGateTestContext(t, quotaGateTestGeminiPath)
				writeSchedulerQuotaExhaustedError(c, tc.input, "quota_gate")
				if got := recorder.Header().Get("Retry-After"); got != tc.expect {
					t.Fatalf("expected Retry-After %s, got %q", tc.expect, got)
				}
			})
		}
	})

	t.Run("response envelope stays protocol differentiated", func(t *testing.T) {
		cases := []struct {
			path   string
			needle string
		}{
			{path: "/v1/messages", needle: "rate_limit_error"},
			{path: "/v1/chat/completions", needle: `"reason":"quota_gate"`},
			{path: "/v1/responses", needle: `"reason":"quota_gate"`},
		}
		for _, tc := range cases {
			t.Run(tc.path, func(t *testing.T) {
				c, recorder := quotaGateTestContext(t, tc.path)
				writeSchedulerQuotaExhaustedError(c, 60, "quota_gate")
				if recorder.Code != http.StatusTooManyRequests {
					t.Fatalf("expected 429 for %s, got %d", tc.path, recorder.Code)
				}
				if !strings.Contains(recorder.Body.String(), tc.needle) {
					t.Fatalf("expected %q in %s body, got %s", tc.needle, tc.path, recorder.Body.String())
				}
			})
		}
	})

	t.Run("schedulerSelectionAPIError recognizes quota exhaustion", func(t *testing.T) {
		bound := &auth.QuotaExhaustedError{Bound: true, RetryAfter: 90 * time.Second}
		apiErr := schedulerSelectionAPIError(bound)
		if apiErr == nil {
			t.Fatal("schedulerSelectionAPIError must recognize auth.QuotaExhaustedError")
		}
		if apiErr.Code != api.ErrCodeRateLimitReached || apiErr.Type != api.ErrorTypeRateLimit {
			t.Fatalf("expected rate limit APIError, got %+v", apiErr)
		}
		if !errors.Is(bound, auth.ErrQuotaExhausted) {
			t.Fatal("QuotaExhaustedError must match errors.Is(err, ErrQuotaExhausted)")
		}
		if !strings.Contains(bound.Error(), "bound") {
			t.Fatalf("bound reason must be distinguishable in the message, got %q", bound.Error())
		}
		poolErr := &auth.QuotaExhaustedError{}
		if strings.Contains(poolErr.Error(), "bound") {
			t.Fatalf("pool exhaustion message must differ from bound, got %q", poolErr.Error())
		}
		// 容量类错误保持既有语义，不与配额 429 混用。
		if apiErr := schedulerSelectionAPIError(auth.ErrSchedulerQueueFull); apiErr == nil {
			t.Fatal("existing queue-full mapping must remain")
		} else if apiErr.Code != api.ErrCodeServiceUnavailable {
			t.Fatalf("queue full must stay a capacity error, got %+v", apiErr)
		}
	})

	t.Run("quota blocked pool skips the queue wait", func(t *testing.T) {
		store := quotaGateTestStore(t, quotaGateTestBlockedAccount(t, 8601))
		quotaGateTestSaturateWaitQueue(t, store)
		h := &Handler{store: store}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		_, _, _, err := h.nextRetryAccountWithGuard(ctx, "", 0, newRetryAccountExclusions(), nil, false, auth.SelectionOptions{Policy: auth.DispatchPolicyStandard})
		elapsed := time.Since(start)

		if !errors.Is(err, auth.ErrQuotaExhausted) {
			t.Fatalf("quota blocked pool must surface ErrQuotaExhausted, got %v", err)
		}
		var quotaErr *auth.QuotaExhaustedError
		if !errors.As(err, &quotaErr) {
			t.Fatalf("expected typed *auth.QuotaExhaustedError, got %T", err)
		}
		if quotaErr.Bound {
			t.Fatal("fresh selection must not report bound continuation")
		}
		if quotaErr.RetryAfter <= 0 || quotaErr.RetryAfter > 3600*time.Second {
			t.Fatalf("Retry-After must stay in (0, 3600s], got %s", quotaErr.RetryAfter)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("quota exhaustion must not burn the queue wait, waited %s", elapsed)
		}
	})

	t.Run("bound continuation surfaces bound quota exhaustion", func(t *testing.T) {
		account := quotaGateTestBlockedAccount(t, 8602)
		store := quotaGateTestStore(t, account)
		quotaGateTestSaturateWaitQueue(t, store)
		store.BindSessionAffinity("quota_gate_bound_key", account, "")
		h := &Handler{store: store}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		_, _, _, err := h.nextRetryAccountWithGuard(ctx, "quota_gate_bound_key", 0, newRetryAccountExclusions(), nil, true, auth.SelectionOptions{Provider: auth.UpstreamAntigravity, Policy: auth.DispatchPolicyStandard})
		elapsed := time.Since(start)

		if !errors.Is(err, auth.ErrQuotaExhausted) {
			t.Fatalf("bound quota block must surface ErrQuotaExhausted, got %v", err)
		}
		var quotaErr *auth.QuotaExhaustedError
		if !errors.As(err, &quotaErr) || !quotaErr.Bound {
			t.Fatalf("expected bound quota exhaustion, got %v (%T)", err, err)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("bound quota block must not queue wait, waited %s", elapsed)
		}
		if id, ok := store.SessionAffinityAccountID("quota_gate_bound_key"); !ok || id != account.DBID {
			t.Fatalf("quota block must keep the durable binding intact, got %d ok=%v", id, ok)
		}
	})

	t.Run("mixed reasons still take the queue path", func(t *testing.T) {
		store := quotaGateTestStore(t, quotaGateTestBlockedAccount(t, 8603), quotaGateTestUnhealthyAccount(8604))
		quotaGateTestSaturateWaitQueue(t, store)
		h := &Handler{store: store}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, _, _, err := h.nextRetryAccountWithGuard(ctx, "", 0, newRetryAccountExclusions(), nil, false, auth.SelectionOptions{Policy: auth.DispatchPolicyStandard})
		if errors.Is(err, auth.ErrQuotaExhausted) {
			t.Fatalf("mixed reasons must not be reported as quota exhaustion, got %v", err)
		}
		if !errors.Is(err, auth.ErrSchedulerQueueFull) {
			t.Fatalf("mixed reasons must keep the existing queue admission path, got %v", err)
		}
	})

	t.Run("countTokens estimate fallback only breaks on quota exhaustion", func(t *testing.T) {
		// 非配额成因（无错误、选号超时）保持既有 200 + 本地估算回退（Spec §6.2 #3）。
		for _, selectionErr := range []error{nil, context.DeadlineExceeded, auth.ErrSchedulerQueueFull} {
			c, recorder := quotaGateTestContext(t, "/v1beta/models/gemini-3-pro:countTokens")
			if writeCountTokensQuotaGateError(c, selectionErr) {
				t.Fatalf("selection error %v must keep the estimate fallback", selectionErr)
			}
			if recorder.Body.Len() != 0 {
				t.Fatalf("selection error %v must not write a response, got %s", selectionErr, recorder.Body.String())
			}
		}

		c, recorder := quotaGateTestContext(t, "/v1beta/models/gemini-3-pro:countTokens")
		if !writeCountTokensQuotaGateError(c, &auth.QuotaExhaustedError{RetryAfter: 90 * time.Second}) {
			t.Fatal("quota exhaustion must answer 429 instead of a fake token estimate")
		}
		if recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("expected 429, got %d", recorder.Code)
		}
		if got := recorder.Header().Get("Retry-After"); got != "90" {
			t.Fatalf("expected Retry-After 90, got %q", got)
		}
		if !strings.Contains(recorder.Body.String(), "quota_gate") {
			t.Fatalf("expected quota_gate reason, got %s", recorder.Body.String())
		}
	})
}
