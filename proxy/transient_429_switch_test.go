package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

func TestTransient429SwitchDisabledHTTPAndSemanticFailures(t *testing.T) {
	for _, model := range []string{"gpt-5.5", "gpt-5.3-codex-spark"} {
		for _, semantic := range []bool{false, true} {
			name := model + "/http"
			if semantic {
				name = model + "/response.failed"
			}
			t.Run(name, func(t *testing.T) {
				s := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 4})
				defer s.Stop()
				acc := &auth.Account{DBID: 1, AccessToken: "synthetic-token", PlanType: "pro", Status: auth.StatusReady}
				s.AddAccount(acc)
				h := &Handler{store: s}
				resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: make(http.Header)}
				resp.Header.Set("Retry-After", "120")
				body := []byte(`{"error":{"type":"rate_limit_exceeded","message":"slow down"}}`)
				var decision codex429Decision
				if semantic {
					resp.StatusCode = http.StatusOK
					body = []byte(`{"type":"response.failed","response":{"error":{"type":"rate_limit_exceeded","message":"slow down"}}}`)
					decision = h.applyResponseFailedCooldown(acc, body, resp, model)
				} else {
					decision = h.applyCooldownForModel(acc, resp.StatusCode, body, resp, model)
				}
				if decision.Reason != "rate_limited" || decision.Cooldown != 0 || !decision.ResetAt.IsZero() {
					t.Fatalf("disabled decision = %+v, want transient error without synthetic deadline", decision)
				}
				if acc.HasActiveCooldown() || !acc.IsAvailable() || !acc.SparkDispatchEligible() || acc.IsModelRateLimited(model) {
					t.Fatal("disabled transient 429 still blocked dispatch")
				}
				if got := resp.Header.Get("Retry-After"); got != "120" {
					t.Fatalf("upstream Retry-After mutated: %q", got)
				}
			})
		}
	}
}

func TestTransient429SwitchDisabledRetainsQuotaCooldowns(t *testing.T) {
	for _, test := range []struct {
		name, body, model string
		headers           http.Header
	}{
		{name: "usage limit", body: `{"error":{"type":"usage_limit_reached"}}`, model: "gpt-5.5"},
		{name: "5h window", model: "gpt-5.5", headers: http.Header{
			"X-Codex-Primary-Used-Percent": {"100"}, "X-Codex-Primary-Window-Minutes": {"300"}, "X-Codex-Primary-Reset-After-Seconds": {"1800"},
		}},
		{name: "7d window", model: "gpt-5.5", headers: http.Header{
			"X-Codex-Primary-Used-Percent": {"100"}, "X-Codex-Primary-Window-Minutes": {"10080"}, "X-Codex-Primary-Reset-After-Seconds": {"1800"},
		}},
		{name: "Spark quota", body: `{"error":{"type":"usage_limit_reached","resets_in_seconds":1800}}`, model: "gpt-5.3-codex-spark"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 4})
			defer s.Stop()
			acc := &auth.Account{DBID: 1, AccessToken: "synthetic-token", PlanType: "pro", Status: auth.StatusReady}
			decision := Apply429Cooldown(s, acc, []byte(test.body), &http.Response{Header: test.headers}, test.model)
			if decision.Cooldown <= 0 || !decision.ResetAt.After(time.Now()) {
				t.Fatalf("real quota lost its cooldown: %+v", decision)
			}
			if test.name == "Spark quota" {
				if acc.SparkDispatchEligible() {
					t.Fatal("real Spark quota exhaustion was bypassed")
				}
			} else if !acc.HasActiveCooldown() {
				t.Fatal("real account quota exhaustion was bypassed")
			}
		})
	}
}

func TestTransient429SwitchDisabledPreservesFinalErrorAndRetryBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 4, MaxRateLimitRetries: 2})
	defer s.Stop()
	body := []byte(`{"error":{"type":"rate_limit_error","message":"slow down"}}`)
	general, rateLimited := 0, 0
	policy := database.DefaultContinuousRetryPolicy()
	for i := 0; i < 3; i++ {
		if got := shouldRetryHTTPStatus(http.StatusTooManyRequests, body, &general, &rateLimited, 2, s.GetMaxRateLimitRetries(), policy); got != (i < 2) {
			t.Fatalf("retry %d allowed = %t, want %t", i, got, i < 2)
		}
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	ctx.Header("Retry-After", "120")
	(&Handler{store: s}).sendFinalUpstreamError(ctx, http.StatusTooManyRequests, body)
	if recorder.Code != http.StatusTooManyRequests || recorder.Header().Get("Retry-After") != "120" {
		t.Fatalf("final error = %d Retry-After=%q, want 429/120", recorder.Code, recorder.Header().Get("Retry-After"))
	}
}
