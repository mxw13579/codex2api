package proxy

import (
	"context"
	"net/http"
	"testing"
)

func TestRateLimitRetryBudgetAllowsOneHundredRetries(t *testing.T) {
	payload := []byte(`{"type":"response.failed","response":{"status":"failed","status_code":429,"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}}`)
	outcome := classifyResponseFailedOutcome(payload)
	for _, tt := range []struct {
		name  string
		retry func(general, rate *int) bool
	}{
		{"HTTP 429", func(general, rate *int) bool {
			return shouldRetryHTTPStatus(http.StatusTooManyRequests, nil, general, rate, 5, 100)
		}},
		{"stream capacity failure", func(general, rate *int) bool {
			return shouldTransparentRetryStreamWithBudgets(outcome, general, rate, 5, 100, false, nil, nil)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			general, rate := 0, 0
			for retry := 1; retry <= 100; retry++ {
				if !tt.retry(&general, &rate) {
					t.Fatalf("retry %d denied before the 100-retry budget was exhausted", retry)
				}
			}
			if tt.retry(&general, &rate) {
				t.Fatal("retry 101 must be denied")
			}
			if general != 0 || rate != 100 {
				t.Fatalf("general/rate budgets = %d/%d, want 0/100", general, rate)
			}
			if !shouldRetryHTTPStatus(http.StatusServiceUnavailable, nil, &general, &rate, 5, 100) {
				t.Fatal("exhausted 429 budget must not consume the separate general retry budget")
			}
		})
	}
}

func TestRateLimitRetryBudgetStillStopsAfterOutputOrCancellation(t *testing.T) {
	outcome := classifyResponseFailedOutcome([]byte(`{"type":"response.failed","response":{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}}`))
	general, rate := 0, 0
	if shouldTransparentRetryStreamWithBudgets(outcome, &general, &rate, 5, 100, true, nil, nil) {
		t.Fatal("a larger budget must not replay a stream after downstream output")
	}
	if shouldTransparentRetryStreamWithBudgets(outcome, &general, &rate, 5, 100, false, context.Canceled, nil) {
		t.Fatal("a larger budget must not retry a cancelled request")
	}
	if general != 0 || rate != 0 {
		t.Fatalf("blocked retries consumed budgets: general/rate=%d/%d", general, rate)
	}
}
