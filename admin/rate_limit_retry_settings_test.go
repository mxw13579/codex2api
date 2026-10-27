package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/codex2api/auth"
)

func TestSettingsRateLimitRetryRangeAndPersistence(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	initial := invokeTransient429SettingsAdmin(handler, http.MethodPut,
		`{"max_retries":5,"max_rate_limit_retries":10,"retry_interval_ms":100}`)
	if initial.Code != http.StatusOK {
		t.Fatalf("initialize settings: status=%d body=%s", initial.Code, initial.Body.String())
	}

	for _, tt := range []struct {
		name        string
		body        string
		wantRate    int
		wantGeneral int
	}{
		{"disabled", `{"max_rate_limit_retries":0}`, 0, 5},
		{"negative clamps to zero", `{"max_rate_limit_retries":-1}`, 0, 5},
		{"previous maximum", `{"max_rate_limit_retries":10}`, 10, 5},
		{"above previous maximum", `{"max_rate_limit_retries":11}`, 11, 5},
		{"below new maximum", `{"max_rate_limit_retries":99}`, 99, 5},
		{"new maximum", `{"max_rate_limit_retries":100}`, 100, 5},
		{"above maximum clamps", `{"max_rate_limit_retries":101}`, 100, 5},
		{"omitted preserves value", `{"site_name":"retry budget test"}`, 100, 5},
		{"null preserves value", `{"max_rate_limit_retries":null}`, 100, 5},
		{"ordinary retry cap unchanged", `{"max_retries":100}`, 100, 10},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := invokeTransient429SettingsAdmin(handler, http.MethodPut, tt.body)
			if response.Code != http.StatusOK {
				t.Fatalf("PUT status=%d body=%s", response.Code, response.Body.String())
			}
			get := invokeTransient429SettingsAdmin(handler, http.MethodGet, "")
			if get.Code != http.StatusOK {
				t.Fatalf("GET status=%d", get.Code)
			}
			for _, body := range []string{response.Body.String(), get.Body.String()} {
				var decoded struct {
					Rate     *int `json:"max_rate_limit_retries"`
					General  *int `json:"max_retries"`
					Interval *int `json:"retry_interval_ms"`
				}
				if err := json.Unmarshal([]byte(body), &decoded); err != nil {
					t.Fatal(err)
				}
				if decoded.Rate == nil || *decoded.Rate != tt.wantRate ||
					decoded.General == nil || *decoded.General != tt.wantGeneral ||
					decoded.Interval == nil || *decoded.Interval != 100 {
					got, _ := json.Marshal(decoded)
					t.Fatalf("response retry settings = %s, want rate/general/interval=%d/%d/100", got, tt.wantRate, tt.wantGeneral)
				}
			}
			if got := handler.store.GetMaxRateLimitRetries(); got != tt.wantRate {
				t.Fatalf("runtime 429 budget = %d, want %d", got, tt.wantRate)
			}
			if got := handler.store.GetMaxRetries(); got != tt.wantGeneral {
				t.Fatalf("runtime general budget = %d, want %d", got, tt.wantGeneral)
			}
			persisted, err := db.GetSystemSettings(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if persisted.MaxRateLimitRetries != tt.wantRate ||
				persisted.MaxRetries != tt.wantGeneral || persisted.RetryIntervalMS != 100 {
				t.Fatalf("persisted rate/general/interval = %d/%d/%d, want %d/%d/100",
					persisted.MaxRateLimitRetries, persisted.MaxRetries, persisted.RetryIntervalMS,
					tt.wantRate, tt.wantGeneral)
			}
		})
	}

	persisted, err := db.GetSystemSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reloaded := auth.NewStore(nil, nil, persisted)
	t.Cleanup(reloaded.Stop)
	if got := reloaded.GetMaxRateLimitRetries(); got != 100 {
		t.Fatalf("reloaded 429 budget = %d, want 100", got)
	}
}

func TestSettingsRateLimitRetryRejectsInvalidTypes(t *testing.T) {
	handler, _, _ := newTransient429SettingsAdminHandler(t)
	initial := invokeTransient429SettingsAdmin(handler, http.MethodPut, `{"max_rate_limit_retries":100}`)
	if initial.Code != http.StatusOK {
		t.Fatalf("initialize settings: status=%d", initial.Code)
	}
	for _, value := range []string{`"100"`, `1.5`, `true`, `{}`} {
		response := invokeTransient429SettingsAdmin(handler, http.MethodPut,
			`{"max_rate_limit_retries":`+value+`}`)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("value=%s status=%d, want 400", value, response.Code)
		}
		if got := handler.store.GetMaxRateLimitRetries(); got != 100 {
			t.Fatalf("invalid update changed 429 budget to %d", got)
		}
	}
}
