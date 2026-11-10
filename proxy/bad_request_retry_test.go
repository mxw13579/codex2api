package proxy

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
)

func TestBareBadRequestMatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"exact", 400, `{"detail":"Bad Request"}`, true},
		{"json_whitespace", 400, " \n{ \"detail\" : \"Bad Request\" } \t", true},
		{"json_escape", 400, `{"detail":"Bad\u0020Request"}`, true},
		{"wrong_status", 200, `{"detail":"Bad Request"}`, false},
		{"wrong_status_503", 503, `{"detail":"Bad Request"}`, false},
		{"wrong_case", 400, `{"detail":"bad request"}`, false},
		{"padded_message", 400, `{"detail":" Bad Request "}`, false},
		{"duplicate", 400, `{"detail":"Bad Request","detail":"Bad Request"}`, false},
		{"specific_error", 400, `{"detail":"Bad Request","code":"invalid_value"}`, false},
		{"null_extra", 400, `{"detail":"Bad Request","reason":null}`, false},
		{"different_order_extra", 400, `{"code":"invalid_value","detail":"Bad Request"}`, false},
		{"object_detail", 400, `{"detail":{"message":"Bad Request"}}`, false},
		{"array_detail", 400, `{"detail":[{"msg":"Bad Request"}]}`, false},
		{"top_array", 400, `[{"detail":"Bad Request"}]`, false},
		{"plain_text", 400, `Bad Request`, false},
		{"html", 400, `<html>Bad Request</html>`, false},
		{"sse", 400, "data: {\"detail\":\"Bad Request\"}\n\n", false},
		{"truncated", 400, `{"detail":"Bad Request"`, false},
		{"trailing_json", 400, `{"detail":"Bad Request"}{}`, false},
		{"trailing_junk", 400, `{"detail":"Bad Request"}x`, false},
		{"empty", 400, "", false},
		{"too_large", 400, strings.Repeat(" ", 1024) + `{"detail":"Bad Request"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBareBadRequest(tc.status, []byte(tc.body)); got != tc.want {
				t.Fatalf("match=%t want=%t", got, tc.want)
			}
		})
	}
}

func TestBadRequestRetryDecisionBoundaries(t *testing.T) {
	account := &auth.Account{DBID: 123}
	body := []byte(`{"detail":"Bad Request"}`)
	for _, rotate := range []bool{false, true} {
		s := newBadRequestRetryState(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 2, SwitchAccount: rotate})
		for i := 1; i <= 3; i++ {
			d := s.decide(context.Background(), http.StatusBadRequest, body, nil, account, "", true)
			if !d.matched || d.retry != (i <= 2) || d.rotate != (rotate && i <= 2) || s.used != min(i, 2) {
				t.Fatalf("rotate=%t attempt=%d decision=%+v used=%d", rotate, i, d, s.used)
			}
			exclusions := newRetryAccountExclusions()
			d.markExclusions(exclusions, account.ID(), 400, body, 99, 99, database.ContinuousRetryPolicy{Enabled: true, CatchAll: true})
			if excluded := exclusions.ForSelection()[account.ID()]; excluded != d.rotate {
				t.Fatalf("unexpected exclusion=%t decision=%+v", excluded, d)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		readErr  error
		account  *auth.Account
		canRetry bool
	}{
		{"canceled", ctx, nil, account, true},
		{"incomplete_body", context.Background(), errors.New("synthetic read error"), account, true},
		{"no_account", context.Background(), nil, nil, true},
		{"unsafe", context.Background(), nil, account, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newBadRequestRetryState(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10})
			d := s.decide(tc.ctx, 400, body, tc.readErr, tc.account, "", tc.canRetry)
			if !d.matched || d.retry || s.used != 0 || s.ForceHTTP() {
				t.Fatalf("unsafe attempt consumed budget or delegated to catch-all: %+v", d)
			}
		})
	}
	for _, policy := range []database.BadRequestRetryPolicy{
		{Enabled: false, MaxRetries: 10},
		{Enabled: true, MaxRetries: 0},
	} {
		s := newBadRequestRetryState(policy)
		d := s.decide(context.Background(), 400, body, nil, account, "", true)
		if d.matched != policy.Enabled || d.retry || s.used != 0 {
			t.Fatalf("zero/disabled policy=%+v decision=%+v", policy, d)
		}
	}
}
