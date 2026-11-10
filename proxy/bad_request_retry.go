package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
)

// Match only the observed, opaque HTTP response. In particular, an error
// message containing these words, a structured validation error, extra fields,
// duplicate keys, HTML, and a streaming terminal failure are not this policy.
func isBareBadRequest(status int, body []byte) bool {
	if status != http.StatusBadRequest || len(body) > 1024 {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(body))
	for _, expected := range []any{json.Delim('{'), "detail", "Bad Request", json.Delim('}')} {
		got, err := d.Token()
		if err != nil || got != expected {
			return false
		}
	}
	_, err := d.Token()
	return err == io.EOF
}

// One value snapshot and one independent budget per downstream request/WS
// turn. A later settings change cannot increase an already-running budget.
type badRequestRetryState struct {
	policy        database.BadRequestRetryPolicy
	used          int
	sameAccountID int64
	sameProxyURL  string
	forceHTTP     bool
}

type badRequestRetryDecision struct {
	matched bool
	retry   bool
	rotate  bool
	ordinal int
	limit   int
}

func newBadRequestRetryState(policy database.BadRequestRetryPolicy) *badRequestRetryState {
	return &badRequestRetryState{policy: database.NormalizeBadRequestRetryPolicy(policy)}
}

// canRetry also includes endpoint-specific pinning/continuation/commit checks.
// When enabled this narrow policy owns the result, including exhaustion: a
// catch-all continuous-retry policy cannot turn the finite budget infinite.
func (s *badRequestRetryState) decide(ctx context.Context, status int, body []byte, readErr error, account *auth.Account, proxyURL string, canRetry bool) badRequestRetryDecision {
	if s == nil || !s.policy.Enabled || !isBareBadRequest(status, body) {
		return badRequestRetryDecision{}
	}
	d := badRequestRetryDecision{matched: true, ordinal: s.used, limit: s.policy.MaxRetries}
	if readErr != nil || account == nil || !canRetry || ctx != nil && ctx.Err() != nil || s.used >= s.policy.MaxRetries {
		return d
	}
	s.used++
	d.retry, d.rotate, d.ordinal = true, s.policy.SwitchAccount, s.used
	s.forceHTTP = true
	if !d.rotate {
		s.sameAccountID, s.sameProxyURL = account.ID(), proxyURL
	}
	log.Printf("[bad-request-retry] account=%d retry=%d/%d switch_account=%t", account.ID(), s.used, s.policy.MaxRetries, d.rotate)
	return d
}

// Reacquire a normal lease, rechecking live eligibility, Key scope, capability,
// concurrency and exclusions. Never silently substitute another account when
// same-account mode cannot reacquire its account. No lease is held while waiting.
func (s *badRequestRetryState) takeSameAccount(ctx context.Context, store *auth.Store, apiKeyID int64, exclusions *retryAccountExclusions, filter auth.AccountFilter, policy auth.DispatchPolicy) (*auth.Account, string, bool) {
	if s == nil || s.sameAccountID == 0 {
		return nil, "", false
	}
	id, proxyURL := s.sameAccountID, s.sameProxyURL
	s.sameAccountID, s.sameProxyURL = 0, ""
	if ctx != nil && ctx.Err() != nil {
		return nil, "", true
	}
	account := store.TakePreferredAccountWithDispatch(id, apiKeyID, exclusions.ForSelection(), filter, policy)
	return account, proxyURL, true
}

func (s *badRequestRetryState) ForceHTTP() bool { return s != nil && s.forceHTTP }

func (s *badRequestRetryState) failedSelection(status int, body []byte) bool {
	return s != nil && s.used > 0 && isBareBadRequest(status, body)
}

func (d badRequestRetryDecision) unbind() bool { return !d.matched || d.rotate }

func (d badRequestRetryDecision) markExclusions(exclusions *retryAccountExclusions, accountID int64, status int, body []byte, generalLimit, rateLimit int, policy database.ContinuousRetryPolicy) {
	if !d.matched {
		exclusions.MarkHTTPFailure(accountID, status, body, generalLimit, rateLimit, policy)
	} else if d.rotate {
		// Do not fall back to an already failed account merely because all other
		// candidates are unavailable. Normal Key/group/plan filters still apply.
		exclusions.MarkHard(accountID)
	}
}

func (d badRequestRetryDecision) waitBudget(ordinal, limit int) (int, int) {
	if d.matched {
		return d.ordinal, d.limit
	}
	return ordinal, limit
}
