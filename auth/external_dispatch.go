package auth

import (
	"sync/atomic"
	"time"
)

// PinnedAccountFailure classifies why an externally pinned account cannot
// serve a request (issue #831). Only facts that make the account unusable
// regardless of scheduling policy are reported. Scheduler-owned gates —
// cooldowns, concurrency caps, dispatch-count limits, usage-window and quota
// auto-pauses, intelligence pauses and health scoring — are deliberately not
// consulted: the external scheduler owns those decisions.
type PinnedAccountFailure string

const (
	PinnedAccountAvailable PinnedAccountFailure = ""
	// PinnedAccountNotFound covers both a missing account and one this API key
	// may not use, so a key cannot probe accounts outside its scope.
	PinnedAccountNotFound PinnedAccountFailure = "not_found"
	// PinnedAccountUnsupported means the account cannot serve this endpoint or model.
	PinnedAccountUnsupported PinnedAccountFailure = "unsupported"
	// PinnedAccountDisabled means an administrator disabled the account.
	PinnedAccountDisabled PinnedAccountFailure = "disabled"
	// PinnedAccountCredentialUnavailable means the account cannot authenticate
	// upstream: credentials are missing, revoked, or failed with 401.
	PinnedAccountCredentialUnavailable PinnedAccountFailure = "credential_unavailable"
	// PinnedAccountCredentialRefreshing means an expired access token is being
	// refreshed in the background; the same account is worth retrying shortly.
	PinnedAccountCredentialRefreshing PinnedAccountFailure = "credential_refreshing"
	// PinnedAccountEgressUnavailable means the account's proxy policy has no
	// usable route right now. It never falls back to direct egress.
	PinnedAccountEgressUnavailable PinnedAccountFailure = "egress_unavailable"
)

// AcquirePinnedAccount reserves a request slot on exactly one account without
// running the scheduler. filter carries the endpoint's capability checks
// (account type, model support) and must not contain scheduling gates.
//
// The slot is counted in ActiveRequests/OccupiedRequests even above the
// account's concurrency cap, so live views and the internal scheduler still
// see the load. It does not consume the account's dispatch-count budget.
// Callers release it with Release, like any scheduled account.
func (s *Store) AcquirePinnedAccount(id int64, apiKeyID int64, filter AccountFilter) (*Account, PinnedAccountFailure) {
	if s == nil || id <= 0 {
		return nil, PinnedAccountNotFound
	}
	target := s.FindByID(id)
	if target == nil || !s.accountAllowedForAPIKey(target, apiKeyID) {
		return nil, PinnedAccountNotFound
	}
	if failure := s.pinnedAccountUsability(target); failure != PinnedAccountAvailable {
		return nil, failure
	}
	if filter != nil && !filter(target) {
		return nil, PinnedAccountUnsupported
	}
	if !s.accountHasUsableEgress(target) {
		return nil, PinnedAccountEgressUnavailable
	}
	reservePinnedAccountSlot(target)
	// A 401 or an admin disable may land between the check and the reservation.
	if failure := pinnedAccountFlagFailure(target); failure != PinnedAccountAvailable {
		s.Release(target)
		return nil, failure
	}
	return target, PinnedAccountAvailable
}

func pinnedAccountFlagFailure(acc *Account) PinnedAccountFailure {
	if atomic.LoadInt32(&acc.DispatchPaused) != 0 {
		return PinnedAccountDisabled
	}
	if atomic.LoadInt32(&acc.Disabled) != 0 {
		return PinnedAccountCredentialUnavailable
	}
	return PinnedAccountAvailable
}

func (s *Store) pinnedAccountUsability(acc *Account) PinnedAccountFailure {
	if failure := pinnedAccountFlagFailure(acc); failure != PinnedAccountAvailable {
		return failure
	}
	if s.GetLazyMode() && s.lazyNeedsDispatchRefresh(acc) {
		s.triggerLazyRefreshAsync(acc)
		return PinnedAccountCredentialRefreshing
	}
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	if acc.Status == StatusError {
		return PinnedAccountCredentialUnavailable
	}
	// Banned is the unauthorized cooldown. Antigravity keeps its narrow 401
	// recovery window, matching isAvailableLocked.
	if acc.healthTierLocked() == HealthTierBanned &&
		!(acc.isAntigravityAPILocked() && acc.antigravityUnauthorizedRecoveryLocked(time.Now())) {
		return PinnedAccountCredentialUnavailable
	}
	if !acc.hasDispatchCredentialLocked() {
		return PinnedAccountCredentialUnavailable
	}
	return PinnedAccountAvailable
}

func reservePinnedAccountSlot(acc *Account) {
	acc.OccupiedRequests.Add(1)
	acc.ActiveRequests.Add(1)
	acc.TotalRequests.Add(1)
	acc.LastUsedAt.Store(time.Now().UnixNano())
}
