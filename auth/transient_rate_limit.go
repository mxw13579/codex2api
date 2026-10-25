package auth

import (
	"math"
	"time"

	"github.com/codex2api/cache"
	"github.com/codex2api/database"
)

const (
	// TransientRateLimitBackoffBase is the first account-wide freeze after a
	// Codex throttle that is not usage_limit / 5h / 7d exhaustion.
	TransientRateLimitBackoffBase = 15 * time.Second
	// TransientRateLimitBackoffMax caps Retry-After and the exponential ladder.
	TransientRateLimitBackoffMax = 5 * time.Minute
	// transientRateLimitStableReset is how long an account must stay free of
	// new upstream 429s after LastRateLimitedAt before the backoff ladder
	// resets. Clearing on the first success would recreate a 15s retry loop
	// while concurrent in-flight requests are still being rejected.
	transientRateLimitStableReset = 5 * time.Minute
)

// transient429CooldownPolicy is published atomically so readers cannot mix
// the enabled flag with a generation from a different committed admin update.
type transient429CooldownPolicy struct {
	enabled    bool
	generation int64
}

// CodexTransient429CooldownEnabled reports whether transient Codex 429s freeze
// the account. A bare Store retains the legacy policy; NewStore loads the
// persisted setting, whose default in this fork is false.
func (s *Store) CodexTransient429CooldownEnabled() bool {
	return s.CodexTransient429CooldownPolicy().Enabled
}

// CodexTransient429CooldownPolicy reads the pair atomically. A bare test Store
// retains the legacy enabled generation-zero policy; NewStore defaults off.
func (s *Store) CodexTransient429CooldownPolicy() database.CodexTransient429CooldownPolicy {
	if s != nil {
		if policy := s.transient429Policy.Load(); policy != nil {
			return database.CodexTransient429CooldownPolicy{Enabled: policy.enabled, Generation: policy.generation}
		}
	}
	return database.CodexTransient429CooldownPolicy{Enabled: true}
}

// SetCodexTransient429CooldownEnabled is only a convenience for bare/in-memory
// Stores. DB-backed Stores must publish a durable policy, not invent local
// generations. Production callers use SetCodexTransient429CooldownPolicy.
func (s *Store) SetCodexTransient429CooldownEnabled(enabled bool) {
	if s == nil || s.db != nil {
		return
	}
	s.transient429PolicyMu.Lock()
	defer s.transient429PolicyMu.Unlock()
	current := s.CodexTransient429CooldownPolicy()
	if current.Enabled == enabled || current.Generation == math.MaxInt64 {
		return
	}
	s.setCodexTransient429CooldownPolicyLocked(database.CodexTransient429CooldownPolicy{Enabled: enabled, Generation: current.Generation + 1})
}

// SetCodexTransient429CooldownPolicy publishes a committed DB revision. Older
// reads and conflicting values for the same revision cannot roll it back.
func (s *Store) SetCodexTransient429CooldownPolicy(policy database.CodexTransient429CooldownPolicy) {
	if s == nil || policy.Generation < 0 {
		return
	}
	s.transient429PolicyMu.Lock()
	defer s.transient429PolicyMu.Unlock()
	s.setCodexTransient429CooldownPolicyLocked(policy)
}

func (s *Store) setCodexTransient429CooldownPolicyLocked(policy database.CodexTransient429CooldownPolicy) {
	if current := s.transient429Policy.Load(); current != nil && policy.Generation <= current.generation {
		return
	}
	s.transient429Policy.Store(&transient429CooldownPolicy{enabled: policy.Enabled, generation: policy.Generation})
	// A consumer can skip the intermediate off event. Advancing on -> on must
	// still clear the old ladder/window. Mark/apply read the policy under acc.mu
	// and tag that exact revision; do not clear a new-generation window created
	// after publication while this sweep was waiting for the account lock.
	for _, acc := range s.accountSnapshotAccounts() {
		if acc == nil {
			continue
		}
		acc.mu.Lock()
		cleared := false
		if !policy.Enabled || acc.transientRateLimitGeneration != policy.Generation {
			cleared = acc.clearTransient429CooldownLocked()
		}
		if cleared {
			acc.recomputeSchedulerLocked(s.maxConcurrency.Load())
		}
		acc.mu.Unlock()
		if cleared {
			s.fastSchedulerUpdate(acc)
		}
	}
	s.notifySchedulerAvailability()
}

// ignoreTransient429CooldownRecord only bypasses explicit transient records.
// A missing kind or a quota/auth reason must retain its protective semantics.
func (s *Store) ignoreTransient429CooldownRecord(record runtimeCooldownRecord) bool {
	if s == nil || record.Kind != cache.CooldownKindTransient || normalizeCooldownReason(record.Reason) != ResponsesRateLimitedCooldownReason {
		return false
	}
	policy := s.CodexTransient429CooldownPolicy()
	// Missing generations are legacy zero, accepted only by bare/in-memory
	// generation-zero policies. DB migration seeds an already-enabled policy
	// at one. Future records wait for the outbox; cache publications never
	// change the policy itself.
	return !policy.Enabled || record.PolicyGeneration != policy.Generation
}

// clearTransient429CooldownLocked resets the transient ladder and timer, but
// clears account status/deadline only when they still belong to that throttle.
// LastRateLimitedAt remains available for scoring and in-flight-success fences.
func (a *Account) clearTransient429CooldownLocked() bool {
	cleared := a.isTransientRateLimitCooldownLocked()
	if cleared {
		a.Status = StatusReady
		a.CooldownUtil = time.Time{}
		a.CooldownReason = ""
	}
	a.transientRateLimitBackoff = 0
	a.transientRateLimitGeneration = 0
	a.transientRateLimitUntil = time.Time{}
	if a.transientRateLimitTimer != nil {
		a.transientRateLimitTimer.Stop()
		a.transientRateLimitTimer = nil
	}
	return cleared
}

// nextTransientRateLimitCooldown returns the freeze duration for the given
// backoff level, never shorter than Retry-After and never longer than the cap.
func nextTransientRateLimitCooldown(level int, retryAfter time.Duration) time.Duration {
	if level < 0 {
		level = 0
	}
	cooldown := TransientRateLimitBackoffBase
	for step := 0; step < level && cooldown < TransientRateLimitBackoffMax; step++ {
		cooldown *= 2
	}
	if retryAfter > cooldown {
		cooldown = retryAfter
	}
	if cooldown > TransientRateLimitBackoffMax {
		cooldown = TransientRateLimitBackoffMax
	}
	if cooldown < TransientRateLimitBackoffBase {
		cooldown = TransientRateLimitBackoffBase
	}
	return cooldown
}

// MarkTransientRateLimited applies an account-wide short freeze for a Codex
// throttle. Concurrent 429s that land while the current window is still open
// reuse that deadline instead of climbing the backoff ladder. An already
// longer quota cooldown is left untouched.
//
// Unlike a quota cooldown the freeze is seconds long, so it neither triggers
// a WHAM usage probe nor is written to the database: under a burst that would
// turn every throttled account into one probe plus one write per window.
// The scheduler and the cross-instance cooldown cache are still updated when
// enabled. When disabled, only failure evidence/scoring is recorded; no new
// account pause, recovery timer, quota probe or cooldown-cache write is created.
func (s *Store) MarkTransientRateLimited(acc *Account, retryAfter time.Duration) time.Duration {
	if s == nil || acc == nil {
		return nextTransientRateLimitCooldown(0, retryAfter)
	}
	acc.mu.Lock()
	// Capture once under the account lock. If Set publishes while we hold it,
	// this window retains the old revision and its sweep will clear it. If Set
	// published first, discard the old window before computing a new deadline.
	policy := s.CodexTransient429CooldownPolicy()
	if !policy.Enabled || acc.transientRateLimitGeneration != policy.Generation {
		acc.clearTransient429CooldownLocked()
	}
	now := time.Now()
	until := acc.CooldownUtil
	sameWindow := acc.Status == StatusCooldown && until.After(now)
	// A quota/auth window cannot be replaced, shortened or cleared by a
	// throttle, even when the transient-pause switch is off.
	if sameWindow && !acc.isTransientRateLimitCooldownLocked() {
		acc.mu.Unlock()
		return until.Sub(now)
	}
	if !policy.Enabled {
		if accountDispatchBlocked(acc) || acc.Status == StatusError || acc.healthTierLocked() == HealthTierBanned {
			acc.mu.Unlock()
			return 0
		}
		acc.LastRateLimitedAt = now
		acc.LastFailureAt = now
		acc.FailureStreak = clampInt(acc.FailureStreak+1, 0, 20)
		acc.SuccessStreak = 0
		acc.recomputeSchedulerLocked(s.maxConcurrency.Load())
		acc.mu.Unlock()
		s.fastSchedulerUpdate(acc)
		return 0
	}
	if sameWindow {
		remaining := until.Sub(now)
		acc.LastRateLimitedAt = now
		extension := now.Add(min(retryAfter, TransientRateLimitBackoffMax))
		if retryAfter <= 0 || !extension.After(until) {
			acc.mu.Unlock()
			return remaining
		}
		until = extension
	} else {
		if accountDispatchBlocked(acc) || acc.Status == StatusError || acc.healthTierLocked() == HealthTierBanned {
			acc.mu.Unlock()
			return nextTransientRateLimitCooldown(0, retryAfter)
		}
		cooldown := nextTransientRateLimitCooldown(acc.transientRateLimitBackoff, retryAfter)
		until = now.Add(cooldown)
		// An upstream hint at the cap must not prevent this new window from
		// advancing the local ladder; hints and backoff are separate inputs.
		if nextTransientRateLimitCooldown(acc.transientRateLimitBackoff, 0) < TransientRateLimitBackoffMax {
			acc.transientRateLimitBackoff++
		}
		acc.LastFailureAt = now
		acc.FailureStreak++
		acc.SuccessStreak = 0
	}
	// Publish all local fields in one critical section; concurrent failures
	// cannot observe the new backoff without the window that caused it.
	acc.LastRateLimitedAt = now
	acc.setCooldownUntilLocked(until, ResponsesRateLimitedCooldownReason)
	acc.transientRateLimitUntil = until
	acc.transientRateLimitGeneration = policy.Generation
	acc.recomputeSchedulerLocked(s.maxConcurrency.Load())
	acc.armTransientRateLimitRecoveryLocked(s)
	record := runtimeCooldownRecord{
		Kind: cache.CooldownKindTransient, Reason: ResponsesRateLimitedCooldownReason,
		ResetAt: until, UpdatedAt: now, BackoffLevel: acc.transientRateLimitBackoff,
		PolicyGeneration: policy.Generation,
	}
	acc.mu.Unlock()
	s.fastSchedulerUpdate(acc)
	merged := s.cacheAccountCooldownRecord(acc.DBID, record)
	if merged.Kind != record.Kind || merged.Reason != record.Reason || merged.PolicyGeneration != record.PolicyGeneration || !merged.ResetAt.Equal(record.ResetAt) || merged.BackoffLevel != record.BackoffLevel {
		s.applyCachedAccountCooldown(acc, merged)
	}
	_, deadline := acc.GetCooldownSnapshot()
	return max(time.Until(deadline), 0)
}

func (a *Account) isTransientRateLimitCooldownLocked() bool {
	return a.Status == StatusCooldown && a.CooldownReason == ResponsesRateLimitedCooldownReason &&
		!a.transientRateLimitUntil.IsZero() && a.CooldownUtil.Equal(a.transientRateLimitUntil)
}

// Short freezes restore the local index directly, without a WHAM probe or a
// database write. Only active throttles allocate timers, at most one per
// account; changing the deadline cancels the previous one. The deadline and
// account identity checks fence callbacks that already started before a reset,
// removal, re-import or a stronger cooldown.
func (a *Account) armTransientRateLimitRecoveryLocked(s *Store) {
	if s == nil || s.backgroundCtx == nil || s.backgroundCtx.Err() != nil {
		return
	}
	if a.transientRateLimitTimer != nil {
		a.transientRateLimitTimer.Stop()
	}
	until := a.CooldownUtil
	generation := a.transientRateLimitGeneration
	a.transientRateLimitTimer = time.AfterFunc(max(time.Until(until), 0), func() {
		if s.backgroundCtx.Err() != nil {
			return
		}
		s.mu.RLock()
		current := s.lookupByIDLocked(a.DBID) == a
		s.mu.RUnlock()
		if !current {
			return
		}
		a.mu.Lock()
		if !a.isTransientRateLimitCooldownLocked() || a.transientRateLimitGeneration != generation || !a.CooldownUtil.Equal(until) || time.Now().Before(until) {
			a.mu.Unlock()
			return
		}
		a.transientRateLimitTimer = nil
		a.recomputeSchedulerLocked(s.maxConcurrency.Load())
		a.mu.Unlock()
		s.fastSchedulerUpdate(a)
	})
}

// transientRateLimitRemainingLocked reports how long the current freeze still
// lasts when, and only when, the active cooldown is the one created by
// MarkTransientRateLimited and no quota window blocks the account underneath.
func (a *Account) transientRateLimitRemainingLocked(now time.Time) (time.Duration, bool) {
	return a.transientRateLimitRemainingForPolicyLocked(now, DispatchPolicyStandard)
}

func (a *Account) transientRateLimitRemainingForPolicyLocked(now time.Time, policy DispatchPolicy) (time.Duration, bool) {
	if !a.isTransientRateLimitCooldownLocked() || !a.CooldownUtil.After(now) {
		return 0, false
	}
	if policy == DispatchPolicySpark {
		if a.sparkUsageExhaustedLocked(now) {
			return 0, false
		}
	} else if a.usageWindowBlocksFreshDispatchLocked(now) {
		return 0, false
	}
	return a.CooldownUtil.Sub(now), true
}

// TransientRateLimitRemaining is the exported, locking form of
// transientRateLimitRemainingLocked.
func (a *Account) TransientRateLimitRemaining(now time.Time) (time.Duration, bool) {
	return a.transientRateLimitRemainingForPolicy(now, DispatchPolicyStandard)
}

func (a *Account) transientRateLimitRemainingForPolicy(now time.Time, policy DispatchPolicy) (time.Duration, bool) {
	if a == nil {
		return 0, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.transientRateLimitRemainingForPolicyLocked(now, policy)
}

func (a *Account) observeTransientRateLimitSuccessLocked(now time.Time) {
	if a == nil || a.transientRateLimitBackoff == 0 {
		return
	}
	if a.Status == StatusCooldown && a.CooldownUtil.After(now) {
		return
	}
	if a.LastRateLimitedAt.IsZero() || now.Sub(a.LastRateLimitedAt) < transientRateLimitStableReset {
		return
	}
	a.transientRateLimitBackoff = 0
}

// TransientRateLimitBackoff reports the in-memory throttle backoff exponent.
func (a *Account) TransientRateLimitBackoff() int {
	if a == nil {
		return 0
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.transientRateLimitBackoff
}
