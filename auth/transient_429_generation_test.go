package auth

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/codex2api/cache"
	"github.com/codex2api/database"
)

func newTransient429PolicyTestDB(t *testing.T) *database.DB {
	t.Helper()
	t.Setenv("CODEX_SCHEDULER_ENGINE", "legacy")
	db, err := database.New("sqlite", filepath.Join(t.TempDir(), "policy.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func commitTransient429Policy(t *testing.T, db *database.DB, enabled bool) database.CodexTransient429CooldownPolicy {
	t.Helper()
	policy, err := db.UpdateCodexTransient429CooldownPolicy(context.Background(), enabled)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func newTransient429PolicyTestStore(t *testing.T, db *database.DB, tc cache.TokenCache) (*Store, *Account) {
	t.Helper()
	settings, err := db.GetSystemSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(db, tc, settings)
	t.Cleanup(s.Stop)
	acc := newFastSchedulerTestAccount(91, HealthTierHealthy, 100, 4)
	s.AddAccount(acc)
	return s, acc
}

func TestTransient429SharedPolicyReenableReplacesOldLongFreeze(t *testing.T) {
	db := newTransient429PolicyTestDB(t)
	initial := commitTransient429Policy(t, db, true)
	tc := cache.NewMemory(1)
	t.Cleanup(func() { _ = tc.Close() })
	a, aa := newTransient429PolicyTestStore(t, db, tc)
	b, bb := newTransient429PolicyTestStore(t, db, tc)
	a.MarkTransientRateLimited(aa, 4*time.Minute)
	if !b.accountHasCachedCooldown(bb) {
		t.Fatal("fixture: initial freeze was not shared")
	}
	off := commitTransient429Policy(t, db, false)
	a.SetCodexTransient429CooldownPolicy(off)
	b.SetCodexTransient429CooldownPolicy(off)
	on := commitTransient429Policy(t, db, true)
	a.SetCodexTransient429CooldownPolicy(on)
	b.SetCodexTransient429CooldownPolicy(on)
	if on.Generation <= initial.Generation {
		t.Fatal("fixture: database policy did not advance")
	}
	if got := a.MarkTransientRateLimited(aa, 0); got <= 0 || got > 16*time.Second {
		t.Fatalf("new freeze reused old deadline: %v", got)
	}
	if !b.accountHasCachedCooldown(bb) {
		t.Fatal("new short freeze lost to the old long cached record")
	}
	assertTransient429AccountRevision(t, bb, on.Generation, 1)
	_, deadline := bb.GetCooldownSnapshot()
	if time.Until(deadline) > 16*time.Second {
		t.Fatal("peer retained the previous generation's longer freeze")
	}
}

func TestTransient429SharedPolicyLateEnableConsumerAcceptsEarlierRecord(t *testing.T) {
	db := newTransient429PolicyTestDB(t)
	commitTransient429Policy(t, db, false)
	tc := cache.NewMemory(1)
	t.Cleanup(func() { _ = tc.Close() })
	a, aa := newTransient429PolicyTestStore(t, db, tc)
	b, bb := newTransient429PolicyTestStore(t, db, tc)
	on := commitTransient429Policy(t, db, true)
	a.SetCodexTransient429CooldownPolicy(on)
	a.MarkTransientRateLimited(aa, time.Minute)
	// b consumes after a has already published. No sleeps or independent
	// wall-clock cutoffs are needed to order this reproduction.
	if err := b.reloadSchedulerEngineSetting(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !b.accountHasCachedCooldown(bb) {
		t.Fatal("late consumer rejected an earlier publication of the same DB policy")
	}
	assertTransient429AccountRevision(t, bb, on.Generation, 1)
}

func TestTransient429SharedPolicyCoalescedOffOnClearsLocalWindow(t *testing.T) {
	db := newTransient429PolicyTestDB(t)
	commitTransient429Policy(t, db, true)
	s, acc := newTransient429PolicyTestStore(t, db, nil)
	s.SetSchedulerEngine("indexed")
	s.MarkTransientRateLimited(acc, 4*time.Minute)
	commitTransient429Policy(t, db, false)
	on := commitTransient429Policy(t, db, true)
	// The outbox intentionally coalesces settings events into one latest read.
	// It must reset on -> on even though this replica never observed off.
	s.SetCodexTransient429CooldownPolicy(on)
	if acc.HasActiveCooldown() || acc.TransientRateLimitBackoff() != 0 {
		t.Fatal("coalescing off/on retained an old-generation local pause")
	}
	acc.mu.RLock()
	timer := acc.transientRateLimitTimer
	acc.mu.RUnlock()
	if timer != nil {
		t.Fatal("coalescing off/on retained the old recovery timer")
	}
	if got := s.Next(); got != acc {
		t.Fatal("generation reset did not restore indexed availability")
	} else {
		s.Release(got)
	}
}

func TestTransient429SharedPolicyRestartAndDelayedPublication(t *testing.T) {
	db := newTransient429PolicyTestDB(t)
	commitTransient429Policy(t, db, true)
	tc := cache.NewMemory(1)
	t.Cleanup(func() { _ = tc.Close() })
	a, aa := newTransient429PolicyTestStore(t, db, tc)
	a.MarkTransientRateLimited(aa, 4*time.Minute)
	old, ok := a.getCachedAccountCooldown(aa.DBID)
	if !ok {
		t.Fatal("fixture: no old record")
	}
	commitTransient429Policy(t, db, false)
	on := commitTransient429Policy(t, db, true)
	// A new Store has no local toggle history or time cutoff to rely on.
	restarted, acc := newTransient429PolicyTestStore(t, db, tc)
	if restarted.CodexTransient429CooldownPolicy() != on || restarted.accountHasCachedCooldown(acc) {
		t.Fatal("restart failed to fence the old shared freeze using the durable policy")
	}
	restarted.MarkTransientRateLimited(acc, 0)
	peer, peerAcc := newTransient429PolicyTestStore(t, db, tc)
	// Simulate a publication admitted before the toggle, completing its cache
	// operation after the new-generation publication (without invoking DB IO
	// from the request path). It must neither replace nor donate its ladder.
	merged, err := tc.(cache.RuntimeCooldownMerger).MergeRuntimeCooldown(context.Background(), accountCooldownCacheNamespace, accountCooldownRuntimeKey(acc.DBID), old)
	if err != nil {
		t.Fatal(err)
	}
	restarted.applyCachedAccountCooldown(acc, old)
	if merged.PolicyGeneration != on.Generation || merged.BackoffLevel != 1 || !peer.accountHasCachedCooldown(peerAcc) {
		t.Fatal("delayed old publication defeated the current policy")
	}
	assertTransient429AccountRevision(t, acc, on.Generation, 1)
	assertTransient429AccountRevision(t, peerAcc, on.Generation, 1)
	// Even with no current cache entry, an old delayed write cannot resurrect
	// a pause on a freshly restarted consumer of the current DB policy.
	if err := tc.DeleteRuntime(context.Background(), accountCooldownCacheNamespace, accountCooldownRuntimeKey(acc.DBID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.(cache.RuntimeCooldownMerger).MergeRuntimeCooldown(context.Background(), accountCooldownCacheNamespace, accountCooldownRuntimeKey(acc.DBID), old); err != nil {
		t.Fatal(err)
	}
	latest, latestAcc := newTransient429PolicyTestStore(t, db, tc)
	if latest.accountHasCachedCooldown(latestAcc) || latestAcc.HasActiveCooldown() {
		t.Fatal("old publication resurrected a freeze after restart/cache eviction")
	}
}

func TestTransient429SharedPolicyStaleAndSameValueDoNotReset(t *testing.T) {
	db := newTransient429PolicyTestDB(t)
	old := commitTransient429Policy(t, db, true)
	s, acc := newTransient429PolicyTestStore(t, db, nil)
	off := commitTransient429Policy(t, db, false)
	on := commitTransient429Policy(t, db, true)
	s.SetCodexTransient429CooldownPolicy(on)
	s.MarkTransientRateLimited(acc, time.Minute)
	_, deadline := acc.GetCooldownSnapshot()
	for _, policy := range []database.CodexTransient429CooldownPolicy{
		old, off, {Enabled: false, Generation: on.Generation}, {Enabled: true, Generation: -1}, commitTransient429Policy(t, db, true),
	} {
		s.SetCodexTransient429CooldownPolicy(policy)
		_, got := acc.GetCooldownSnapshot()
		if s.CodexTransient429CooldownPolicy() != on || !got.Equal(deadline) {
			t.Fatalf("stale/conflicting/same policy reset a current freeze: %+v", policy)
		}
	}
	// The bool-only test convenience must not manufacture production revisions.
	s.SetCodexTransient429CooldownEnabled(false)
	if s.CodexTransient429CooldownPolicy() != on {
		t.Fatal("DB-backed Store accepted a process-local production generation")
	}
}

func TestTransient429PolicyMarkAndCacheApplyDuringSweep(t *testing.T) {
	for _, mode := range []string{"mark", "cache apply"} {
		t.Run(mode, func(t *testing.T) {
			db := newTransient429PolicyTestDB(t)
			commitTransient429Policy(t, db, true)
			s, blocker := newTransient429PolicyTestStore(t, db, nil)
			target := newFastSchedulerTestAccount(92, HealthTierHealthy, 100, 4)
			s.AddAccount(target)
			s.MarkTransientRateLimited(target, 4*time.Minute)
			commitTransient429Policy(t, db, false)
			on := commitTransient429Policy(t, db, true)

			// Hold the first account so Set publishes the new atomic pair but
			// cannot yet sweep the target's old window. Mark/apply must clear it
			// themselves rather than relabel the old deadline as new generation.
			blocker.mu.Lock()
			done := make(chan struct{})
			go func() { s.SetCodexTransient429CooldownPolicy(on); close(done) }()
			defer func() { blocker.mu.Unlock(); <-done }()
			waitForSchedulerProjection(t, func() bool { return s.CodexTransient429CooldownPolicy() == on })
			if mode == "mark" {
				if got := s.MarkTransientRateLimited(target, 0); got <= 0 || got > 16*time.Second {
					t.Fatalf("old long window was relabeled as current policy: %v", got)
				}
			} else {
				s.applyCachedAccountCooldown(target, runtimeCooldownRecord{
					Kind: cache.CooldownKindTransient, Reason: ResponsesRateLimitedCooldownReason,
					ResetAt: time.Now().Add(15 * time.Second), PolicyGeneration: on.Generation, BackoffLevel: 1,
				})
			}
			assertTransient429AccountRevision(t, target, on.Generation, 1)
			_, deadline := target.GetCooldownSnapshot()
			if time.Until(deadline) > 16*time.Second {
				t.Fatal("new generation inherited the old long deadline")
			}
			// Deferred unlock lets Set finish. Check after it finishes as well,
			// using cleanup (after defers) so failures cannot strand the lock.
			t.Cleanup(func() { assertTransient429AccountRevision(t, target, on.Generation, 1) })
		})
	}
}

func TestTransient429InitRefreshesPolicyAfterCapturingWatermark(t *testing.T) {
	for _, initiallyEnabled := range []bool{false, true} {
		name := "enable before Init"
		if initiallyEnabled {
			name = "disable before Init"
		}
		t.Run(name, func(t *testing.T) {
			db := newTransient429PolicyTestDB(t)
			commitTransient429Policy(t, db, initiallyEnabled)
			snapshot, err := db.GetSystemSettings(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := commitTransient429Policy(t, db, !initiallyEnabled)
			// This commit is already at/below Init's watermark; the consumer
			// cannot discover it by polling events after that watermark.
			s := NewStore(db, nil, snapshot)
			t.Cleanup(s.Stop)
			if err := s.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := s.CodexTransient429CooldownPolicy(); got != want {
				t.Fatalf("startup retained pre-watermark settings: %+v, want %+v", got, want)
			}
			// Changes after startup still use the normal settings outbox path.
			later := commitTransient429Policy(t, db, initiallyEnabled)
			waitForSchedulerProjection(t, func() bool { return s.CodexTransient429CooldownPolicy() == later })
		})
	}
}

func TestTransient429PeriodicSettingsReconcileWithoutNewEvents(t *testing.T) {
	for _, transition := range []struct {
		name             string
		initiallyEnabled bool
		updates          []bool
	}{
		{"disable", true, []bool{false}},
		{"enable", false, []bool{true}},
		{"coalesced off on", true, []bool{false, true}},
	} {
		for _, pin := range []struct{ name, value, wantEngine string }{
			{"unpinned", "", "indexed"},
			{"pinned", "legacy", "legacy"},
		} {
			t.Run(transition.name+"/"+pin.name, func(t *testing.T) {
				ctx := context.Background()
				db := newTransient429PolicyTestDB(t)
				t.Setenv("CODEX_SCHEDULER_ENGINE", pin.value)
				initial := commitTransient429Policy(t, db, transition.initiallyEnabled)
				s, acc := newTransient429PolicyTestStore(t, db, nil)
				s.SetSchedulerEngine("legacy")
				if transition.initiallyEnabled {
					s.MarkTransientRateLimited(acc, 4*time.Minute)
				}
				_, oldDeadline := acc.GetCooldownSnapshot()

				settings, err := db.GetSystemSettings(ctx)
				if err != nil {
					t.Fatal(err)
				}
				settings.SchedulerEngine = "indexed"
				if err := db.UpdateSystemSettings(ctx, settings); err != nil {
					t.Fatal(err)
				}
				var want database.CodexTransient429CooldownPolicy
				for _, enabled := range transition.updates {
					want = commitTransient429Policy(t, db, enabled)
				}

				// Model a stale projection whose missing notification is at/below
				// the initial watermark, with no hole or newer event to consume.
				// This SQLite fixture does not reproduce PostgreSQL commit ordering.
				watermark, err := db.SchedulerOutboxHighWatermark(ctx)
				if err != nil {
					t.Fatal(err)
				}
				cursor := &schedulerOutboxCursor{watermark: watermark, holes: make(map[int64]time.Time)}
				if err := s.consumeSchedulerOutbox(ctx, cursor); err != nil {
					t.Fatal(err)
				}
				if s.CodexTransient429CooldownPolicy() != initial || s.GetSchedulerMetrics().OutboxEvents != 0 {
					t.Fatal("fixture: event polling unexpectedly corrected the stale policy")
				}

				// A failed periodic read retains the last valid state; a later
				// invocation of the same ticker helper must still be able to repair it.
				expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				if err := s.reconcileSchedulerSettings(expired); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expired reconcile error = %v, want deadline exceeded", err)
				}
				_, afterFailure := acc.GetCooldownSnapshot()
				if s.CodexTransient429CooldownPolicy() != initial || !afterFailure.Equal(oldDeadline) || s.SchedulerEngine() != "legacy" {
					t.Fatal("failed settings read changed the last valid projection")
				}
				if err := s.reconcileSchedulerSettings(ctx); err != nil {
					t.Fatal(err)
				}
				if s.CodexTransient429CooldownPolicy() != want || acc.HasActiveCooldown() || acc.TransientRateLimitBackoff() != 0 {
					t.Fatal("periodic settings reread did not correct the stale policy/window")
				}
				if got := s.SchedulerEngine(); got != pin.wantEngine {
					t.Fatalf("scheduler engine = %q, want %q", got, pin.wantEngine)
				}
				if want.Enabled {
					s.MarkTransientRateLimited(acc, time.Minute)
					_, deadline := acc.GetCooldownSnapshot()
					if err := s.reconcileSchedulerSettings(ctx); err != nil {
						t.Fatal(err)
					}
					assertTransient429AccountRevision(t, acc, want.Generation, 1)
					if _, got := acc.GetCooldownSnapshot(); !got.Equal(deadline) {
						t.Fatal("same-policy periodic reread reset a current freeze")
					}
				}
				if high, err := db.SchedulerOutboxHighWatermark(ctx); err != nil || high != watermark {
					t.Fatalf("reconciliation needed or produced a new event: watermark=%d, err=%v", high, err)
				}
			})
		}
	}
}

func TestTransient429SharedPolicyMissingGenerationAndStrongCooldowns(t *testing.T) {
	db := newTransient429PolicyTestDB(t)
	on := commitTransient429Policy(t, db, true)
	s, acc := newTransient429PolicyTestStore(t, db, nil)
	legacy := runtimeCooldownRecord{Kind: cache.CooldownKindTransient, Reason: ResponsesRateLimitedCooldownReason, ResetAt: time.Now().Add(time.Minute), BackoffLevel: 5}
	s.applyCachedAccountCooldown(acc, legacy)
	if acc.HasActiveCooldown() {
		t.Fatal("generationless transient revived after a durable policy change")
	}
	for _, reason := range []string{ResponsesRateLimitedCooldownReason, "usage_limit", "rate_limited_5h", "rate_limited_7d", "unauthorized"} {
		for _, kind := range []string{"", cache.CooldownKindTransient} {
			if kind == cache.CooldownKindTransient && reason == ResponsesRateLimitedCooldownReason {
				continue
			}
			target := newFastSchedulerTestAccount(int64(len(s.accountSnapshotAccounts())+100), HealthTierHealthy, 100, 4)
			s.AddAccount(target)
			record := legacy
			record.Kind, record.Reason = kind, reason
			s.applyCachedAccountCooldown(target, record)
			s.SetCodexTransient429CooldownPolicy(commitTransient429Policy(t, db, false))
			s.SetCodexTransient429CooldownPolicy(commitTransient429Policy(t, db, true))
			_, deadline := target.GetCooldownSnapshot()
			if !deadline.Equal(record.ResetAt) || target.GetCooldownReason() != reason {
				t.Fatal("policy change weakened an untyped/strong quota or auth cooldown")
			}
		}
	}
	if s.CodexTransient429CooldownPolicy().Generation <= on.Generation {
		t.Fatal("fixture: no policy changes exercised")
	}
}

func assertTransient429AccountRevision(t *testing.T, acc *Account, generation int64, backoff int) {
	t.Helper()
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	if !acc.isTransientRateLimitCooldownLocked() || acc.transientRateLimitGeneration != generation || acc.transientRateLimitBackoff != backoff {
		t.Fatalf("transient window generation/backoff = %d/%d, want %d/%d", acc.transientRateLimitGeneration, acc.transientRateLimitBackoff, generation, backoff)
	}
}
