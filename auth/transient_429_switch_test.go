package auth

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/codex2api/cache"
	"github.com/codex2api/database"
)

func TestTransient429SwitchDefaultsOffAndDoesNotPause(t *testing.T) {
	tc := cache.NewMemory(1)
	defer tc.Close()
	s := NewStore(nil, tc, &database.SystemSettings{MaxConcurrency: 4})
	defer s.Stop()
	acc := newFastSchedulerTestAccount(1, HealthTierHealthy, 100, 4)
	s.AddAccount(acc)
	if s.CodexTransient429CooldownEnabled() {
		t.Fatal("new Store should use this fork's default-off setting")
	}
	for i := 0; i < 3; i++ {
		if got := s.MarkTransientRateLimited(acc, 2*time.Minute); got != 0 {
			t.Fatalf("disabled pause = %v, want zero even with Retry-After", got)
		}
	}
	if acc.HasActiveCooldown() || !acc.IsAvailable() || !acc.SparkDispatchEligible() {
		t.Fatal("disabled transient pause blocked the account")
	}
	acc.mu.RLock()
	if acc.LastRateLimitedAt.IsZero() || acc.LastFailureAt.IsZero() || acc.FailureStreak == 0 {
		t.Error("disabling the pause must retain failure/scoring evidence")
	}
	if acc.transientRateLimitTimer != nil || acc.transientRateLimitBackoff != 0 {
		t.Error("disabled pause created a timer or advanced the backoff ladder")
	}
	acc.mu.RUnlock()
	if _, ok, _ := tc.GetRuntime(context.Background(), accountCooldownCacheNamespace, accountCooldownRuntimeKey(acc.DBID)); ok {
		t.Fatal("disabled throttle wrote a runtime cooldown")
	}
	if len(s.boundaryProbeWakeCh) != 0 {
		t.Fatal("disabled throttle armed a quota probe")
	}
	if summary := s.UsageLimitedCandidateSummary(0, nil, nil, DispatchPolicyStandard); summary.Found {
		t.Fatalf("disabled throttle still reported as an exhausted pool: %+v", summary)
	}
}

func TestTransient429SwitchReleasesOnlyTransientAndRestoresIndex(t *testing.T) {
	s := newTransientRateLimitTestStore()
	defer s.Stop()
	s.SetSchedulerEngine("indexed")
	transient := newFastSchedulerTestAccount(1, HealthTierHealthy, 100, 4)
	s.AddAccount(transient)
	s.MarkTransientRateLimited(transient, 0)
	accounts := []*Account{transient}
	reasons := []string{ResponsesRateLimitedCooldownReason, "usage_limit", "rate_limited_5h", "rate_limited_7d", "unauthorized"}
	for i, reason := range reasons {
		acc := newFastSchedulerTestAccount(int64(i+2), HealthTierHealthy, 100, 4)
		s.AddAccount(acc)
		s.MarkCooldown(acc, time.Hour, reason)
		accounts = append(accounts, acc)
	}
	deadlines := make([]time.Time, len(accounts))
	for i, acc := range accounts {
		_, deadlines[i] = acc.GetCooldownSnapshot()
	}
	s.SetCodexTransient429CooldownEnabled(false)
	if transient.HasActiveCooldown() || !transient.IsAvailable() || transient.TransientRateLimitBackoff() != 0 {
		t.Fatal("active transient cooldown not released immediately")
	}
	transient.mu.RLock()
	if transient.transientRateLimitTimer != nil {
		t.Error("recovery timer survived disabling the switch")
	}
	transient.mu.RUnlock()
	for i, acc := range accounts[1:] {
		reason, deadline := acc.GetCooldownSnapshot()
		if reason != reasons[i] || !deadline.Equal(deadlines[i+1]) || !acc.HasActiveCooldown() {
			t.Fatalf("disabling transient pause altered stronger cooldown %q", reasons[i])
		}
		if got := s.MarkTransientRateLimited(acc, 0); got < 59*time.Minute {
			t.Fatalf("bare 429 shortened stronger cooldown %q: %v", reasons[i], got)
		}
	}
	selected := s.Next()
	if selected != transient {
		t.Fatalf("indexed scheduler did not restore transient account: %p", selected)
	}
	s.Release(selected)
}

func TestTransient429SwitchIgnoresSharedTransientWithoutDeletingIt(t *testing.T) {
	tc := cache.NewMemory(1)
	defer tc.Close()
	s := NewStore(nil, tc, &database.SystemSettings{MaxConcurrency: 4})
	defer s.Stop()
	for i, test := range []struct {
		name, kind, reason string
		blocked            bool
	}{
		{"transient", cache.CooldownKindTransient, ResponsesRateLimitedCooldownReason, false},
		{"untyped Responses quota", "", ResponsesRateLimitedCooldownReason, true},
		{"quota", "", "usage_limit", true},
		{"unauthorized", "", "unauthorized", true},
		{"unauthorized wins malformed kind", cache.CooldownKindTransient, "unauthorized", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			acc := newFastSchedulerTestAccount(int64(i+1), HealthTierHealthy, 100, 4)
			record := cache.RuntimeCooldown{Kind: test.kind, Reason: test.reason, ResetAt: time.Now().Add(time.Minute), UpdatedAt: time.Now(), BackoffLevel: 3}
			_, err := tc.(cache.RuntimeCooldownMerger).MergeRuntimeCooldown(context.Background(), accountCooldownCacheNamespace, accountCooldownRuntimeKey(acc.DBID), record)
			if err != nil {
				t.Fatal(err)
			}
			if got := s.accountHasCachedCooldown(acc); got != test.blocked {
				t.Fatalf("cached blocked = %t, want %t", got, test.blocked)
			}
			// Also exercise delayed publication's direct application path.
			s.applyCachedAccountCooldown(acc, record)
			if acc.HasActiveCooldown() != test.blocked {
				t.Fatal("direct cache application bypassed the switch or quota guard")
			}
			if _, ok, _ := tc.GetRuntime(context.Background(), accountCooldownCacheNamespace, accountCooldownRuntimeKey(acc.DBID)); !ok {
				t.Fatal("switch destructively removed a shared cache record")
			}
		})
	}
}

func TestTransient429SwitchFencesDelayedPublicationAcrossOffOn(t *testing.T) {
	tc := &delayedTransientCooldownCache{TokenCache: cache.NewMemory(1), entered: make(chan struct{}), resume: make(chan struct{})}
	defer tc.Close()
	acc := newFastSchedulerTestAccount(1, HealthTierHealthy, 100, 4)
	s := (&Store{accounts: []*Account{acc}, tokenCache: tc}).withMaxConcurrency(4)
	done := make(chan struct{})
	go func() { s.MarkTransientRateLimited(acc, 2*time.Minute); close(done) }()
	<-tc.entered
	s.SetCodexTransient429CooldownEnabled(false)
	s.SetCodexTransient429CooldownEnabled(true)
	close(tc.resume)
	<-done
	if acc.HasActiveCooldown() || s.accountHasCachedCooldown(acc) || acc.TransientRateLimitBackoff() != 0 {
		t.Fatal("old publication resurrected a pause after off/on")
	}
	// Use the underlying cache to avoid blocking a second publication in the fixture.
	s.tokenCache = tc.TokenCache
	if got := s.MarkTransientRateLimited(acc, 0); got < 14*time.Second || got > 16*time.Second {
		t.Fatalf("new throttle after re-enabling = %v, want the first 15s tier", got)
	}
	if !acc.HasActiveCooldown() || acc.TransientRateLimitBackoff() != 1 {
		t.Fatal("re-enabling did not restore the original throttle policy")
	}
}

func TestTransient429SwitchConcurrentToggleAndThrottle(t *testing.T) {
	s := newTransientRateLimitTestStore()
	defer s.Stop()
	acc := newFastSchedulerTestAccount(1, HealthTierHealthy, 100, 4)
	s.AddAccount(acc)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.MarkTransientRateLimited(acc, 0)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		s.SetCodexTransient429CooldownEnabled(i%2 == 0)
	}
	wg.Wait()
	s.SetCodexTransient429CooldownEnabled(false)
	if acc.HasActiveCooldown() || acc.TransientRateLimitBackoff() != 0 || !acc.IsAvailable() {
		t.Fatal("concurrent throttles left a pause after disabling")
	}
}

func TestTransient429SwitchReloadsAcrossInstancesWithPinnedEngine(t *testing.T) {
	t.Setenv("CODEX_SCHEDULER_ENGINE", "legacy")
	ctx := context.Background()
	db, err := database.New("sqlite", filepath.Join(t.TempDir(), "transient-switch.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.UpdateCodexTransient429CooldownEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	settings, err := db.GetSystemSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var stores []*Store
	var accounts []*Account
	for i := 0; i < 2; i++ {
		s := NewStore(db, nil, settings)
		defer s.Stop()
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if !s.CodexTransient429CooldownEnabled() {
			t.Fatal("startup ignored the persisted switch")
		}
		acc := newFastSchedulerTestAccount(91, HealthTierHealthy, 100, 4)
		s.AddAccount(acc)
		s.MarkTransientRateLimited(acc, 0)
		stores = append(stores, s)
		accounts = append(accounts, acc)
	}
	if err := db.UpdateCodexTransient429CooldownEnabled(ctx, false); err != nil {
		t.Fatal(err)
	}
	for i, s := range stores {
		acc := accounts[i]
		waitForSchedulerProjection(t, func() bool {
			return !s.CodexTransient429CooldownEnabled() && !acc.HasActiveCooldown()
		})
	}
	if err := db.UpdateCodexTransient429CooldownEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	for _, s := range stores {
		waitForSchedulerProjection(t, s.CodexTransient429CooldownEnabled)
	}
}
