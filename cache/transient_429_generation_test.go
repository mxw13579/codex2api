package cache

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"
)

// Called by both existing Memory and Redis fixtures; no external service or new
// dependency is introduced by this suite. Redis is run only by the test owner
// against its explicitly provisioned isolated fixture.
func testTransient429GenerationMerge(t *testing.T, c TokenCache) {
	t.Helper()
	ctx := context.Background()
	merger := c.(RuntimeCooldownMerger)
	now := time.Now().UTC()
	old := RuntimeCooldown{
		Kind: CooldownKindTransient, Reason: "responses_rate_limited",
		ResetAt: now.Add(4 * time.Minute), UpdatedAt: now, BackoffLevel: 5, PolicyGeneration: 1,
	}
	fresh := old
	fresh.PolicyGeneration = 3
	fresh.BackoffLevel = 1
	fresh.ResetAt = now.Add(15 * time.Second)
	// The current policy's record can carry an older wall clock on another
	// instance. UpdatedAt is evidence, not the policy ordering key.
	fresh.UpdatedAt = now.Add(-time.Minute)
	legacy := old
	legacy.PolicyGeneration = 0
	maxOld, maxFresh := old, fresh
	maxOld.PolicyGeneration, maxFresh.PolicyGeneration = math.MaxInt64-1, math.MaxInt64

	for _, test := range []struct {
		name       string
		old, fresh RuntimeCooldown
	}{
		{"ordinary", old, fresh},
		{"missing generation", legacy, fresh},
		{"int64 precision", maxOld, maxFresh},
	} {
		t.Run(test.name, func(t *testing.T) {
			ns, key := "transient-policy-test", t.Name()
			t.Cleanup(func() { _ = c.DeleteRuntime(ctx, ns, key) })
			for _, incoming := range []RuntimeCooldown{test.old, test.fresh, test.old, test.fresh} {
				if _, err := merger.MergeRuntimeCooldown(ctx, ns, key, incoming); err != nil {
					t.Fatal(err)
				}
			}
			got := readTestCooldown(t, c, ns, key)
			if got.PolicyGeneration != test.fresh.PolicyGeneration || !got.ResetAt.Equal(test.fresh.ResetAt) || got.BackoffLevel != 1 || got.ExpiresAtMS != test.fresh.ResetAt.UnixMilli() {
				t.Fatalf("old policy won or leaked backoff/TTL: %+v", got)
			}
			// Within the same generation retain the longest deadline and the
			// highest ladder step, independently of arrival order.
			same := test.fresh
			same.ResetAt = now.Add(30 * time.Second)
			same.BackoffLevel = 0
			if _, err := merger.MergeRuntimeCooldown(ctx, ns, key, same); err != nil {
				t.Fatal(err)
			}
			got = readTestCooldown(t, c, ns, key)
			if !got.ResetAt.Equal(same.ResetAt) || got.BackoffLevel != 1 {
				t.Fatalf("same-generation merge lost deadline/ladder: %+v", got)
			}
		})
	}

	t.Run("concurrent generations", func(t *testing.T) {
		ns, key := "transient-policy-test", t.Name()
		t.Cleanup(func() { _ = c.DeleteRuntime(ctx, ns, key) })
		var wg sync.WaitGroup
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				incoming := old
				if i%2 == 1 {
					incoming = fresh
					incoming.ResetAt = now.Add(time.Minute + time.Duration(i)*time.Second)
				}
				if _, err := merger.MergeRuntimeCooldown(ctx, ns, key, incoming); err != nil {
					t.Error(err)
				}
			}(i)
		}
		wg.Wait()
		got := readTestCooldown(t, c, ns, key)
		if got.PolicyGeneration != fresh.PolicyGeneration || got.BackoffLevel != 1 || !got.ResetAt.Equal(now.Add(time.Minute+23*time.Second)) {
			t.Fatalf("concurrent generations lost the current policy window: %+v", got)
		}
	})

	for _, reason := range []string{"usage_limit", "rate_limited_5h", "rate_limited_7d", "unauthorized"} {
		for _, kind := range []string{"", CooldownKindTransient} {
			t.Run("strong/"+reason+"/"+kind, func(t *testing.T) {
				ns, key := "transient-policy-test", t.Name()
				t.Cleanup(func() { _ = c.DeleteRuntime(ctx, ns, key) })
				strong := RuntimeCooldown{Kind: kind, Reason: reason, ResetAt: time.Now().UTC().Add(5 * time.Second), UpdatedAt: now}
				for _, incoming := range []RuntimeCooldown{old, strong, fresh, old} {
					if _, err := merger.MergeRuntimeCooldown(ctx, ns, key, incoming); err != nil {
						t.Fatal(err)
					}
				}
				got := readTestCooldown(t, c, ns, key)
				if got.Reason != strong.Reason || !got.ResetAt.Equal(strong.ResetAt) || got.PolicyGeneration != 0 || got.BackoffLevel != 0 {
					t.Fatalf("generation weakened quota/auth: %+v", got)
				}
			})
		}
	}
}
