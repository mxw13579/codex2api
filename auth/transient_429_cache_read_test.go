package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/cache"
	"github.com/codex2api/database"
)

// transient429BlockingGetRuntimeCache pauses after a successful cache read,
// before the caller can parse/check the captured snapshot. It deliberately
// returns that response even if ctx expires while paused, modeling a goroutine
// suspended after its cache I/O already completed.
type transient429BlockingGetRuntimeCache struct {
	cache.TokenCache
	key      string
	captured chan json.RawMessage
	resume   chan struct{}
	once     sync.Once
	deletes  atomic.Int64
}

func (c *transient429BlockingGetRuntimeCache) GetRuntime(ctx context.Context, namespace, key string) (json.RawMessage, bool, error) {
	payload, ok, err := c.TokenCache.GetRuntime(ctx, namespace, key)
	if err == nil && ok && namespace == accountCooldownCacheNamespace && key == c.key {
		c.once.Do(func() {
			c.captured <- append(json.RawMessage(nil), payload...)
			<-c.resume
		})
	}
	return payload, ok, err
}

func (c *transient429BlockingGetRuntimeCache) DeleteRuntime(ctx context.Context, namespace, key string) error {
	if namespace == accountCooldownCacheNamespace && key == c.key {
		c.deletes.Add(1)
	}
	return c.TokenCache.DeleteRuntime(ctx, namespace, key)
}

func TestTransient429CachedReadPreservesConcurrentPublication(t *testing.T) {
	t.Setenv("CODEX_SCHEDULER_ENGINE", "legacy")
	for _, staleKind := range []string{"expired transient", "malformed"} {
		for _, replacement := range []struct {
			name, kind, reason string
			generation         int64
		}{
			{"new generation transient", cache.CooldownKindTransient, ResponsesRateLimitedCooldownReason, 3},
			{"quota", "", "usage_limit", 0},
			{"auth", "", "unauthorized", 0},
		} {
			t.Run(staleKind+"/"+replacement.name, func(t *testing.T) {
				ctx := context.Background()
				const accountID int64 = 91
				key := accountCooldownRuntimeKey(accountID)
				tc := cache.NewMemory(1)
				t.Cleanup(func() { _ = tc.Close() })
				blocked := &transient429BlockingGetRuntimeCache{
					TokenCache: tc, key: key,
					captured: make(chan json.RawMessage, 1), resume: make(chan struct{}),
				}
				reader := NewStore(nil, blocked, &database.SystemSettings{
					MaxConcurrency:                      4,
					CodexTransient429CooldownEnabled:    true,
					CodexTransient429CooldownGeneration: 3,
				})
				t.Cleanup(reader.Stop)

				stale := json.RawMessage(`{"reset_at":`)
				if staleKind == "expired transient" {
					var err error
					stale, err = json.Marshal(runtimeCooldownRecord{
						Kind: cache.CooldownKindTransient, Reason: ResponsesRateLimitedCooldownReason,
						ResetAt: time.Now().Add(-time.Minute), PolicyGeneration: 1, BackoffLevel: 5,
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				// Keep the backend TTL live while the payload deadline is expired.
				// This deterministically exercises the post-read expiry branch,
				// without sleeping or relying on the wall clock to cross a deadline.
				if err := tc.SetRuntime(ctx, accountCooldownCacheNamespace, key, stale, time.Minute); err != nil {
					t.Fatal(err)
				}
				var staleReadOK bool
				done := make(chan struct{})
				go func() {
					_, staleReadOK = reader.getCachedAccountCooldown(accountID)
					close(done)
				}()
				resumed := false
				defer func() {
					if !resumed {
						close(blocked.resume)
					}
					<-done
				}()
				select {
				case captured := <-blocked.captured:
					if !bytes.Equal(captured, stale) {
						t.Fatal("fixture: reader did not capture the old cache value")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("reader did not reach the post-GetRuntime barrier")
				}

				// Another publisher replaces the key while the old read is paused.
				published, err := tc.(cache.RuntimeCooldownMerger).MergeRuntimeCooldown(ctx, accountCooldownCacheNamespace, key, runtimeCooldownRecord{
					Kind: replacement.kind, Reason: replacement.reason,
					ResetAt: time.Now().Add(time.Minute), PolicyGeneration: replacement.generation, BackoffLevel: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				want, ok, err := tc.GetRuntime(ctx, accountCooldownCacheNamespace, key)
				if err != nil || !ok || bytes.Equal(want, stale) {
					t.Fatalf("fixture: replacement was not published: found=%t, err=%v", ok, err)
				}
				close(blocked.resume)
				resumed = true
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("old cache read did not finish after release")
				}
				if staleReadOK {
					t.Fatal("expired/malformed snapshot was accepted as a cooldown")
				}
				if got := blocked.deletes.Load(); got != 0 {
					t.Errorf("snapshot validation issued %d unconditional cache deletes", got)
				}
				got, ok, err := tc.GetRuntime(ctx, accountCooldownCacheNamespace, key)
				if err != nil || !ok || !bytes.Equal(got, want) {
					t.Fatalf("old reader removed or changed the newer publication: found=%t, err=%v", ok, err)
				}
				latest, ok := reader.getCachedAccountCooldown(accountID)
				if !ok || latest.Kind != published.Kind || latest.Reason != published.Reason ||
					latest.PolicyGeneration != published.PolicyGeneration || !latest.ResetAt.Equal(published.ResetAt) {
					t.Fatal("new generation/quota/auth cooldown is no longer readable")
				}
			})
		}
	}
}
