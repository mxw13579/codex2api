package auth

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/database"
)

func newPinnedDispatchTestStore(t *testing.T) *Store {
	t.Helper()
	store := NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1, TestConcurrency: 1})
	t.Cleanup(store.Stop)
	return store
}

// An external scheduler owns cooldowns, pauses and concurrency, so a pinned
// acquisition must succeed where the internal scheduler refuses the account,
// and the slot it takes must stay visible to the scheduler until released.
func TestAcquirePinnedAccountIgnoresSchedulingGates(t *testing.T) {
	store := newPinnedDispatchTestStore(t)
	acc := &Account{DBID: 7, AccessToken: "at-7", PlanType: "plus"}
	store.AddAccount(acc)
	store.MarkCooldown(acc, time.Hour, "rate_limited")
	store.MarkModelCooldown(acc, "gpt-5.5", time.Hour, "rate_limited")
	atomic.StoreInt32(&acc.IntelligencePaused, 1)
	acc.OccupiedRequests.Store(1)
	acc.ActiveRequests.Store(1)

	if got := store.TakePreferredAccountWithFilter(7, 0, nil, nil); got != nil {
		store.Release(got)
		t.Fatal("internal scheduler selected an account in cooldown at its concurrency cap; the test no longer exercises the bypass")
	}

	got, failure := store.AcquirePinnedAccount(7, 0, nil)
	if failure != PinnedAccountAvailable || got != acc {
		t.Fatalf("AcquirePinnedAccount = (%v, %q), want the pinned account", got, failure)
	}
	if active, occupied := acc.ActiveRequests.Load(), acc.OccupiedRequests.Load(); active != 2 || occupied != 2 {
		t.Fatalf("active/occupied = %d/%d, want 2/2 while the pinned slot is held", active, occupied)
	}
	store.Release(got)
	if active, occupied := acc.ActiveRequests.Load(), acc.OccupiedRequests.Load(); active != 1 || occupied != 1 {
		t.Fatalf("active/occupied = %d/%d after release, want 1/1", active, occupied)
	}
}

func TestAcquirePinnedAccountRefusesUnusableAccounts(t *testing.T) {
	cases := []struct {
		name     string
		id       int64
		apiKeyID int64
		setup    func(*Store, *Account)
		filter   AccountFilter
		want     PinnedAccountFailure
	}{
		{name: "missing account", id: 404, want: PinnedAccountNotFound},
		{
			name:     "account outside the API key scope looks missing",
			id:       1,
			apiKeyID: 5,
			setup:    func(_ *Store, acc *Account) { acc.SetAllowedAPIKeyIDs([]int64{99}) },
			want:     PinnedAccountNotFound,
		},
		{
			name:  "administrator disabled",
			id:    1,
			setup: func(store *Store, acc *Account) { store.ApplyAccountEnabled(acc.DBID, false) },
			want:  PinnedAccountDisabled,
		},
		{
			name:  "credential rejected with 401",
			id:    1,
			setup: func(_ *Store, acc *Account) { atomic.StoreInt32(&acc.Disabled, 1) },
			want:  PinnedAccountCredentialUnavailable,
		},
		{
			name:  "unauthorized cooldown",
			id:    1,
			setup: func(store *Store, acc *Account) { store.MarkCooldown(acc, time.Hour, "unauthorized") },
			want:  PinnedAccountCredentialUnavailable,
		},
		{
			name: "no access token",
			id:   1,
			setup: func(_ *Store, acc *Account) {
				acc.mu.Lock()
				acc.AccessToken = ""
				acc.mu.Unlock()
			},
			want: PinnedAccountCredentialUnavailable,
		},
		{
			name:   "capability filter rejects the account",
			id:     1,
			filter: func(*Account) bool { return false },
			want:   PinnedAccountUnsupported,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newPinnedDispatchTestStore(t)
			acc := &Account{DBID: 1, AccessToken: "at-1", PlanType: "plus"}
			store.AddAccount(acc)
			if tc.setup != nil {
				tc.setup(store, acc)
			}
			got, failure := store.AcquirePinnedAccount(tc.id, tc.apiKeyID, tc.filter)
			if got != nil || failure != tc.want {
				t.Fatalf("AcquirePinnedAccount = (%v, %q), want (nil, %q)", got, failure, tc.want)
			}
			if active, occupied := acc.ActiveRequests.Load(), acc.OccupiedRequests.Load(); active != 0 || occupied != 0 {
				t.Fatalf("active/occupied = %d/%d after a refusal, want no held slot", active, occupied)
			}
		})
	}
}
