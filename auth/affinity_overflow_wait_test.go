package auth

import (
	"testing"
	"time"

	"github.com/codex2api/cache"
)

func newOverflowWaitStore(t *testing.T) (*Store, *Account, *Account) {
	t.Helper()
	bound := &Account{DBID: 1, AccessToken: "tok-1"}
	fallback := &Account{DBID: 2, AccessToken: "tok-2"}
	tokenCache := cache.NewMemory(1)
	t.Cleanup(func() { tokenCache.Close() })
	store := (&Store{accounts: []*Account{bound, fallback}, tokenCache: tokenCache}).withMaxConcurrency(1)
	store.bindSessionAffinity("overflow-wait", bound, "")
	held := store.TakePreferredAccountWithDispatch(bound.DBID, 0, nil, nil, DispatchPolicyStandard)
	if held != bound {
		t.Fatalf("held account = %p, want bound %p", held, bound)
	}
	return store, bound, fallback
}

func TestAffinityOverflowWaitReturnsBoundAccountWhenSlotFrees(t *testing.T) {
	store, bound, fallback := newOverflowWaitStore(t)
	go func() {
		time.Sleep(80 * time.Millisecond)
		store.Release(bound)
	}()
	started := time.Now()
	selected, _, guard := store.NextForSessionWithDispatchGuardForProvider(
		SelectionOptions{Policy: DispatchPolicyStandard, AffinityOverflowWait: 2 * time.Second}, "overflow-wait", 0, nil, nil)
	if selected != bound {
		t.Fatalf("selected = %p, want bound %p (fallback %p)", selected, bound, fallback)
	}
	defer store.Release(selected)
	if guard.PreservesExisting() {
		t.Fatal("waiting on the bound account must not mark the request as a spillover")
	}
	if waited := time.Since(started); waited >= 2*time.Second {
		t.Fatalf("waited %s, expected to be woken by the release", waited)
	}
	if got := accountOccupiedRequests(fallback); got != 0 {
		t.Fatalf("fallback still holds %d slots; borrowed slot must be returned", got)
	}
}

func TestAffinityOverflowWaitFallsBackToBorrowAfterTimeout(t *testing.T) {
	store, bound, fallback := newOverflowWaitStore(t)
	defer store.Release(bound)
	started := time.Now()
	selected, _, guard := store.NextForSessionWithDispatchGuardForProvider(
		SelectionOptions{Policy: DispatchPolicyStandard, AffinityOverflowWait: 120 * time.Millisecond}, "overflow-wait", 0, nil, nil)
	if selected != fallback {
		t.Fatalf("selected = %p, want fallback %p after timeout", selected, fallback)
	}
	defer store.Release(selected)
	if !guard.PreservesExisting() {
		t.Fatal("borrow after timeout must preserve the original binding")
	}
	if waited := time.Since(started); waited < 100*time.Millisecond || waited > 2*time.Second {
		t.Fatalf("waited %s, want about the configured 120ms", waited)
	}
	requireSessionBinding(t, store, "overflow-wait", bound.DBID)
}

func TestAffinityOverflowWaitDisabledBorrowsImmediately(t *testing.T) {
	store, bound, fallback := newOverflowWaitStore(t)
	defer store.Release(bound)
	started := time.Now()
	selected, _, guard := store.NextForSessionWithDispatchGuardForProvider(
		SelectionOptions{Policy: DispatchPolicyStandard}, "overflow-wait", 0, nil, nil)
	if selected != fallback || !guard.PreservesExisting() {
		t.Fatalf("selected = %p guard=%v, want immediate borrow of %p", selected, guard.PreservesExisting(), fallback)
	}
	defer store.Release(selected)
	if waited := time.Since(started); waited > 50*time.Millisecond {
		t.Fatalf("disabled wait still blocked for %s", waited)
	}
}
