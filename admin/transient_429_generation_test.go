package admin

import (
	"context"
	"net/http"
	"testing"

	"github.com/codex2api/database"
)

func TestTransient429AdminPublishesCommittedRevisionNotLocalToggle(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	var policies []database.CodexTransient429CooldownPolicy
	for _, enabled := range []bool{true, false, true} {
		policy, err := db.UpdateCodexTransient429CooldownPolicy(context.Background(), enabled)
		if err != nil {
			t.Fatal(err)
		}
		policies = append(policies, policy)
	}
	want := policies[len(policies)-1]
	// This replica missed all three changes. A same-value admin save must use
	// the DB's current revision, not manufacture generation 1 locally.
	response := invokeTransient429SettingsAdmin(handler, http.MethodPut, `{"codex_transient_429_cooldown_enabled":true}`)
	assertTransient429AdminResponse(t, response, true)
	if got := handler.store.CodexTransient429CooldownPolicy(); got != want {
		t.Fatalf("admin published local/stale revision: %+v, want %+v", got, want)
	}
	settings, err := db.GetSystemSettings(context.Background())
	if err != nil || settings.CodexTransient429Policy() != want {
		t.Fatalf("same-value admin save advanced the generation: %v", err)
	}
	// A delayed publication from a previously committed request cannot undo
	// the newer committed pair that the admin endpoint has already published.
	for _, stale := range policies[:len(policies)-1] {
		handler.store.SetCodexTransient429CooldownPolicy(stale)
		if handler.store.CodexTransient429CooldownPolicy() != want {
			t.Fatal("delayed admin publication rolled policy back")
		}
	}
}
