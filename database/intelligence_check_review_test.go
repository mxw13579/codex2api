package database

import (
	"context"
	"database/sql"
	ic "github.com/codex2api/internal/intelligencecheck"
	"strings"
	"testing"
	"time"
)

func TestIntelligenceCheckManualGroupOwnershipIntents(t *testing.T) {
	edits := map[string]struct {
		enabled bool
		fn      func(*DB, int64, int64) error
	}{
		"single": {false, func(db *DB, id, g int64) error { return db.SetAccountGroups(context.Background(), id, []int64{g}) }},
		"batch_groups": {false, func(db *DB, id, g int64) error {
			return db.BatchSetAccountGroups(context.Background(), []int64{id}, []int64{g})
		}},
		"scheduler": {false, func(db *DB, id, g int64) error {
			return db.UpdateAccountSchedulerMetadata(context.Background(), id, OptionalNullInt64{}, OptionalNullInt64{}, OptionalBool{}, OptionalInt64Slice{}, OptionalStringSlice{}, OptionalInt64Slice{Set: true, Values: []int64{g}}, OptionalString{}, nil)
		}},
		"batch_metadata": {false, func(db *DB, id, g int64) error {
			_, e := db.BatchUpdateAccountMetadata(context.Background(), []int64{id}, BatchAccountMetadataUpdate{GroupIDs: OptionalInt64Slice{Set: true, Values: []int64{g}}})
			return e
		}},
		"batch_combined": {true, func(db *DB, id, g int64) error {
			_, e := db.BatchUpdateAccountMetadata(context.Background(), []int64{id}, BatchAccountMetadataUpdate{Enabled: OptionalBool{Set: true, Value: true}, GroupIDs: OptionalInt64Slice{Set: true, Values: []int64{g}}})
			return e
		}},
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			db := intelligenceDB(t)
			ctx := context.Background()
			now := time.Now().UTC()
			id := intelligenceAccount(t, db)
			group, e := db.CreateAccountGroup(ctx, "group", "", "", 0, 0, sql.NullInt64{})
			if e != nil {
				t.Fatal(e)
			}
			s, _ := db.GetIntelligenceCheckSettings(ctx)
			s.Action = "pause"
			s.DegradeThreshold = 1
			if _, e = db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
				t.Fatal(e)
			}
			intelligenceRun(t, db, id, now, "29")
			before, _, _ := db.ListIntelligenceCheckAccounts(ctx, IntelligenceCheckAccountFilter{AccountID: id})
			if _, e = db.EnqueueIntelligenceChecks(ctx, []int64{id}, now.Add(time.Minute)); e != nil {
				t.Fatal(e)
			}
			run, e := db.ClaimIntelligenceCheck(ctx, "worker", now.Add(time.Minute), time.Minute)
			if e != nil || run == nil {
				t.Fatal(run, e)
			}
			if e = edit.fn(db, id, group); e != nil {
				t.Fatal(e)
			}
			after, _, e := db.ListIntelligenceCheckAccounts(ctx, IntelligenceCheckAccountFilter{AccountID: id})
			if e != nil || len(after) != 1 {
				t.Fatal(after, e)
			}
			a := after[0]
			if a.QualityPaused == edit.enabled || a.DegradeStreak != 0 || a.RecoverStreak != 0 || a.ManualRevision != before[0].ManualRevision+1 {
				t.Fatalf("incorrect group intent result: %+v", a)
			}
			if !edit.enabled && a.OwnedAction != "pause" || edit.enabled && a.OwnedAction != "" {
				t.Fatal(a)
			}
			if _, e = db.FinishIntelligenceCheck(ctx, run.ID, "worker", ic.Observation{Complete: true, Output: "21"}, now.Add(61*time.Second)); e != nil {
				t.Fatal(e)
			}
			paused, _ := db.IsIntelligenceCheckPaused(ctx, id)
			if paused == edit.enabled {
				t.Fatal("stale run altered pause after manual group edit")
			}
			// A manual group write revokes restoration even when it repeats the target.
			db.SetAccountEnabled(ctx, id, true)
			db.SetAccountGroups(ctx, id, []int64{group})
			target, e := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
			if e != nil {
				t.Fatal(e)
			}
			s, _ = db.GetIntelligenceCheckSettings(ctx)
			s.Action = "switch_group"
			s.TargetGroupID = target
			if _, e = db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
				t.Fatal(e)
			}
			intelligenceRun(t, db, id, now.Add(2*time.Minute), "29")
			if e = edit.fn(db, id, target); e != nil {
				t.Fatal(e)
			}
			intelligenceRun(t, db, id, now.Add(3*time.Minute), "21")
			groups, e := db.GetAccountGroupIDs(ctx, id)
			if e != nil || len(groups) != 1 || groups[0] != target {
				t.Fatal(groups, e)
			}
		})
	}
}
func TestIntelligenceCheckTargetCompatibilityPreflight(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	codex := intelligenceAccount(t, db)
	claude, e := db.InsertAccountWithUpstream(ctx, "claude", "anthropic", "oauth", map[string]interface{}{"upstream_type": "claude"}, "")
	if e != nil {
		t.Fatal(e)
	}
	target, e := db.CreateAccountGroup(ctx, "codex", "", "", 0, 0, sql.NullInt64{})
	if e != nil {
		t.Fatal(e)
	}
	bad, e := db.CreateAccountGroup(ctx, "grok", "", "", 0, 0, sql.NullInt64{})
	if e != nil {
		t.Fatal(e)
	}
	grok := "grok"
	if e = db.UpdateAccountGroup(ctx, bad, nil, nil, nil, &UpdateAccountGroupOpts{Channel: &grok}); e != nil {
		t.Fatal(e)
	}
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	s.Action = "switch_group"
	s.TargetGroupID = bad
	if _, e = db.SaveIntelligenceCheckSettings(ctx, s); e == nil {
		t.Fatal("accepted unsupported target channel")
	}
	s.TargetGroupID = target
	if _, e = db.SaveIntelligenceCheckSettings(ctx, s); e == nil {
		t.Fatal("accepted target incompatible with enabled Claude detector")
	}
	if e = db.SetIntelligenceCheckEnabled(ctx, []int64{claude}, false); e != nil {
		t.Fatal(e)
	}
	s.Enabled = true
	s, e = db.SaveIntelligenceCheckSettings(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if e = db.ValidateIntelligenceCheckAccountSettings(ctx, s, "codex"); e != nil {
		t.Fatal(e)
	}
	if e = db.ValidateIntelligenceCheckAccountSettings(ctx, s, "claude"); e == nil {
		t.Fatal("Claude preflight accepted Codex target")
	}
	if e = db.SetIntelligenceCheckEnabled(ctx, []int64{claude}, true); e == nil {
		t.Fatal("enabled incompatible detector")
	}
	if _, e = db.EnqueueIntelligenceChecks(ctx, []int64{claude}, now); e == nil {
		t.Fatal("enqueued incompatible manual task")
	}
	if _, e = db.InsertAccountWithUpstream(ctx, "new claude", "anthropic", "oauth", map[string]interface{}{"upstream_type": "claude"}, ""); e != nil {
		t.Fatal(e)
	}
	if n, e := db.EnqueueDueIntelligenceChecks(ctx, now, 50); e != nil || n != 1 {
		t.Fatal("incompatible new account blocked compatible automatic tasks", n, e)
	}
	states, _, e := db.ListIntelligenceCheckAccounts(ctx, IntelligenceCheckAccountFilter{Channel: "claude"})
	if e != nil || len(states) != 2 || states[0].ConfigError != "" || states[1].ConfigError != "" || states[1].Enabled {
		t.Fatal("new incompatible account opted into detection", states, e)
	}
	r, e := db.ClaimIntelligenceCheck(ctx, "worker", now, time.Minute)
	if e != nil || r == nil || r.AccountID != codex {
		t.Fatal(r, e)
	}
	if e = db.DeleteAccountGroup(ctx, target); e != nil {
		t.Fatal(e)
	}
	if e = db.ValidateIntelligenceCheckAccountSettings(ctx, s, "codex"); e == nil {
		t.Fatal("deleted target passed preflight")
	}
}
func TestIntelligenceCheckClaudeSnapshotEffort(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	id, e := db.InsertAccountWithUpstream(ctx, "claude", "anthropic", "oauth", map[string]interface{}{"upstream_type": "claude"}, "")
	if e != nil {
		t.Fatal(e)
	}
	ids, e := db.EnqueueIntelligenceChecks(ctx, []int64{id}, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	run, e := db.GetIntelligenceCheckRun(ctx, ids[0])
	if e != nil || run.ReasoningEffort != "" || run.Settings.ReasoningEffort != "high" {
		t.Fatal(run, e)
	}
}
func TestIntelligenceCheckQuestionRotationAfterDisablingEarlierQuestion(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := intelligenceAccount(t, db)
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	q := s.Questions[0]
	s.Questions = nil
	for _, name := range []string{"A", "B", "C"} {
		q.ID = name
		q.Name = name
		s.Questions = append(s.Questions, q)
	}
	s, e := db.SaveIntelligenceCheckSettings(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	first := intelligenceRun(t, db, id, now, "21")
	if first.Question.ID != "A" {
		t.Fatal(first.Question)
	}
	s.Questions[0].Enabled = false
	s, e = db.SaveIntelligenceCheckSettings(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	second := intelligenceRun(t, db, id, now.Add(time.Minute), "21")
	if second.Question.ID != "B" {
		t.Fatal("disabled A caused next B to be skipped", second.Question)
	}
	s.Questions[0].Enabled = true
	s.Questions = append(s.Questions[:1], s.Questions[2])
	if _, e = db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
		t.Fatal(e)
	}
	third := intelligenceRun(t, db, id, now.Add(2*time.Minute), "21")
	if third.Question.ID != "C" {
		t.Fatal(third.Question)
	}
}
func TestIntelligenceCheckRestoreSkipsChangedGroupChannel(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := intelligenceAccount(t, db)
	old, _ := db.CreateAccountGroup(ctx, "old", "", "", 0, 0, sql.NullInt64{})
	target, _ := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
	db.SetAccountGroups(ctx, id, []int64{old})
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	s.Action = "switch_group"
	s.TargetGroupID = target
	s.DegradeThreshold = 1
	if _, e := db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
		t.Fatal(e)
	}
	intelligenceRun(t, db, id, now, "29")
	grok := "grok"
	if e := db.UpdateAccountGroup(ctx, old, nil, nil, nil, &UpdateAccountGroupOpts{Channel: &grok}); e != nil {
		t.Fatal(e)
	}
	run := intelligenceRun(t, db, id, now.Add(time.Minute), "21")
	groups, e := db.GetAccountGroupIDs(ctx, id)
	if e != nil || len(groups) != 0 {
		t.Fatal("restored incompatible original group", groups, e)
	}
	stored, e := db.GetIntelligenceCheckRun(ctx, run.ID)
	if e != nil || !strings.Contains(stored.ActionMessage, "incompatible") {
		t.Fatal(stored.ActionMessage, e)
	}
}
func TestIntelligenceCheckPendingRecoveryIgnoresNewTargetRemoval(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := intelligenceAccount(t, db)
	original, _ := db.CreateAccountGroup(ctx, "original", "", "", 0, 0, sql.NullInt64{})
	target, _ := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
	newTarget, _ := db.CreateAccountGroup(ctx, "new target", "", "", 0, 0, sql.NullInt64{})
	db.SetAccountGroups(ctx, id, []int64{original})
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	s.Enabled = true
	s.Action = "switch_group"
	s.TargetGroupID = target
	s.DegradeThreshold = 1
	s, e := db.SaveIntelligenceCheckSettings(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	intelligenceRun(t, db, id, now, "29")
	s.TargetGroupID = newTarget
	s, e = db.SaveIntelligenceCheckSettings(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if e = db.DeleteAccountGroup(ctx, newTarget); e != nil {
		t.Fatal(e)
	}
	if e = db.ValidateIntelligenceCheckAccountSettings(ctx, s, "codex", id); e != nil {
		t.Fatal("pending recovery blocked", e)
	}
	if n, e := db.EnqueueDueIntelligenceChecks(ctx, now.Add(10*time.Minute), 50); e != nil || n != 1 {
		t.Fatal("pending automatic recovery blocked", n, e)
	}
	r, e := db.ClaimIntelligenceCheck(ctx, "worker", now.Add(10*time.Minute), time.Minute)
	if e != nil || r == nil {
		t.Fatal(r, e)
	}
	if ok, e := db.FinishIntelligenceCheck(ctx, r.ID, "worker", ic.Observation{Complete: true, Output: "21"}, now.Add(601*time.Second)); e != nil || !ok {
		t.Fatal(ok, e)
	}
	groups, e := db.GetAccountGroupIDs(ctx, id)
	if e != nil || len(groups) != 1 || groups[0] != original {
		t.Fatal(groups, e)
	}
}
func TestIntelligenceCheckClaimRejectsRemovedTargetBeforeSending(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := intelligenceAccount(t, db)
	target, _ := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	s.Action = "switch_group"
	s.TargetGroupID = target
	if _, e := db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
		t.Fatal(e)
	}
	ids, e := db.EnqueueIntelligenceChecks(ctx, []int64{id}, now)
	if e != nil {
		t.Fatal(e)
	}
	if e = db.DeleteAccountGroup(ctx, target); e != nil {
		t.Fatal(e)
	}
	r, e := db.ClaimIntelligenceCheck(ctx, "worker", now, time.Minute)
	if e != nil || r != nil {
		t.Fatal("claim returned invalid target request", r, e)
	}
	stored, e := db.GetIntelligenceCheckRun(ctx, ids[0])
	if e != nil || stored.Status != "interrupted" || !strings.Contains(stored.Error, "target") {
		t.Fatal(stored.Status, stored.Error, e)
	}
}
func TestIntelligenceCheckRestorePreservesUnchangedLegacyMixedGroup(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id, e := db.InsertAccountWithUpstream(ctx, "legacy claude", "anthropic", "oauth", map[string]interface{}{"upstream_type": "claude"}, "")
	if e != nil {
		t.Fatal(e)
	}
	original, _ := db.CreateAccountGroup(ctx, "legacy mixed", "", "", 0, 0, sql.NullInt64{})
	target, _ := db.CreateAccountGroup(ctx, "claude target", "", "", 0, 0, sql.NullInt64{})
	claude := "claude"
	if e = db.UpdateAccountGroup(ctx, target, nil, nil, nil, &UpdateAccountGroupOpts{Channel: &claude}); e != nil {
		t.Fatal(e)
	}
	// Existing provider migration intentionally preserves these mixed memberships.
	if e = db.SetAccountGroups(ctx, id, []int64{original}); e != nil {
		t.Fatal(e)
	}
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	s.Action = "switch_group"
	s.TargetGroupID = target
	s.DegradeThreshold = 1
	if _, e = db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
		t.Fatal(e)
	}
	intelligenceRun(t, db, id, now, "29")
	intelligenceRun(t, db, id, now.Add(time.Minute), "21")
	groups, e := db.GetAccountGroupIDs(ctx, id)
	if e != nil || len(groups) != 1 || groups[0] != original {
		t.Fatal("unchanged legacy membership was dropped", groups, e)
	}
}
func TestIntelligenceCheckRunSnapshotOmitsUnrelatedQuestionBank(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	other := s.Questions[0]
	other.ID = "unrelated"
	other.Prompt = "unrelated secret-ish question should not be copied into this run"
	s.Questions = append(s.Questions, other)
	if _, e := db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
		t.Fatal(e)
	}
	id := intelligenceAccount(t, db)
	ids, e := db.EnqueueIntelligenceChecks(ctx, []int64{id}, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	run, e := db.GetIntelligenceCheckRun(ctx, ids[0])
	if e != nil || len(run.Settings.Questions) != 0 || run.Question.ID != "candy" {
		t.Fatal("snapshot includes unrelated questions", len(run.Settings.Questions), e)
	}
	var raw string
	if e = db.conn.QueryRowContext(ctx, `SELECT run_json FROM intelligence_check_runs WHERE id=$1`, ids[0]).Scan(&raw); e != nil || strings.Contains(raw, other.Prompt) {
		t.Fatal("stored payload contains unrelated prompt", e)
	}
}
