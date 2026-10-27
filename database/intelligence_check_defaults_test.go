package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIntelligenceCheckNewAccountDefaults(t *testing.T) {
	for _, action := range []string{"none", "pause", "switch_group"} {
		for _, channel := range []string{"codex", "claude"} {
			for _, defaultEnabled := range []bool{false, true} {
				name := action + "/" + channel + "/disabled"
				if defaultEnabled {
					name = action + "/" + channel + "/enabled"
				}
				t.Run(name, func(t *testing.T) {
					db := intelligenceDB(t)
					ctx := context.Background()
					target, err := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
					if err != nil {
						t.Fatal(err)
					}
					if err := db.UpdateAccountGroup(ctx, target, nil, nil, nil, &UpdateAccountGroupOpts{Channel: &channel}); err != nil {
						t.Fatal(err)
					}
					s, err := db.GetIntelligenceCheckSettings(ctx)
					if err != nil {
						t.Fatal(err)
					}
					s.Action, s.TargetGroupID, s.NewAccountsEnabled = action, target, defaultEnabled
					s, err = db.SaveIntelligenceCheckSettings(ctx, s)
					if err != nil {
						t.Fatal(err)
					}
					for _, account := range []struct{ platform, upstream, channel string }{
						{"openai", "", "codex"}, {"AnThRoPiC", "", "claude"}, {"openai", "ClAuDe", "claude"},
						{"xai", "grok", "grok"}, {"google", "antigravity", "antigravity"},
					} {
						id, err := db.InsertAccountWithUpstream(ctx, "fixture", account.platform, "oauth", map[string]interface{}{"upstream_type": account.upstream}, "")
						if err != nil {
							t.Fatal(err)
						}
						var enabled bool
						if err := db.conn.QueryRowContext(ctx, `SELECT enabled FROM intelligence_check_accounts WHERE account_id=$1`, id).Scan(&enabled); err != nil {
							t.Fatal(err)
						}
						want := defaultEnabled && (account.channel == "codex" || account.channel == "claude") && (action != "switch_group" || account.channel == channel)
						if enabled != want {
							t.Errorf("%s/%s enabled=%v want=%v", account.platform, account.upstream, enabled, want)
						}
					}
					s.IntervalSeconds++
					if _, err := db.SaveIntelligenceCheckSettings(ctx, s); err != nil {
						t.Fatalf("new accounts blocked settings save: %v", err)
					}
				})
			}
		}
	}
}

func TestIntelligenceCheckDefaultTriggerUpgrade(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	id := intelligenceAccount(t, db)
	target, err := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.GetIntelligenceCheckSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Action, s.TargetGroupID, s.DegradeThreshold = "switch_group", target, 1
	s, err = db.SaveIntelligenceCheckSettings(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	intelligenceRun(t, db, id, time.Now(), "29")
	var stateBefore string
	if err := db.conn.QueryRowContext(ctx, `SELECT state_json FROM intelligence_check_accounts WHERE account_id=$1`, id).Scan(&stateBefore); err != nil {
		t.Fatal(err)
	}
	// Install the legacy trigger to model an existing deployment before upgrade.
	if db.isSQLite() {
		if _, err := db.conn.ExecContext(ctx, `DROP TRIGGER intelligence_check_new_account`); err != nil {
			t.Fatal(err)
		}
		_, err = db.conn.ExecContext(ctx, `CREATE TRIGGER intelligence_check_new_account AFTER INSERT ON accounts BEGIN INSERT INTO intelligence_check_accounts(account_id,enabled) VALUES(NEW.id,COALESCE((SELECT new_accounts_enabled FROM intelligence_check_settings WHERE id=1),FALSE)) ON CONFLICT(account_id) DO NOTHING; END`)
	} else {
		_, err = db.conn.ExecContext(ctx, `CREATE OR REPLACE FUNCTION intelligence_check_new_account_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO intelligence_check_accounts(account_id,enabled) VALUES(NEW.id,COALESCE((SELECT new_accounts_enabled FROM intelligence_check_settings WHERE id=1),FALSE)) ON CONFLICT(account_id) DO NOTHING; RETURN NEW; END $$`)
	}
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := db.InsertAccountWithUpstream(ctx, "legacy claude", "anthropic", "oauth", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := db.ensureIntelligenceCheckSchema(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var stateAfter string
	if err := db.conn.QueryRowContext(ctx, `SELECT state_json FROM intelligence_check_accounts WHERE account_id=$1`, id).Scan(&stateAfter); err != nil {
		t.Fatal(err)
	}
	if stateBefore != stateAfter {
		t.Fatal("upgrade changed action ownership or recovery state")
	}
	if runs, total, err := db.ListIntelligenceCheckRuns(ctx, IntelligenceCheckRunFilter{AccountID: id}); err != nil || total != 1 || runs[0].Action != "switch_group" {
		t.Fatalf("history changed: %v %v", runs, err)
	}
	newID, err := db.InsertAccountWithUpstream(ctx, "new claude", "anthropic", "oauth", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	for accountID, want := range map[int64]bool{legacy: true, newID: false, id: true} {
		var enabled bool
		if err := db.conn.QueryRowContext(ctx, `SELECT enabled FROM intelligence_check_accounts WHERE account_id=$1`, accountID).Scan(&enabled); err != nil {
			t.Fatal(err)
		}
		if enabled != want {
			t.Fatalf("account %d enabled=%v want=%v", accountID, enabled, want)
		}
	}
	_, err = db.SaveIntelligenceCheckSettings(ctx, s)
	var conflict *IntelligenceCheckTargetConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, ErrIntelligenceCheckTargetGroup) || conflict.ConflictCount != 1 || conflict.Conflicts[0].AccountID != legacy || conflict.Conflicts[0].AccountName != "legacy claude" || conflict.Conflicts[0].Channel != "claude" || conflict.TargetGroupID != target || conflict.TargetGroupChannel != "codex" {
		t.Fatalf("missing legacy conflict details: %+v %v", conflict, err)
	}
	if err := db.SetIntelligenceCheckEnabled(ctx, []int64{legacy}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveIntelligenceCheckSettings(ctx, s); err != nil {
		t.Fatal(err)
	}
}

func TestIntelligenceCheckTargetConflictBatches(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	for i := 0; i < 201; i++ {
		if _, err := db.InsertAccountWithUpstream(ctx, fmt.Sprintf("claude-%d", i), "anthropic", "oauth", nil, ""); err != nil {
			t.Fatal(err)
		}
	}
	target, err := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.GetIntelligenceCheckSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Action, s.TargetGroupID = "switch_group", target
	for _, want := range []int{201, 1} {
		_, err := db.SaveIntelligenceCheckSettings(ctx, s)
		var conflict *IntelligenceCheckTargetConflictError
		if !errors.As(err, &conflict) || conflict.ConflictCount != want || len(conflict.Conflicts) != min(want, 200) {
			t.Fatalf("unexpected conflict batch: %+v %v", conflict, err)
		}
		ids := []int64{}
		for _, account := range conflict.Conflicts {
			ids = append(ids, account.AccountID)
		}
		if err := db.SetIntelligenceCheckEnabled(ctx, ids, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.SaveIntelligenceCheckSettings(ctx, s); err != nil {
		t.Fatal(err)
	}
}

func TestIntelligenceCheckNewAccountWithMissingTarget(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	target, err := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.GetIntelligenceCheckSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Action, s.TargetGroupID = "switch_group", target
	if _, err := db.SaveIntelligenceCheckSettings(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteAccountGroup(ctx, target); err != nil {
		t.Fatal(err)
	}
	id := intelligenceAccount(t, db)
	var enabled bool
	if err := db.conn.QueryRowContext(ctx, `SELECT enabled FROM intelligence_check_accounts WHERE account_id=$1`, id).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("account created with missing target should not opt into detection")
	}
}

func TestIntelligenceCheckNewAccountConcurrentSettings(t *testing.T) {
	db := intelligenceDB(t)
	if db.isSQLite() {
		t.Skip("PostgreSQL settings/enrollment locking; SQLite serializes writers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	target, err := db.CreateAccountGroup(ctx, "target", "", "", 0, 0, sql.NullInt64{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.GetIntelligenceCheckSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Action, s.TargetGroupID = "switch_group", target
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.intelligenceSettingsTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	s.Revision++
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intelligence_check_settings SET config_json=$1,revision=$2 WHERE id=1`, string(raw), s.Revision); err != nil {
		t.Fatal(err)
	}
	type result struct {
		id  int64
		err error
	}
	inserted := make(chan result, 1)
	go func() {
		id, err := db.InsertAccountWithUpstream(ctx, "concurrent claude", "anthropic", "oauth", nil, "")
		inserted <- result{id, err}
	}()
	select {
	case r := <-inserted:
		t.Fatalf("enrollment bypassed uncommitted settings: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-inserted:
		if r.err != nil {
			t.Fatal(r.err)
		}
		var enabled bool
		if err := db.conn.QueryRowContext(ctx, `SELECT enabled FROM intelligence_check_accounts WHERE account_id=$1`, r.id).Scan(&enabled); err != nil {
			t.Fatal(err)
		}
		if enabled {
			t.Fatal("enrollment used the previous action rather than the committed switch_group policy")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := db.SaveIntelligenceCheckSettings(ctx, s); err != nil {
		t.Fatalf("concurrent enrollment left a conflict: %v", err)
	}
}
