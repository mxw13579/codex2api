package database

import (
	"context"
	"encoding/json"
	ic "github.com/codex2api/internal/intelligencecheck"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestIntelligenceCheckClaimRefreshesQueuedAndBusySnapshots(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := intelligenceAccount(t, db)
	ids, e := db.EnqueueIntelligenceChecks(ctx, []int64{id}, now)
	if e != nil {
		t.Fatal(e)
	}
	s, _ := db.GetIntelligenceCheckSettings(ctx)
	s.GPTModel = "gpt-6-sol"
	s.ReasoningEffort = "low"
	s.TimeoutSeconds = 300
	s.Questions[0].Prompt = "updated prompt"
	s.Questions[0].ExpectedAnswer = "42"
	s, e = db.SaveIntelligenceCheckSettings(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if e = db.SetAccountEnabled(ctx, id, false); e != nil {
		t.Fatal(e)
	}
	r, e := db.ClaimIntelligenceCheck(ctx, "first", now.Add(time.Second), time.Second)
	if e != nil || r == nil {
		t.Fatal(r, e)
	}
	if r.ID != ids[0] || r.ConfigRevision != s.Revision || r.Model != s.GPTModel || r.Question.Prompt != "updated prompt" || r.ReasoningEffort != "low" || r.ManualRevision != 1 || r.Settings.TimeoutSeconds != 300 {
		t.Fatalf("queued snapshot was not refreshed: %+v", r)
	}
	if e = db.RequeueIntelligenceCheck(ctx, r.ID, "first", now.Add(2*time.Second)); e != nil {
		t.Fatal(e)
	}
	s.GPTModel = "gpt-6.1-sol"
	s.Questions[0].ExpectedAnswer = "84"
	s, e = db.SaveIntelligenceCheckSettings(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if e = db.SetAccountEnabled(ctx, id, false); e != nil {
		t.Fatal(e)
	}
	second, e := db.ClaimIntelligenceCheck(ctx, "second", now.Add(10*time.Second), time.Second)
	if e != nil || second == nil {
		t.Fatal(second, e)
	}
	if second.ID != r.ID || !second.CreatedAt.Equal(r.CreatedAt) || second.ConfigRevision != s.Revision || second.ManualRevision != 2 || second.Model != s.GPTModel || second.Question.ExpectedAnswer != "84" {
		t.Fatalf("busy requeue retained stale snapshot: %+v", second)
	}
	if ok, e := db.FinishIntelligenceCheck(ctx, r.ID, "first", ic.Observation{Complete: true, Output: "84"}, now.Add(11*time.Second)); e != nil || ok {
		t.Fatal("previous claimant finalized", ok, e)
	}
	// A new save after dispatch must not rewrite an actually running request.
	s.GPTModel = "gpt-6-astra"
	if _, e = db.SaveIntelligenceCheckSettings(ctx, s); e != nil {
		t.Fatal(e)
	}
	stored, e := db.GetIntelligenceCheckRun(ctx, second.ID)
	if e != nil || stored.Model != "gpt-6.1-sol" || stored.ConfigRevision != second.ConfigRevision {
		t.Fatal(stored.Model, e)
	}
	if ok, e := db.FinishIntelligenceCheck(ctx, second.ID, "second", ic.Observation{Complete: true, Output: "84"}, now.Add(12*time.Second)); e != nil || !ok {
		t.Fatal(ok, e)
	}
	stored, e = db.GetIntelligenceCheckRun(ctx, second.ID)
	if e != nil || stored.Status != "normal" || stored.ConfigRevision != second.ConfigRevision {
		t.Fatal(stored.Status, e)
	}
}
func TestIntelligenceCheckBarsAvoidHistoryBodyAllocations(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	ids := []int64{intelligenceAccount(t, db), intelligenceAccount(t, db)}
	body := strings.Repeat("x", 256*1024)
	var first int64
	for _, id := range ids {
		for i := 0; i < 22; i++ {
			r := IntelligenceCheckRun{AccountID: id, Status: "normal", Question: ic.Question{Name: "summary question"}, Observation: ic.Observation{Output: body, DurationMS: 123}, CompletedAt: &now}
			raw, _ := json.Marshal(r)
			var runID int64
			e := db.conn.QueryRowContext(ctx, `INSERT INTO intelligence_check_runs(account_id,status,run_json,created_ms,ready_ms,completed_ms) VALUES($1,'normal',$2,$3,$3,$3) RETURNING id`, id, string(raw), now.UnixMilli()).Scan(&runID)
			if e != nil {
				t.Fatal(e)
			}
			if first == 0 {
				first = runID
			}
		}
	}
	// Reconstruct the previous schema before upgrading its untouched history.
	if _, e := db.conn.ExecContext(ctx, `DROP INDEX IF EXISTS idx_intelligence_bars`); e != nil {
		t.Fatal(e)
	}
	for _, column := range []string{"question_name", "duration_ms", "summary_version"} {
		if _, e := db.conn.ExecContext(ctx, `ALTER TABLE intelligence_check_runs DROP COLUMN `+column); e != nil {
			t.Fatal(e)
		}
	}
	// Exercise backfill of pre-summary history and its repeated-startup path.
	if e := db.ensureIntelligenceCheckSchema(ctx); e != nil {
		t.Fatal(e)
	}
	if e := db.ensureIntelligenceCheckSchema(ctx); e != nil {
		t.Fatal(e)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	bars, e := db.IntelligenceCheckBars(ctx, ids)
	runtime.ReadMemStats(&after)
	if e != nil {
		t.Fatal(e)
	}
	for _, id := range ids {
		if len(bars[id].Records) != 20 {
			t.Fatal(len(bars[id].Records))
		}
		for _, r := range bars[id].Records {
			if r.QuestionName != "summary question" || r.DurationMS != 123 || r.CompletedAt == nil || !r.CompletedAt.Equal(now) {
				t.Fatal(r)
			}
		}
	}
	t.Logf("bars allocated %d bytes for forty summaries", after.TotalAlloc-before.TotalAlloc)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4*1024*1024 {
		t.Fatalf("summary request allocated %d bytes for forty tiny summaries; history bodies were loaded", allocated)
	}
	stored, e := db.GetIntelligenceCheckRun(ctx, first)
	if e != nil || stored.Output != body {
		t.Fatal("summary migration modified historical detail", e)
	}
}

func TestIntelligenceCheckBarsTrackActiveRun(t *testing.T) {
	db := intelligenceDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := intelligenceAccount(t, db)
	ids, err := db.EnqueueIntelligenceChecks(ctx, []int64{id}, now)
	if err != nil {
		t.Fatal(err)
	}
	check := func(status string, activeID int64, records int) {
		t.Helper()
		bars, err := db.IntelligenceCheckBars(ctx, []int64{id})
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(bars[id])
		var bar struct {
			ActiveRunID  int64  `json:"active_run_id"`
			ActiveStatus string `json:"active_status"`
		}
		if err := json.Unmarshal(raw, &bar); err != nil {
			t.Fatal(err)
		}
		if bar.ActiveRunID != activeID || bar.ActiveStatus != status || len(bars[id].Records) != records {
			t.Fatalf("unexpected bar: %s", raw)
		}
	}
	check("queued", ids[0], 0)
	run, err := db.ClaimIntelligenceCheck(ctx, "bars-test", now.Add(time.Second), time.Minute)
	if err != nil || run == nil {
		t.Fatal(run, err)
	}
	check("running", ids[0], 0)
	ok, err := db.FinishIntelligenceCheck(ctx, run.ID, "bars-test", ic.Observation{Complete: true, Output: "21"}, now.Add(2*time.Second))
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	check("", 0, 1)
}

func TestIntelligenceCheckBarsAllowManualAfterDisablingAutomatic(t *testing.T) {
	for _, disable := range []string{"global", "account", "dispatch"} {
		t.Run(disable, func(t *testing.T) {
			db := intelligenceDB(t)
			ctx := context.Background()
			now := time.Now().UTC()
			id := intelligenceAccount(t, db)
			settings, err := db.GetIntelligenceCheckSettings(ctx)
			if err != nil {
				t.Fatal(err)
			}
			settings.Enabled = true
			settings, err = db.SaveIntelligenceCheckSettings(ctx, settings)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := db.EnqueueDueIntelligenceChecks(ctx, now, 1); err != nil || n != 1 {
				t.Fatal(n, err)
			}
			check := func(status string) int64 {
				t.Helper()
				bars, err := db.IntelligenceCheckBars(ctx, []int64{id})
				if err != nil {
					t.Fatal(err)
				}
				bar := bars[id]
				if bar.ActiveStatus != status || (bar.ActiveRunID == 0) != (status == "") {
					t.Fatalf("expected active status %q, got %+v", status, bar)
				}
				return bar.ActiveRunID
			}
			setEnabled := func(enabled bool) {
				t.Helper()
				switch disable {
				case "global":
					settings.Enabled = enabled
					settings, err = db.SaveIntelligenceCheckSettings(ctx, settings)
				case "account":
					err = db.SetIntelligenceCheckEnabled(ctx, []int64{id}, enabled)
				default:
					err = db.SetAccountEnabled(ctx, id, enabled)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			queuedID := check("queued")
			setEnabled(false)
			check("") // The manual action must be available while automatic work is paused.
			setEnabled(true)
			if check("queued") != queuedID {
				t.Fatal("re-enabling should retain the queued task")
			}
			setEnabled(false)
			ids, err := db.EnqueueIntelligenceChecks(ctx, []int64{id}, now)
			if err != nil || len(ids) != 1 || ids[0] != queuedID {
				t.Fatalf("manual promotion: %v %v", ids, err)
			}
			check("queued") // Manual queues remain active despite either automatic switch.
			run, err := db.ClaimIntelligenceCheck(ctx, "bars-test", now, time.Minute)
			if err != nil || run == nil || run.Source != "manual" {
				t.Fatal(run, err)
			}
			check("running")
			if ok, err := db.FinishIntelligenceCheck(ctx, run.ID, "bars-test", ic.Observation{Complete: true, Output: "21"}, now.Add(time.Second)); err != nil || !ok {
				t.Fatal(ok, err)
			}
			setEnabled(true)
			later := now.Add(time.Hour)
			if n, err := db.EnqueueDueIntelligenceChecks(ctx, later, 1); err != nil || n != 1 {
				t.Fatal(n, err)
			}
			run, err = db.ClaimIntelligenceCheck(ctx, "bars-test", later, time.Minute)
			if err != nil || run == nil || run.Source != "automatic" {
				t.Fatal(run, err)
			}
			setEnabled(false)
			check("running") // Disabling automation must not hide an in-flight request.
		})
	}
}
