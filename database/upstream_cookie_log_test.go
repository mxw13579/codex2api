package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

const syntheticUpstreamCookieTrace = `{"entries":[{"event":"response","cookie":["synthetic_cookie=fixture"],"set_cookie":["synthetic_cookie=updated; Secure"]}],"truncated":false}`

func newUpstreamCookieDB(t *testing.T) *DB {
	t.Helper()
	db, err := New("sqlite", filepath.Join(t.TempDir(), "cookies.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestUpstreamCookieSQLiteMigrationAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	db, err := New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	for _, query := range []string{
		`ALTER TABLE system_settings DROP COLUMN codex_upstream_cookie_log_enabled`,
		`ALTER TABLE usage_logs DROP COLUMN upstream_cookie_trace`,
		`INSERT INTO system_settings (id) VALUES (1) ON CONFLICT DO NOTHING`,
		`INSERT INTO usage_logs (id, model, status_code) VALUES (41, 'fixture-model', 200)`,
	} {
		if _, err := legacy.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	db, err = New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	settings, err := db.GetSystemSettings(ctx)
	if err != nil || settings == nil || settings.CodexUpstreamCookieLogEnabled {
		t.Fatal("migration did not default diagnostics to disabled")
	}
	if trace, err := db.GetUsageLogUpstreamCookies(ctx, 41); err != nil || trace != "" {
		t.Fatal("legacy usage row did not acquire the empty default")
	}
	for table, column := range map[string]string{"system_settings": "codex_upstream_cookie_log_enabled", "usage_logs": "upstream_cookie_trace"} {
		var notNull int
		if err := db.conn.QueryRowContext(ctx, `SELECT "notnull" FROM pragma_table_info($1) WHERE name = $2`, table, column).Scan(&notNull); err != nil || notNull != 1 {
			t.Fatal("migrated column must be NOT NULL")
		}
	}
	if err := db.UpdateCodexUpstreamCookieLogEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	settings, err = db.GetSystemSettings(ctx)
	if err != nil || settings == nil || !settings.CodexUpstreamCookieLogEnabled {
		t.Fatal("diagnostic setting did not survive restart")
	}
}

func TestUpstreamCookieNarrowSettingsAndOutbox(t *testing.T) {
	db := newUpstreamCookieDB(t)
	ctx := context.Background()
	// Also exercises creating the first settings row through the narrow update.
	if err := db.UpdateCodexUpstreamCookieLogEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListSchedulerOutboxEventsAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		found = found || event.EntityType == SchedulerEntitySettings
	}
	if !found {
		t.Fatal("narrow setting update did not emit a settings outbox event")
	}
	stale := &SystemSettings{SiteName: "unrelated fixture update"}
	if err := db.UpdateSystemSettings(ctx, stale); err != nil {
		t.Fatal(err)
	}
	settings, err := db.GetSystemSettings(ctx)
	if err != nil || settings == nil || !settings.CodexUpstreamCookieLogEnabled || settings.SiteName != stale.SiteName {
		t.Fatal("unrelated bulk settings update reset the diagnostic flag")
	}
	if err := db.UpdateCodexUpstreamCookieLogEnabled(ctx, false); err != nil {
		t.Fatal(err)
	}
	settings, err = db.GetSystemSettings(ctx)
	if err != nil || settings == nil || settings.CodexUpstreamCookieLogEnabled || settings.SiteName != stale.SiteName {
		t.Fatal("narrow disable failed or changed unrelated settings")
	}
}

func TestUpstreamCookieBufferedRoundTripAndCleanup(t *testing.T) {
	db := newUpstreamCookieDB(t)
	ctx := context.Background()
	for _, trace := range []string{syntheticUpstreamCookieTrace, ""} {
		if err := db.InsertUsageLog(ctx, &UsageLogInput{Model: "fixture-model", StatusCode: 200, UpstreamCookieTrace: trace}); err != nil {
			t.Fatal("could not enqueue fixture usage")
		}
	}
	db.FlushUsageLogs()
	logs, err := db.ListRecentUsageLogs(ctx, 10)
	if err != nil || len(logs) != 2 {
		t.Fatal("fixture usage rows were not written")
	}
	for i, want := range []string{"", syntheticUpstreamCookieTrace} {
		got, err := db.GetUsageLogUpstreamCookies(ctx, logs[i].ID)
		if err != nil || got != want {
			t.Fatal("dedicated cookie read did not round trip")
		}
	}
	ordinary, err := json.Marshal(logs)
	if err != nil || strings.Contains(string(ordinary), "synthetic_cookie") || strings.Contains(string(ordinary), "upstream_cookie") {
		t.Fatal("ordinary usage list exposed cookie diagnostics")
	}
	if trace, err := db.GetUsageLogUpstreamCookies(ctx, 999999); !errors.Is(err, sql.ErrNoRows) || trace != "" {
		t.Fatal("missing row did not return sql.ErrNoRows")
	}
	id := logs[1].ID
	if err := db.ClearUsageLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetUsageLogUpstreamCookies(ctx, id); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("manual usage cleanup left cookie diagnostics behind")
	}
}

func TestUpstreamCookiePostgresInsertConstruction(t *testing.T) {
	db := newUpstreamCookieDB(t)
	ctx := context.Background()
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// SQLite supports numbered binds and RETURNING. Execute the actual PostgreSQL
	// VALUES builder locally to check column/argument alignment across two rows.
	ids, err := db.batchInsertLogsChunk(ctx, tx, []usageLogEntry{
		{Model: "fixture-one", StatusCode: 200, VideoSeconds: 17, UpstreamCookieTrace: syntheticUpstreamCookieTrace},
		{Model: "fixture-two", StatusCode: 502, VideoSeconds: 23, UpstreamCookieTrace: `{"entries":[],"truncated":true}`},
	})
	if err != nil || len(ids) != 2 {
		t.Fatal("PostgreSQL insert construction failed local execution")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{syntheticUpstreamCookieTrace, `{"entries":[],"truncated":true}`} {
		got, err := db.GetUsageLogUpstreamCookies(ctx, ids[i])
		if err != nil || got != want {
			t.Fatal("PostgreSQL builder misaligned the cookie column")
		}
		var seconds int
		if err := db.conn.QueryRowContext(ctx, `SELECT video_seconds FROM usage_logs WHERE id = $1`, ids[i]).Scan(&seconds); err != nil || seconds != []int{17, 23}[i] {
			t.Fatal("PostgreSQL builder changed the adjacent column")
		}
	}
}

func TestUpstreamCookieBoundsAndCorruptRead(t *testing.T) {
	db := newUpstreamCookieDB(t)
	ctx := context.Background()
	prefix, suffix := `{"entries":[{"cookie":"`, `"}],"truncated":false}`
	atLimit := prefix + strings.Repeat("x", maxUpstreamCookieTraceBytes-len(prefix)-len(suffix)) + suffix
	cases := []struct {
		name, trace string
		valid       bool
	}{
		{"at limit", atLimit, true},
		{"over limit", atLimit + " ", false},
		{"invalid JSON", `{"entries":[synthetic_cookie`, false},
		{"missing fields", `{}`, false},
		{"null", `null`, false},
		{"null entries", `{"entries":null,"truncated":false}`, false},
		{"primitive entry", `{"entries":[1],"truncated":false}`, false},
		{"null entry", `{"entries":[null],"truncated":false}`, false},
		{"wrong truncated type", `{"entries":[],"truncated":"false"}`, false},
		{"invalid UTF8", prefix + string([]byte{0xff}) + suffix, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.InsertUsageLog(ctx, &UsageLogInput{Model: "fixture-model", StatusCode: 200, UpstreamCookieTrace: tc.trace}); err != nil {
				t.Fatal("diagnostics prevented usage insertion")
			}
			db.FlushUsageLogs()
			var id int64
			if err := db.conn.QueryRowContext(ctx, `SELECT MAX(id) FROM usage_logs`).Scan(&id); err != nil {
				t.Fatal(err)
			}
			got, err := db.GetUsageLogUpstreamCookies(ctx, id)
			if err != nil || (tc.valid && got != tc.trace) || (!tc.valid && got != "") {
				t.Fatal("diagnostics were not kept or dropped atomically")
			}
			if !tc.valid {
				if _, err := db.conn.ExecContext(ctx, `UPDATE usage_logs SET upstream_cookie_trace = $1 WHERE id = $2`, tc.trace, id); err != nil {
					t.Fatal("could not seed corrupt fixture")
				}
				got, err := db.GetUsageLogUpstreamCookies(ctx, id)
				if !errors.Is(err, errInvalidUpstreamCookieTrace) || got != "" {
					t.Fatal("corrupt stored diagnostics were exposed")
				}
			}
		})
	}
}

func TestUpstreamCookieRespectsUsageLogMode(t *testing.T) {
	for _, mode := range []string{UsageLogModeFull, UsageLogModeErrors, UsageLogModeOff} {
		t.Run(mode, func(t *testing.T) {
			db := newUpstreamCookieDB(t)
			db.SetUsageLogConfig(mode, 200, 5)
			for _, status := range []int{200, 502} {
				if err := db.InsertUsageLog(context.Background(), &UsageLogInput{StatusCode: status, UpstreamCookieTrace: syntheticUpstreamCookieTrace}); err != nil {
					t.Fatal("could not queue fixture")
				}
			}
			db.FlushUsageLogs()
			var count int
			if err := db.conn.QueryRow(`SELECT COUNT(*) FROM usage_logs WHERE upstream_cookie_trace != ''`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != map[string]int{UsageLogModeFull: 2, UsageLogModeErrors: 1, UsageLogModeOff: 0}[mode] {
				t.Fatal("cookie storage bypassed usage_log_mode")
			}
		})
	}
}

func TestUpstreamCookieWriteErrorsHideValuesAndPreserveClassification(t *testing.T) {
	batch := []usageLogEntry{{UpstreamCookieTrace: syntheticUpstreamCookieTrace}}
	for _, original := range []error{
		&pgconn.PgError{Code: "23514", Message: syntheticUpstreamCookieTrace, Detail: syntheticUpstreamCookieTrace},
		&pgconn.PgError{Code: "08006", Message: syntheticUpstreamCookieTrace},
		errors.New(syntheticUpstreamCookieTrace),
	} {
		got := upstreamCookieLogWriteError(batch, original)
		if got == nil || strings.Contains(got.Error(), "synthetic_cookie") || isUsageLogDataError(got) != isUsageLogDataError(original) {
			t.Fatal("write error leaked diagnostics or changed retry classification")
		}
		var pgErr *pgconn.PgError
		if errors.As(got, &pgErr) && pgErr.Detail != "" {
			t.Fatal("write error retained raw PostgreSQL detail")
		}
	}
}
