package database

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAPIKeyLimitsKeepWindow(t *testing.T) {
	cases := []struct {
		name   string
		limits APIKeyLimits
		want   time.Duration
	}{
		{"none", APIKeyLimits{}, 0},
		{"model request limits use counters", APIKeyLimits{ModelRequestLimits: []APIKeyModelRequestLimit{{}}}, 0},
		{"rpm", APIKeyLimits{RPM: 10}, time.Minute},
		{"rpd", APIKeyLimits{RPD: 10}, 24 * time.Hour},
		{"daily cost", APIKeyLimits{CostLimitDaily: 1}, 24 * time.Hour},
		{"5h token", APIKeyLimits{TokenLimit5h: 1}, 5 * time.Hour},
		{"7d beats 5h", APIKeyLimits{CostLimit5h: 1, CostLimit7d: 1}, 7 * 24 * time.Hour},
		{"30d token", APIKeyLimits{TokenLimit30d: 1}, 30 * 24 * time.Hour},
		{"scope 1d requests", APIKeyLimits{ScopeLimits: []APIKeyScopeLimit{{Requests1d: 5}}}, 24 * time.Hour},
		{"scope 30d cost", APIKeyLimits{ScopeLimits: []APIKeyScopeLimit{{Cost5h: 1}, {Cost30d: 1}}}, 30 * 24 * time.Hour},
		{"scope cumulative quota only", APIKeyLimits{ScopeLimits: []APIKeyScopeLimit{{QuotaCost: 1, MaxConcurrency: 2}}}, 0},
	}
	for _, tc := range cases {
		if got := apiKeyLimitsKeepWindow(tc.limits); got != tc.want {
			t.Errorf("%s: got %s want %s", tc.name, got, tc.want)
		}
	}
}

func TestUsageLogRowBudget(t *testing.T) {
	avg := 1400.0
	budget, perRow := usageLogRowBudget(1<<30, avg)
	if perRow != int64((avg+usageLogTupleOverhead)*usageLogIndexOverhead) {
		t.Fatalf("perRow = %d", perRow)
	}
	if budget != (1<<30)/perRow {
		t.Fatalf("budget = %d", budget)
	}
	if b, p := usageLogRowBudget(100, -1000); p != 1 || b != 100 {
		t.Fatalf("degenerate width must not divide by zero: %d %d", b, p)
	}
}

func TestPruneUsageLogsSkipsSQLiteAndDisabled(t *testing.T) {
	db, err := New("sqlite", t.TempDir()+"/retention.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if r, err := db.PruneUsageLogs(ctx, UsageLogRetentionPolicy{}, time.Now()); err != nil || r.Skipped != "disabled" {
		t.Fatalf("disabled: %+v %v", r, err)
	}
	if r, err := db.PruneUsageLogs(ctx, UsageLogRetentionPolicy{MaxAge: time.Hour}, time.Now()); err != nil || r.Skipped != "sqlite" {
		t.Fatalf("sqlite: %+v %v", r, err)
	}
}

// 使用独立 schema 的 PostgreSQL 集成测试，结束后删除 schema。
func TestPruneUsageLogsPostgres(t *testing.T) {
	db := newIsolatedPostgresTestDB(t, "retention")
	ctx := context.Background()
	now := time.Now()

	// reset 写入 total 行：old 行在 3 天前，其余在 1 小时前；返回 (最小 id, 最大 id)。
	reset := func(old, recent int) (int64, int64) {
		t.Helper()
		mustExec(t, db, `DELETE FROM usage_logs`)
		mustExec(t, db, `INSERT INTO usage_logs (account_id, created_at)
			SELECT 1, $1::timestamptz FROM generate_series(1, $2)`, now.Add(-72*time.Hour), old)
		mustExec(t, db, `INSERT INTO usage_logs (account_id, created_at)
			SELECT 1, $1::timestamptz FROM generate_series(1, $2)`, now.Add(-time.Hour), recent)
		var lo, hi int64
		if err := db.conn.QueryRow(`SELECT MIN(id), MAX(id) FROM usage_logs`).Scan(&lo, &hi); err != nil {
			t.Fatal(err)
		}
		return lo, hi
	}
	// setProgress 把两份汇总进度都设为 statsID / hourlyID。
	setProgress := func(statsID, hourlyID int64) {
		t.Helper()
		mustExec(t, db, `INSERT INTO usage_stats_rollup_state (id, initialized, last_log_id, updated_at)
			VALUES (1, 1, $1, CURRENT_TIMESTAMP)
			ON CONFLICT (id) DO UPDATE SET initialized = 1, last_log_id = excluded.last_log_id`, statsID)
		mustExec(t, db, `INSERT INTO usage_log_hourly_state (id, version, ready, backfill_to_id, backfill_cursor, last_log_id, updated_at)
			VALUES (1, $1, 1, 0, 0, $2, CURRENT_TIMESTAMP)
			ON CONFLICT (id) DO UPDATE SET version = excluded.version, ready = 1, last_log_id = excluded.last_log_id`,
			usageHourlyRollupVersion, hourlyID)
	}
	count := func() (n int64, lo int64) {
		t.Helper()
		if err := db.conn.QueryRow(`SELECT COUNT(*), COALESCE(MIN(id), 0) FROM usage_logs`).Scan(&n, &lo); err != nil {
			t.Fatal(err)
		}
		return
	}
	prune := func(p UsageLogRetentionPolicy) UsageLogPruneResult {
		t.Helper()
		r, err := db.PruneUsageLogs(ctx, p, now)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	t.Run("age deletes only old rows", func(t *testing.T) {
		_, hi := reset(60, 40)
		setProgress(hi, hi)
		r := prune(UsageLogRetentionPolicy{MaxAge: 48 * time.Hour})
		if n, _ := count(); n != 40 || r.Deleted != 60 {
			t.Fatalf("remaining=%d deleted=%d (%+v)", n, r.Deleted, r)
		}
	})

	t.Run("never deletes rows the rollups have not consumed", func(t *testing.T) {
		lo, hi := reset(60, 40)
		setProgress(lo+29, hi)
		r := prune(UsageLogRetentionPolicy{MaxAge: 48 * time.Hour})
		if n, min := count(); r.Deleted != 30 || n != 70 || min != lo+30 {
			t.Fatalf("deleted=%d remaining=%d min=%d lo=%d (%+v)", r.Deleted, n, min, lo, r)
		}
		setProgress(0, hi)
		if r := prune(UsageLogRetentionPolicy{MaxAge: time.Hour}); r.Skipped != "rollups not ready" || r.Deleted != 0 {
			t.Fatalf("not-ready rollups must skip: %+v", r)
		}
	})

	t.Run("size cap keeps newest rows", func(t *testing.T) {
		_, hi := reset(60, 40)
		setProgress(hi, hi)
		var avg float64
		if err := db.conn.QueryRow(`SELECT AVG(pg_column_size(u.*)) FROM (SELECT * FROM usage_logs ORDER BY id DESC LIMIT 2000) u`).Scan(&avg); err != nil {
			t.Fatal(err)
		}
		_, perRow := usageLogRowBudget(0, avg)
		r := prune(UsageLogRetentionPolicy{MaxBytes: perRow*25 + perRow/2})
		if n, min := count(); n != 25 || min != hi-24 {
			t.Fatalf("remaining=%d min=%d hi=%d (%+v)", n, min, hi, r)
		}
	})

	t.Run("api key window limits protect rows", func(t *testing.T) {
		_, hi := reset(60, 40)
		setProgress(hi, hi)
		id, err := db.InsertAPIKeyWithOptions(ctx, APIKeyInput{Name: "retention", Key: "sk-retention-" + uuid.NewString(),
			Limits: APIKeyLimits{CostLimit7d: 10}})
		if err != nil {
			t.Fatal(err)
		}
		defer db.DeleteAPIKey(ctx, id)
		r := prune(UsageLogRetentionPolicy{MaxAge: 48 * time.Hour, MaxBytes: 1})
		if n, _ := count(); n != 100 || r.KeepWindow != 7*24*time.Hour {
			t.Fatalf("7d limit must keep 3-day-old rows: remaining=%d (%+v)", n, r)
		}
	})
}

// newIsolatedPostgresTestDB 在 CODEX2API_TEST_POSTGRES_DSN 指向的库里建一个随机 schema，
// 测试结束后删除。
func newIsolatedPostgresTestDB(t *testing.T, prefix string) *DB {
	t.Helper()
	base := os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
	if base == "" {
		t.Skip("CODEX2API_TEST_POSTGRES_DSN is not set")
	}
	schema := prefix + "_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	// 与 config.DatabaseConfig.DSN 一致：search_path 通过连接启动参数作用于连接池里的每条连接。
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	dsn := base + sep + "options=" + url.QueryEscape("-c search_path="+schema+",public")
	db, err := New("postgres", dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.conn.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		db.Close()
	})
	return db
}

func mustExec(t *testing.T, db *DB, query string, args ...any) {
	t.Helper()
	if _, err := db.conn.Exec(query, args...); err != nil {
		t.Fatalf("%s: %v", strings.Fields(query)[0], err)
	}
}
