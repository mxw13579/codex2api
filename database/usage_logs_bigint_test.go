package database

import (
	"context"
	"testing"
	"time"
)

func usageLogsIDTypes(t *testing.T, db *DB) (string, string) {
	t.Helper()
	var col, seq string
	if err := db.conn.QueryRow(`SELECT c.data_type, COALESCE(s.data_type, '')
		FROM information_schema.columns c
		LEFT JOIN information_schema.sequences s
		  ON s.sequence_schema = c.table_schema AND s.sequence_name = 'usage_logs_id_seq'
		WHERE c.table_schema = current_schema() AND c.table_name = 'usage_logs' AND c.column_name = 'id'`).Scan(&col, &seq); err != nil {
		t.Fatal(err)
	}
	return col, seq
}

func runUsageLogsBigint(t *testing.T, db *DB) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := ensureUsageLogsBigintID(ctx, conn); err != nil {
		t.Fatal(err)
	}
}

// makeLegacyUsageLogs 把迁移建好的表降回旧版 SERIAL(INT4) 主键，并写入 rows 行。
func makeLegacyUsageLogs(t *testing.T, db *DB, rows int) {
	t.Helper()
	// New 会异步跑一次 ensureUsageLogsBigintID；先等它结束，否则它可能在降级后、
	// 写入数据前看到空表并抢先升级，测试就测不到我们自己的调用。
	if !db.DrainBackgroundTasks(30 * time.Second) {
		t.Fatal("background migrations did not finish")
	}
	mustExec(t, db, `ALTER TABLE usage_logs ALTER COLUMN id TYPE INTEGER`)
	mustExec(t, db, `ALTER SEQUENCE usage_logs_id_seq AS INTEGER`)
	if col, seq := usageLogsIDTypes(t, db); col != "integer" || seq != "integer" {
		t.Fatalf("legacy setup: column=%s sequence=%s", col, seq)
	}
	if rows > 0 {
		mustExec(t, db, `INSERT INTO usage_logs (account_id) SELECT 1 FROM generate_series(1, $1)`, rows)
	}
}

func TestUsageLogsIDIsBigintOnFreshInstall(t *testing.T) {
	db := newIsolatedPostgresTestDB(t, "bigint_fresh")
	if col, seq := usageLogsIDTypes(t, db); col != "bigint" || seq != "bigint" {
		t.Fatalf("fresh install: column=%s sequence=%s", col, seq)
	}
}

func TestUsageLogsLegacySerialUpgradesWhenSmall(t *testing.T) {
	db := newIsolatedPostgresTestDB(t, "bigint_small")
	makeLegacyUsageLogs(t, db, 10)
	runUsageLogsBigint(t, db)
	if col, seq := usageLogsIDTypes(t, db); col != "bigint" || seq != "bigint" {
		t.Fatalf("small legacy table must upgrade: column=%s sequence=%s", col, seq)
	}
	// 序列与已有行保持连续，新行能越过 INT4 上限。
	mustExec(t, db, `SELECT setval('usage_logs_id_seq', 2147483647)`)
	var id int64
	if err := db.conn.QueryRow(`INSERT INTO usage_logs (account_id) VALUES (1) RETURNING id`).Scan(&id); err != nil || id != 2147483648 {
		t.Fatalf("insert past INT4 max: id=%d err=%v", id, err)
	}
	var n int64
	db.conn.QueryRow(`SELECT COUNT(*) FROM usage_logs`).Scan(&n)
	if n != 11 {
		t.Fatalf("rows lost during upgrade: %d", n)
	}
	runUsageLogsBigint(t, db) // idempotent
}

func TestUsageLogsLegacySerialSkippedWhenLarge(t *testing.T) {
	db := newIsolatedPostgresTestDB(t, "bigint_large")
	makeLegacyUsageLogs(t, db, usageLogsBigintMaxRows+1)
	runUsageLogsBigint(t, db)
	if col, _ := usageLogsIDTypes(t, db); col != "integer" {
		t.Fatalf("large legacy table must not be rewritten online: column=%s", col)
	}
}
