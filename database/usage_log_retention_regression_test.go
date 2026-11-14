package database

import (
	"context"
	"slices"
	"testing"
	"time"
)

// PruneUsageLogs 本身仅支持 PG；这里在临时 SQLite 上执行生产 DELETE 常量，
// 再调用真实小时维护器。PG 专属锁、水位读取和体积估算由 PG 集成测试覆盖。
func TestUsageLogRetentionDeleteRegression(t *testing.T) {
	current := time.Now().UTC().Truncate(time.Hour)
	// 固定在小时中间，区分整点 -3h 与滚动 now -3h。
	now := current.Add(37 * time.Minute)
	hourlyBefore := current.Add(-usageHourlyVerifyHours * time.Hour)
	protectedHours := []time.Time{
		hourlyBefore.Add(-time.Second), hourlyBefore, hourlyBefore.Add(time.Minute),
		current.Add(-2*time.Hour + time.Minute), current.Add(-time.Hour + time.Minute),
		current.Add(time.Minute), now.Add(-72 * time.Hour),
	}
	oldRows := []time.Time{now.Add(-96 * time.Hour), now.Add(-72 * time.Hour), now.Add(-80 * time.Hour)}
	keyRows := []time.Time{
		now.Add(-3 * 24 * time.Hour), now.Add(-8 * 24 * time.Hour),
		now.Add(-7 * 24 * time.Hour), now.Add(-7*24*time.Hour - time.Second), now.Add(-9 * 24 * time.Hour),
	}
	cases := []struct {
		name       string
		times      []time.Time
		maxAge     time.Duration
		keepWindow time.Duration
		sizeCutRow int // 1-based；0 表示未触发体积裁剪。
		safeRows   int // 0 表示全部行已汇总。
		wantRows   []int
	}{
		{
			name:   "age preserves newer lower IDs outside hourly window",
			times:  []time.Time{now.Add(-10 * time.Hour), now.Add(-72 * time.Hour), now.Add(-73 * time.Hour)},
			maxAge: 48 * time.Hour, wantRows: []int{1, 3},
		},
		{
			name:   "age boundary is exclusive",
			times:  []time.Time{now.Add(-48 * time.Hour), now.Add(-48*time.Hour - time.Second), now.Add(-80 * time.Hour)},
			maxAge: 48 * time.Hour, wantRows: []int{1, 3},
		},
		{
			name:  "size protects three complete hours and current hour",
			times: protectedHours, sizeCutRow: len(protectedHours), wantRows: []int{2, 3, 4, 5, 6, 7},
		},
		{
			name:  "short age protects three complete hours and current hour",
			times: protectedHours, maxAge: time.Minute, wantRows: []int{2, 3, 4, 5, 6, 7},
		},
		{
			name:  "all expired keeps highest ID",
			times: oldRows, maxAge: 48 * time.Hour, wantRows: []int{3},
		},
		{
			name:  "sub-row size budget keeps highest ID",
			times: oldRows, sizeCutRow: len(oldRows), wantRows: []int{3},
		},
		{
			name: "age or size independently deletes eligible rows",
			times: []time.Time{
				now.Add(-10 * time.Hour), now.Add(-72 * time.Hour), now.Add(-12 * time.Hour),
				now.Add(-80 * time.Hour), now.Add(-10 * time.Hour),
			},
			maxAge: 48 * time.Hour, sizeCutRow: 1, wantRows: []int{3, 5},
		},
		{
			name:  "key window protects out of order rows from age",
			times: keyRows, maxAge: 48 * time.Hour, keepWindow: 7 * 24 * time.Hour, wantRows: []int{1, 3, 5},
		},
		{
			name:  "key window protects out of order rows from size and age",
			times: keyRows, maxAge: 48 * time.Hour, keepWindow: 7 * 24 * time.Hour,
			sizeCutRow: len(keyRows), wantRows: []int{1, 3, 5},
		},
		{
			name:  "unconsumed rows are protected",
			times: oldRows, maxAge: 48 * time.Hour, sizeCutRow: len(oldRows), safeRows: 1, wantRows: []int{2, 3},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := New("sqlite", t.TempDir()+"/retention-regression.db")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ids := resetUsageLogRetentionFixture(t, db, tc.times)
			before := usageLogRetentionHourlyTotals(t, db)
			keepBefore := hourlyBefore
			if keyBefore := now.Add(-tc.keepWindow); keyBefore.Before(keepBefore) {
				keepBefore = keyBefore
			}
			safeID := ids[len(ids)-1]
			if tc.safeRows > 0 {
				safeID = ids[tc.safeRows-1]
			}
			var sizeCut int64
			if tc.sizeCutRow > 0 {
				sizeCut = ids[tc.sizeCutRow-1]
			}
			var deleted int64
			for {
				// 小批次也验证保护条件在每批 DELETE 中生效；没有复制修复 SQL。
				res, err := db.conn.ExecContext(context.Background(), usageLogRetentionDeleteSQL,
					safeID, db.timeArg(keepBefore), tc.maxAge > 0, db.timeArg(now.Add(-tc.maxAge)), sizeCut, 2)
				if err != nil {
					t.Fatal(err)
				}
				n, err := res.RowsAffected()
				if err != nil {
					t.Fatal(err)
				}
				deleted += n
				if n < 2 {
					break
				}
			}
			if want := int64(len(ids) - len(tc.wantRows)); deleted != want {
				t.Fatalf("deleted=%d want=%d", deleted, want)
			}
			wantIDs := make([]int64, len(tc.wantRows))
			for i, row := range tc.wantRows {
				wantIDs[i] = ids[row-1]
			}
			assertUsageLogRetentionIDs(t, db, wantIDs)
			assertUsageLogRetentionHourlyUnchanged(t, db, before)
		})
	}
}

// 合成数据同时带 token/cost，检查维护后不仅请求数不丢，累计值也不变。
func resetUsageLogRetentionFixture(t *testing.T, db *DB, times []time.Time) []int64 {
	t.Helper()
	mustExec(t, db, `DELETE FROM usage_logs`)
	ids := make([]int64, len(times))
	for i, at := range times {
		if err := db.conn.QueryRow(`INSERT INTO usage_logs (account_id, status_code, total_tokens, user_billed, created_at)
			VALUES (1, 200, $1, $2, $3) RETURNING id`, (i+1)*10, float64(i+1)/4, db.timeArg(at)).Scan(&ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.RebuildUsageHourlyRollup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if totals := usageLogRetentionHourlyTotals(t, db); totals.requests != int64(len(times)) {
		t.Fatalf("fixture rollup requests=%d want=%d", totals.requests, len(times))
	}
	return ids
}

func assertUsageLogRetentionIDs(t *testing.T, db *DB, want []int64) {
	t.Helper()
	rows, err := db.conn.Query(`SELECT id FROM usage_logs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("remaining IDs=%v want=%v", got, want)
	}
}

type usageLogRetentionTotals struct {
	requests, tokens int64
	billed           float64
}

func usageLogRetentionHourlyTotals(t *testing.T, db *DB) usageLogRetentionTotals {
	t.Helper()
	var totals usageLogRetentionTotals
	if err := db.conn.QueryRow(`SELECT CAST(COALESCE(SUM(requests), 0) AS BIGINT),
		CAST(COALESCE(SUM(total_tokens), 0) AS BIGINT), COALESCE(SUM(user_billed), 0)
		FROM usage_log_hourly`).Scan(&totals.requests, &totals.tokens, &totals.billed); err != nil {
		t.Fatal(err)
	}
	return totals
}

func assertUsageLogRetentionHourlyUnchanged(t *testing.T, db *DB, want usageLogRetentionTotals) {
	t.Helper()
	ctx := context.Background()
	db.repairUsageHourly(ctx)
	db.verifyRecentUsageHourly(ctx)
	if got := usageLogRetentionHourlyTotals(t, db); got != want {
		t.Fatalf("hourly totals after maintenance=%+v want=%+v", got, want)
	}
	if !db.usageHourlyRollupUsable(ctx) {
		t.Fatal("hourly rollup became unusable after pruning and maintenance")
	}
}
