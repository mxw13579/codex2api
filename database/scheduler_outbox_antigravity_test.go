package database

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestSchedulerOutbox_TriggersOnQuotaAndEpochChange 覆盖 Spec §10.1 #33 的三组场景，
// SQLite 与 Postgres 各自执行一条触发器测试（PG 未配置隔离 DSN 时跳过）。
func TestSchedulerOutbox_TriggersOnQuotaAndEpochChange(t *testing.T) {
	for _, driver := range []string{"sqlite", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			dsn := filepath.Join(t.TempDir(), "outbox.db")
			if driver == "postgres" {
				dsn = os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("requires isolated CODEX2API_TEST_POSTGRES_DSN")
				}
			}
			db, err := New(driver, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()

			insert := func(name, creds string) int64 {
				res, err := db.conn.ExecContext(ctx,
					`INSERT INTO accounts (name, platform, type, credentials, status, enabled, locked, credential_generation) VALUES (?, 'antigravity', 'oauth', ?, 'active', 1, 0, 1)`,
					name, creds)
				if err != nil {
					t.Fatalf("insert account failed: %v", err)
				}
				id, lastIDErr := res.LastInsertId()
				if lastIDErr != nil {
					if scanErr := db.conn.QueryRowContext(ctx, `SELECT id FROM accounts WHERE name = ?`, name).Scan(&id); scanErr != nil {
						t.Fatalf("resolve id failed: %v", scanErr)
					}
				}
				return id
			}
			// 基线必须在场景开始前取：账号 INSERT 自身会产生 created 事件，
			// 同一账号的后续事件按 id 递增覆盖 map，因此最终值即最后一次事件类型。
			baseline := func() int64 {
				watermark, err := db.SchedulerOutboxHighWatermark(ctx)
				if err != nil {
					t.Fatalf("read watermark failed: %v", err)
				}
				return watermark
			}
			accountEventsAfter := func(watermark int64) map[int64]string {
				events, err := db.ListSchedulerOutboxEventsAfter(ctx, watermark, 100)
				if err != nil {
					t.Fatalf("list events failed: %v", err)
				}
				out := map[int64]string{}
				for _, e := range events {
					if e.EntityType == SchedulerEntityAccount {
						out[e.EntityID] = e.EventType
					}
				}
				return out
			}

			// 场景 1：仅 antigravity_quota 变化。
			base1 := baseline()
			quotaID := insert("ag-quota", `{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":1}`)
			if _, err := db.conn.ExecContext(ctx, `UPDATE accounts SET credentials = ? WHERE id = ?`,
				`{"upstream_type":"antigravity","antigravity_quota":"{\"schema_version\":2}","antigravity_quota_epoch":1}`, quotaID); err != nil {
				t.Fatalf("update quota failed: %v", err)
			}
			if ev := accountEventsAfter(base1); ev[quotaID] != "updated" {
				t.Fatalf("expected outbox event on antigravity_quota change, got %v", ev)
			}

			// 场景 2：仅 credential_generation 变化而 token 未变（验证 AFTER UPDATE OF 列清单）。
			base2 := baseline()
			genID := insert("ag-gen", `{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":1}`)
			if _, err := db.conn.ExecContext(ctx, `UPDATE accounts SET credential_generation = credential_generation + 1 WHERE id = ?`, genID); err != nil {
				t.Fatalf("bump generation failed: %v", err)
			}
			if ev := accountEventsAfter(base2); ev[genID] != "updated" {
				t.Fatalf("expected outbox event on credential_generation change, got %v", ev)
			}

			// 场景 3：仅 quota_epoch 变化而 quota 内容等价重写（clear 场景）。
			base3 := baseline()
			epochID := insert("ag-epoch", `{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":1}`)
			if _, err := db.conn.ExecContext(ctx, `UPDATE accounts SET credentials = ? WHERE id = ?`,
				`{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":2}`, epochID); err != nil {
				t.Fatalf("bump epoch failed: %v", err)
			}
			if ev := accountEventsAfter(base3); ev[epochID] != "updated" {
				t.Fatalf("expected outbox event on antigravity_quota_epoch change, got %v", ev)
			}
		})
	}
}
