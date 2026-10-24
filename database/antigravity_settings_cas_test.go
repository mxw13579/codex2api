package database

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSaveAntigravityConfigCAS_WholeJSONCompareAndSwap 覆盖整 JSON CAS 保存与过期快照拒绝语义。
func TestSaveAntigravityConfigCAS_WholeJSONCompareAndSwap(t *testing.T) {
	for _, driver := range []string{"sqlite", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			dsn := filepath.Join(t.TempDir(), "settings.db")
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

			v1 := `{"quota_refresh_enabled":true,"config_revision":1}`
			if err := db.SaveAntigravityConfig(ctx, v1); err != nil {
				t.Fatalf("seed v1 failed: %v", err)
			}

			v2 := `{"quota_refresh_enabled":true,"quota_gate_enabled":false,"config_revision":2}`
			applied, err := db.SaveAntigravityConfigCAS(ctx, v1, v2)
			if err != nil || !applied {
				t.Fatalf("expected CAS apply, applied=%v err=%v", applied, err)
			}

			// 基于过期快照的写入必须整包失败（0 行），不得覆盖 v2。
			stale := `{"quota_refresh_interval_minutes":3,"config_revision":3}`
			applied, err = db.SaveAntigravityConfigCAS(ctx, v1, stale)
			if err != nil {
				t.Fatalf("stale CAS returned error: %v", err)
			}
			if applied {
				t.Fatal("stale CAS must not apply")
			}
			got, err := db.GetAntigravityConfigRaw(ctx)
			if err != nil {
				t.Fatalf("read back failed: %v", err)
			}
			if !strings.Contains(got, `"quota_gate_enabled":false`) || strings.Contains(got, `"quota_refresh_interval_minutes":3`) {
				t.Fatalf("stale write leaked into stored config: %s", got)
			}
		})
	}
}
