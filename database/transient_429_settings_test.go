package database

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteTransient429DefaultFalse(t *testing.T) {
	if (SystemSettings{}).CodexTransient429CooldownEnabled {
		t.Fatal("zero-value settings must disable transient 429 cooldowns")
	}

	path := filepath.Join(t.TempDir(), "transient-429-default.sqlite")
	// An existing empty file bypasses the test schema template so this test
	// exercises the real new-install schema, not just a copied fixture.
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	db := openTransient429SettingsTestDB(t, path)
	ctx := context.Background()
	settings, err := db.GetSystemSettings(ctx)
	if err != nil || settings != nil {
		t.Fatalf("empty settings must retain the nil/defaults contract: present=%t err=%v", settings != nil, err)
	}

	// A full save must use the schema default even when its snapshot says true.
	if err := db.UpdateSystemSettings(ctx, &SystemSettings{
		SiteName:                         "synthetic default",
		CodexTransient429CooldownEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	assertTransient429SettingsValue(t, db, false)

	if _, err := db.conn.ExecContext(ctx, `DELETE FROM system_settings WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.ExecContext(ctx, `INSERT INTO system_settings (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	assertTransient429SettingsValue(t, db, false)
}

func TestSQLiteTransient429RoundTripAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transient-429-roundtrip.sqlite")
	db := openTransient429SettingsTestDB(t, path)
	for _, enabled := range []bool{false, true, false} {
		// The first narrow save also tests upserting an absent settings row.
		if err := db.UpdateCodexTransient429CooldownEnabled(context.Background(), enabled); err != nil {
			t.Fatalf("set %t: %v", enabled, err)
		}
		assertTransient429SettingsValue(t, db, enabled)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = openTransient429SettingsTestDB(t, path)
		assertTransient429SettingsValue(t, db, enabled)
	}
}

func TestSQLiteTransient429FullSavePreservesSwitch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transient-429-stale.sqlite")
	db := openTransient429SettingsTestDB(t, path)
	ctx := context.Background()
	if err := db.UpdateSystemSettings(ctx, &SystemSettings{SiteName: "synthetic baseline", MaxConcurrency: 3}); err != nil {
		t.Fatal(err)
	}

	for _, enabled := range []bool{true, false} {
		stale, err := db.GetSystemSettings(ctx)
		if err != nil || stale == nil {
			t.Fatalf("read snapshot: present=%t err=%v", stale != nil, err)
		}
		if stale.CodexTransient429CooldownEnabled == enabled {
			t.Fatal("test requires a snapshot older than the narrow update")
		}
		if err := db.UpdateCodexTransient429CooldownEnabled(ctx, enabled); err != nil {
			t.Fatal(err)
		}
		afterNarrow := assertTransient429SettingsValue(t, db, enabled)
		if afterNarrow.SiteName != stale.SiteName || afterNarrow.MaxConcurrency != stale.MaxConcurrency {
			t.Fatal("narrow update changed unrelated settings")
		}

		stale.SiteName = "synthetic unrelated save"
		stale.MaxConcurrency++
		if err := db.UpdateSystemSettings(ctx, stale); err != nil {
			t.Fatal(err)
		}
		got := assertTransient429SettingsValue(t, db, enabled)
		if got.SiteName != stale.SiteName || got.MaxConcurrency != stale.MaxConcurrency {
			t.Fatal("unrelated settings were not saved")
		}
	}
}

func TestSQLiteTransient429UpgradeDefaultsFalse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transient-429-upgrade.sqlite")
	db := openTransient429SettingsTestDB(t, path)
	ctx := context.Background()
	if err := db.UpdateSystemSettings(ctx, &SystemSettings{SiteName: "synthetic legacy", MaxConcurrency: 7}); err != nil {
		t.Fatal(err)
	}
	// Emulate the immediately preceding schema, with an existing settings row.
	if _, err := db.conn.ExecContext(ctx, `ALTER TABLE system_settings DROP COLUMN codex_transient_429_cooldown_enabled`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTransient429SettingsTestDB(t, path)
	got := assertTransient429SettingsValue(t, db, false)
	if got.SiteName != "synthetic legacy" || got.MaxConcurrency != 7 {
		t.Fatal("migration changed unrelated settings")
	}
	if err := db.UpdateCodexTransient429CooldownEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTransient429SettingsTestDB(t, path)
	assertTransient429SettingsValue(t, db, true)
}

func TestSQLiteTransient429FailedWritePreservesValue(t *testing.T) {
	db := openTransient429SettingsTestDB(t, filepath.Join(t.TempDir(), "transient-429-failure.sqlite"))
	ctx := context.Background()
	for _, enabled := range []bool{false, true} {
		if err := db.UpdateCodexTransient429CooldownEnabled(ctx, enabled); err != nil {
			t.Fatal(err)
		}
		if _, err := db.conn.ExecContext(ctx, `CREATE TRIGGER reject_transient_429_update
			BEFORE UPDATE OF codex_transient_429_cooldown_enabled ON system_settings
			BEGIN SELECT RAISE(ABORT, 'synthetic switch write failure'); END`); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateCodexTransient429CooldownEnabled(ctx, !enabled); err == nil {
			t.Fatal("failed write must return an error")
		}
		assertTransient429SettingsValue(t, db, enabled)
		if _, err := db.conn.ExecContext(ctx, `DROP TRIGGER reject_transient_429_update`); err != nil {
			t.Fatal(err)
		}

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := db.UpdateCodexTransient429CooldownEnabled(cancelled, !enabled); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled write error = %v, want context.Canceled", err)
		}
		assertTransient429SettingsValue(t, db, enabled)
	}
}

func TestTransient429SetterRequiresDatabase(t *testing.T) {
	for _, db := range []*DB{nil, {}} {
		if err := db.UpdateCodexTransient429CooldownEnabled(context.Background(), true); err == nil {
			t.Fatal("uninitialized database must not report a successful save")
		}
	}
}

func openTransient429SettingsTestDB(t *testing.T, path string) *DB {
	t.Helper()
	db, err := New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Reopen tests explicitly close older handles; DB.Close is not idempotent.
		select {
		case <-db.logStop:
		default:
			_ = db.Close()
		}
	})
	return db
}

func assertTransient429SettingsValue(t *testing.T, db *DB, want bool) *SystemSettings {
	t.Helper()
	got, err := db.GetSystemSettings(context.Background())
	if err != nil || got == nil {
		t.Fatalf("read settings: present=%t err=%v", got != nil, err)
	}
	if got.CodexTransient429CooldownEnabled != want {
		t.Fatalf("transient 429 cooldown enabled = %t, want %t", got.CodexTransient429CooldownEnabled, want)
	}
	return got
}
