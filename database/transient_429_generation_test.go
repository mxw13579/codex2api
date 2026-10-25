package database

import (
	"context"
	"math"
	"path/filepath"
	"sync"
	"testing"
)

func TestTransient429PolicyGenerationRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.sqlite")
	db := openTransient429SettingsTestDB(t, path)
	ctx := context.Background()
	for _, step := range []CodexTransient429CooldownPolicy{
		{false, 0}, {true, 1}, {true, 1}, {false, 2}, {false, 2}, {true, 3},
	} {
		got, err := db.UpdateCodexTransient429CooldownPolicy(ctx, step.Enabled)
		if err != nil || got != step {
			t.Fatalf("narrow policy = %+v, %v; want %+v", got, err, step)
		}
		settings := assertTransient429SettingsValue(t, db, step.Enabled)
		if settings.CodexTransient429Policy() != step {
			t.Fatal("GetSystemSettings did not read the committed policy pair")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = openTransient429SettingsTestDB(t, path)
		if got := assertTransient429SettingsValue(t, db, step.Enabled).CodexTransient429Policy(); got != step {
			t.Fatalf("reopen policy = %+v, want %+v", got, step)
		}
	}
}

func TestTransient429PolicyConcurrentWritersShareGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared-policy.sqlite")
	writers := []*DB{openTransient429SettingsTestDB(t, path), openTransient429SettingsTestDB(t, path)}
	ctx := context.Background()
	// Separate DB handles ensure this does not accidentally rely on the
	// process-local SQLite write semaphore to implement the generation.
	for _, want := range []CodexTransient429CooldownPolicy{{true, 1}, {false, 2}, {true, 3}} {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(db *DB) {
				defer wg.Done()
				<-start
				got, err := db.UpdateCodexTransient429CooldownPolicy(ctx, want.Enabled)
				if err != nil || got != want {
					t.Errorf("concurrent same-value save = %+v, %v; want %+v", got, err, want)
				}
			}(writers[i%len(writers)])
		}
		close(start)
		wg.Wait()
	}

	// Interleaved opposing writes must return their own committed revision,
	// not another writer's later value. Initial generation 3 is enabled.
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			enabled := i%2 == 0
			got, err := writers[i%2].UpdateCodexTransient429CooldownPolicy(ctx, enabled)
			if err != nil || got.Enabled != enabled || got.Generation < 3 || (got.Generation%2 == 1) != got.Enabled {
				t.Errorf("policy did not belong to this committed update: %+v, %v", got, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestTransient429PolicyMigrationPreservesLegacyEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-policy.sqlite")
	db := openTransient429SettingsTestDB(t, path)
	ctx := context.Background()
	if err := db.UpdateCodexTransient429CooldownEnabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.ExecContext(ctx, `ALTER TABLE system_settings DROP COLUMN codex_transient_429_cooldown_generation`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTransient429SettingsTestDB(t, path)
	if got := assertTransient429SettingsValue(t, db, true).CodexTransient429Policy(); got.Generation != 1 {
		t.Fatal("enabled legacy policy did not fence generationless records")
	}
	for _, want := range []CodexTransient429CooldownPolicy{{true, 1}, {false, 2}, {true, 3}} {
		got, err := db.UpdateCodexTransient429CooldownPolicy(ctx, want.Enabled)
		if err != nil || got != want {
			t.Fatalf("migrated policy = %+v, %v; want %+v", got, err, want)
		}
	}
}

func TestTransient429PolicyFailedUpdateReturnsNoRevision(t *testing.T) {
	db := openTransient429SettingsTestDB(t, filepath.Join(t.TempDir(), "failed-policy.sqlite"))
	ctx := context.Background()
	before, err := db.UpdateCodexTransient429CooldownPolicy(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.ExecContext(ctx, `CREATE TRIGGER reject_policy BEFORE UPDATE ON system_settings
		BEGIN SELECT RAISE(ABORT, 'synthetic policy failure'); END`); err != nil {
		t.Fatal(err)
	}
	if got, err := db.UpdateCodexTransient429CooldownPolicy(ctx, false); err == nil || got != (CodexTransient429CooldownPolicy{}) {
		t.Fatalf("failed update returned publishable policy: %+v, %v", got, err)
	}
	if got := assertTransient429SettingsValue(t, db, true).CodexTransient429Policy(); got != before {
		t.Fatalf("failed update changed generation: %+v", got)
	}
}

func TestTransient429PolicyGenerationDoesNotWrap(t *testing.T) {
	db := openTransient429SettingsTestDB(t, filepath.Join(t.TempDir(), "max-policy.sqlite"))
	ctx := context.Background()
	if _, err := db.UpdateCodexTransient429CooldownPolicy(ctx, true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.ExecContext(ctx, `UPDATE system_settings SET codex_transient_429_cooldown_generation = $1 WHERE id = 1`, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if got, err := db.UpdateCodexTransient429CooldownPolicy(ctx, true); err != nil || got.Generation != math.MaxInt64 {
		t.Fatalf("same-value save at maximum revision failed: %+v, %v", got, err)
	}
	if got, err := db.UpdateCodexTransient429CooldownPolicy(ctx, false); err == nil || got != (CodexTransient429CooldownPolicy{}) {
		t.Fatalf("overflow produced a publishable policy: %+v, %v", got, err)
	}
	if got := assertTransient429SettingsValue(t, db, true); got.CodexTransient429CooldownGeneration != math.MaxInt64 {
		t.Fatal("overflow changed the durable revision")
	}
}

func TestTransient429FirstNarrowWriteEmitsSettingsEvent(t *testing.T) {
	db := openTransient429SettingsTestDB(t, filepath.Join(t.TempDir(), "initial-policy.sqlite"))
	ctx := context.Background()
	before, err := db.SchedulerOutboxHighWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpdateCodexTransient429CooldownPolicy(ctx, true); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListSchedulerOutboxEventsAfter(ctx, before, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.EntityType == "settings" {
			return
		}
	}
	t.Fatal("first policy change on an absent settings row was not observable")
}
