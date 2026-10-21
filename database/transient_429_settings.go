package database

import (
	"context"
	"errors"
)

// UpdateCodexTransient429CooldownEnabled atomically updates only the transient
// Codex 429 account-cooldown switch, creating the singleton settings row if
// necessary. UpdateSystemSettings deliberately ignores this field so unrelated
// saves cannot overwrite it with a stale snapshot. Callers must wait for success
// before publishing the value to runtime state.
func (db *DB) UpdateCodexTransient429CooldownEnabled(ctx context.Context, enabled bool) error {
	if db == nil || db.conn == nil {
		return errors.New("database is not initialized")
	}
	return db.withSQLiteWriteLock(ctx, func() error {
		_, err := db.conn.ExecContext(ctx, `
			INSERT INTO system_settings (id, codex_transient_429_cooldown_enabled)
			VALUES (1, $1)
			ON CONFLICT (id) DO UPDATE SET
				codex_transient_429_cooldown_enabled = EXCLUDED.codex_transient_429_cooldown_enabled
		`, enabled)
		return err
	})
}
