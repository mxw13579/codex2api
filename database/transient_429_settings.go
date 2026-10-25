package database

import (
	"context"
	"errors"
)

// CodexTransient429CooldownPolicy is a single durable policy revision. Generation
// zero is the initial disabled policy. Migration seeds an enabled legacy policy
// at one to fence generationless records; subsequent increments require a real
// switch change.
// It must never be derived from an instance's clock or a read/modify/write pair.
type CodexTransient429CooldownPolicy struct {
	Enabled    bool
	Generation int64
}

func (s *SystemSettings) CodexTransient429Policy() CodexTransient429CooldownPolicy {
	if s == nil {
		return CodexTransient429CooldownPolicy{}
	}
	return CodexTransient429CooldownPolicy{
		Enabled: s.CodexTransient429CooldownEnabled, Generation: s.CodexTransient429CooldownGeneration,
	}
}

// UpdateCodexTransient429CooldownEnabled retains the error-only API. Runtime
// publishers must use UpdateCodexTransient429CooldownPolicy instead of rereading
// settings after this call (another writer may have committed in between).
func (db *DB) UpdateCodexTransient429CooldownEnabled(ctx context.Context, enabled bool) error {
	_, err := db.UpdateCodexTransient429CooldownPolicy(ctx, enabled)
	return err
}

// UpdateCodexTransient429CooldownPolicy returns the policy from this exact
// committed narrow update. Row locking and the SQL increment serialize writers
// across processes on both PostgreSQL and SQLite. Full settings saves never
// write either column. Even an initially absent row goes through UPDATE so the
// settings outbox trigger observes the first switch change.
func (db *DB) UpdateCodexTransient429CooldownPolicy(ctx context.Context, enabled bool) (CodexTransient429CooldownPolicy, error) {
	var policy CodexTransient429CooldownPolicy
	if db == nil || db.conn == nil {
		return policy, errors.New("database is not initialized")
	}
	err := db.withSQLiteWriteLock(ctx, func() error {
		tx, err := db.conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `INSERT INTO system_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING`); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `
			UPDATE system_settings SET
				codex_transient_429_cooldown_generation = codex_transient_429_cooldown_generation +
					CASE WHEN codex_transient_429_cooldown_enabled <> $1 THEN 1 ELSE 0 END,
				codex_transient_429_cooldown_enabled = $1
			WHERE id = 1 AND (codex_transient_429_cooldown_enabled = $1 OR
				codex_transient_429_cooldown_generation < 9223372036854775807)
			RETURNING codex_transient_429_cooldown_enabled, codex_transient_429_cooldown_generation
		`, enabled).Scan(&policy.Enabled, &policy.Generation); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		return CodexTransient429CooldownPolicy{}, err
	}
	return policy, nil
}
