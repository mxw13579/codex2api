package database

import (
	"context"
	"database/sql"
	"encoding/json"
)

// BadRequestRetryPolicy is the independent, opt-in extra retry budget for an
// upstream HTTP 400 whose body contains only {"detail":"Bad Request"}.
// This configuration does not implement matching, scheduling or replay safety.
// Disabling it preserves the operator's budget and account-switch preference.
type BadRequestRetryPolicy struct {
	Enabled       bool `json:"enabled"`
	MaxRetries    int  `json:"max_retries"`
	SwitchAccount bool `json:"switch_account"`
}

// BadRequestRetryPolicyUpdate merges only explicitly supplied fields into the
// latest persisted policy. Nil means omitted; zero and false are real updates.
type BadRequestRetryPolicyUpdate struct {
	Enabled       *bool
	MaxRetries    *int
	SwitchAccount *bool
}

const (
	DefaultBadRequestRetryMaxRetries = 1
	MaxBadRequestRetryMaxRetries     = 10
)

func DefaultBadRequestRetryPolicy() BadRequestRetryPolicy {
	return BadRequestRetryPolicy{MaxRetries: DefaultBadRequestRetryMaxRetries}
}

func NormalizeBadRequestRetryPolicy(policy BadRequestRetryPolicy) BadRequestRetryPolicy {
	if policy.MaxRetries < 0 {
		policy.MaxRetries = 0
	} else if policy.MaxRetries > MaxBadRequestRetryMaxRetries {
		policy.MaxRetries = MaxBadRequestRetryMaxRetries
	}
	return policy
}

// ParseBadRequestRetryPolicy defaults missing fields, but never changes an
// explicit zero to one. Invalid JSON/types fail closed to the disabled default,
// rather than publishing fields partially decoded before an error.
func ParseBadRequestRetryPolicy(raw string) BadRequestRetryPolicy {
	policy := DefaultBadRequestRetryPolicy()
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return DefaultBadRequestRetryPolicy()
	}
	return NormalizeBadRequestRetryPolicy(policy)
}

func EncodeBadRequestRetryPolicy(policy BadRequestRetryPolicy) string {
	// The normalized value contains only bools and a bounded int.
	raw, _ := json.Marshal(NormalizeBadRequestRetryPolicy(policy))
	return string(raw)
}

// UpdateBadRequestRetryPolicy atomically merges a partial update without
// touching any other setting. PostgreSQL locks the row; SQLite uses its write
// gate and immediate transactions, including across separate DB handles.
func (db *DB) UpdateBadRequestRetryPolicy(ctx context.Context, update BadRequestRetryPolicyUpdate) (BadRequestRetryPolicy, error) {
	var committed BadRequestRetryPolicy
	err := db.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO system_settings (id, bad_request_retry_policy)
			VALUES (1, $1)
			ON CONFLICT (id) DO NOTHING
		`, EncodeBadRequestRetryPolicy(DefaultBadRequestRetryPolicy())); err != nil {
			return err
		}
		var raw string
		if err := tx.QueryRowContext(ctx, badRequestRetryPolicySelectQuery(!db.isSQLite())).Scan(&raw); err != nil {
			return err
		}
		next := ParseBadRequestRetryPolicy(raw)
		if update.Enabled != nil {
			next.Enabled = *update.Enabled
		}
		if update.MaxRetries != nil {
			next.MaxRetries = *update.MaxRetries
		}
		if update.SwitchAccount != nil {
			next.SwitchAccount = *update.SwitchAccount
		}
		next = NormalizeBadRequestRetryPolicy(next)
		encoded := EncodeBadRequestRetryPolicy(next)
		if encoded != raw {
			if _, err := tx.ExecContext(ctx, `UPDATE system_settings SET bad_request_retry_policy = $1 WHERE id = 1`, encoded); err != nil {
				return err
			}
		}
		committed = next
		return nil
	})
	if err != nil {
		return BadRequestRetryPolicy{}, err
	}
	return committed, nil
}

func badRequestRetryPolicySelectQuery(forUpdate bool) string {
	query := `SELECT COALESCE(bad_request_retry_policy, '') FROM system_settings WHERE id = 1`
	if forUpdate {
		query += " FOR UPDATE"
	}
	return query
}
