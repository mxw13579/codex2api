package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

const maxUpstreamCookieTraceBytes = 1 << 20

var errInvalidUpstreamCookieTrace = errors.New("invalid upstream cookie diagnostics")

// UpdateCodexUpstreamCookieLogEnabled changes only this opt-in flag. Keeping it
// outside UpdateSystemSettings prevents unrelated stale snapshots from resetting it.
func (db *DB) UpdateCodexUpstreamCookieLogEnabled(ctx context.Context, enabled bool) error {
	return db.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO system_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING`); err != nil {
			return err
		}
		// Always UPDATE so the existing settings outbox trigger also fires when
		// this call creates the first settings row.
		_, err := tx.ExecContext(ctx, `UPDATE system_settings SET codex_upstream_cookie_log_enabled = $1 WHERE id = 1`, enabled)
		return err
	})
}

// GetUsageLogUpstreamCookies is the only usage-log read that exposes cookie
// diagnostics. Missing rows retain sql.ErrNoRows; legacy rows return "".
func (db *DB) GetUsageLogUpstreamCookies(ctx context.Context, id int64) (string, error) {
	var trace string
	if err := db.conn.QueryRowContext(ctx, `SELECT upstream_cookie_trace FROM usage_logs WHERE id = $1`, id).Scan(&trace); err != nil {
		return "", err
	}
	if !validUpstreamCookieTrace(trace) {
		return "", errInvalidUpstreamCookieTrace
	}
	return trace, nil
}

func validUpstreamCookieTrace(trace string) bool {
	if trace == "" {
		return true
	}
	if len(trace) > maxUpstreamCookieTraceBytes || !utf8.ValidString(trace) {
		return false
	}
	var envelope struct {
		Entries   []json.RawMessage `json:"entries"`
		Truncated *bool             `json:"truncated"`
	}
	if json.Unmarshal([]byte(trace), &envelope) != nil || envelope.Entries == nil || envelope.Truncated == nil {
		return false
	}
	for _, entry := range envelope.Entries {
		if !strings.HasPrefix(strings.TrimSpace(string(entry)), "{") {
			return false
		}
	}
	return true
}

func usageLogUpstreamCookieTrace(trace string, storeUsageLog bool) string {
	if !storeUsageLog || !validUpstreamCookieTrace(trace) {
		// Never truncate JSON or reject an otherwise valid billing/usage event.
		return ""
	}
	return trace
}

// Database error details can contain the rejected row. Retain only SQLSTATE
// classification, which the writer needs to distinguish bad rows from retries.
func upstreamCookieLogWriteError(batch []usageLogEntry, err error) error {
	if err == nil {
		return nil
	}
	for _, entry := range batch {
		if entry.UpstreamCookieTrace == "" {
			continue
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return &pgconn.PgError{Code: pgErr.Code, Message: "usage log write failed"}
		}
		return errors.New("usage log write failed")
	}
	return err
}
