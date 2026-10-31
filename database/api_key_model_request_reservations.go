package database

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

// ErrAPIKeyModelRequestReservationUnavailable deliberately contains no storage
// error, SQL, logical request identity, key identity or receipt contents.
var ErrAPIKeyModelRequestReservationUnavailable = errors.New("model request quota reservation is unavailable")

type apiKeyModelRequestReceiptPhase uint8

const (
	modelRequestReceiptPending apiKeyModelRequestReceiptPhase = iota
	modelRequestReceiptReleased
	modelRequestReceiptRetained
	modelRequestReceiptPoisoned
)

type apiKeyModelRequestReceiptRow struct {
	ruleID                          string
	windowStart, resetAt, createdAt int64
}

type apiKeyModelRequestReceiptState struct {
	mu        sync.Mutex
	phase     apiKeyModelRequestReceiptPhase
	db        *DB
	keyID     int64
	requestID string
	rows      []apiKeyModelRequestReceiptRow
}

// APIKeyModelRequestReceipt is a process-local, single-use capability, not a
// serializable refund identity. Even a value copy shares the same lifecycle.
// There is intentionally no constructor from ledger fields or logical IDs.
//
// Only the dispatcher owning this logical request may reserve/release it. It
// must serialize attempts through settlement, and call ReleaseUnsent only with
// proof that THIS dispatch never sent. A send, uncertain send or abandoned
// receipt keeps its charge; process restarts cannot reconstruct a release.
// Existing ledger rows never become part of a new receipt.
//
// The shared terminal lifecycle, not created_at's second resolution, prevents
// ABA: after A releases, B may charge identical rows, but every copy of A is
// already terminal and cannot execute a second DELETE.
type APIKeyModelRequestReceipt struct {
	state *apiKeyModelRequestReceiptState
}

func (APIKeyModelRequestReceipt) String() string   { return "model request quota receipt" }
func (APIKeyModelRequestReceipt) GoString() string { return "model request quota receipt" }

// ReserveAPIKeyModelRequest charges before dispatch, using exactly the legacy
// consumer's authoritative rules and logical identity. Only newly inserted rows
// enter the receipt. Any storage/commit error poisons the returned capability;
// callers must poison their request too and must not attempt another send.
func (db *DB) ReserveAPIKeyModelRequest(ctx context.Context, keyID int64, requestID, effectiveModel string, rules []APIKeyModelRequestLimit, now time.Time) (*APIKeyModelRequestReceipt, *APIKeyModelRequestExhaustion, error) {
	s := &apiKeyModelRequestReceiptState{db: db, keyID: keyID, requestID: requestID}
	receipt := &APIKeyModelRequestReceipt{state: s}
	hit, err := db.consumeAPIKeyModelRequest(ctx, keyID, requestID, effectiveModel, rules, now, s)
	if err != nil {
		s.phase = modelRequestReceiptPoisoned
		return receipt, nil, ErrAPIKeyModelRequestReservationUnavailable
	}
	if hit != nil {
		return nil, hit, nil
	}
	return receipt, nil, nil
}

// Retain irreversibly closes the release capability when sending may have
// started. It performs no database IO and is idempotent across receipt copies.
func (r *APIKeyModelRequestReceipt) Retain() error {
	if r == nil || r.state == nil {
		return ErrAPIKeyModelRequestReservationUnavailable
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.phase {
	case modelRequestReceiptPending, modelRequestReceiptRetained:
		s.phase = modelRequestReceiptRetained
		return nil
	default:
		return ErrAPIKeyModelRequestReservationUnavailable
	}
}

// ReleaseUnsent atomically removes just this transaction's new ledger entries
// and decrements their ORIGINAL counter windows. It never looks up current
// rules or recomputes the week. Callers supply a bounded cleanup context that
// survives downstream cancellation, and finish before starting another attempt.
// No transaction is held during network IO. An error (including uncertain
// commit) is terminal: retrying a poisoned receipt could delete a later charge.
func (r *APIKeyModelRequestReceipt) ReleaseUnsent(ctx context.Context) error {
	if r == nil || r.state == nil {
		return ErrAPIKeyModelRequestReservationUnavailable
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase == modelRequestReceiptReleased {
		return nil
	}
	if s.phase != modelRequestReceiptPending {
		return ErrAPIKeyModelRequestReservationUnavailable
	}
	if len(s.rows) == 0 {
		s.phase = modelRequestReceiptReleased
		return nil
	}
	err := s.db.withWriteTx(ctx, func(tx *sql.Tx) error {
		// Use the same per-key serialization as consumption and limit edits.
		if _, err := s.db.lockAPIKeyLimits(ctx, tx, s.keyID); err != nil {
			return err
		}
		for _, row := range s.rows {
			result, err := tx.ExecContext(ctx, `DELETE FROM api_key_model_request_ledger WHERE api_key_id=$1 AND rule_id=$2 AND request_id=$3 AND window_start=$4 AND created_at=$5`, s.keyID, row.ruleID, s.requestID, row.windowStart, row.createdAt)
			if err != nil {
				return err
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				return ErrAPIKeyModelRequestReservationUnavailable
			}
			result, err = tx.ExecContext(ctx, `UPDATE api_key_model_request_counters SET used_requests=used_requests-1 WHERE api_key_id=$1 AND rule_id=$2 AND window_start=$3 AND reset_at=$4 AND used_requests>0`, s.keyID, row.ruleID, row.windowStart, row.resetAt)
			if err != nil {
				return err
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				return ErrAPIKeyModelRequestReservationUnavailable
			}
		}
		return nil
	})
	if err != nil {
		s.phase = modelRequestReceiptPoisoned
		return ErrAPIKeyModelRequestReservationUnavailable
	}
	s.phase = modelRequestReceiptReleased
	return nil
}
