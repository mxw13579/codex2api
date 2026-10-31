package database

// Synthetic temporary SQLite fixtures only. These tests deliberately avoid the
// optional PostgreSQL DSN used by older suites. Faults are SQL triggers or a
// test-only connector around the local driver, not product injection hooks.
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var mrReservationNow = time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)

func mrReservationDB(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic-model-reservations.db")
	db, err := New("sqlite", path)
	if err != nil {
		t.Fatal("could not create isolated reservation database")
	}
	db.SetUsageLogConfig("off", 100, 60)
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func mrReservationKey(t *testing.T, db *DB, name string, rules []APIKeyModelRequestLimit) (int64, []APIKeyModelRequestLimit) {
	t.Helper()
	id, err := db.InsertAPIKeyWithOptions(context.Background(), APIKeyInput{
		Name: "synthetic-reservation", Key: "synthetic-not-an-auth-key-" + name,
		Limits: APIKeyLimits{ModelRequestLimits: rules},
	})
	if err != nil {
		t.Fatal("could not insert synthetic reservation key")
	}
	row, err := db.GetAPIKeyByID(context.Background(), id)
	if err != nil || row == nil {
		t.Fatal("could not read synthetic reservation rules")
	}
	return id, row.Limits.ModelRequestLimits
}

func mrReservationRules() []APIKeyModelRequestLimit {
	return []APIKeyModelRequestLimit{
		{ID: "a-series", Model: "gpt-5*", MaxRequests: 10, Timezone: "UTC"},
		{ID: "z-exact", Model: "gpt-5.5", MaxRequests: 10, Timezone: "UTC"},
	}
}

func mrReserve(t *testing.T, db *DB, id int64, request, model string, rules []APIKeyModelRequestLimit, now time.Time) *APIKeyModelRequestReceipt {
	t.Helper()
	r, hit, err := db.ReserveAPIKeyModelRequest(context.Background(), id, request, model, rules, now)
	if err != nil || hit != nil || r == nil {
		t.Fatal("synthetic quota reservation did not succeed")
	}
	return r
}

func mrConsume(t *testing.T, db *DB, id int64, request, model string, rules []APIKeyModelRequestLimit, now time.Time) {
	t.Helper()
	if hit, err := db.ConsumeAPIKeyModelRequest(context.Background(), id, request, model, rules, now); err != nil || hit != nil {
		t.Fatal("synthetic legacy consumption did not succeed")
	}
}

func mrReservationUsage(t *testing.T, db *DB, id int64, rules []APIKeyModelRequestLimit, now time.Time, want ...int64) {
	t.Helper()
	usage, err := db.GetAPIKeyModelRequestUsage(context.Background(), id, rules, now)
	if err != nil || len(usage) != len(want) {
		t.Fatal("could not read synthetic reservation counters")
	}
	for i := range want {
		if usage[i].Used != want[i] {
			t.Errorf("rule index %d: used=%d, want %d", i, usage[i].Used, want[i])
		}
	}
}

func mrReservationTotals(t *testing.T, db *DB, id int64, wantUsed, wantLedger int64) {
	t.Helper()
	var used, negative, ledger int64
	if err := db.conn.QueryRow(`SELECT COALESCE(SUM(used_requests),0),COALESCE(SUM(CASE WHEN used_requests<0 THEN 1 ELSE 0 END),0) FROM api_key_model_request_counters WHERE api_key_id=$1`, id).Scan(&used, &negative); err != nil {
		t.Fatal("could not inspect synthetic reservation totals")
	}
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM api_key_model_request_ledger WHERE api_key_id=$1`, id).Scan(&ledger); err != nil {
		t.Fatal("could not inspect synthetic reservation ledger")
	}
	if used != wantUsed || ledger != wantLedger || negative != 0 {
		t.Errorf("all-window totals: used=%d ledger=%d negative=%d, want %d/%d/0", used, ledger, negative, wantUsed, wantLedger)
	}
}

func mrRelease(t *testing.T, r *APIKeyModelRequestReceipt) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.ReleaseUnsent(ctx); err != nil {
		t.Fatal("synthetic proven-unsent release did not succeed")
	}
}

func mrReservationSQL(t *testing.T, db *DB, query string) {
	t.Helper()
	if _, err := db.conn.Exec(query); err != nil {
		t.Fatal("could not install or remove isolated synthetic storage fault")
	}
}

func TestAPIKeyModelRequestReservationOnlyNewOverlappingRules(t *testing.T) {
	db, _ := mrReservationDB(t)
	id, rules := mrReservationKey(t, db, "overlap", mrReservationRules())
	// The broad charge belongs to a prior real attempt and must never enter
	// this retry's receipt, even when the effective model gains an exact rule.
	mrConsume(t, db, id, "synthetic-logical", "gpt-5-mini", rules, mrReservationNow)
	r := mrReserve(t, db, id, "synthetic-logical", "gpt-5.5", rules, mrReservationNow)
	if len(r.state.rows) != 1 || r.state.rows[0].ruleID != "z-exact" {
		t.Fatal("receipt included an existing charge or omitted its new exact rule")
	}
	mrReservationUsage(t, db, id, rules, mrReservationNow, 1, 1)
	// A later consume of the same logical ID cannot recreate the capability.
	empty := mrReserve(t, db, id, "synthetic-logical", "gpt-5.5", rules, mrReservationNow)
	if len(empty.state.rows) != 0 {
		t.Fatal("existing rows were reconstructed into a reusable receipt")
	}
	mrRelease(t, empty)
	mrReservationTotals(t, db, id, 2, 2)
	mrRelease(t, r)
	mrReservationUsage(t, db, id, rules, mrReservationNow, 1, 0)
	mrReservationTotals(t, db, id, 1, 1)
}

func TestAPIKeyModelRequestReservationAuthoritativeNewAndRemovedRules(t *testing.T) {
	db, _ := mrReservationDB(t)
	id, old := mrReservationKey(t, db, "rule-change", mrReservationRules()[:1])
	mrConsume(t, db, id, "synthetic-logical", "gpt-5.5", old, mrReservationNow)
	fresh := mrReservationRules()
	if err := db.UpdateAPIKeyLimits(context.Background(), id, APIKeyLimits{ModelRequestLimits: fresh}); err != nil {
		t.Fatal("could not add synthetic exact rule")
	}
	// An empty/stale auth snapshot cannot hide an authoritative newly added rule.
	r := mrReserve(t, db, id, "synthetic-logical", "gpt-5.5", nil, mrReservationNow)
	mrReservationTotals(t, db, id, 2, 2)
	if err := db.UpdateAPIKeyLimits(context.Background(), id, APIKeyLimits{}); err != nil {
		t.Fatal("could not remove synthetic rules")
	}
	mrRelease(t, r)
	mrReservationUsage(t, db, id, fresh, mrReservationNow, 1, 0)
	mrReservationTotals(t, db, id, 1, 1)
}

func TestAPIKeyModelRequestReservationOriginalWindowsAcrossWeekAndDST(t *testing.T) {
	db, _ := mrReservationDB(t)
	rules := mrReservationRules()
	rules[0].Timezone = "America/New_York"
	id, rules := mrReservationKey(t, db, "calendar", rules)
	r := mrReserve(t, db, id, "synthetic-old-week", "gpt-5.5", rules, mrReservationNow)
	if len(r.state.rows) != 2 || r.state.rows[0].resetAt-r.state.rows[0].windowStart != int64(167*time.Hour/time.Second) {
		t.Fatal("receipt did not preserve the short DST calendar window")
	}
	nextWeek := mrReservationNow.AddDate(0, 0, 7)
	// The retry still deduplicates across windows. Its empty receipt cannot
	// touch the previous window, and unrelated new-week counts survive release.
	empty := mrReserve(t, db, id, "synthetic-old-week", "gpt-5.5", rules, nextWeek)
	mrRelease(t, empty)
	mrReservationUsage(t, db, id, rules, nextWeek, 0, 0)
	mrConsume(t, db, id, "synthetic-new-week", "gpt-5.5", rules, nextWeek)
	mrRelease(t, r)
	mrReservationUsage(t, db, id, rules, mrReservationNow, 0, 0)
	mrReservationUsage(t, db, id, rules, nextWeek, 1, 1)
	mrReservationTotals(t, db, id, 2, 2)
	// Released requests may be charged anew in the actual new dispatch window.
	fresh := mrReserve(t, db, id, "synthetic-old-week", "gpt-5.5", rules, nextWeek)
	if err := fresh.Retain(); err != nil {
		t.Fatal("could not retain synthetic new-week dispatch")
	}
	mrRelease(t, r)
	mrReservationUsage(t, db, id, rules, nextWeek, 2, 2)
}

func TestAPIKeyModelRequestReservationCopiedConcurrentReleaseAndABA(t *testing.T) {
	db, _ := mrReservationDB(t)
	id, rules := mrReservationKey(t, db, "aba", mrReservationRules())
	a := mrReserve(t, db, id, "synthetic-same-id", "gpt-5.5", rules, mrReservationNow)
	copyA := *a
	start := make(chan struct{})
	results := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func(copy APIKeyModelRequestReceipt) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results <- copy.ReleaseUnsent(ctx)
		}(copyA)
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Error("concurrent duplicate release was not idempotent")
		}
	}
	mrReservationTotals(t, db, id, 0, 0)
	// Deliberately identical ID, window AND creation second. A timestamp is
	// not an ABA token: the shared irreversible lifecycle is the protection.
	b := mrReserve(t, db, id, "synthetic-same-id", "gpt-5.5", rules, mrReservationNow)
	if err := b.Retain(); err != nil {
		t.Fatal("could not retain recharged synthetic request")
	}
	mrRelease(t, a)
	mrRelease(t, &copyA)
	if err := b.ReleaseUnsent(context.Background()); !errors.Is(err, ErrAPIKeyModelRequestReservationUnavailable) {
		t.Error("retained receipt became releasable")
	}
	mrReservationTotals(t, db, id, 2, 2)
}

func TestAPIKeyModelRequestReservationRetainReleaseSerialization(t *testing.T) {
	db, _ := mrReservationDB(t)
	id, rules := mrReservationKey(t, db, "retain-release", mrReservationRules()[:1])
	r := mrReserve(t, db, id, "synthetic-race", "gpt-5.5", rules, mrReservationNow)
	copyR := *r
	start := make(chan struct{})
	retained, released := make(chan error, 1), make(chan error, 1)
	go func() { <-start; retained <- r.Retain() }()
	go func() { <-start; released <- copyR.ReleaseUnsent(context.Background()) }()
	close(start)
	retainErr, releaseErr := <-retained, <-released
	switch {
	case retainErr == nil && errors.Is(releaseErr, ErrAPIKeyModelRequestReservationUnavailable):
		mrReservationTotals(t, db, id, 1, 1)
	case releaseErr == nil && errors.Is(retainErr, ErrAPIKeyModelRequestReservationUnavailable):
		mrReservationTotals(t, db, id, 0, 0)
	default:
		t.Error("retain and release did not choose exactly one terminal state")
	}
}

func TestAPIKeyModelRequestReservationCrossKeyAndLegacyConsume(t *testing.T) {
	db, _ := mrReservationDB(t)
	id, rules := mrReservationKey(t, db, "key-one", mrReservationRules())
	other, _ := mrReservationKey(t, db, "key-two", rules)
	r := mrReserve(t, db, id, "synthetic-same-id", "gpt-5.5", rules, mrReservationNow)
	mrConsume(t, db, other, "synthetic-same-id", "gpt-5.5", rules, mrReservationNow)
	mrRelease(t, r)
	mrReservationTotals(t, db, id, 0, 0)
	mrReservationTotals(t, db, other, 2, 2)
	// Legacy failed/uncertain sends remain charged without a release token.
	mrConsume(t, db, other, "synthetic-same-id", "gpt-5.5", rules, mrReservationNow.AddDate(0, 0, 7))
	empty := mrReserve(t, db, other, "synthetic-same-id", "gpt-5.5", rules, mrReservationNow)
	mrRelease(t, empty)
	mrReservationTotals(t, db, other, 2, 2)
	var invalid APIKeyModelRequestReceipt
	if invalid.ReleaseUnsent(context.Background()) == nil || invalid.Retain() == nil {
		t.Error("zero-value receipt acquired a release capability")
	}
	unrestricted, _ := mrReservationKey(t, db, "unrestricted", nil)
	mrConsume(t, db, unrestricted, "synthetic-unrestricted", "gpt-5.5", nil, mrReservationNow)
	mrRelease(t, mrReserve(t, db, unrestricted, "synthetic-unrestricted", "gpt-5.5", nil, mrReservationNow))
	mrReservationTotals(t, db, unrestricted, 0, 0)
}

func TestAPIKeyModelRequestReservationExhaustionIsAtomic(t *testing.T) {
	db, _ := mrReservationDB(t)
	rules := mrReservationRules()
	rules[1].MaxRequests = 1
	id, rules := mrReservationKey(t, db, "exhaustion", rules)
	mrConsume(t, db, id, "synthetic-existing", "gpt-5.5", rules, mrReservationNow)
	r, hit, err := db.ReserveAPIKeyModelRequest(context.Background(), id, "synthetic-denied", "gpt-5.5", rules, mrReservationNow)
	if err != nil || hit == nil || r != nil || hit.RuleID != "z-exact" {
		t.Error("quota exhaustion lost its dedicated result or returned a receipt")
	}
	mrReservationTotals(t, db, id, 2, 2)
}

func TestAPIKeyModelRequestReservationStorageFaultRollbackAndPoison(t *testing.T) {
	for _, phase := range []string{"charge", "release"} {
		t.Run(phase, func(t *testing.T) {
			db, _ := mrReservationDB(t)
			id, rules := mrReservationKey(t, db, "storage-fault", mrReservationRules())
			var r *APIKeyModelRequestReceipt
			if phase == "charge" {
				mrReservationSQL(t, db, `CREATE TRIGGER synthetic_reservation_fault BEFORE INSERT ON api_key_model_request_ledger WHEN NEW.rule_id='z-exact' BEGIN SELECT RAISE(ABORT,'synthetic-storage-detail-do-not-expose'); END`)
				var hit *APIKeyModelRequestExhaustion
				var err error
				r, hit, err = db.ReserveAPIKeyModelRequest(context.Background(), id, "synthetic-fault", "gpt-5.5", rules, mrReservationNow)
				if !errors.Is(err, ErrAPIKeyModelRequestReservationUnavailable) || hit != nil || r == nil {
					t.Fatal("charge storage fault did not return a poisoned capability and fixed error")
				}
				mrReservationTotals(t, db, id, 0, 0)
			} else {
				r = mrReserve(t, db, id, "synthetic-fault", "gpt-5.5", rules, mrReservationNow)
				// Failure AFTER the first rule and the second ledger deletion
				// must roll back ALL decrements and deletions in the release.
				mrReservationSQL(t, db, `CREATE TRIGGER synthetic_reservation_fault BEFORE UPDATE ON api_key_model_request_counters WHEN OLD.rule_id='z-exact' AND NEW.used_requests<OLD.used_requests BEGIN SELECT RAISE(ABORT,'synthetic-storage-detail-do-not-expose'); END`)
				if err := r.ReleaseUnsent(context.Background()); !errors.Is(err, ErrAPIKeyModelRequestReservationUnavailable) {
					t.Fatal("release storage fault was silently accepted")
				}
				mrReservationTotals(t, db, id, 2, 2)
			}
			mrReservationSQL(t, db, `DROP TRIGGER synthetic_reservation_fault`)
			copyR := *r
			if copyR.ReleaseUnsent(context.Background()) != ErrAPIKeyModelRequestReservationUnavailable || r.Retain() != ErrAPIKeyModelRequestReservationUnavailable {
				t.Error("repaired storage revived a poisoned receipt")
			}
			want := int64(2)
			if phase == "charge" {
				want = 0
			}
			mrReservationTotals(t, db, id, want, want)
			if fmt.Sprintf("%v / %#v", r, copyR) != "model request quota receipt / model request quota receipt" {
				t.Error("receipt formatting exposed capability fields")
			}
		})
	}
}

// The wrapper commits to real temporary SQLite and then reports a synthetic
// failure. The caller cannot distinguish committed from uncommitted storage:
// it must poison, never retry settlement or issue a fresh capability for A.
type mrReservationFaultConnector struct {
	driver driver.Driver
	dsn    string
	armed  atomic.Bool
}

func (c *mrReservationFaultConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &mrReservationFaultConn{Conn: conn, owner: c}, nil
}
func (c *mrReservationFaultConnector) Driver() driver.Driver { return c.driver }

type mrReservationFaultConn struct {
	driver.Conn
	owner *mrReservationFaultConnector
}

func (c *mrReservationFaultConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &mrReservationFaultTx{Tx: tx, owner: c.owner}, nil
}
func (c *mrReservationFaultConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err := beginner.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &mrReservationFaultTx{Tx: tx, owner: c.owner}, nil
	}
	return c.Begin()
}

type mrReservationFaultTx struct {
	driver.Tx
	owner *mrReservationFaultConnector
}

func (tx *mrReservationFaultTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.owner.armed.Swap(false) {
		return errors.New("synthetic-uncertain-commit-detail-do-not-expose")
	}
	return nil
}

func TestAPIKeyModelRequestReservationUncertainCommitPoison(t *testing.T) {
	for _, phase := range []string{"charge", "release"} {
		t.Run(phase, func(t *testing.T) {
			db, path := mrReservationDB(t)
			id, rules := mrReservationKey(t, db, "uncertain", mrReservationRules())
			connector := &mrReservationFaultConnector{driver: db.conn.Driver(), dsn: sqliteConnectDSN(path)}
			conn := sql.OpenDB(connector)
			conn.SetMaxOpenConns(1)
			t.Cleanup(func() { _ = conn.Close() })
			faultDB := &DB{conn: conn, driver: "sqlite", sqliteWriteSem: make(chan struct{}, 1)}
			var r *APIKeyModelRequestReceipt
			if phase == "charge" {
				connector.armed.Store(true)
				var hit *APIKeyModelRequestExhaustion
				var err error
				r, hit, err = faultDB.ReserveAPIKeyModelRequest(context.Background(), id, "synthetic-uncertain", "gpt-5.5", rules, mrReservationNow)
				if err != ErrAPIKeyModelRequestReservationUnavailable || r == nil || hit != nil || connector.armed.Load() {
					t.Fatal("uncertain charge commit was not injected and poisoned")
				}
			} else {
				r = mrReserve(t, faultDB, id, "synthetic-uncertain", "gpt-5.5", rules, mrReservationNow)
				connector.armed.Store(true)
				if r.ReleaseUnsent(context.Background()) != ErrAPIKeyModelRequestReservationUnavailable || connector.armed.Load() {
					t.Fatal("uncertain release commit was not injected and poisoned")
				}
				mrReservationTotals(t, db, id, 0, 0)
				// Even after the release actually committed, the old uncertain
				// receipt must never delete a new identical charge.
				mrConsume(t, db, id, "synthetic-uncertain", "gpt-5.5", rules, mrReservationNow)
			}
			copyR := *r
			if r.Retain() != ErrAPIKeyModelRequestReservationUnavailable || copyR.ReleaseUnsent(context.Background()) != ErrAPIKeyModelRequestReservationUnavailable {
				t.Error("uncertain commit allowed capability reuse")
			}
			mrReservationTotals(t, db, id, 2, 2)
		})
	}
}
