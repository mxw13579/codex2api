package database

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestBadRequestRetryPolicyDefaultsNormalizeAndParse(t *testing.T) {
	def := BadRequestRetryPolicy{MaxRetries: 1}
	if got := DefaultBadRequestRetryPolicy(); got != def {
		t.Fatalf("default = %+v, want %+v", got, def)
	}
	for _, tt := range []struct{ input, want int }{{-10, 0}, {0, 0}, {1, 1}, {10, 10}, {11, 10}} {
		got := NormalizeBadRequestRetryPolicy(BadRequestRetryPolicy{MaxRetries: tt.input, SwitchAccount: true})
		if got != (BadRequestRetryPolicy{MaxRetries: tt.want, SwitchAccount: true}) {
			t.Fatalf("normalize %d = %+v", tt.input, got)
		}
		if roundTrip := ParseBadRequestRetryPolicy(EncodeBadRequestRetryPolicy(got)); roundTrip != got {
			t.Fatalf("round trip = %+v, want %+v", roundTrip, got)
		}
	}
	for _, raw := range []string{"", " ", "{}", "null", "{", "[]", `{"enabled":true,"max_retries":"2"}`, `{"enabled":true,"max_retries":1.5}`, `{"enabled":true} trailing`} {
		if got := ParseBadRequestRetryPolicy(raw); got != def {
			t.Errorf("parse %q = %+v, want disabled default", raw, got)
		}
	}
	for _, tt := range []struct {
		raw  string
		want BadRequestRetryPolicy
	}{
		{`{"enabled":true}`, BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}},
		{`{"enabled":true,"max_retries":0}`, BadRequestRetryPolicy{Enabled: true}},
		{`{"max_retries":null}`, def},
		{`{"max_retries":100,"switch_account":true}`, BadRequestRetryPolicy{MaxRetries: 10, SwitchAccount: true}},
		{`{"max_retries":-1}`, BadRequestRetryPolicy{}},
	} {
		if got := ParseBadRequestRetryPolicy(tt.raw); got != tt.want {
			t.Errorf("parse %q = %+v, want %+v", tt.raw, got, tt.want)
		}
	}
}

func TestBadRequestRetryPolicySelectLocksPostgresOnly(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		if got := strings.Contains(badRequestRetryPolicySelectQuery(postgres), "FOR UPDATE"); got != postgres {
			t.Fatalf("row lock for postgres=%t: %t", postgres, got)
		}
	}
	var db *DB
	if _, err := db.UpdateBadRequestRetryPolicy(context.Background(), BadRequestRetryPolicyUpdate{}); err == nil {
		t.Fatal("uninitialized database must not report a successful commit")
	}
}

func TestBadRequestRetryPolicySQLitePartialPersistenceAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bad-request-policy.sqlite")
	db, err := New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Full settings inserts, as well as updates, must ignore this field.
	stale := &SystemSettings{SiteName: "synthetic initial", BadRequestRetryPolicy: `{"enabled":true,"max_retries":9,"switch_account":true}`}
	if err := db.UpdateSystemSettings(ctx, stale); err != nil {
		t.Fatal(err)
	}
	assertBadRequestRetryPersisted(t, db, DefaultBadRequestRetryPolicy())
	want := BadRequestRetryPolicy{Enabled: true, MaxRetries: 0, SwitchAccount: true}
	for _, update := range []BadRequestRetryPolicyUpdate{
		{MaxRetries: &want.MaxRetries}, {SwitchAccount: &want.SwitchAccount}, {Enabled: &want.Enabled}, {},
	} {
		if _, err := db.UpdateBadRequestRetryPolicy(ctx, update); err != nil {
			t.Fatal(err)
		}
	}
	assertBadRequestRetryPersisted(t, db, want)
	// Even a stale explicit policy in the broad struct must not overwrite it.
	stale.SiteName = "synthetic unrelated save"
	if err := db.UpdateSystemSettings(ctx, stale); err != nil {
		t.Fatal(err)
	}
	assertBadRequestRetryPersisted(t, db, want)
	continuousEnabled := true
	if _, err := db.UpdateContinuousRetryPolicy(ctx, ContinuousRetryPolicyUpdate{Enabled: &continuousEnabled}); err != nil {
		t.Fatal(err)
	}
	assertBadRequestRetryPersisted(t, db, want)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	assertBadRequestRetryPersisted(t, db, want)
	want.Enabled = false
	if got, err := db.UpdateBadRequestRetryPolicy(ctx, BadRequestRetryPolicyUpdate{Enabled: &want.Enabled}); err != nil || got != want {
		t.Fatalf("explicit false update = %+v, err=%v", got, err)
	}
	assertBadRequestRetryPersisted(t, db, want)
	settings, err := db.GetSystemSettings(ctx)
	if err != nil || !ParseContinuousRetryPolicy(settings.ContinuousRetryPolicy).Enabled {
		t.Fatal("independent update changed continuous retry policy")
	}
}

func TestBadRequestRetryPolicySQLiteLegacyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-bad-request.sqlite")
	db, err := New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.conn.Exec(`INSERT INTO system_settings (id, site_name) VALUES (1, 'synthetic legacy')`); err != nil {
		t.Fatal(err)
	}
	// Remove only this column from the synthetic database to model an old schema.
	if _, err := db.conn.Exec(`ALTER TABLE system_settings DROP COLUMN bad_request_retry_policy`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	assertBadRequestRetryPersisted(t, db, DefaultBadRequestRetryPolicy())
	for _, raw := range []any{nil, "", `{"enabled":true,"max_retries":"invalid"}`} {
		if _, err := db.conn.Exec(`UPDATE system_settings SET bad_request_retry_policy = $1 WHERE id = 1`, raw); err != nil {
			t.Fatal(err)
		}
		assertBadRequestRetryPersisted(t, db, DefaultBadRequestRetryPolicy())
	}
}

func TestBadRequestRetryPolicySQLiteConcurrentPartialUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent-bad-request.sqlite")
	db1, err := New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db1.Close() })
	db2, err := New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	want := BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true}
	start := make(chan struct{})
	errs := make(chan error, 3)
	for i, update := range []BadRequestRetryPolicyUpdate{{Enabled: &want.Enabled}, {MaxRetries: &want.MaxRetries}, {SwitchAccount: &want.SwitchAccount}} {
		db := []*DB{db1, db2, db1}[i]
		go func() {
			<-start
			_, err := db.UpdateBadRequestRetryPolicy(context.Background(), update)
			errs <- err
		}()
	}
	close(start)
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	assertBadRequestRetryPersisted(t, db1, want)
}

func assertBadRequestRetryPersisted(t *testing.T, db *DB, want BadRequestRetryPolicy) {
	t.Helper()
	settings, err := db.GetSystemSettings(context.Background())
	if err != nil || settings == nil {
		t.Fatalf("load settings: present=%t err=%v", settings != nil, err)
	}
	if got := ParseBadRequestRetryPolicy(settings.BadRequestRetryPolicy); got != want {
		t.Fatalf("persisted policy = %+v, want %+v", got, want)
	}
}
