package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

func TestUpstreamCookieRetentionByUsageLogMode(t *testing.T) {
	cookieLogTestSetup(t)
	for _, tt := range []struct {
		name   string
		mode   string
		status int
		nilDB  bool
		retain bool
	}{
		{name: "full_success", mode: database.UsageLogModeFull, status: 200, retain: true},
		{name: "full_error", mode: database.UsageLogModeFull, status: 500, retain: true},
		{name: "errors_success", mode: database.UsageLogModeErrors, status: 200},
		{name: "errors_below_boundary", mode: database.UsageLogModeErrors, status: 399},
		{name: "errors_boundary", mode: database.UsageLogModeErrors, status: 400, retain: true},
		{name: "errors_rate_limit", mode: database.UsageLogModeErrors, status: 429, retain: true},
		{name: "errors_client_closed", mode: database.UsageLogModeErrors, status: 499, retain: true},
		{name: "off_success", mode: database.UsageLogModeOff, status: 200},
		{name: "off_error", mode: database.UsageLogModeOff, status: 500},
		{name: "nil_db_default_full", status: 200, nilDB: true, retain: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var db *database.DB
			if !tt.nilDB {
				db = &database.DB{}
				db.SetUsageLogConfig(tt.mode, 0, 0)
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			attachUpstreamTrace(c, nil)
			ctx := c.Request.Context()
			account := &auth.Account{DBID: 4}
			resp := &http.Response{StatusCode: tt.status, Header: http.Header{
				"X-Request-Id":       {upstreamResponseTestUUID},
				codexTurnStateHeader: {"synthetic-turn-state"},
				"Set-Cookie":         {"__cflb=synthetic-retention-fixture"},
			}}
			beginUpstreamTrace(ctx, account, "", false)(resp)
			NewUpstreamCookieTrace(ctx, account, "http", "", nil, resp)
			input := &database.UsageLogInput{
				AccountID: 4, StatusCode: tt.status, APIKeyID: 7,
				InputTokens: 11, OutputTokens: 3, TotalTokens: 14,
			}
			retain := db.ShouldStoreUsageLog(input)
			if retain != tt.retain {
				t.Fatalf("ShouldStoreUsageLog = %v, want %v", retain, tt.retain)
			}
			populateUpstreamTrace(c, input, retain)
			if input.RequestID != cookieLogRequestID(ctx) || input.RequestID == "" ||
				input.UpstreamRequestID != upstreamResponseTestUUID || input.UpstreamProxyName != "direct/no_proxy" ||
				input.UpstreamTurnState != "synthetic-turn-state" {
				t.Fatal("retention decision changed non-Cookie trace metadata")
			}
			if input.AccountID != 4 || input.StatusCode != tt.status || input.APIKeyID != 7 ||
				input.InputTokens != 11 || input.OutputTokens != 3 || input.TotalTokens != 14 {
				t.Fatal("retention decision changed usage/accounting input")
			}
			if !tt.retain {
				if input.UpstreamCookieTrace != "" {
					t.Fatal("discarded usage row encoded a final Cookie snapshot")
				}
				return
			}
			entries := cookieLogTestRead(t, input.UpstreamCookieTrace)
			if len(entries) != 1 || entries[0].RequestID != input.RequestID || entries[0].Status != tt.status ||
				entries[0].UpstreamRequestID != input.UpstreamRequestID || len(entries[0].SetCookie) != 1 {
				t.Fatal("retained usage row lost its Cookie snapshot")
			}
		})
	}
}

func TestShouldStoreUsageLogNilContracts(t *testing.T) {
	var missing *database.DB
	if missing.GetUsageLogMode() != database.UsageLogModeFull || !missing.ShouldStoreUsageLog(nil) {
		t.Fatal("nil DB must preserve the existing full-mode default")
	}
	db := &database.DB{}
	if db.GetUsageLogMode() != database.UsageLogModeFull || !db.ShouldStoreUsageLog(nil) {
		t.Fatal("unconfigured DB must preserve the existing full-mode default")
	}
	for _, mode := range []string{database.UsageLogModeErrors, database.UsageLogModeOff} {
		db.SetUsageLogConfig(mode, 0, 0)
		if db.ShouldStoreUsageLog(nil) {
			t.Fatal("nil input must not be retained in errors/off mode")
		}
	}
}

func TestSnapshotUpstreamTraceSkipsUnretainedCookieSnapshot(t *testing.T) {
	cookieLogTestSetup(t)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	ctx := c.Request.Context()
	requestID := cookieLogRequestID(ctx)
	account := &auth.Account{DBID: 4}
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{
		"X-Request-Id":       {upstreamResponseTestUUID},
		codexTurnStateHeader: {"synthetic-turn-state"},
		"Set-Cookie":         {"__cflb=synthetic-snapshot-fixture"},
	}}
	beginUpstreamTrace(ctx, account, "", false)(resp)
	NewUpstreamCookieTrace(ctx, account, "http", "", nil, resp)
	collector := upstreamCookieCollectorFromContext(ctx)

	// Hold the collector lock to distinguish skipping the snapshot from
	// encoding it and then discarding the JSON. Always release and join.
	collector.mu.Lock()
	var frozen upstreamTraceSnapshot
	done := make(chan struct{})
	go func() {
		frozen = snapshotUpstreamTrace(ctx, false)
		close(done)
	}()
	completed := false
	select {
	case <-done:
		completed = true
	case <-time.After(2 * time.Second):
	}
	collector.mu.Unlock()
	<-done
	if !completed {
		t.Fatal("unretained snapshot attempted to lock the Cookie collector")
	}

	resetUpstreamRequestTrace(c)
	later := &http.Response{StatusCode: http.StatusOK, Header: http.Header{
		"X-Request-Id":       {"req_later"},
		codexTurnStateHeader: {"later-turn-state"},
		"Set-Cookie":         {"__cflb=synthetic-later-fixture"},
	}}
	laterAccount := &auth.Account{DBID: 5}
	beginUpstreamTrace(c.Request.Context(), laterAccount, "", false)(later)
	NewUpstreamCookieTrace(c.Request.Context(), laterAccount, "http", "", nil, later)
	input := &database.UsageLogInput{AccountID: 4}
	frozen.apply(input)
	populateUpstreamTrace(c, input, true)
	if requestID == "" || input.RequestID != requestID || input.UpstreamCookieTrace != "" ||
		input.UpstreamRequestID != upstreamResponseTestUUID || input.UpstreamProxyName != "direct/no_proxy" ||
		input.UpstreamTurnState != "synthetic-turn-state" {
		t.Fatal("skipping Cookie encoding lost frozen identity or inherited later diagnostics")
	}
	otherAccount := &database.UsageLogInput{AccountID: 5}
	frozen.apply(otherAccount)
	if otherAccount.UpstreamRequestID != "" || otherAccount.UpstreamProxyID != 0 ||
		otherAccount.UpstreamProxyName != "" || otherAccount.UpstreamTurnState != "" {
		t.Fatal("unretained snapshot leaked another account's identity")
	}
}

func TestPopulateUpstreamTraceSkipsUnretainedCookieSnapshot(t *testing.T) {
	cookieLogTestSetup(t)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	ctx := c.Request.Context()
	collector := upstreamCookieCollectorFromContext(ctx)
	collector.append(upstreamCookieEntry{Event: "synthetic-retention-fixture"})
	input := &database.UsageLogInput{StatusCode: 200}
	db := &database.DB{}
	db.SetUsageLogConfig(database.UsageLogModeErrors, 0, 0)

	// An unretained row must not even acquire the collector's snapshot lock.
	// Release and join on failure too, so a regression cannot leak a goroutine.
	collector.mu.Lock()
	done := make(chan struct{})
	go func() {
		populateUpstreamTrace(c, input, db.ShouldStoreUsageLog(input))
		close(done)
	}()
	completed := false
	select {
	case <-done:
		completed = true
	case <-time.After(2 * time.Second):
	}
	collector.mu.Unlock()
	<-done
	if !completed {
		t.Fatal("unretained usage row attempted to snapshot the Cookie collector")
	}
	if input.RequestID == "" || input.UpstreamCookieTrace != "" {
		t.Fatal("skipping Cookie encoding must still populate request identity")
	}
}
