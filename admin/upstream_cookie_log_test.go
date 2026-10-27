package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/cache"
	"github.com/codex2api/database"
	"github.com/codex2api/proxy"
	"github.com/gin-gonic/gin"
)

const upstreamCookieAdminFixture = `{"entries":[{"cookie":["synthetic_cookie=fixture"],"set_cookie":["synthetic_cookie=updated; Secure"]}],"truncated":true}`
const upstreamCookieAdminKey = "synthetic-cookie-admin-key"

type upstreamCookieAdminFixtureState struct {
	db     *database.DB
	conn   *sql.DB
	router *gin.Engine
}

func newUpstreamCookieAdminFixture(t *testing.T) upstreamCookieAdminFixtureState {
	t.Helper()
	gin.SetMode(gin.TestMode)
	previous := proxy.CurrentRuntimeSettings()
	t.Cleanup(func() { proxy.ApplyRuntimeSettings(previous) })
	path := filepath.Join(t.TempDir(), "cookies.sqlite")
	db, err := database.New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	settings := defaultBootstrapSettings()
	if err := db.UpdateSystemSettings(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	proxy.ApplyRuntimeSettingsFromSystem(settings)
	tc := cache.NewMemory(4)
	t.Cleanup(func() { _ = tc.Close() })
	store := auth.NewStore(db, tc, settings)
	t.Cleanup(store.Stop)
	handler := NewHandler(store, db, tc, proxy.NewRateLimiter(0), upstreamCookieAdminKey)
	router := gin.New()
	handler.RegisterRoutes(router)
	return upstreamCookieAdminFixtureState{db: db, conn: conn, router: router}
}

func (f upstreamCookieAdminFixtureState) request(method, path, body, adminKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if adminKey != "" {
		req.Header.Set("X-Admin-Key", adminKey)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

func (f upstreamCookieAdminFixtureState) checkSetting(t *testing.T, want bool) {
	t.Helper()
	rec := f.request(http.MethodGet, "/api/admin/settings", "", upstreamCookieAdminKey)
	var response map[string]json.RawMessage
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &response) != nil {
		t.Fatal("could not get settings response")
	}
	var enabled bool
	if raw, ok := response["codex_upstream_cookie_log_enabled"]; !ok || json.Unmarshal(raw, &enabled) != nil || enabled != want {
		t.Fatal("settings response has incorrect cookie diagnostic flag")
	}
	saved, err := f.db.GetSystemSettings(context.Background())
	if err != nil || saved == nil || saved.CodexUpstreamCookieLogEnabled != want || proxy.CurrentRuntimeSettings().CodexUpstreamCookieLogEnabled != want {
		t.Fatal("persisted and runtime diagnostic settings differ")
	}
}

func TestUpstreamCookieSettingsToggleReloadAndUnrelatedUpdates(t *testing.T) {
	f := newUpstreamCookieAdminFixture(t)
	f.checkSetting(t, false)
	for _, enabled := range []bool{true, false, true} {
		body := fmt.Sprintf(`{"codex_upstream_cookie_log_enabled":%t}`, enabled)
		rec := f.request(http.MethodPut, "/api/admin/settings", body, upstreamCookieAdminKey)
		if rec.Code != http.StatusOK {
			t.Fatalf("toggle status = %d", rec.Code)
		}
		var response settingsResponse
		if json.Unmarshal(rec.Body.Bytes(), &response) != nil || response.CodexUpstreamCookieLogEnabled != enabled {
			t.Fatal("PUT response has incorrect diagnostic flag")
		}
		f.checkSetting(t, enabled)
	}
	for _, body := range []string{`{"site_name":"fixture update"}`, `{"stream_flush_interval_ms":40}`, `{"codex_upstream_cookie_log_enabled":null}`} {
		if rec := f.request(http.MethodPut, "/api/admin/settings", body, upstreamCookieAdminKey); rec.Code != http.StatusOK {
			t.Fatalf("unrelated update status = %d", rec.Code)
		}
		f.checkSetting(t, true)
	}
	if rec := f.request(http.MethodPut, "/api/admin/settings", `{"codex_upstream_cookie_log_enabled":"true"}`, upstreamCookieAdminKey); rec.Code != http.StatusBadRequest {
		t.Fatal("non-boolean diagnostic setting was accepted")
	}
	f.checkSetting(t, true)
	saved, err := f.db.GetSystemSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	proxy.ApplyRuntimeSettings(proxy.DefaultRuntimeSettings())
	if proxy.CurrentRuntimeSettings().CodexUpstreamCookieLogEnabled {
		t.Fatal("default runtime unexpectedly enables diagnostics")
	}
	proxy.ApplyRuntimeSettingsFromSystem(saved)
	f.checkSetting(t, true)
	if err := f.db.UpdateCodexUpstreamCookieLogEnabled(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	saved, err = f.db.GetSystemSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	proxy.ApplyRuntimeSettingsFromSystem(saved)
	f.checkSetting(t, false)
}

func TestUpstreamCookieSettingsPersistenceFailureDoesNotApply(t *testing.T) {
	for _, column := range []string{"site_name", "codex_upstream_cookie_log_enabled"} {
		for _, initiallyEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", column, initiallyEnabled), func(t *testing.T) {
				f := newUpstreamCookieAdminFixture(t)
				if initiallyEnabled {
					if rec := f.request(http.MethodPut, "/api/admin/settings", `{"codex_upstream_cookie_log_enabled":true}`, upstreamCookieAdminKey); rec.Code != http.StatusOK {
						t.Fatal("could not enable fixture diagnostics")
					}
				}
				// Fail the bulk write or the dedicated write independently.
				if _, err := f.conn.Exec(`CREATE TRIGGER fail_cookie_settings BEFORE UPDATE OF ` + column + ` ON system_settings BEGIN SELECT RAISE(ABORT, 'synthetic persistence failure'); END`); err != nil {
					t.Fatal(err)
				}
				rec := f.request(http.MethodPut, "/api/admin/settings", fmt.Sprintf(`{"codex_upstream_cookie_log_enabled":%t}`, !initiallyEnabled), upstreamCookieAdminKey)
				if rec.Code != http.StatusInternalServerError {
					t.Fatalf("failed persistence status = %d", rec.Code)
				}
				f.checkSetting(t, initiallyEnabled)
			})
		}
	}
}

func TestUpstreamCookieAdminDetailAuthOldMissingAndInvalid(t *testing.T) {
	f := newUpstreamCookieAdminFixture(t)
	for _, trace := range []string{upstreamCookieAdminFixture, ""} {
		if err := f.db.InsertUsageLog(context.Background(), &database.UsageLogInput{Model: "fixture-model", StatusCode: 200, UpstreamCookieTrace: trace}); err != nil {
			t.Fatal("could not queue fixture usage")
		}
	}
	f.db.FlushUsageLogs()
	logs, err := f.db.ListRecentUsageLogs(context.Background(), 10)
	if err != nil || len(logs) != 2 {
		t.Fatal("fixture usage rows were not stored")
	}
	path := fmt.Sprintf("/api/admin/usage/logs/%d/upstream-cookies", logs[1].ID)
	for _, key := range []string{"", "synthetic-wrong-key"} {
		rec := f.request(http.MethodGet, path, "", key)
		if rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), "synthetic_cookie") {
			t.Fatal("unauthenticated request exposed diagnostics")
		}
	}
	rec := f.request(http.MethodGet, path, "", upstreamCookieAdminKey)
	var response struct {
		Entries   []json.RawMessage `json:"entries"`
		Truncated bool              `json:"truncated"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &response) != nil || len(response.Entries) != 1 || !response.Truncated || !strings.Contains(rec.Body.String(), "synthetic_cookie") {
		t.Fatal("authenticated detail did not return parsed diagnostics")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("cookie detail is cacheable")
	}
	old := f.request(http.MethodGet, fmt.Sprintf("/api/admin/usage/logs/%d/upstream-cookies", logs[0].ID), "", upstreamCookieAdminKey)
	if old.Code != http.StatusOK || old.Body.String() != `{"entries":[],"truncated":false}` {
		t.Fatal("old row did not return an empty diagnostic envelope")
	}
	for _, tc := range []struct {
		id     string
		status int
	}{{"9999999", http.StatusNotFound}, {"0", http.StatusBadRequest}, {"-1", http.StatusBadRequest}, {"invalid", http.StatusBadRequest}, {"9223372036854775808", http.StatusBadRequest}} {
		rec := f.request(http.MethodGet, "/api/admin/usage/logs/"+tc.id+"/upstream-cookies", "", upstreamCookieAdminKey)
		if rec.Code != tc.status || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("detail ID validation status = %d, want %d", rec.Code, tc.status)
		}
	}
	for _, corrupt := range []string{`{"entries":["synthetic_cookie"],"truncated":false}`, `synthetic_cookie-invalid-json`} {
		if _, err := f.conn.Exec(`UPDATE usage_logs SET upstream_cookie_trace = $1 WHERE id = $2`, corrupt, logs[1].ID); err != nil {
			t.Fatal("could not seed corrupt fixture")
		}
		rec := f.request(http.MethodGet, path, "", upstreamCookieAdminKey)
		if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "synthetic_cookie") || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("corrupt detail did not return a generic, uncacheable error")
		}
	}
	for _, route := range f.router.Routes() {
		if strings.Contains(route.Path, "upstream-cookies") && route.Path != "/api/admin/usage/logs/:id/upstream-cookies" {
			t.Fatal("cookie handler was exposed outside its admin route")
		}
	}
}

func TestUpstreamCookieOrdinaryListsPublicAndExportsDoNotExposeRaw(t *testing.T) {
	f := newUpstreamCookieAdminFixture(t)
	const fixtureKey = "sk-synthetic-cookie-public-usage"
	keyID, err := f.db.InsertAPIKey(context.Background(), "fixture-key", fixtureKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.InsertUsageLog(context.Background(), &database.UsageLogInput{Model: "fixture-cookie-model", Endpoint: "/v1/responses", StatusCode: 502, APIKeyID: keyID, UpstreamCookieTrace: upstreamCookieAdminFixture}); err != nil {
		t.Fatal("could not queue fixture usage")
	}
	f.db.FlushUsageLogs()
	query := url.Values{"start": {time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)}, "end": {time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}}.Encode()
	for _, path := range []string{
		"/api/admin/usage/logs",
		"/api/admin/usage/logs?" + query,
		"/api/admin/usage/logs?page=1&" + query,
		"/api/admin/ops/errors?" + query,
		"/api/admin/ops/errors/export?" + query,
		"/api/key-usage/summary?range=all",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-Admin-Key", upstreamCookieAdminKey)
		req.Header.Set("Authorization", "Bearer "+fixtureKey)
		rec := httptest.NewRecorder()
		f.router.ServeHTTP(rec, req)
		body := rec.Body.String()
		if rec.Code != http.StatusOK || !strings.Contains(body, "fixture-cookie-model") {
			t.Fatalf("usage fixture absent from %s (status %d)", path, rec.Code)
		}
		if strings.Contains(body, "synthetic_cookie") || strings.Contains(body, "upstream_cookie") {
			t.Fatal("ordinary list, export, or public response exposed cookie diagnostics")
		}
	}
}
