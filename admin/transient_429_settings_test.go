package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/cache"
	"github.com/codex2api/database"
	"github.com/codex2api/proxy"
	"github.com/gin-gonic/gin"
)

func TestTransient429SettingsGetPutAndOmittedField(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	for _, step := range []struct {
		name   string
		method string
		body   string
		want   bool
	}{
		{"default", http.MethodGet, "", false},
		{"enable", http.MethodPut, `{"codex_transient_429_cooldown_enabled":true}`, true},
		{"get enabled", http.MethodGet, "", true},
		{"omitted", http.MethodPut, `{"site_name":"synthetic unrelated save"}`, true},
		{"null is omitted", http.MethodPut, `{"codex_transient_429_cooldown_enabled":null}`, true},
		{"disable", http.MethodPut, `{"codex_transient_429_cooldown_enabled":false}`, false},
		{"get disabled", http.MethodGet, "", false},
	} {
		t.Run(step.name, func(t *testing.T) {
			response := invokeTransient429SettingsAdmin(handler, step.method, step.body)
			assertTransient429AdminResponse(t, response, step.want)
			assertTransient429AdminState(t, handler, db, step.want, step.want)
		})
	}
}

func TestTransient429SettingsSaveFailureDoesNotToggle(t *testing.T) {
	for _, stage := range []struct {
		name    string
		trigger string
	}{
		{"full save", `CREATE TRIGGER reject_transient_429_settings BEFORE INSERT ON system_settings
			BEGIN SELECT RAISE(ABORT, 'synthetic full save failure'); END`},
		{"narrow save", `CREATE TRIGGER reject_transient_429_settings
			BEFORE UPDATE OF codex_transient_429_cooldown_enabled ON system_settings
			BEGIN SELECT RAISE(ABORT, 'synthetic narrow save failure'); END`},
	} {
		for _, initial := range []bool{false, true} {
			name := stage.name + "/enable"
			if initial {
				name = stage.name + "/disable"
			}
			t.Run(name, func(t *testing.T) {
				handler, db, path := newTransient429SettingsAdminHandler(t)
				if err := db.UpdateCodexTransient429CooldownEnabled(context.Background(), initial); err != nil {
					t.Fatal(err)
				}
				handler.store.SetCodexTransient429CooldownEnabled(initial)
				installTransient429SettingsTrigger(t, path, stage.trigger)
				body := `{"codex_transient_429_cooldown_enabled":true}`
				if initial {
					body = `{"codex_transient_429_cooldown_enabled":false}`
				}
				response := invokeTransient429SettingsAdmin(handler, http.MethodPut, body)
				if response.Code != http.StatusInternalServerError {
					t.Fatalf("failed save status = %d, want 500", response.Code)
				}
				assertTransient429AdminState(t, handler, db, initial, initial)
				assertTransient429AdminResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodGet, ""), initial)
			})
		}
	}
}

func TestTransient429SettingsOmittedFieldPreservesNewerDatabaseValue(t *testing.T) {
	handler, db, path := newTransient429SettingsAdminHandler(t)
	// Simulate another replica's committed change before this Store reloads it.
	if err := db.UpdateCodexTransient429CooldownEnabled(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	handler.store.SetCodexTransient429CooldownEnabled(false)
	installTransient429SettingsTrigger(t, path, `CREATE TRIGGER reject_omitted_transient_429
		BEFORE UPDATE OF codex_transient_429_cooldown_enabled ON system_settings
		BEGIN SELECT RAISE(ABORT, 'omitted switch must not be written'); END`)

	response := invokeTransient429SettingsAdmin(handler, http.MethodPut, `{"site_name":"synthetic stale instance"}`)
	assertTransient429AdminResponse(t, response, false)
	// An omitted field neither rewrites the DB nor publishes a stale snapshot.
	// Cross-instance runtime reload is owned by the main integration, not admin.
	assertTransient429AdminState(t, handler, db, false, true)
	settings, err := db.GetSystemSettings(context.Background())
	if err != nil || settings == nil || settings.SiteName != "synthetic stale instance" {
		t.Fatalf("unrelated save failed: %v", err)
	}
}

func TestTransient429SettingsInvalidJSONTypeDoesNotToggle(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	for _, value := range []string{`"true"`, `1`, `{}`} {
		response := invokeTransient429SettingsAdmin(handler, http.MethodPut, `{"codex_transient_429_cooldown_enabled":`+value+`}`)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid type status = %d, want 400", response.Code)
		}
		assertTransient429AdminState(t, handler, db, false, false)
	}
}

func newTransient429SettingsAdminHandler(t *testing.T) (*Handler, *database.DB, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	previous := proxy.CurrentRuntimeSettings()
	t.Cleanup(func() { proxy.ApplyRuntimeSettings(previous) })
	path := filepath.Join(t.TempDir(), "transient-429-admin.sqlite")
	db, err := database.New("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	settings := defaultBootstrapSettings()
	if err := db.UpdateSystemSettings(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	proxy.ApplyRuntimeSettingsFromSystem(settings)
	tc := cache.NewMemory(4)
	t.Cleanup(func() { _ = tc.Close() })
	store := auth.NewStore(db, tc, settings)
	t.Cleanup(store.Stop)
	handler := NewHandler(store, db, tc, proxy.NewRateLimiter(settings.GlobalRPM), "synthetic-admin-secret")
	return handler, db, path
}

func invokeTransient429SettingsAdmin(handler *Handler, method, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(response)
	ctx.Request = httptest.NewRequest(method, "/api/admin/settings", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	if method == http.MethodGet {
		handler.GetSettings(ctx)
	} else {
		handler.UpdateSettings(ctx)
	}
	return response
}

func assertTransient429AdminResponse(t *testing.T, response *httptest.ResponseRecorder, want bool) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("settings status = %d, want 200", response.Code)
	}
	var decoded struct {
		Enabled *bool `json:"codex_transient_429_cooldown_enabled"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode settings response: %v", err)
	}
	if decoded.Enabled == nil {
		t.Fatal("settings response omitted the boolean switch")
	}
	if *decoded.Enabled != want {
		t.Fatalf("response transient 429 cooldown enabled = %t, want %t", *decoded.Enabled, want)
	}
}

func assertTransient429AdminState(t *testing.T, handler *Handler, db *database.DB, runtimeWant, persistedWant bool) {
	t.Helper()
	if got := handler.store.CodexTransient429CooldownEnabled(); got != runtimeWant {
		t.Fatalf("runtime transient 429 cooldown enabled = %t, want %t", got, runtimeWant)
	}
	settings, err := db.GetSystemSettings(context.Background())
	if err != nil || settings == nil {
		t.Fatalf("read settings: present=%t err=%v", settings != nil, err)
	}
	if settings.CodexTransient429CooldownEnabled != persistedWant {
		t.Fatalf("persisted transient 429 cooldown enabled = %t, want %t", settings.CodexTransient429CooldownEnabled, persistedWant)
	}
}

func installTransient429SettingsTrigger(t *testing.T, path, trigger string) {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(trigger); err != nil {
		t.Fatal(err)
	}
}
