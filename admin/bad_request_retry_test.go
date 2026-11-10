package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/codex2api/proxy"
)

func TestBadRequestRetrySettingsAPIAndReload(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	def := database.DefaultBadRequestRetryPolicy()
	assertBadRequestRetryResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodGet, ""), def)
	general, rate := handler.store.GetMaxRetries(), handler.store.GetMaxRateLimitRetries()
	continuous := database.EncodeContinuousRetryPolicy(handler.store.GetContinuousRetryPolicy())
	for _, step := range []struct {
		name, body string
		want       database.BadRequestRetryPolicy
	}{
		{"enable default budget", `{"bad_request_retry_enabled":true}`, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 1}},
		{"zero is not default", `{"bad_request_retry_max_retries":0}`, database.BadRequestRetryPolicy{Enabled: true}},
		{"switch only", `{"bad_request_retry_switch_account":true}`, database.BadRequestRetryPolicy{Enabled: true, SwitchAccount: true}},
		{"max boundary", `{"bad_request_retry_max_retries":10}`, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true}},
		{"unrelated save", `{"site_name":"synthetic retry test"}`, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true}},
		{"null omitted", `{"bad_request_retry_enabled":null,"bad_request_retry_max_retries":null,"bad_request_retry_switch_account":null}`, database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 10, SwitchAccount: true}},
		{"disable preserves preferences", `{"bad_request_retry_enabled":false}`, database.BadRequestRetryPolicy{MaxRetries: 10, SwitchAccount: true}},
		{"explicit false and zero", `{"bad_request_retry_switch_account":false,"bad_request_retry_max_retries":0}`, database.BadRequestRetryPolicy{}},
	} {
		t.Run(step.name, func(t *testing.T) {
			assertBadRequestRetryResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodPut, step.body), step.want)
			assertBadRequestRetryResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodGet, ""), step.want)
			assertBadRequestRetryAdminState(t, handler, db, step.want, step.want)
			settings, err := db.GetSystemSettings(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			// Reconstruct Store and proxy settings as startup does; no accounts,
			// authentication or upstream requests are involved.
			reloaded := auth.NewStore(nil, nil, settings)
			t.Cleanup(reloaded.Stop)
			if got := reloaded.GetBadRequestRetryPolicy(); got != step.want {
				t.Fatalf("reloaded store = %+v, want %+v", got, step.want)
			}
			if got := proxy.ApplyRuntimeSettingsFromSystem(settings).BadRequestRetryPolicy; got != step.want {
				t.Fatalf("reloaded runtime = %+v, want %+v", got, step.want)
			}
		})
	}
	if handler.store.GetMaxRetries() != general || handler.store.GetMaxRateLimitRetries() != rate ||
		database.EncodeContinuousRetryPolicy(handler.store.GetContinuousRetryPolicy()) != continuous {
		t.Fatal("Bad Request settings changed an existing retry budget")
	}
}

func TestBadRequestRetrySettingsInvalidValuesHaveNoSideEffects(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	want := database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 3, SwitchAccount: true}
	assertBadRequestRetryResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodPut,
		`{"bad_request_retry_enabled":true,"bad_request_retry_max_retries":3,"bad_request_retry_switch_account":true}`), want)
	for _, field := range []struct {
		name   string
		values []string
	}{
		{"bad_request_retry_max_retries", []string{"-1", "11", "1.5", `"2"`, "true", "{}", "[]", "999999999999999999999"}},
		{"bad_request_retry_enabled", []string{"1", `"true"`, "{}", "[]"}},
		{"bad_request_retry_switch_account", []string{"0", `"false"`, "{}", "[]"}},
	} {
		for _, value := range field.values {
			response := invokeTransient429SettingsAdmin(handler, http.MethodPut,
				`{"max_retries":9,"`+field.name+`":`+value+`}`)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("%s=%s status=%d, want 400", field.name, value, response.Code)
			}
			assertBadRequestRetryAdminState(t, handler, db, want, want)
			if handler.store.GetMaxRetries() == 9 {
				t.Fatal("invalid policy mutated an unrelated setting before validation")
			}
		}
	}
}

func TestBadRequestRetrySettingsPersistBeforePublish(t *testing.T) {
	for _, stage := range []struct{ name, trigger string }{
		{"full save", `CREATE TRIGGER reject_bad_request_full BEFORE INSERT ON system_settings
			BEGIN SELECT RAISE(ABORT, 'synthetic full save failure'); END`},
		{"policy save", `CREATE TRIGGER reject_bad_request_policy BEFORE UPDATE OF bad_request_retry_policy ON system_settings
			BEGIN SELECT RAISE(ABORT, 'synthetic policy save failure'); END`},
	} {
		for _, enabled := range []bool{false, true} {
			name := stage.name + "/enable"
			if enabled {
				name = stage.name + "/disable"
			}
			t.Run(name, func(t *testing.T) {
				handler, db, path := newTransient429SettingsAdminHandler(t)
				want := database.BadRequestRetryPolicy{Enabled: enabled, MaxRetries: 4, SwitchAccount: true}
				if _, err := db.UpdateBadRequestRetryPolicy(context.Background(), database.BadRequestRetryPolicyUpdate{
					Enabled: &want.Enabled, MaxRetries: &want.MaxRetries, SwitchAccount: &want.SwitchAccount,
				}); err != nil {
					t.Fatal(err)
				}
				handler.store.SetBadRequestRetryPolicy(want)
				proxy.UpdateRuntimeSettings(func(current proxy.RuntimeSettings) proxy.RuntimeSettings {
					current.BadRequestRetryPolicy = want
					return current
				})
				installTransient429SettingsTrigger(t, path, stage.trigger)
				body := `{"bad_request_retry_enabled":true,"bad_request_retry_max_retries":0,"bad_request_retry_switch_account":false}`
				if enabled {
					body = `{"bad_request_retry_enabled":false,"bad_request_retry_max_retries":0,"bad_request_retry_switch_account":false}`
				}
				response := invokeTransient429SettingsAdmin(handler, http.MethodPut, body)
				if response.Code != http.StatusInternalServerError {
					t.Fatalf("failed save status=%d, want 500", response.Code)
				}
				assertBadRequestRetryAdminState(t, handler, db, want, want)
				assertBadRequestRetryResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodGet, ""), want)
			})
		}
	}
}

func TestBadRequestRetrySettingsPartialUpdateUsesLatestDatabase(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	want := database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 8, SwitchAccount: true}
	if _, err := db.UpdateBadRequestRetryPolicy(context.Background(), database.BadRequestRetryPolicyUpdate{
		Enabled: &want.Enabled, MaxRetries: &want.MaxRetries, SwitchAccount: &want.SwitchAccount,
	}); err != nil {
		t.Fatal(err)
	}
	// An old Store must neither write its snapshot nor hot-publish it on an
	// unrelated save. The integration thread owns cross-instance reload.
	assertBadRequestRetryResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodPut,
		`{"site_name":"synthetic stale instance"}`), database.DefaultBadRequestRetryPolicy())
	assertBadRequestRetryAdminState(t, handler, db, database.DefaultBadRequestRetryPolicy(), want)
	want.MaxRetries = 0
	assertBadRequestRetryResponse(t, invokeTransient429SettingsAdmin(handler, http.MethodPut,
		`{"bad_request_retry_max_retries":0}`), want)
	assertBadRequestRetryAdminState(t, handler, db, want, want)
}

func TestBadRequestRetryStoreAndRuntimeDefaults(t *testing.T) {
	var nilStore *auth.Store
	store := &auth.Store{}
	def := database.DefaultBadRequestRetryPolicy()
	if nilStore.GetBadRequestRetryPolicy() != def || store.GetBadRequestRetryPolicy() != def ||
		proxy.DefaultRuntimeSettings().BadRequestRetryPolicy != def {
		t.Fatal("uninitialized policy should be disabled with one extra retry")
	}
	nilStore.SetBadRequestRetryPolicy(def)
	store.SetBadRequestRetryPolicy(database.BadRequestRetryPolicy{Enabled: true, MaxRetries: 11})
	if store.GetBadRequestRetryPolicy().MaxRetries != 10 {
		t.Fatal("store did not normalize upper bound")
	}
	copy := store.GetBadRequestRetryPolicy()
	copy.MaxRetries = 0
	if store.GetBadRequestRetryPolicy().MaxRetries != 10 {
		t.Fatal("getter did not return a value copy")
	}
	settings := proxy.DefaultRuntimeSettings()
	settings.BadRequestRetryPolicy = copy
	if proxy.NormalizeRuntimeSettings(settings).BadRequestRetryPolicy.MaxRetries != 0 {
		t.Fatal("runtime must preserve zero")
	}
}

func assertBadRequestRetryResponse(t *testing.T, response *httptest.ResponseRecorder, want database.BadRequestRetryPolicy) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("settings response status=%d, want 200", response.Code)
	}
	var decoded struct {
		Enabled       *bool `json:"bad_request_retry_enabled"`
		MaxRetries    *int  `json:"bad_request_retry_max_retries"`
		SwitchAccount *bool `json:"bad_request_retry_switch_account"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Enabled == nil || decoded.MaxRetries == nil || decoded.SwitchAccount == nil {
		t.Fatal("response must include all Bad Request retry fields, even false/zero")
	}
	got := database.BadRequestRetryPolicy{Enabled: *decoded.Enabled, MaxRetries: *decoded.MaxRetries, SwitchAccount: *decoded.SwitchAccount}
	if got != want {
		t.Fatalf("response policy=%+v, want %+v", got, want)
	}
}

func assertBadRequestRetryAdminState(t *testing.T, handler *Handler, db *database.DB, runtimeWant, persistedWant database.BadRequestRetryPolicy) {
	t.Helper()
	if got := handler.store.GetBadRequestRetryPolicy(); got != runtimeWant {
		t.Fatalf("store policy=%+v, want %+v", got, runtimeWant)
	}
	if got := proxy.CurrentRuntimeSettings().BadRequestRetryPolicy; got != runtimeWant {
		t.Fatalf("runtime policy=%+v, want %+v", got, runtimeWant)
	}
	settings, err := db.GetSystemSettings(context.Background())
	if err != nil || settings == nil {
		t.Fatalf("load settings: present=%t err=%v", settings != nil, err)
	}
	if got := database.ParseBadRequestRetryPolicy(settings.BadRequestRetryPolicy); got != persistedWant {
		t.Fatalf("persisted policy=%+v, want %+v", got, persistedWant)
	}
}
