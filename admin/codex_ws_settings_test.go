package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy"
)

func TestCodexWSChatRetentionSettingsPersistence(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	for _, tt := range []struct {
		name, body  string
		chat, blank int
	}{
		{"defaults preserved", `{"site_name":"retention test"}`, 8, 8},
		{"existing values preserved", `{"codex_ws_downstream_keepalive_slots":32,"codex_ws_stateless_slots":32}`, 32, 32},
		{"disabled", `{"codex_ws_downstream_keepalive_slots":0}`, 0, 32},
		{"negative restores default", `{"codex_ws_downstream_keepalive_slots":-1}`, 8, 32},
		{"above old maximum", `{"codex_ws_downstream_keepalive_slots":33}`, 33, 32},
		{"48 connections", `{"codex_ws_downstream_keepalive_slots":48}`, 48, 32},
		{"64 connections", `{"codex_ws_downstream_keepalive_slots":64}`, 64, 32},
		{"96 connections", `{"codex_ws_downstream_keepalive_slots":96}`, 96, 32},
		{"new maximum", `{"codex_ws_downstream_keepalive_slots":128}`, 128, 32},
		{"above maximum clamps", `{"codex_ws_downstream_keepalive_slots":129}`, 128, 32},
		{"omitted preserves value", `{"site_name":"unrelated update"}`, 128, 32},
		{"null preserves value", `{"codex_ws_downstream_keepalive_slots":null}`, 128, 32},
		{"blank maximum unchanged", `{"codex_ws_stateless_slots":128}`, 128, 32},
		{"hot decrease", `{"codex_ws_downstream_keepalive_slots":32}`, 32, 32},
		{"hot disable", `{"codex_ws_downstream_keepalive_slots":0}`, 0, 32},
	} {
		t.Run(tt.name, func(t *testing.T) {
			put := invokeTransient429SettingsAdmin(handler, http.MethodPut, tt.body)
			get := invokeTransient429SettingsAdmin(handler, http.MethodGet, "")
			if put.Code != http.StatusOK || get.Code != http.StatusOK {
				t.Fatalf("PUT/GET status=%d/%d, body=%s", put.Code, get.Code, put.Body.String())
			}
			for _, body := range [][]byte{put.Body.Bytes(), get.Body.Bytes()} {
				var got struct {
					Chat  *int `json:"codex_ws_downstream_keepalive_slots"`
					Blank *int `json:"codex_ws_stateless_slots"`
				}
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatal(err)
				}
				if got.Chat == nil || got.Blank == nil || *got.Chat != tt.chat || *got.Blank != tt.blank {
					t.Fatalf("settings response chat/blank=%+v, want %d/%d", got, tt.chat, tt.blank)
				}
			}
			persisted, err := db.GetSystemSettings(context.Background())
			if err != nil || persisted == nil {
				t.Fatalf("GetSystemSettings: present=%t err=%v", persisted != nil, err)
			}
			if persisted.CodexWSDownstreamKeepaliveSlots != tt.chat || persisted.CodexWSStatelessSlots != tt.blank {
				t.Fatalf("persisted chat/blank=%d/%d, want %d/%d", persisted.CodexWSDownstreamKeepaliveSlots, persisted.CodexWSStatelessSlots, tt.chat, tt.blank)
			}
			reloaded := auth.NewStore(nil, nil, persisted)
			defer reloaded.Stop()
			for _, store := range []*auth.Store{handler.store, reloaded} {
				if store.CodexWSDownstreamKeepaliveSlots() != tt.chat || store.CodexWSStatelessSlots() != tt.blank {
					t.Fatalf("store chat/blank=%d/%d, want %d/%d", store.CodexWSDownstreamKeepaliveSlots(), store.CodexWSStatelessSlots(), tt.chat, tt.blank)
				}
			}
			runtime := proxy.CurrentRuntimeSettings()
			if runtime.CodexWSDownstreamKeepaliveSlots != tt.chat || runtime.CodexWSStatelessSlots != tt.blank {
				t.Fatalf("runtime chat/blank=%d/%d, want %d/%d", runtime.CodexWSDownstreamKeepaliveSlots, runtime.CodexWSStatelessSlots, tt.chat, tt.blank)
			}
		})
	}
}

func TestCodexWSChatRetentionSettingsRejectInvalidTypes(t *testing.T) {
	handler, db, _ := newTransient429SettingsAdminHandler(t)
	put := invokeTransient429SettingsAdmin(handler, http.MethodPut, `{"codex_ws_downstream_keepalive_slots":64}`)
	if put.Code != http.StatusOK {
		t.Fatalf("initial PUT: status=%d", put.Code)
	}
	for _, value := range []string{`"128"`, `64.5`, `true`, `{}`, `[]`, `9223372036854775808`} {
		response := invokeTransient429SettingsAdmin(handler, http.MethodPut, `{"codex_ws_downstream_keepalive_slots":`+value+`}`)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("value=%s status=%d, want 400", value, response.Code)
		}
		persisted, err := db.GetSystemSettings(context.Background())
		if err != nil || persisted == nil {
			t.Fatalf("GetSystemSettings: present=%t err=%v", persisted != nil, err)
		}
		if persisted.CodexWSDownstreamKeepaliveSlots != 64 || handler.store.CodexWSDownstreamKeepaliveSlots() != 64 || proxy.CurrentRuntimeSettings().CodexWSDownstreamKeepaliveSlots != 64 {
			t.Fatal("invalid update changed chat retention")
		}
	}
}
