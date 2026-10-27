package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

func TestIntelligenceSettingsConflictDetailsAndResolution(t *testing.T) {
	db := newTestAdminDB(t)
	ctx := context.Background()
	claude, err := db.InsertAccountWithUpstream(ctx, "conflicting claude", "anthropic", "oauth", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	target, err := db.CreateAccountGroup(ctx, "codex target", "", "", 0, 0, sql.NullInt64{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.GetIntelligenceCheckSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Action, s.TargetGroupID = "switch_group", target
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{db: db, adminSecretEnv: "local-admin-key"}
	router := gin.New()
	group := router.Group("/api/admin")
	group.Use(h.adminAuthMiddleware())
	h.registerIntelligenceCheckRoutes(group)
	request := func(method, path, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/admin/intelligence-check/"+path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Admin-Key", key)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	if w := request("PUT", "settings", string(raw), "wrong"); w.Code != 401 || strings.Contains(w.Body.String(), "conflicting claude") {
		t.Fatalf("unauthenticated details: %d %s", w.Code, w.Body.String())
	}
	w := request("PUT", "settings", string(raw), "local-admin-key")
	var body struct {
		Code  string `json:"code"`
		Error string `json:"error"`
		database.IntelligenceCheckTargetConflictError
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 400 || body.Code != "intelligence_check_target_conflict" || body.Error == "" || body.TargetGroupID != target || body.TargetGroupChannel != "codex" || body.ConflictCount != 1 || len(body.Conflicts) != 1 || body.Conflicts[0].AccountID != claude || body.Conflicts[0].AccountName != "conflicting claude" || body.Conflicts[0].Channel != "claude" {
		t.Fatalf("conflict response: %d %s", w.Code, w.Body.String())
	}
	before, err := db.GetIntelligenceCheckSettings(ctx)
	if err != nil || before.Revision != s.Revision || before.Action != "none" {
		t.Fatalf("failed save changed settings: %+v %v", before, err)
	}
	if w := request("PATCH", "accounts", fmt.Sprintf(`{"account_ids":[%d],"enabled":false}`, claude), "local-admin-key"); w.Code != 200 {
		t.Fatalf("disable=%d %s", w.Code, w.Body.String())
	}
	if w := request("PUT", "settings", string(raw), "local-admin-key"); w.Code != 200 {
		t.Fatalf("save=%d %s", w.Code, w.Body.String())
	}
	if w := request("PUT", "settings", string(raw), "local-admin-key"); w.Code != 409 {
		t.Fatalf("revision=%d %s", w.Code, w.Body.String())
	}
}
