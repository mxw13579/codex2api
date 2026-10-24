package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/cache"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

// callClearAntigravityQuota 以 HTTP 契约（JSON body）驱动清除入口，返回响应码与 body。
func callClearAntigravityQuota(t *testing.T, handler *Handler, body string) (int, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/accounts/antigravity/quota/clear", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req
	handler.ClearAntigravityQuotaHandler(c)
	return recorder.Code, recorder.Body.String()
}

// TestAdmin_ClearAntigravityQuotaAPI 断言单账号清除配额事实接口成功递增 epoch 并清空门禁。
func TestAdmin_ClearAntigravityQuotaAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newTestAdminDB(t)
	ctx := context.Background()

	// 插入带 quota 事实与 epoch=1 的 AG 账号（helper 见 admin/handler_test.go:3734）
	accountID, err := db.InsertAccountWithCredentials(ctx, "ag-clear", map[string]interface{}{
		"upstream_type":           "antigravity",
		"access_token":            "at",
		"credential_family_id":    "fam1",
		"antigravity_quota":       `{"schema_version":2}`,
		"antigravity_quota_epoch": 1,
	}, "")
	if err != nil {
		t.Fatalf("insert antigravity account failed: %v", err)
	}

	handler := &Handler{db: db}
	body := `{"account_ids":[` + strconv.FormatInt(accountID, 10) + `]}`
	code, respBody := callClearAntigravityQuota(t, handler, body)

	if code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", code, respBody)
	}

	// 清除必须同事务递增 quota_epoch（Spec §8.2 运维止血、验收 #30/#36）并删除事实
	epoch, epochErr := db.GetAntigravityQuotaEpoch(ctx, accountID)
	if epochErr != nil || epoch != 2 {
		t.Fatalf("expected epoch 2 after clear, got (%d, %v)", epoch, epochErr)
	}

	// 响应必须回显每号清除结果，供前端二次确认后核对
	var response struct {
		Cleared int `json:"cleared"`
		Failed  int `json:"failed"`
		Items   []struct {
			ID         int64 `json:"id"`
			OK         bool  `json:"ok"`
			QuotaEpoch int64 `json:"quota_epoch"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		t.Fatalf("decode response %q: %v", respBody, err)
	}
	if response.Cleared != 1 || response.Failed != 0 || len(response.Items) != 1 {
		t.Fatalf("response = %+v, want cleared=1 failed=0 items=1", response)
	}
	if response.Items[0].ID != accountID || !response.Items[0].OK || response.Items[0].QuotaEpoch != 2 {
		t.Fatalf("cleared item = %+v, want id=%d ok epoch=2", response.Items[0], accountID)
	}

	// 凭据与 models 分区必须保留（Spec §8.2：只删配额 source 事实）
	row, err := db.GetAccountByID(ctx, accountID)
	if err != nil {
		t.Fatalf("reload account: %v", err)
	}
	if strings.TrimSpace(row.GetCredential("access_token")) == "" {
		t.Fatalf("clear wiped credentials: %+v", row.Credentials)
	}
}

// TestAdmin_ClearAntigravityQuotaAPIRejectsInvalidBatches 断言空批量或超出 100 限制的请求被 400 拒绝且不推进纪元。
func TestAdmin_ClearAntigravityQuotaAPIRejectsInvalidBatches(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newTestAdminDB(t)
	ctx := context.Background()
	accountID, err := db.InsertAccountWithCredentials(ctx, "ag-clear-batch", map[string]interface{}{
		"upstream_type":           "antigravity",
		"access_token":            "at",
		"antigravity_quota":       `{"schema_version":2}`,
		"antigravity_quota_epoch": 4,
	}, "")
	if err != nil {
		t.Fatalf("insert antigravity account failed: %v", err)
	}
	handler := &Handler{db: db}

	if code, body := callClearAntigravityQuota(t, handler, `{"account_ids":[]}`); code != http.StatusBadRequest {
		t.Fatalf("empty batch status = %d body=%s, want 400", code, body)
	}

	ids := make([]string, 0, 101)
	for i := 0; i < 101; i++ {
		ids = append(ids, strconv.FormatInt(accountID, 10))
	}
	tooMany := `{"account_ids":[` + strings.Join(ids, ",") + `]}`
	if code, body := callClearAntigravityQuota(t, handler, tooMany); code != http.StatusBadRequest {
		t.Fatalf("101 ids status = %d body=%s, want 400", code, body)
	}

	// 两次非法请求都不得推进纪元，否则会把在途观测无端作废
	epoch, err := db.GetAntigravityQuotaEpoch(ctx, accountID)
	if err != nil || epoch != 4 {
		t.Fatalf("epoch after rejected batches = (%d, %v), want 4", epoch, err)
	}
}

// TestAdmin_ClearAntigravityQuotaAPIRejectsNonAntigravityAccount 断言非 Antigravity 渠道账号被 400 拦截且零副作用。
func TestAdmin_ClearAntigravityQuotaAPIRejectsNonAntigravityAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newTestAdminDB(t)
	ctx := context.Background()
	grokID, err := db.InsertAccountWithCredentials(ctx, "grok-clear", map[string]interface{}{
		"upstream_type": "grok",
		"api_key":       "secret",
	}, "")
	if err != nil {
		t.Fatalf("insert grok account failed: %v", err)
	}
	handler := &Handler{db: db}

	code, body := callClearAntigravityQuota(t, handler, `{"account_ids":[`+strconv.FormatInt(grokID, 10)+`]}`)
	if code != http.StatusBadRequest {
		t.Fatalf("non-antigravity status = %d body=%s, want 400", code, body)
	}
	epoch, err := db.GetAntigravityQuotaEpoch(ctx, grokID)
	if err != nil || epoch != 0 {
		t.Fatalf("non-antigravity epoch = (%d, %v), want untouched 0", epoch, err)
	}
}

// TestAdmin_ClearAntigravityQuotaRouteRegistered 断言 /api/admin/accounts/antigravity/quota/clear 路由正确挂载。
func TestAdmin_ClearAntigravityQuotaRouteRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newTestAdminDB(t)
	tokenCache := cache.NewMemory(4)
	t.Cleanup(func() { _ = tokenCache.Close() })
	store := auth.NewStore(db, tokenCache, nil)
	t.Cleanup(store.Stop)
	handler := NewHandler(store, db, tokenCache, nil, "admin-secret")

	router := gin.New()
	handler.RegisterRoutes(router)

	found := false
	for _, route := range router.Routes() {
		if route.Method == http.MethodPost && route.Path == "/api/admin/accounts/antigravity/quota/clear" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("quota clear route is not registered on the admin router")
	}
}

// TestAdmin_AntigravityStateExposesQuotaGateV2Metadata 锁定 /state 的 V2 观测契约：
// schema_version、source_metadata 与 stale 四态必须可见（Spec §3.3、§8.2、§9 管理层）。
func TestAdmin_AntigravityStateExposesQuotaGateV2Metadata(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC().Truncate(time.Second)
	observed := now.Add(-3 * time.Hour)
	resetAt := observed.Add(5 * time.Hour)
	quota := fmt.Sprintf(`{
		"schema_version": 2,
		"models": [{"model_id":"gemini-3.6-flash-low","remaining_fraction":0,"fraction_present":true}],
		"quota_groups": [{"display_name":"Antigravity","buckets":[{
			"bucket_id":"5h","window":"5h","remaining_fraction":0,"fraction_present":true,
			"reset_time":%q,"observed_at":%q,"ttl_seconds":3600,"observation_seq":9
		}]}],
		"source_metadata": {"groups": {
			"producer":"quota_summary","trigger":"scheduled","credential_family_id":"fam1",
			"quota_identity_scope":"scope-1","generation":3,"current_attempt_seq":9,"applied_seq":9,
			"observed_at":%q,"last_attempt_at":%q,"last_success_at":%q,
			"failure_count":0,"presence_verified":true,"summary_integrity":true
		}},
		"updated_at": %q
	}`, resetAt.Format(time.RFC3339), observed.Format(time.RFC3339), observed.Format(time.RFC3339),
		observed.Format(time.RFC3339), observed.Format(time.RFC3339), observed.Format(time.RFC3339))

	handler, _, id := newAntigravityStateTestHandler(t, map[string]any{
		"upstream_type": auth.UpstreamAntigravity, "access_token": "access", "project_id": "project",
		"models": []string{"gemini-3.6-flash-low"}, "antigravity_quota": quota, "antigravity_quota_epoch": 3,
	})

	recorder := callAntigravityAdminHandler(t, http.MethodGet, "/state", id, handler.GetAntigravityAccountState)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var state antigravityAccountState
	if err := json.Unmarshal(recorder.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Quota == nil || state.Quota.SchemaVersion != 2 {
		t.Fatalf("state.quota = %+v, want schema_version 2", state.Quota)
	}
	if len(state.Quota.SourceMetadata) != 1 || state.Quota.SourceMetadata["groups"].AppliedSeq != 9 {
		t.Fatalf("state.quota.source_metadata = %+v, want groups applied_seq 9", state.Quota.SourceMetadata)
	}
	if state.QuotaEpoch != 3 {
		t.Fatalf("state.quota_epoch = %d, want 3", state.QuotaEpoch)
	}
	gate := state.QuotaGate
	if gate == nil {
		t.Fatalf("state.quota_gate missing: %s", recorder.Body.String())
	}
	if gate.SchemaVersion != 2 || !gate.Blocked || gate.Reason != "stale_zero" || !gate.Stale {
		t.Fatalf("quota gate = %+v, want schema 2 blocked stale_zero stale", *gate)
	}
	if gate.ResetAt != resetAt.Format(time.RFC3339) {
		t.Fatalf("gate.reset_at = %q, want %q", gate.ResetAt, resetAt.Format(time.RFC3339))
	}
	if len(gate.SourceMetadata) != 1 || !gate.SourceMetadata["groups"].PresenceVerified {
		t.Fatalf("gate.source_metadata = %+v, want verified groups metadata", gate.SourceMetadata)
	}

	// 止血清除后门禁必须立即解除：清除入口把纪元推进后 /state 不再报阻断
	if code, body := callClearAntigravityQuota(t, handler, `{"account_ids":[`+strconv.FormatInt(id, 10)+`]}`); code != http.StatusOK {
		t.Fatalf("clear status=%d body=%s", code, body)
	}
	recorder = callAntigravityAdminHandler(t, http.MethodGet, "/state", id, handler.GetAntigravityAccountState)
	if recorder.Code != http.StatusOK {
		t.Fatalf("state after clear status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var cleared antigravityAccountState
	if err := json.Unmarshal(recorder.Body.Bytes(), &cleared); err != nil {
		t.Fatal(err)
	}
	if cleared.QuotaEpoch != 4 {
		t.Fatalf("state.quota_epoch after clear = %d, want 4", cleared.QuotaEpoch)
	}
	if cleared.QuotaGate == nil || cleared.QuotaGate.Blocked || cleared.QuotaGate.Stale {
		t.Fatalf("quota gate after clear = %+v, want unblocked", cleared.QuotaGate)
	}
	if cleared.Quota != nil && len(cleared.Quota.Groups) != 0 {
		t.Fatalf("quota groups after clear = %+v, want emptied", cleared.Quota.Groups)
	}
}

// TestAdmin_AntigravityManualSyncRoutesThroughStoreCoordinator 锁定 P1 修复契约：
// auth.Store 必须实现 SyncAntigravityAccountForAdmin 使接口探测成功，手动同步真正
// 交由 Store 协调器执行（flight 单飞 + family lease），而不是探测失败后静默回退到
// 直连 client.Sync 空转。store 挂载但不带 database：Store 入口必须返回
// database-backed 错误且不发生直连网络路径。
func TestAdmin_AntigravityManualSyncRoutesThroughStoreCoordinator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newTestAdminDB(t)
	ctx := context.Background()
	accountID, err := db.InsertAccountWithCredentials(ctx, "ag-wired", map[string]interface{}{
		"upstream_type":        "antigravity",
		"access_token":         "at-old",
		"refresh_token":        "rt-old",
		"credential_family_id": "fam-wired",
	}, "")
	if err != nil {
		t.Fatalf("insert antigravity account failed: %v", err)
	}

	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 2})
	t.Cleanup(store.Stop)
	if _, ok := antigravityStoreCoordinator(store); !ok {
		t.Fatal("auth.Store must satisfy antigravityStoreSyncCoordinator; interface probe failed")
	}

	handler := &Handler{db: db, store: store}
	item := handler.refreshAntigravityAccount(ctx, accountID)
	if item.OK {
		t.Fatal("manual sync through store must fail when the store has no database")
	}
	if !strings.Contains(item.Error, "database-backed store") {
		t.Fatalf("error = %q, want Store coordinator path error; fell back to direct client.Sync?", item.Error)
	}
}

// TestAdmin_AntigravityManualSyncSucceedsThroughStoreCoordinator 端到端验证手动
// 同步经 Store 统一入口成功：协调器内完成换票与 quota 事实落库，管理端回显并
// 重载运行时账号。
func TestAdmin_AntigravityManualSyncSucceedsThroughStoreCoordinator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var tokenCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/token":
			atomic.AddInt32(&tokenCalls, 1)
			fmt.Fprint(w, `{"access_token":"wired-access-new","refresh_token":"wired-refresh-new","expires_in":3600}`)
		case "/userinfo":
			fmt.Fprint(w, `{"id":"subject-9","email":"wired@example.com","verified_email":true}`)
		case "/load":
			fmt.Fprint(w, `{"cloudaicompanionProject":"project-new","paidTier":{"name":"Pro"}}`)
		case "/quota":
			fmt.Fprint(w, `{"models":{"gemini-test":{"quotaInfo":{"remainingFraction":0.8}}}}`)
		case "/summary":
			fmt.Fprint(w, `{"groups":[]}`)
		case "/credits":
			fmt.Fprint(w, `{"paidTier":{"availableCredits":[]}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	previousEndpoints := auth.DefaultAntigravityEndpoints
	auth.DefaultAntigravityEndpoints = auth.AntigravityEndpoints{
		TokenURL: server.URL + "/token", UserInfoURL: server.URL + "/userinfo",
		LoadProject: []string{server.URL + "/load"}, Quota: []string{server.URL + "/quota"},
		QuotaSummary: []string{server.URL + "/summary"}, AICredits: []string{server.URL + "/credits"},
	}
	defer func() { auth.DefaultAntigravityEndpoints = previousEndpoints }()

	db := newTestAdminDB(t)
	ctx := context.Background()
	accountID, err := db.InsertAccountWithCredentials(ctx, "ag-manual-sync", map[string]interface{}{
		"upstream_type":             "antigravity",
		"access_token":              "at-old",
		"refresh_token":             "rt-old",
		"oauth_client_key":          "test",
		"antigravity_client_id":     "client-id",
		"antigravity_client_secret": "client-secret",
		// 过期存量 AT：手动同步通道由 client.Sync 按需换票。
		"expires_at":           time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
		"email":                "wired@example.com",
		"credential_family_id": "fam-manual",
	}, "")
	if err != nil {
		t.Fatalf("insert antigravity account failed: %v", err)
	}
	store := auth.NewStore(db, nil, &database.SystemSettings{MaxConcurrency: 2})
	t.Cleanup(store.Stop)
	handler := &Handler{db: db, store: store}

	item := handler.refreshAntigravityAccount(ctx, accountID)
	if !item.OK {
		t.Fatalf("manual sync through store failed: %+v", item)
	}
	if atomic.LoadInt32(&tokenCalls) == 0 {
		t.Fatal("store coordinator never exchanged the token")
	}
	row, err := db.GetAccountByID(ctx, accountID)
	if err != nil {
		t.Fatalf("reload account: %v", err)
	}
	if got := strings.TrimSpace(row.GetCredential("access_token")); got != "wired-access-new" {
		t.Fatalf("persisted access_token = %q, want wired-access-new", got)
	}
	if store.FindByID(accountID) == nil {
		t.Fatal("runtime account missing after manual sync reload")
	}
}
