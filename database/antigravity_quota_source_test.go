package database

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newAntigravityQuotaTestDB 按方言创建测试库（Postgres 需隔离 DSN，未配置则跳过，
// 模式与 database/auto_reset_exhausted_test.go 一致），并插入一个 AG 账号。
// credential_family_id 列回填为 "fam1"，与 credentials 内的 family 保持一致（真实
// 写入路径中两者由 UpdateAccountCredentialsCAS 同事务同步）。
func newAntigravityQuotaTestDB(t *testing.T, ctx context.Context, creds string) (*DB, int64) {
	t.Helper()
	return newAntigravityQuotaTestDBWithFamily(t, ctx, creds, "fam1")
}

// newAntigravityQuotaTestDBWithFamily 同 newAntigravityQuotaTestDB，但可指定
// credential_family_id 列（传空串保持列为 NULL，用于验证严格 scope fence）。
func newAntigravityQuotaTestDBWithFamily(t *testing.T, ctx context.Context, creds, familyID string) (*DB, int64) {
	t.Helper()
	driver := "sqlite"
	dsn := filepath.Join(t.TempDir(), "quota.db")
	if os.Getenv("CODEX2API_TEST_POSTGRES_DSN") != "" {
		driver = "postgres"
		dsn = os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
	}
	db, err := New(driver, dsn)
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	res, err := db.conn.ExecContext(ctx,
		`INSERT INTO accounts (name, platform, type, credentials, status, enabled, locked, credential_generation) VALUES ('ag', 'antigravity', 'oauth', ?, 'active', 1, 0, 1)`,
		creds)
	if err != nil {
		t.Fatalf("insert account failed: %v", err)
	}
	id, lastIDErr := res.LastInsertId()
	if lastIDErr != nil {
		if scanErr := db.conn.QueryRowContext(ctx, `SELECT id FROM accounts WHERE name = ?`, "ag").Scan(&id); scanErr != nil {
			t.Fatalf("resolve account id failed: %v (lastInsertId: %v)", scanErr, lastIDErr)
		}
	}
	if trimmed := strings.TrimSpace(familyID); trimmed != "" {
		if _, famErr := db.conn.ExecContext(ctx, `UPDATE accounts SET credential_family_id = ? WHERE id = ?`, trimmed, id); famErr != nil {
			t.Fatalf("backfill credential_family_id failed: %v", famErr)
		}
	}
	return db, id
}

// loadAntigravityCredentialsDoc 直接读取持久化 credentials 文本并解码，用于断言落库
// **形态**（字符串 vs 对象）而不是内存工作形态。
func loadAntigravityCredentialsDoc(t *testing.T, ctx context.Context, db *DB, accountID int64) map[string]any {
	t.Helper()
	var raw string
	if err := db.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accountID).Scan(&raw); err != nil {
		t.Fatalf("reload credentials failed: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decode persisted credentials failed: %v (%s)", err, raw)
	}
	return doc
}

// TestAntigravityQuotaSource_LatestSuccessWinsAndEpochBarrier 覆盖 Latest-Success-Wins
// 并发提交判定、旧成功拒绝与 clear 动作同事务递增 epoch 屏障。
func TestAntigravityQuotaSource_LatestSuccessWinsAndEpochBarrier(t *testing.T) {
	ctx := context.Background()
	db, accID := newAntigravityQuotaTestDB(t, ctx,
		`{"access_token":"token1","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`)

	// 1. A 先预约（慢请求）、B 后预约并提交
	seqA, epochA, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam1")
	if err != nil {
		t.Fatalf("reserve A failed: %v", err)
	}
	seqB, epochB, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam1")
	if err != nil {
		t.Fatalf("reserve B failed: %v", err)
	}
	if seqB <= seqA {
		t.Fatalf("expected seqB > seqA, got %d <= %d", seqB, seqA)
	}

	// B 提交成功：推进 applied_seq（实现负责把 source_metadata.groups.applied_seq 写为 mySeq）
	okB, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB, epochB, 1, "fam1", `{"schema_version":2}`)
	if err != nil || !okB {
		t.Fatalf("commit B failed: ok=%v, err=%v", okB, err)
	}

	// 2. A 迟到提交：seqA < applied_seq，被 Latest-Success-Wins 拒绝（更旧成功被拒）
	okA, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqA, epochA, 1, "fam1", `{"schema_version":2}`)
	if err != nil {
		t.Fatalf("commit A returned error: %v", err)
	}
	if okA {
		t.Fatal("expected slower A with smaller seq to be rejected by Latest-Success-Wins")
	}

	// 3. Clear 同事务递增 quota_epoch
	newEpoch, err := db.ClearAntigravityQuotaSourcesCAS(ctx, accID)
	if err != nil {
		t.Fatalf("clear quota failed: %v", err)
	}
	if newEpoch != epochB+1 {
		t.Fatalf("expected newEpoch %d, got %d", epochB+1, newEpoch)
	}

	// 4. 清除后使用旧 epoch 的迟到提交必须被屏障拦截（验收 #36）
	okLate, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB+10, epochB, 1, "fam1", `{"schema_version":2}`)
	if err != nil {
		t.Fatalf("late commit returned error: %v", err)
	}
	if okLate {
		t.Fatal("expected late commit with old epoch to be blocked by epoch barrier")
	}

	// 5. GetAntigravityQuotaEpoch 读取 credentials JSON 内嵌纪元
	got, err := db.GetAntigravityQuotaEpoch(ctx, accID)
	if err != nil || got != newEpoch {
		t.Fatalf("GetAntigravityQuotaEpoch = (%d, %v), want (%d, nil)", got, err, newEpoch)
	}
}

// TestAntigravityQuotaSource_UpdateCASSameIdentityCarryforward 覆盖例行同身份令牌轮换时的
// 配额事实受控继承：不洗新时标，不递增 epoch。
func TestAntigravityQuotaSource_UpdateCASSameIdentityCarryforward(t *testing.T) {
	ctx := context.Background()
	quotaFacts := `{"schema_version":2,"source_metadata":{"groups":{"applied_seq":5,"observed_at":"2026-10-21T10:00:00Z"}},"quota_groups":[{"display_name":"Weekly","buckets":[{"window":"weekly","remaining_fraction":0,"fraction_present":true,"observed_at":"2026-10-21T10:00:00Z","observation_seq":5,"ttl_seconds":3600}]}]}`
	creds := `{"access_token":"old-at","refresh_token":"old-rt","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1,"antigravity_quota":` + jsonStringLiteral(quotaFacts) + `}`
	db, accID := newAntigravityQuotaTestDB(t, ctx, creds)

	// 例行同身份 token 轮换（UpdateCAS 递增 generation）
	newGen, applied, err := db.UpdateAccountCredentialsCAS(ctx, accID, 1, map[string]any{
		"access_token": "new-at", "refresh_token": "new-rt",
	})
	if err != nil || !applied || newGen != 2 {
		t.Fatalf("UpdateAccountCredentialsCAS = (gen=%d, applied=%v, err=%v)", newGen, applied, err)
	}

	var raw string
	if err := db.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accID).Scan(&raw); err != nil {
		t.Fatalf("reload credentials failed: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decode credentials failed: %v", err)
	}
	quotaRaw, _ := doc["antigravity_quota"].(string)
	if quotaRaw == "" {
		t.Fatal("carryforward must keep antigravity_quota facts on same-identity rotation")
	}
	var carried AntigravityQuotaSnapshotLite
	if err := json.Unmarshal([]byte(quotaRaw), &carried); err != nil {
		t.Fatalf("decode carried quota failed: %v", err)
	}
	if carried.SourceMetadata["groups"].AppliedSeq != 5 {
		t.Fatalf("carryforward must preserve applied_seq, got %+v", carried.SourceMetadata)
	}
	bucket := carried.Groups[0].Buckets[0]
	if bucket.RemainingFraction == nil || *bucket.RemainingFraction != 0 || !bucket.FractionPresent {
		t.Fatalf("carryforward must keep known_zero bucket, got %+v", bucket)
	}
	if !bucket.ObservedAt.Equal(time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("carryforward must not refresh observed_at, got %v", bucket.ObservedAt)
	}
	if got, _ := doc["antigravity_quota_epoch"].(float64); got != 1 {
		t.Fatalf("routine rotation must not bump quota_epoch, got %v", doc["antigravity_quota_epoch"])
	}
}

// jsonStringLiteral 把 JSON 文本编码为可嵌入另一 JSON 字符串字面量的引号形式。
// （既有 jsonString 是 tags 序列化 helper，包内不可重名。）
func jsonStringLiteral(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// AntigravityQuotaSnapshotLite 是测试用最小解码视图，避免测试耦合 auth 包类型。
type AntigravityQuotaSnapshotLite struct {
	Groups []struct {
		Buckets []struct {
			Window            string    `json:"window"`
			RemainingFraction *float64  `json:"remaining_fraction"`
			FractionPresent   bool      `json:"fraction_present"`
			ObservedAt        time.Time `json:"observed_at"`
			ObservationSeq    int64     `json:"observation_seq"`
		} `json:"buckets"`
	} `json:"quota_groups"`
	SourceMetadata map[string]struct {
		AppliedSeq int64 `json:"applied_seq"`
	} `json:"source_metadata"`
}

// TestAntigravityQuotaSource_RTConflictThreeBranches 覆盖 RT 冲突的三分支处置决策：
// 信任 winner、凭据单次重放与身份断裂隔离。
func TestAntigravityQuotaSource_RTConflictThreeBranches(t *testing.T) {
	ctx := context.Background()
	pre := `{"access_token":"pre-at","refresh_token":"pre-rt","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`
	rotated := map[string]any{"access_token": "rotated-at", "refresh_token": "rotated-rt"}

	// 分支 (a)：durable winner 已完成同身份换票 → 信任 winner，绝不回写。
	dbA, accA := newAntigravityQuotaTestDB(t, ctx, pre)
	if _, err := dbA.conn.ExecContext(ctx, `UPDATE accounts SET credentials = ? WHERE id = ?`,
		`{"access_token":"winner-at","refresh_token":"winner-rt","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`, accA); err != nil {
		t.Fatalf("simulate winner failed: %v", err)
	}
	outcome, err := dbA.HandleAntigravityRTConflict(ctx, accA, 1, pre, rotated)
	if err != nil || outcome != RTConflictTrustWinner {
		t.Fatalf("branch (a) = (%v, %v), want trustWinner", outcome, err)
	}
	var rawA string
	if err := dbA.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accA).Scan(&rawA); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if !strings.Contains(rawA, `"winner-at"`) || strings.Contains(rawA, `"rotated-at"`) {
		t.Fatalf("branch (a) must keep winner tokens and never write rotated RT: %s", rawA)
	}

	// 分支 (b)：durable 行仍等于预轮换凭据（无人抢先）→ 受控补写一次凭据-only CAS。
	dbB, accB := newAntigravityQuotaTestDB(t, ctx, pre)
	outcome, err = dbB.HandleAntigravityRTConflict(ctx, accB, 1, pre, rotated)
	if err != nil || outcome != RTConflictReplayCredentialOnly {
		t.Fatalf("branch (b) = (%v, %v), want replayCredentialOnly", outcome, err)
	}
	var rawB, genB string
	// 一次成功补写后凭据落库；quota 事实（此处无）与 epoch 不被该重放触碰。
	if err := dbB.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accB).Scan(&rawB); err != nil {
		t.Fatalf("reload branch (b) failed: %v", err)
	}
	_ = dbB.conn.QueryRowContext(ctx, `SELECT credential_generation FROM accounts WHERE id = ?`, accB).Scan(&genB)
	if !strings.Contains(rawB, `"rotated-at"`) {
		t.Fatalf("branch (b) must persist the consumed RT exactly once: %s", rawB)
	}

	// 分支 (c)：family 已更换（身份谱系断裂）→ 禁止写入，落隔离标记。
	dbC, accC := newAntigravityQuotaTestDB(t, ctx, pre)
	if _, err := dbC.conn.ExecContext(ctx, `UPDATE accounts SET credential_family_id = 'fam2' WHERE id = ?`, accC); err != nil {
		t.Fatalf("simulate family change failed: %v", err)
	}
	outcome, err = dbC.HandleAntigravityRTConflict(ctx, accC, 1, pre, rotated)
	if err != nil || outcome != RTConflictIsolateIdentity {
		t.Fatalf("branch (c) = (%v, %v), want isolateIdentity", outcome, err)
	}
	var rawC string
	if err := dbC.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accC).Scan(&rawC); err != nil {
		t.Fatalf("reload branch (c) failed: %v", err)
	}
	if strings.Contains(rawC, `"rotated-at"`) || !strings.Contains(rawC, "antigravity_permanent_refresh_error") {
		t.Fatalf("branch (c) must isolate identity without writing rotated RT: %s", rawC)
	}
	// 分支 (c) 除日志外还须留下可查询的账号事件审计。
	var auditEvents int
	if err := dbC.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM account_events WHERE account_id = ? AND event_type = ?`,
		accC, antigravityRTConflictAuditEventType).Scan(&auditEvents); err != nil {
		t.Fatalf("query rt-conflict audit events failed: %v", err)
	}
	if auditEvents == 0 {
		t.Fatalf("branch (c) must persist an account_events audit record (type=%s)", antigravityRTConflictAuditEventType)
	}
}

// TestAntigravityQuotaSource_CommitMergesOnlyReturnedBuckets 锁定 Spec §4.1 #2 的合并
// 语义：一次 groups 提交只覆盖本次返回的桶，未返回的旧桶保留原时标与 known_zero，
// 其他来源的 source_metadata 保持不变。
func TestAntigravityQuotaSource_CommitMergesOnlyReturnedBuckets(t *testing.T) {
	ctx := context.Background()
	facts := `{"schema_version":2,"source_metadata":{"groups":{"applied_seq":5,"current_attempt_seq":6},` +
		`"models":{"applied_seq":3,"observed_at":"2026-10-20T09:00:00Z"}},` +
		`"quota_groups":[{"display_name":"Weekly","buckets":[{"window":"weekly","remaining_fraction":0,"fraction_present":true,"observed_at":"2026-10-21T10:00:00Z","ttl_seconds":3600}]}]}`
	creds := `{"access_token":"at","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1,"antigravity_quota":` + jsonStringLiteral(facts) + `}`
	db, accID := newAntigravityQuotaTestDB(t, ctx, creds)

	seq, epoch, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam1")
	if err != nil || seq != 7 || epoch != 1 {
		t.Fatalf("reserve = (%d, %d, %v), want (7, 1, nil)", seq, epoch, err)
	}

	incoming := `{"schema_version":2,"quota_groups":[{"display_name":"Weekly","buckets":[{"window":"daily","remaining_fraction":0.5,"fraction_present":true,"observed_at":"2026-10-21T12:00:00Z","ttl_seconds":3600}]}],"source_metadata":{"groups":{"producer":"summary"}}}`
	applied, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seq, epoch, 1, "fam1", incoming)
	if err != nil || !applied {
		t.Fatalf("commit = (%v, %v)", applied, err)
	}

	var raw string
	if err := db.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accID).Scan(&raw); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decode credentials: %v", err)
	}
	quotaRaw, _ := doc["antigravity_quota"].(string)
	var merged struct {
		Groups []struct {
			DisplayName string `json:"display_name"`
			Buckets     []struct {
				Window            string    `json:"window"`
				RemainingFraction *float64  `json:"remaining_fraction"`
				FractionPresent   bool      `json:"fraction_present"`
				ObservedAt        time.Time `json:"observed_at"`
			} `json:"buckets"`
		} `json:"quota_groups"`
		SourceMetadata map[string]struct {
			Producer   string    `json:"producer"`
			AppliedSeq int64     `json:"applied_seq"`
			ObservedAt time.Time `json:"observed_at"`
		} `json:"source_metadata"`
	}
	if err := json.Unmarshal([]byte(quotaRaw), &merged); err != nil {
		t.Fatalf("decode merged quota: %v", err)
	}
	if len(merged.Groups) != 1 || len(merged.Groups[0].Buckets) != 2 {
		t.Fatalf("commit must keep unreturned buckets, got %+v", merged.Groups)
	}
	var weekly, daily *struct {
		Window            string    `json:"window"`
		RemainingFraction *float64  `json:"remaining_fraction"`
		FractionPresent   bool      `json:"fraction_present"`
		ObservedAt        time.Time `json:"observed_at"`
	}
	for i := range merged.Groups[0].Buckets {
		switch merged.Groups[0].Buckets[i].Window {
		case "weekly":
			weekly = &merged.Groups[0].Buckets[i]
		case "daily":
			daily = &merged.Groups[0].Buckets[i]
		}
	}
	if weekly == nil || daily == nil {
		t.Fatalf("expected weekly + daily buckets, got %+v", merged.Groups[0].Buckets)
	}
	if weekly.RemainingFraction == nil || *weekly.RemainingFraction != 0 || !weekly.FractionPresent ||
		!weekly.ObservedAt.Equal(time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("unreturned weekly bucket must keep known_zero + original observed_at, got %+v", weekly)
	}
	if daily.RemainingFraction == nil || *daily.RemainingFraction != 0.5 ||
		!daily.ObservedAt.Equal(time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("returned daily bucket must be overwritten, got %+v", daily)
	}
	if merged.SourceMetadata["groups"].AppliedSeq != seq {
		t.Fatalf("applied_seq must advance to mySeq=%d, got %+v", seq, merged.SourceMetadata["groups"])
	}
	if merged.SourceMetadata["groups"].Producer != "summary" {
		t.Fatalf("commit must keep this source's evidence, got %+v", merged.SourceMetadata["groups"])
	}
	models := merged.SourceMetadata["models"]
	if models.AppliedSeq != 3 || !models.ObservedAt.Equal(time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("other sources must stay untouched, got %+v", models)
	}
}

// TestAntigravityQuotaSource_ReserveOnlyPersistsQuotaAsString 是 B1 的专项回归：
// 预约路径（只递增 current_attempt_seq、不提交）同样必须把 antigravity_quota 以
// **JSON 字符串**形态落库；并且随后 ClearAntigravityQuotaSourcesCAS 必须彻底清空
// facts 并推进失效纪元，不得因类型断言失败而残留事实。
func TestAntigravityQuotaSource_ReserveOnlyPersistsQuotaAsString(t *testing.T) {
	ctx := context.Background()
	db, accID := newAntigravityQuotaTestDB(t, ctx,
		`{"access_token":"at","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":7}`)

	seq, epoch, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam1")
	if err != nil || seq != 1 || epoch != 7 {
		t.Fatalf("reserve = (seq=%d, epoch=%d, err=%v), want (1, 7, nil)", seq, epoch, err)
	}

	doc := loadAntigravityCredentialsDoc(t, ctx, db, accID)
	quotaRaw, isString := doc[antigravityQuotaCredentialKey].(string)
	if !isString {
		t.Fatalf("antigravity_quota must persist as a JSON string, got %T: %v",
			doc[antigravityQuotaCredentialKey], doc[antigravityQuotaCredentialKey])
	}
	if strings.TrimSpace(quotaRaw) == "" {
		t.Fatal("reservation must persist the reserved attempt sequence")
	}
	// 读取端硬门禁：只有字符串形态下 GetCredential 才拿得到文本（map 会 fail-open）。
	if got := strings.TrimSpace((&AccountRow{ID: accID, Credentials: doc}).GetCredential(antigravityQuotaCredentialKey)); got == "" {
		t.Fatal("AccountRow.GetCredential must not fail-open on durable quota facts")
	}
	var facts map[string]any
	if err := json.Unmarshal([]byte(quotaRaw), &facts); err != nil {
		t.Fatalf("persisted quota is not valid JSON: %v (%s)", err, quotaRaw)
	}
	entry, _ := facts[antigravityQuotaKeySourceMetadata].(map[string]any)["groups"].(map[string]any)
	if entry == nil {
		t.Fatalf("reservation must write source_metadata.groups, got %s", quotaRaw)
	}
	if attempts, _ := int64FromJSONValue(entry[antigravitySourceMetaCurrentAttemptSeq]); attempts != 1 {
		t.Fatalf("current_attempt_seq = %v, want 1", entry[antigravitySourceMetaCurrentAttemptSeq])
	}
	if _, committed := entry[antigravitySourceMetaAppliedSeq]; committed {
		t.Fatalf("reservation alone must not fake applied_seq: %v", entry)
	}

	// 只预约未提交后，清除必须彻底清空 facts 并推进失效纪元。
	newEpoch, err := db.ClearAntigravityQuotaSourcesCAS(ctx, accID)
	if err != nil || newEpoch != 8 {
		t.Fatalf("clear = (epoch=%d, err=%v), want (8, nil)", newEpoch, err)
	}
	after := loadAntigravityCredentialsDoc(t, ctx, db, accID)
	quotaAfter, isString := after[antigravityQuotaCredentialKey].(string)
	if !isString {
		t.Fatalf("cleared antigravity_quota must stay a JSON string, got %T", after[antigravityQuotaCredentialKey])
	}
	if strings.TrimSpace(quotaAfter) != "" ||
		strings.Contains(quotaAfter, antigravityQuotaKeySourceMetadata) ||
		strings.Contains(quotaAfter, antigravitySourceMetaCurrentAttemptSeq) {
		t.Fatalf("clear must wipe reserved facts, got %q", quotaAfter)
	}
	// 旧 epoch 的在途提交必须被失效屏障拒绝。
	applied, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seq+1, epoch, 1, "fam1", `{"schema_version":2}`)
	if err != nil || applied {
		t.Fatalf("in-flight commit after clear = (applied=%v, err=%v), want blocked", applied, err)
	}
}

// TestAntigravityQuotaSource_ClearWipesFactsStoredAsJSONObject 覆盖清除路径的防御性
// 兼容：既有数据若曾被写成嵌套 JSON 对象（历史脏数据），清除仍必须彻底清空 facts 并
// 推进 epoch，绝不允许因 `.(string)` 断言失败而静默跳过。
func TestAntigravityQuotaSource_ClearWipesFactsStoredAsJSONObject(t *testing.T) {
	ctx := context.Background()
	dirty := `{"access_token":"at","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":4,` +
		`"antigravity_quota":{"schema_version":2,"source_metadata":{"groups":{"current_attempt_seq":3,"applied_seq":2}},` +
		`"quota_groups":[{"display_name":"Weekly","buckets":[{"window":"weekly","remaining_fraction":0,"fraction_present":true}]}]}}`
	db, accID := newAntigravityQuotaTestDBWithFamily(t, ctx, dirty, "fam1")

	before := loadAntigravityCredentialsDoc(t, ctx, db, accID)
	if _, isObject := before[antigravityQuotaCredentialKey].(map[string]any); !isObject {
		t.Fatalf("fixture must stay a JSON object to reproduce the legacy shape, got %T", before[antigravityQuotaCredentialKey])
	}
	// 该形态正是 B1 的危害现场：读取端 GetCredential 会 fail-open。
	if got := (&AccountRow{ID: accID, Credentials: before}).GetCredential(antigravityQuotaCredentialKey); got != "" {
		t.Fatalf("object-shaped quota is expected to fail-open on GetCredential, got %q", got)
	}

	newEpoch, err := db.ClearAntigravityQuotaSourcesCAS(ctx, accID)
	if err != nil || newEpoch != 5 {
		t.Fatalf("clear = (epoch=%d, err=%v), want (5, nil)", newEpoch, err)
	}
	after := loadAntigravityCredentialsDoc(t, ctx, db, accID)
	quotaAfter, isString := after[antigravityQuotaCredentialKey].(string)
	if !isString || strings.TrimSpace(quotaAfter) != "" {
		t.Fatalf("clear must wipe object-shaped facts into an empty string, got %T: %v",
			after[antigravityQuotaCredentialKey], after[antigravityQuotaCredentialKey])
	}
	if encoded, _ := json.Marshal(after); strings.Contains(string(encoded), antigravityQuotaKeyQuotaGroups) ||
		strings.Contains(string(encoded), antigravityQuotaKeySourceMetadata) {
		t.Fatalf("facts residue after clear: %s", encoded)
	}
}

// TestAntigravityQuotaSource_RTConflictReplayLostRaceTrustsWinner 是 M1 的回归：
// 分支 (b) 的重放若被同身份 winner 抢先（generation 已推进 / token 已变），必须归为
// trust_winner，不得误报身份谱系断裂并落下永久隔离标记。
func TestAntigravityQuotaSource_RTConflictReplayLostRaceTrustsWinner(t *testing.T) {
	ctx := context.Background()
	pre := `{"access_token":"pre-at","refresh_token":"pre-rt","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`
	db, accID := newAntigravityQuotaTestDB(t, ctx, pre)
	t.Cleanup(func() { antigravityRTReplayRaceHook = nil })
	// 在重放窗口内模拟另一路同身份刷新抢先落库（generation 1 → 2、token 变更）。
	antigravityRTReplayRaceHook = func(accountID int64) {
		if _, applied, err := db.UpdateAccountCredentialsCAS(ctx, accountID, 1, map[string]any{
			"access_token": "winner-at", "refresh_token": "winner-rt",
		}); err != nil || !applied {
			t.Fatalf("simulated winner write failed: applied=%v err=%v", applied, err)
		}
	}

	outcome, err := db.HandleAntigravityRTConflict(ctx, accID, 1, pre, map[string]any{
		"access_token": "loser-at", "refresh_token": "loser-rt",
	})
	if err != nil || outcome != RTConflictTrustWinner {
		t.Fatalf("replay that lost the race = (%v, %v), want trust_winner", outcome, err)
	}
	doc := loadAntigravityCredentialsDoc(t, ctx, db, accID)
	if got, _ := doc["access_token"].(string); got != "winner-at" {
		t.Fatalf("trust_winner must keep the durable winner token, got %v", doc["access_token"])
	}
	if marker, ok := doc[antigravityQuotaPermanentRefreshErrorKey].(string); ok && strings.TrimSpace(marker) != "" {
		t.Fatalf("a lost race must not isolate the identity, marker=%q", marker)
	}
	var auditEvents int
	if err := db.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM account_events WHERE account_id = ? AND event_type = ?`,
		accID, antigravityRTConflictAuditEventType).Scan(&auditEvents); err != nil {
		t.Fatalf("query audit events failed: %v", err)
	}
	if auditEvents != 0 {
		t.Fatalf("trust_winner must not write an isolation audit event, got %d", auditEvents)
	}
}

// TestAntigravityQuotaSource_StrictIdentityScopeFence 是 M2 的回归：expectedScope 非空时
// durable family 必须完全相等才放行——不一致要拒、family 列尚未回填（空）同样要拒；
// 只有空 expectedScope 才表示不校验身份。
func TestAntigravityQuotaSource_StrictIdentityScopeFence(t *testing.T) {
	ctx := context.Background()
	db, accID := newAntigravityQuotaTestDB(t, ctx,
		`{"access_token":"at","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`)

	if _, _, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam2"); err == nil {
		t.Fatal("reserve with a mismatched scope must be rejected")
	}
	seq, epoch, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam1")
	if err != nil {
		t.Fatalf("reserve with a matching scope failed: %v", err)
	}
	if applied, commitErr := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seq, epoch, 1, "fam2", `{"schema_version":2}`); commitErr != nil || applied {
		t.Fatalf("commit with a mismatched scope = (applied=%v, err=%v), want silent rejection", applied, commitErr)
	}
	if applied, commitErr := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seq, epoch, 1, "fam1", `{"schema_version":2}`); commitErr != nil || !applied {
		t.Fatalf("commit with a matching scope = (applied=%v, err=%v), want applied", applied, commitErr)
	}
	if _, _, err := db.ReserveAntigravityQuotaSource(ctx, accID, "models", 1, ""); err != nil {
		t.Fatalf("an empty expected scope must stay unchecked: %v", err)
	}

	// family 列尚未回填（durable 身份为空）同样严格拒绝，且拒绝路径不得留下事实。
	dbNoFam, accNoFam := newAntigravityQuotaTestDBWithFamily(t, ctx,
		`{"access_token":"at","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`, "")
	if _, _, err := dbNoFam.ReserveAntigravityQuotaSource(ctx, accNoFam, "groups", 1, "fam1"); err == nil {
		t.Fatal("reserve against an account without a durable family must be rejected")
	}
	if applied, commitErr := dbNoFam.CommitAntigravityQuotaSource(ctx, accNoFam, "groups", 1, 1, 1, "fam1", `{"schema_version":2}`); commitErr != nil || applied {
		t.Fatalf("commit against an account without a durable family = (applied=%v, err=%v), want rejection", applied, commitErr)
	}
	doc := loadAntigravityCredentialsDoc(t, ctx, dbNoFam, accNoFam)
	if value, present := doc[antigravityQuotaCredentialKey]; present {
		if text, isString := value.(string); !isString || strings.TrimSpace(text) != "" {
			t.Fatalf("rejected writes must not persist facts, got %v", value)
		}
	}
}
