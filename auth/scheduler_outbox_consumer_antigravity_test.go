package auth

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/database"
)

// antigravityQuotaRawForTest 生成一份 V2 配额事实的落库原文（credentials 里
// antigravity_quota 的字符串形态）。appliedSeq 写进 source_metadata.groups.applied_seq；
// facts=false 表示 clear 之后的空事实（无 quota_groups / source_metadata / ai_credits）。
func antigravityQuotaRawForTest(t *testing.T, appliedSeq int64, remaining float64, facts bool) string {
	t.Helper()
	if !facts {
		return ""
	}
	value := remaining
	snapshot := AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{{
			DisplayName: "Weekly",
			Buckets: []AntigravityQuotaBucket{{
				Window:            "weekly",
				RemainingFraction: &value,
				FractionPresent:   true,
				ObservedAt:        time.Now(),
				ObservationSeq:    appliedSeq,
				TTLSeconds:        3600,
			}},
		}},
		SourceMetadata: map[string]SourceMeta{
			"groups": {AppliedSeq: appliedSeq, ObservedAt: time.Now()},
		},
		UpdatedAt: time.Now(),
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal quota snapshot: %v", err)
	}
	return string(encoded)
}

// newAntigravitySnapshotTestAccount 构造一个带配额事实与 epoch 基线的内存账号。
func newAntigravitySnapshotTestAccount(t *testing.T, store *Store, raw string, epoch int64) *Account {
	t.Helper()
	account := newFastSchedulerTestAccount(1, HealthTierWarm, 100, 1)
	account.UpstreamType = UpstreamAntigravity
	account.CredentialGeneration = 1
	src := newFastSchedulerTestAccount(1, HealthTierWarm, 100, 1)
	src.UpstreamType = UpstreamAntigravity
	src.CredentialGeneration = 1
	src.applyAntigravityQuotaSchedulingLocked(raw)
	src.antigravityQuotaEpoch = epoch
	store.applyPersistentAccountSnapshot(account, src, true)
	return account
}

// antigravityQuotaState 读取账号运行时的配额与纪元相关内存状态。
func antigravityQuotaState(t *testing.T, account *Account) (float64, bool, int64, int) {
	t.Helper()
	account.mu.RLock()
	defer account.mu.RUnlock()
	return account.antigravityQuotaUsedPercent, account.antigravityQuotaValid,
		account.antigravityQuotaEpoch, len(account.AntigravityQuota.Groups)
}

// TestApplyPersistentAccountSnapshotAntigravityQuotaEpochMonotonic 覆盖 Spec §4.3
// 的跨实例单调发布：更旧 epoch 与相同 epoch 的空事实一律丢弃；相同 epoch 按
// applied_seq 判定；epoch 递增时空事实直接覆盖清空旧事实（自愈义务）。
func TestApplyPersistentAccountSnapshotAntigravityQuotaEpochMonotonic(t *testing.T) {
	store := newIndexedRoutingTestStore(nil)
	// 内存基线：epoch=3、weekly 余量 0（known_zero）、applied_seq=7。
	dst := newAntigravitySnapshotTestAccount(t, store, antigravityQuotaRawForTest(t, 7, 0, true), 3)
	if used, valid, epoch, groups := antigravityQuotaState(t, dst); !valid || used != 100 || epoch != 3 || groups != 1 {
		t.Fatalf("baseline = used %v valid %v epoch %d groups %d, want 100 true 3 1", used, valid, epoch, groups)
	}

	apply := func(raw string, epoch int64) {
		t.Helper()
		src := newFastSchedulerTestAccount(1, HealthTierWarm, 100, 1)
		src.UpstreamType = UpstreamAntigravity
		src.CredentialGeneration = dst.CredentialGeneration
		src.applyAntigravityQuotaSchedulingLocked(raw)
		src.antigravityQuotaEpoch = epoch
		store.applyPersistentAccountSnapshot(dst, src, true)
	}

	// 1. 更旧 epoch（2）即便序号更大、事实更新，也必须整体丢弃。
	apply(antigravityQuotaRawForTest(t, 99, 1, true), 2)
	if used, valid, epoch, _ := antigravityQuotaState(t, dst); !valid || used != 100 || epoch != 3 {
		t.Fatalf("older epoch projection must be dropped, got used %v valid %v epoch %d", used, valid, epoch)
	}

	// 2. 相同 epoch 的空事实（伪 clear）必须丢弃，绝不能以相同纪元解除门禁。
	apply("", 3)
	if used, valid, epoch, groups := antigravityQuotaState(t, dst); !valid || used != 100 || epoch != 3 || groups != 1 {
		t.Fatalf("same-epoch empty fact must be dropped, got used %v valid %v epoch %d groups %d", used, valid, epoch, groups)
	}

	// 3. 相同 epoch 但 applied_seq 更旧（7→6）的迟到观测必须丢弃。
	apply(antigravityQuotaRawForTest(t, 6, 1, true), 3)
	if used, valid, _, _ := antigravityQuotaState(t, dst); !valid || used != 100 {
		t.Fatalf("older applied_seq must be dropped, got used %v valid %v", used, valid)
	}

	// 4. 相同 epoch 且 applied_seq 相等 → 幂等重放允许原位合并。
	apply(antigravityQuotaRawForTest(t, 7, 0.5, true), 3)
	if used, valid, epoch, _ := antigravityQuotaState(t, dst); !valid || used != 50 || epoch != 3 {
		t.Fatalf("equal applied_seq must merge in place, got used %v valid %v epoch %d", used, valid, epoch)
	}

	// 5. epoch 递增（4）的空事实直接覆盖：清空旧事实并重算门禁派生键。
	apply("", 4)
	if used, valid, epoch, groups := antigravityQuotaState(t, dst); valid || used != 0 || epoch != 4 || groups != 0 {
		t.Fatalf("newer epoch empty fact must overwrite facts and gate, got used %v valid %v epoch %d groups %d", used, valid, epoch, groups)
	}

	// 6. 新 epoch 的序号空间重新初始化：applied_seq=1 的新观测必须被接受。
	apply(antigravityQuotaRawForTest(t, 1, 0, true), 4)
	if used, valid, epoch, _ := antigravityQuotaState(t, dst); !valid || used != 100 || epoch != 4 {
		t.Fatalf("new epoch sequence space must restart from 1, got used %v valid %v epoch %d", used, valid, epoch)
	}
}

// TestSchedulerOutboxSettingsEventReloadsAntigravityConfig 覆盖 Spec §8.2 的
// settings 分支落地：跨实例的 antigravity_config 变更必须经 consumer 真正重载；
// 检测到 revision 回退且 quota_* 丢失时按 Task 4 机制以 CAS 回写。
func TestSchedulerOutboxSettingsEventReloadsAntigravityConfig(t *testing.T) {
	previous := ConfiguredAntigravitySettings()
	t.Cleanup(func() { SetConfiguredAntigravitySettings(previous) })

	ctx := context.Background()
	db, err := database.New("sqlite", filepath.Join(t.TempDir(), "outbox-settings.db"))
	if err != nil {
		t.Fatalf("database.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := NewStore(db, nil, &database.SystemSettings{MaxConcurrency: 1, SchedulerEngine: "indexed"})
	t.Cleanup(store.Stop)

	SetConfiguredAntigravitySettings(AntigravitySettings{})
	if err := db.SaveAntigravityConfig(ctx, `{"model_redirects":{"gemini-3":"gemini-3-flash"},"quota_refresh_enabled":false,"quota_refresh_interval_minutes":7,"quota_gate_enabled":false,"config_revision":5}`); err != nil {
		t.Fatalf("SaveAntigravityConfig: %v", err)
	}
	if err := store.applySchedulerOutboxEvent(ctx, database.SchedulerOutboxEvent{ID: 1, EntityType: database.SchedulerEntitySettings, EntityID: 1, EventType: "updated"}); err != nil {
		t.Fatalf("applySchedulerOutboxEvent(settings): %v", err)
	}
	loaded := ConfiguredAntigravitySettings()
	if AntigravityModelRedirect("gemini-3") != "gemini-3-flash" {
		t.Fatalf("settings event must reload antigravity_config, got redirects %+v", loaded.ModelRedirects)
	}
	if loaded.GetQuotaRefreshEnabled() || loaded.GetQuotaRefreshIntervalMinutes() != 7 || loaded.GetQuotaGateEnabled() {
		t.Fatalf("quota_* fields not applied: %+v", loaded)
	}
	if AppliedAntigravityConfigRevision() != 5 {
		t.Fatalf("revision baseline = %d, want 5", AppliedAntigravityConfigRevision())
	}

	// 混布窗口的旧写侧把整包覆盖成 revision=1 且丢掉全部 quota_* 字段。
	if err := db.SaveAntigravityConfig(ctx, `{"model_redirects":{"gemini-2.5":"gemini-2.5-pro"},"config_revision":1}`); err != nil {
		t.Fatalf("SaveAntigravityConfig rollback: %v", err)
	}
	if err := store.applySchedulerOutboxEvent(ctx, database.SchedulerOutboxEvent{ID: 2, EntityType: database.SchedulerEntitySettings, EntityID: 2, EventType: "updated"}); err != nil {
		t.Fatalf("applySchedulerOutboxEvent(rollback): %v", err)
	}
	applied := ConfiguredAntigravitySettings()
	if applied.ModelRedirects["gemini-2.5"] != "gemini-2.5-pro" {
		t.Fatalf("revision rollback must still be applied, got %+v", applied.ModelRedirects)
	}
	if applied.GetQuotaRefreshEnabled() || applied.GetQuotaRefreshIntervalMinutes() != 7 || applied.GetQuotaGateEnabled() {
		t.Fatalf("held quota_* fields must survive a rollback, got %+v", applied)
	}
	stored, err := db.LoadAntigravityConfig(ctx)
	if err != nil {
		t.Fatalf("LoadAntigravityConfig: %v", err)
	}
	for _, want := range []string{`"quota_refresh_enabled":false`, `"quota_refresh_interval_minutes":7`, `"quota_gate_enabled":false`} {
		if !strings.Contains(stored, want) {
			t.Fatalf("lost quota_* fields must be written back, stored=%s", stored)
		}
	}
	if AppliedAntigravityConfigRevision() <= 5 {
		t.Fatalf("rollback must rebuild the revision baseline above 5, got %d", AppliedAntigravityConfigRevision())
	}
}
