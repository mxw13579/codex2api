// auth/antigravity_quota_integration_test.go
package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codex2api/database"
)

// TestAntigravityQuota_AcceptanceMatrix 是 Spec §10.1 39 项验收的集成装配层：
// 逐项断言由各专项 Task 的测试覆盖（见 Plan Task 16 的覆盖矩阵），本文件对必须跨模块
// 组合的场景给出端到端验证。任一子测试失败即整体验收不通过。
//
// 时钟基线取 time.Now() 而非固定日期：s4_s5 的整池选号断言走生产路径，门禁在
// selectAntigravityBalancedAccount → dispatchableForPolicy → isAvailableLocked 内以
// time.Now() 求值（同 auth/antigravity_quota_dispatch_test.go 的既有说明），固定日期会
// 让 resetAt/TTL 相对真实时钟滚动，断言随时钟漂移失真。Truncate 到秒是因为 ResetTime
// 以 RFC3339 秒级精度落库再解析回来，保留亚秒位会让 s7 的 reset.Equal 恒不成立。
func TestAntigravityQuota_AcceptanceMatrix(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ctx := context.Background()

	// 门禁隔离：硬门禁谓词读全局止血开关（缺省开启）。s4_s5 / s7 的阻断断言以「门禁
	// 开启」为前提，这里在保留其余设置项的前提下显式固定并复原，避免同包其它用例改写
	// 全局开关后串扰验收结论。
	prevSettings := ConfiguredAntigravitySettings()
	gateOn := true
	gateEnabled := prevSettings
	gateEnabled.QuotaGateEnabled = &gateOn
	SetConfiguredAntigravitySettings(gateEnabled)
	defer SetConfiguredAntigravitySettings(prevSettings)
	if !ConfiguredAntigravitySettings().GetQuotaGateEnabled() {
		t.Fatal("acceptance matrix requires the hard quota gate to be enabled")
	}

	newAccount := func(id int64) *Account {
		return &Account{
			DBID:                 id,
			UpstreamType:         UpstreamAntigravity,
			AccessToken:          "at",
			RefreshToken:         "rt",
			AntigravityProjectID: "project-id",
			HealthTier:           HealthTierHealthy,
		}
	}
	verifiedMeta := func(observed time.Time) map[string]SourceMeta {
		return map[string]SourceMeta{"groups": {AppliedSeq: 3, ObservedAt: observed, PresenceVerified: true}}
	}

	t.Run("s4_s5_any_effective_zero_bucket_blocks_whole_account", func(t *testing.T) {
		zero, pos := 0.0, 0.5
		// #5：weekly 正、5h 零 —— 周配额充足也必须整账号阻断。
		snap := AntigravityQuotaSnapshot{
			SchemaVersion:  2,
			SourceMetadata: verifiedMeta(now),
			Groups: []AntigravityQuotaGroup{{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &pos, FractionPresent: true, ObservedAt: now, ObservationSeq: 3, TTLSeconds: 3600},
					{Window: "5h", RemainingFraction: &zero, FractionPresent: true, ResetTime: now.Add(2 * time.Hour).Format(time.RFC3339), ObservedAt: now, ObservationSeq: 3, TTLSeconds: 3600},
				},
			}},
		}
		acc := newAccount(501)
		acc.AntigravityQuota = snap
		exhausted, reason, _ := acc.IsAntigravityQuotaExhaustedLocked(now)
		if !exhausted || reason != "known_zero" {
			t.Fatalf("5h zero with positive weekly must block, got (%v,%q)", exhausted, reason)
		}

		// #4/#31 组合：唯一账号被阻断时整池 Fresh 选号返回 nil（无可选账号；
		// proxy 层映射 429 + Retry-After 的出口见 Task 13）。
		store := (&Store{accounts: []*Account{acc}}).withMaxConcurrency(2)
		prevAffinity := GetAntigravityBalancedAffinity()
		SetAntigravityBalancedAffinity(true)
		defer SetAntigravityBalancedAffinity(prevAffinity)
		if got := store.nextAccountForFreshAffinityWithDispatch("s4", 0, nil, nil, SelectionOptions{Provider: UpstreamAntigravity}); got != nil {
			store.Release(got)
			t.Fatalf("fully gated pool must select nothing, got %+v", got)
		}
	})

	t.Run("s7_reset_expiry_drops_only_that_bucket", func(t *testing.T) {
		zero := 0.0
		// #7：5h 零（reset 近期，已到期）+ weekly 零（reset 远期，未到期）——5h 转
		// unknown 后 weekly 零桶继续阻断（Spec §3.3 账户级阻断与桶隔离）。
		//
		// 【桶顺序是本用例断言的一部分，不可随意调整】
		// antigravityQuotaSnapshotGateBlock 按 Buckets 切片顺序遍历，命中未到期零桶
		// 即立即 return，因此「已到期」的 5h 桶必须排在 [0] 位：遍历器先走到
		// resetAt 已过 -> continue 分支（窗口滚动转 unknown，仅解除本桶），再命中
		// [1] 位 weekly 的 known_zero 阻断。
		// 反若把 weekly 提到 [0]，遍历在首位就 return，5h 的 continue 分支根本不
		// 参与求值，本用例退化为只验证「未到期零桶仍阻断」；此时生产代码哪怕把到期
		// 桶误写成 return false（任一桶到期即放行整账号），s7 依然假通过，Spec §3.3
		// 「到期只解除当前桶、其他有效零桶仍需阻断」的后半段语义就失去实质断言。
		snap := AntigravityQuotaSnapshot{
			SchemaVersion:  2,
			SourceMetadata: verifiedMeta(now),
			Groups: []AntigravityQuotaGroup{{
				Buckets: []AntigravityQuotaBucket{
					// [0] 必须先到期：驱动 gate 的 continue 分支。
					{Window: "5h", RemainingFraction: &zero, FractionPresent: true, ResetTime: now.Add(10 * time.Minute).Format(time.RFC3339), ObservedAt: now, ObservationSeq: 3, TTLSeconds: 7200},
					// [1] 必须未到期：在 [0] 被 continue 后才命中阻断。
					{Window: "weekly", RemainingFraction: &zero, FractionPresent: true, ResetTime: now.Add(72 * time.Hour).Format(time.RFC3339), ObservedAt: now, ObservationSeq: 3, TTLSeconds: 7200},
				},
			}},
		}
		acc := newAccount(502)
		acc.AntigravityQuota = snap
		exhausted, reason, reset := acc.IsAntigravityQuotaExhaustedLocked(now.Add(11 * time.Minute))
		if !exhausted || reason != "known_zero" || !reset.Equal(now.Add(72*time.Hour)) {
			t.Fatalf("weekly zero must keep blocking after 5h reset expiry, got (%v,%q,%v)", exhausted, reason, reset)
		}
	})

	t.Run("s22_fake_clock_steady_state", func(t *testing.T) {
		// #22：N=0 空闲退化；N=1 严格按 I（含半间隔相位偏移，与公式一致）。
		epoch := now
		if got := NominalSlotTime(0, 0, 60*time.Minute, 0, epoch, 0); !got.Equal(epoch) {
			t.Fatalf("N=0 must idle at epoch, got %v", got)
		}
		if got := NominalSlotTime(0, 1, 60*time.Minute, 1, epoch, 0); got.Sub(epoch) != 90*time.Minute {
			t.Fatalf("N=1 slot (k=1) = %v, want epoch+90m", got)
		}
	})

	t.Run("s24_shortened_interval_no_catch_up", func(t *testing.T) {
		// #24：60m -> 3m 后首个新周期槽位必须全部落在 (now, now+2I] 内，
		// 不得集中补发历史任务（Spec §5.3 无历史补偿）。
		interval := 3 * time.Minute
		for j := 0; j < 4; j++ {
			slot := NominalSlotTime(j, 1, interval, 4, now, 0)
			if !slot.After(now) {
				t.Fatalf("j=%d slot %v not in the future (catch-up forbidden)", j, slot)
			}
			if slot.Sub(now) > 2*interval {
				t.Fatalf("j=%d slot %v later than 2I after shortening", j, slot)
			}
		}
	})

	t.Run("s34_legacy_snapshot_soft_only", func(t *testing.T) {
		// #34/#2：旧 JSON 无 schema_version/metadata，显式 0 不可信——绝不硬封。
		// legacy models 条目缺 fraction_present → 不计入回退平均，整体回落中性
		// 0.5（Spec §3.1 legacy 软排序语义）；presence 有效的 models 回退见 Task 10 场景 3。
		legacyJSON := `{"remaining_fraction":0,"models":[{"model_id":"m","remaining_fraction":0.8}]}`
		var snap AntigravityQuotaSnapshot
		if err := json.Unmarshal([]byte(legacyJSON), &snap); err != nil {
			t.Fatalf("legacy decode must not fail: %v", err)
		}
		acc := newAccount(503)
		acc.AntigravityQuota = snap
		if ex, _, _ := acc.IsAntigravityQuotaExhaustedLocked(now); ex {
			t.Fatal("legacy explicit zero must never hard-block")
		}
		if p := snap.ComputeQuotaPressure(now); p != 0.5 {
			t.Fatalf("legacy snapshot without presence must stay neutral 0.5, got %v", p)
		}
	})

	t.Run("s36_s37_epoch_barrier_and_reservation_order", func(t *testing.T) {
		// #36/#37/#28/#16 跨模块（auth + database）：预约序单调、LSW 拒绝更早
		// 预约、clear epoch 屏障拦截迟到探测、新 epoch 观测正常提交。
		driver := "sqlite"
		dsn := filepath.Join(t.TempDir(), "it.db")
		if os.Getenv("CODEX2API_TEST_POSTGRES_DSN") != "" {
			driver = "postgres"
			dsn = os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
		}
		db, err := database.New(driver, dsn)
		if err != nil {
			t.Fatalf("open db failed: %v", err)
		}
		defer db.Close()

		accID, err := db.InsertAccountWithCredentials(ctx, "ag-integration", map[string]interface{}{
			"upstream_type": "antigravity", "access_token": "at", "credential_family_id": "fam1",
			"antigravity_quota_epoch": 1,
		}, "")
		if err != nil {
			t.Fatalf("insert account failed: %v", err)
		}
		row, err := db.GetAccountByID(ctx, accID)
		if err != nil || row == nil {
			t.Fatalf("load account failed: %v", err)
		}
		gen, fam := row.CredentialGeneration, row.CredentialFamilyID

		seqA, epochA, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", gen, fam)
		if err != nil {
			t.Fatalf("reserve A failed: %v", err)
		}
		seqB, epochB, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", gen, fam)
		if err != nil {
			t.Fatalf("reserve B failed: %v", err)
		}
		if seqA >= seqB || epochA != epochB {
			t.Fatalf("per-source reservations must be monotonic within one epoch, got (%d,%d,%d,%d)", seqA, seqB, epochA, epochB)
		}

		// 更晚预约的 B 提交成功（LSW）；更早预约的 A 被拒绝。
		if ok, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB, epochB, gen, fam, `{"schema_version":2}`); err != nil || !ok {
			t.Fatalf("later reservation must win: ok=%v err=%v", ok, err)
		}
		if ok, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqA, epochA, gen, fam, `{"schema_version":2}`); err != nil || ok {
			t.Fatalf("earlier reservation must be rejected by LSW: ok=%v err=%v", ok, err)
		}

		// clear 同事务递增 epoch；旧 epoch 的迟到探测被屏障拒绝；
		// 新 epoch 的预约/提交不受旧序号影响。
		newEpoch, err := db.ClearAntigravityQuotaSourcesCAS(ctx, accID)
		if err != nil || newEpoch != epochB+1 {
			t.Fatalf("clear must bump epoch exactly once: (%d,%v)", newEpoch, err)
		}
		if ok, _ := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB+100, epochB, gen, fam, `{"schema_version":2}`); ok {
			t.Fatal("late probe with stale epoch must be rejected by epoch barrier")
		}
		seqNew, epochNew, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", gen, fam)
		if err != nil {
			t.Fatalf("reserve after clear failed: %v", err)
		}
		if epochNew != newEpoch {
			t.Fatalf("post-clear reservation must bind new epoch: %d != %d", epochNew, newEpoch)
		}
		if ok, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqNew, epochNew, gen, fam, `{"schema_version":2}`); err != nil || !ok {
			t.Fatalf("post-clear probe must commit under new epoch: ok=%v err=%v", ok, err)
		}
	})
}
