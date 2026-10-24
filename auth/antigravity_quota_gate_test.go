package auth

import (
	"testing"
	"time"
)

// TestIsAntigravityQuotaExhaustedLocked_Rules 覆盖门禁核心规则：周 0 阻断、
// 超龄已知零 stale 阻断、正余额 TTL 后转 unknown、旧快照放行与止血开关放行。
func TestIsAntigravityQuotaExhaustedLocked_Rules(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	zero := 0.0
	pos := 0.5

	// verifiedMeta 构造满足 Spec §3.1 元数据可信条件的 groups 来源元数据。
	verifiedMeta := func(observed time.Time) map[string]SourceMeta {
		return map[string]SourceMeta{
			"groups": {AppliedSeq: 5, ObservedAt: observed, PresenceVerified: true},
		}
	}
	weeklyZero := func(observed, reset time.Time, ttl int) AntigravityQuotaSnapshot {
		return AntigravityQuotaSnapshot{
			SchemaVersion:  2,
			SourceMetadata: verifiedMeta(observed),
			Groups: []AntigravityQuotaGroup{
				{
					Buckets: []AntigravityQuotaBucket{
						{
							Window:            "weekly",
							RemainingFraction: &zero,
							FractionPresent:   true,
							ResetTime:         reset.Format(time.RFC3339),
							ObservedAt:        observed,
							ObservationSeq:    5,
							TTLSeconds:        ttl,
						},
					},
				},
			},
		}
	}

	// 1. 周 0 且 resetAt 在 2h 后：阻断，resetAt 精确上浮
	acc := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: weeklyZero(now, now.Add(2*time.Hour), 3600)}
	exhausted, _, reset := acc.IsAntigravityQuotaExhaustedLocked(now)
	if !exhausted {
		t.Fatal("expected weekly 0 to be exhausted")
	}
	if reset.Sub(now) != 2*time.Hour {
		t.Fatalf("expected reset in 2h, got %v", reset)
	}

	// 2. TTL 到期 (2小时后) 但 resetAt 还在未来 (3天后)：绝不放行，继续阻断并标记 stale
	accStale := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: weeklyZero(now, now.Add(3*24*time.Hour), 3600)}
	// 时间推进 2 小时 (超过 TTL)
	exhaustedStale, reason, _ := accStale.IsAntigravityQuotaExhaustedLocked(now.Add(2 * time.Hour))
	if !exhaustedStale {
		t.Fatal("stale zero must NOT be silently unblocked after TTL")
	}
	if reason != "stale_zero" && reason != "known_zero" {
		t.Fatalf("expected stale_zero or known_zero, got %q", reason)
	}

	// 3. 正余额在 TTL 到期后变为 unknown 不阻断
	snapPos := AntigravityQuotaSnapshot{
		SchemaVersion:  2,
		SourceMetadata: verifiedMeta(now),
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{
						Window:            "weekly",
						RemainingFraction: &pos,
						FractionPresent:   true,
						ObservedAt:        now,
						ObservationSeq:    6,
						TTLSeconds:        3600,
					},
				},
			},
		},
	}
	accPos := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: snapPos}
	exPos, _, _ := accPos.IsAntigravityQuotaExhaustedLocked(now.Add(2 * time.Hour))
	if exPos {
		t.Fatal("positive quota after TTL should become unknown and not block")
	}

	// 4. legacy 快照（无 schema_version=2 / 无可信 metadata）：绝不硬封（验收 #2）
	legacy := weeklyZero(now, now.Add(2*time.Hour), 3600)
	legacy.SchemaVersion = 0
	legacy.SourceMetadata = nil
	accLegacy := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: legacy}
	if ex, _, _ := accLegacy.IsAntigravityQuotaExhaustedLocked(now); ex {
		t.Fatal("legacy snapshot without V2 metadata must never hard-block")
	}

	// 5. quota_gate_enabled=false 止血开关立即放行（验收 #30 前半；测后恢复全局设置）
	prev := ConfiguredAntigravitySettings()
	gateOff := false
	SetConfiguredAntigravitySettings(AntigravitySettings{QuotaGateEnabled: &gateOff})
	defer SetConfiguredAntigravitySettings(prev)
	if ex, _, _ := acc.IsAntigravityQuotaExhaustedLocked(now); ex {
		t.Fatal("quota_gate_enabled=false must immediately unblock")
	}
}
