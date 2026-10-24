package auth

import (
	"encoding/json"
	"testing"
)

// TestAntigravitySettings_V2DefaultsAndNormalization 覆盖 V2 设置项的缺省填充、范围钳位与往返序列化保真。
func TestAntigravitySettings_V2DefaultsAndNormalization(t *testing.T) {
	raw := `{}`
	var s AntigravitySettings
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	norm, err := NormalizeAntigravitySettings(s)
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if !norm.GetQuotaRefreshEnabled() {
		t.Fatal("expected default QuotaRefreshEnabled=true")
	}
	if norm.GetQuotaRefreshIntervalMinutes() != 60 {
		t.Fatalf("expected default interval 60, got %d", norm.GetQuotaRefreshIntervalMinutes())
	}
	if !norm.GetQuotaGateEnabled() {
		t.Fatal("expected default QuotaGateEnabled=true")
	}

	// 越界间隔钳位到 [3, 60]
	badInterval := 1
	s.QuotaRefreshIntervalMinutes = &badInterval
	norm2, err := NormalizeAntigravitySettings(s)
	if err != nil {
		t.Fatalf("normalize with interval failed: %v", err)
	}
	if norm2.GetQuotaRefreshIntervalMinutes() != 3 {
		t.Fatalf("expected clamped interval 3, got %d", norm2.GetQuotaRefreshIntervalMinutes())
	}

	// 显式 false 必须保留（Parse/Normalize/Copy/Encode 全链路不丢字段，Spec §8.2 #4）
	gateOff := false
	s.QuotaGateEnabled = &gateOff
	norm3, err := NormalizeAntigravitySettings(s)
	if err != nil {
		t.Fatalf("normalize with gate failed: %v", err)
	}
	if norm3.GetQuotaGateEnabled() {
		t.Fatal("explicit quota_gate_enabled=false must be preserved by normalization")
	}
	encoded, err := EncodeAntigravitySettings(norm3)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	parsed, err := ParseAntigravitySettings(encoded)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if parsed.GetQuotaGateEnabled() || parsed.GetQuotaRefreshIntervalMinutes() != 3 {
		t.Fatalf("round-trip lost explicit false or interval: %+v", parsed)
	}
}

// TestApplyConfiguredAntigravitySettingsRevisionRollback 覆盖设置版本回退检测：基线自适应重建与强制应用。
func TestApplyConfiguredAntigravitySettingsRevisionRollback(t *testing.T) {
	previous := ConfiguredAntigravitySettings()
	previousBaseline := appliedAntigravityConfigRevision.Load()
	t.Cleanup(func() {
		SetConfiguredAntigravitySettings(previous)
		appliedAntigravityConfigRevision.Store(previousBaseline)
	})
	SetConfiguredAntigravitySettings(AntigravitySettings{})

	if AppliedAntigravityConfigRevision() != 0 {
		t.Fatalf("baseline should start at 0, got %d", AppliedAntigravityConfigRevision())
	}
	on := true
	if outcome := ApplyConfiguredAntigravitySettings(AntigravitySettings{QuotaRefreshEnabled: &on, ConfigRevision: 5}); outcome != AntigravityApplyApplied {
		t.Fatalf("fresh revision outcome = %v", outcome)
	}
	if AppliedAntigravityConfigRevision() != 5 {
		t.Fatalf("baseline = %d, want 5", AppliedAntigravityConfigRevision())
	}

	// revision 缺失（被旧副本整包覆盖归零）必须判定为回退：告警 + 基线重建 + 强制应用，
	// 而不是静默拒绝后续所有更新（Spec §8.2 #5）。
	if outcome := ApplyConfiguredAntigravitySettings(AntigravitySettings{ExposeThoughts: true}); outcome != AntigravityApplyRevisionRollback {
		t.Fatalf("missing revision outcome = %v, want rollback", outcome)
	}
	if baseline := AppliedAntigravityConfigRevision(); baseline != 6 {
		t.Fatalf("rebuilt baseline = %d, want max(5, 0)+1 = 6", baseline)
	}
	if !ConfiguredAntigravitySettings().ExposeThoughts {
		t.Fatal("rollback must force-apply the incoming content")
	}

	// 基线重建后，介于新旧之间的合法更新不得被永久拒绝。
	if outcome := ApplyConfiguredAntigravitySettings(AntigravitySettings{ConfigRevision: 2}); outcome != AntigravityApplyRevisionRollback {
		t.Fatalf("stale-looking follow-up outcome = %v, want rollback-apply", outcome)
	}
	if outcome := ApplyConfiguredAntigravitySettings(AntigravitySettings{ConfigRevision: 9}); outcome != AntigravityApplyApplied {
		t.Fatalf("newer revision outcome = %v", outcome)
	}
	if AppliedAntigravityConfigRevision() != 9 {
		t.Fatalf("baseline = %d, want 9", AppliedAntigravityConfigRevision())
	}
}

// TestAntigravityQuotaFieldsLostDetectsMixedVersionDamage 覆盖多实例混布时的丢失字段探测与深拷贝隔离。
func TestAntigravityQuotaFieldsLostDetectsMixedVersionDamage(t *testing.T) {
	gateOff := false
	interval := 15
	held := AntigravitySettings{QuotaRefreshEnabled: &gateOff, QuotaRefreshIntervalMinutes: &interval, QuotaGateEnabled: &gateOff}
	incoming := AntigravitySettings{ExposeThoughts: true}

	restore := AntigravityQuotaFieldsLost(incoming, held)
	if restore.Empty() {
		t.Fatal("quota_* fields held locally but missing in DB must be reported")
	}
	if restore.QuotaGateEnabled == nil || *restore.QuotaGateEnabled {
		t.Fatalf("restored gate = %v, want explicit false", restore.QuotaGateEnabled)
	}
	if restore.QuotaRefreshIntervalMinutes == nil || *restore.QuotaRefreshIntervalMinutes != 15 {
		t.Fatalf("restored interval = %v, want 15", restore.QuotaRefreshIntervalMinutes)
	}

	// DB 已带有同名字段时不得覆盖 DB 值（只有缺失才算被抹掉）。
	present := AntigravitySettings{QuotaGateEnabled: &gateOff}
	if restore := AntigravityQuotaFieldsLost(present, AntigravitySettings{QuotaGateEnabled: &gateOff}); !restore.Empty() {
		t.Fatalf("present fields must not be reported as lost: %+v", restore)
	}
	// 返回的指针必须是副本，调用方改写不得影响本机已生效配置。
	if restore.QuotaGateEnabled == held.QuotaGateEnabled || restore.QuotaRefreshIntervalMinutes == held.QuotaRefreshIntervalMinutes {
		t.Fatal("restore pointers must be copies, not aliases of the held configuration")
	}
	*restore.QuotaGateEnabled = true
	if held.GetQuotaGateEnabled() {
		t.Fatal("writing the restore copy must not change the held configuration")
	}
}
