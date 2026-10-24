package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestStore_DispatchQuotaGateLinearizationAndSessionBinding 断言硬门禁线性化拦截、
// 新请求借号重绑（D4=C）、续链保留绑定与止血开关放行。
func TestStore_DispatchQuotaGateLinearizationAndSessionBinding(t *testing.T) {
	// 生产选号路径以 time.Now() 求值门禁，测试事实必须以「当前时刻」为观测基准，
	// 否则 resetAt/TTL 相对真实时钟滚动，断言会随时钟漂移而失真。
	now := time.Now().UTC()
	zero := 0.0

	newAccount := func(id int64) *Account {
		// 可调度最小构造，参照 auth/antigravity_types_test.go:15-21 的既有模式。
		return &Account{
			DBID:                 id,
			UpstreamType:         UpstreamAntigravity,
			AccessToken:          "at",
			RefreshToken:         "rt",
			AntigravityProjectID: "project-id",
			HealthTier:           HealthTierHealthy,
		}
	}

	// 已耗尽账号 A（weekly known_zero + 可信 metadata）与健康账号 B
	accA := newAccount(1)
	accA.AntigravityQuota = AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {AppliedSeq: 3, ObservedAt: now, PresenceVerified: true},
		},
		Groups: []AntigravityQuotaGroup{{
			Buckets: []AntigravityQuotaBucket{{
				Window:            "weekly",
				RemainingFraction: &zero,
				FractionPresent:   true,
				ResetTime:         now.Add(1 * time.Hour).Format(time.RFC3339),
				ObservedAt:        now,
				ObservationSeq:    3,
				TTLSeconds:        3600,
			}},
		}},
	}
	accB := newAccount(2)

	store := (&Store{accounts: []*Account{accA, accB}}).withMaxConcurrency(2)
	prevAffinity := GetAntigravityBalancedAffinity()
	SetAntigravityBalancedAffinity(true)
	defer SetAntigravityBalancedAffinity(prevAffinity)
	opts := SelectionOptions{Provider: UpstreamAntigravity}

	// 1. 会话先绑定 A（硬零）。新请求（preserveBinding=false 语义）按 D4=C
	//    fallthrough 到 Fresh 选号：硬零只是"不可用原因"之一，与容量满/鉴权失败
	//    并列，借号并重绑到健康账号 B，不返回错误。
	store.bindSessionAffinity("session_key_1", accA, "")
	selected := store.nextAccountForFreshAffinityWithDispatch("session_key_1", 0, nil, nil, opts)
	if selected == nil || selected.DBID != accB.DBID {
		t.Fatalf("expected borrowing and rebinding to healthy accB, got %+v", selected)
	}

	// 2. 续链请求（preserveBinding=true）绑定到 A：遇阻断必须返回 nil 并保留绑定，
	//    不借号、不静默重绑（映射为 429 + reason=quota_gate_bound 的职责在 proxy
	//    边缘层，见 Task 13）。
	store.bindSessionAffinity("session_key_bound_a", accA, "")
	selectedBound, _ := store.NextForContinuationWithDispatchForProvider(opts, "session_key_bound_a", 0, nil, nil)
	if selectedBound != nil {
		t.Fatalf("expected nil for preserveBinding=true with quota exhausted account, got %+v", selectedBound)
	}
	if current, ok := store.sessionBindings["session_key_bound_a"]; !ok || current.accountID != accA.DBID {
		t.Fatalf("quota gate must keep the durable binding intact, got %+v ok=%v", current, ok)
	}

	// 3. 门禁关闭（kill switch）后同一 Fresh 选号放行被阻断的账号（止血路径连通性）。
	prevGate := ConfiguredAntigravitySettings()
	gateOff := false
	SetConfiguredAntigravitySettings(AntigravitySettings{QuotaGateEnabled: &gateOff})
	defer SetConfiguredAntigravitySettings(prevGate)
	selectedAfterKillSwitch := store.nextAccountForFreshAffinityWithDispatch("session_key_kill_switch", 0, nil, nil, opts)
	if selectedAfterKillSwitch == nil {
		t.Fatal("quota_gate_enabled=false must restore schedulability for gated accounts")
	}
	store.Release(selectedAfterKillSwitch)
}

// TestStore_DispatchQuotaGateLinearizationBlockReason 断言类型化阻断原因上浮：
// known_zero 与 stale_zero 统一归为 quota_exhausted，且不冒充上游用量受限。
func TestStore_DispatchQuotaGateLinearizationBlockReason(t *testing.T) {
	now := time.Now().UTC()
	zero := 0.0

	blocked := func(observed, reset time.Time) *Account {
		return &Account{
			DBID:                 11,
			UpstreamType:         UpstreamAntigravity,
			AccessToken:          "at",
			RefreshToken:         "rt",
			AntigravityProjectID: "project-id",
			HealthTier:           HealthTierHealthy,
			AntigravityQuota: AntigravityQuotaSnapshot{
				SchemaVersion: 2,
				SourceMetadata: map[string]SourceMeta{
					"groups": {AppliedSeq: 3, ObservedAt: observed, PresenceVerified: true},
				},
				Groups: []AntigravityQuotaGroup{{
					Buckets: []AntigravityQuotaBucket{{
						Window:            "weekly",
						RemainingFraction: &zero,
						FractionPresent:   true,
						ResetTime:         reset.Format(time.RFC3339),
						ObservedAt:        observed,
						ObservationSeq:    3,
						TTLSeconds:        3600,
					}},
				}},
			},
		}
	}

	// 类型化阻断原因：known_zero 与 stale_zero 都对上层暴露同一个 quota_exhausted。
	acc := blocked(now, now.Add(2*time.Hour))
	acc.mu.RLock()
	reason := acc.antigravityQuotaBlockReasonLocked(now)
	acc.mu.RUnlock()
	if reason != "quota_exhausted" {
		t.Fatalf("known_zero must report quota_exhausted, got %q", reason)
	}

	accStale := blocked(now, now.Add(72*time.Hour))
	accStale.mu.RLock()
	staleReason := accStale.antigravityQuotaBlockReasonLocked(now.Add(2 * time.Hour))
	accStale.mu.RUnlock()
	if staleReason != "quota_exhausted" {
		t.Fatalf("stale_zero must report quota_exhausted, got %q", staleReason)
	}

	// resetAt 已过（观测早于 reset、resetAt 可信、现在越界）：窗口滚动转 unknown，放行。
	accRolled := blocked(now.Add(-2*time.Hour), now.Add(-time.Hour))
	accRolled.mu.RLock()
	rolledReason := accRolled.antigravityQuotaBlockReasonLocked(now)
	availableWhileRolled := accRolled.isAvailableLocked(now)
	accRolled.mu.RUnlock()
	if rolledReason != "" {
		t.Fatalf("expired resetAt must unblock, got %q", rolledReason)
	}
	if !availableWhileRolled {
		t.Fatal("rolled-over quota window must stay dispatchable")
	}

	// 非 Antigravity 账号不受门禁影响。
	other := &Account{DBID: 12, UpstreamType: "openai", APIKey: "k", HealthTier: HealthTierHealthy}
	other.mu.RLock()
	otherReason := other.antigravityQuotaBlockReasonLocked(now)
	other.mu.RUnlock()
	if otherReason != "" {
		t.Fatalf("non-antigravity account must not be gated, got %q", otherReason)
	}

	// kill switch：门禁关闭后同一事实立即放行。
	prevGate := ConfiguredAntigravitySettings()
	gateOff := false
	SetConfiguredAntigravitySettings(AntigravitySettings{QuotaGateEnabled: &gateOff})
	defer SetConfiguredAntigravitySettings(prevGate)
	accOff := blocked(now, now.Add(2*time.Hour))
	accOff.mu.RLock()
	offReason := accOff.antigravityQuotaBlockReasonLocked(now)
	accOff.mu.RUnlock()
	if offReason != "" {
		t.Fatalf("quota_gate_enabled=false must unblock, got %q", offReason)
	}

	// 阻断账号在锁内选号处线性化生效：既非可选，也绝不冒充实并发满（capacityFull=false）。
	SetConfiguredAntigravitySettings(prevGate)
	store := (&Store{accounts: []*Account{acc}}).withMaxConcurrency(2)
	acc.OccupiedRequests.Store(0)
	acc.ActiveRequests.Store(0)
	if acc.IsAvailable() {
		t.Fatal("quota exhausted account must not be available")
	}
	if _, _, _, _, available := acc.fastSchedulerSnapshot(2, now); available {
		t.Fatal("fast scheduler snapshot must fence a quota exhausted account")
	}
	taken, capacityFull := store.takeByIDModeWithCapacity(acc.DBID, 0, nil, nil, false, "", DispatchPolicyStandard)
	if taken != nil || capacityFull {
		t.Fatalf("quota blocked account must miss without capacityFull, got acc=%v capacityFull=%v", taken, capacityFull)
	}
	if store.HasUsageLimitedCandidateWithFilter(0, nil, nil) {
		t.Fatal("quota gate block is not an upstream usage-limit signal")
	}
	// 排队准入同样看不到该账号：被阻断不得把请求推进 WaitForDispatchAvailable 的
	// 长等待（新请求与续链两种准入都要为假）。
	if store.HasDispatchCandidate("session_key_gated", 0, nil, nil, false, DispatchPolicyStandard) {
		t.Fatal("quota blocked account must not be a fresh dispatch candidate")
	}
	if store.HasDispatchCandidate("session_key_gated", 0, nil, nil, true, DispatchPolicyStandard) {
		t.Fatal("quota blocked account must not be a continuation dispatch candidate")
	}
	store.bindSessionAffinity("session_key_gated", acc, "")
	waitedAt := time.Now()
	if waited, _ := store.WaitForContinuationAvailableWithDispatch(context.Background(), "session_key_gated", 5*time.Second, 0, nil, nil, DispatchPolicyStandard); waited != nil {
		t.Fatalf("quota blocked account must not be handed out after waiting, got %d", waited.DBID)
	}
	if elapsed := time.Since(waitedAt); elapsed > time.Second {
		t.Fatalf("quota blocked account entered the wait queue (elapsed %v)", elapsed)
	}
	if _, ok := store.sessionBindings["session_key_gated"]; !ok {
		t.Fatal("waiting on a gated continuation must not drop the binding")
	}
	if !errors.Is(ErrQuotaExhausted, ErrQuotaExhausted) {
		t.Fatal("ErrQuotaExhausted must be a comparable sentinel")
	}
	if ErrQuotaExhausted.Error() != "antigravity account quota exhausted" {
		t.Fatalf("unexpected sentinel message %q", ErrQuotaExhausted.Error())
	}
}
