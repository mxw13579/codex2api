package auth

import (
	"sync/atomic"
	"time"
)

const (
	// quotaGateRetryAfterDefault 是没有可信 resetAt 时对外暴露的默认重试间隔
	// （Spec §6.2 #2）：短到不至于把客户端长时间误导，又不至于让重试风暴打满。
	quotaGateRetryAfterDefault = 60 * time.Second
	// quotaGateRetryAfterMax 是 Retry-After 的上限（Spec §6.2 #2）：可信 resetAt 常常
	// 落在数小时之后，但把 429 的 Retry-After 写成小时级会让下游网关直接放弃。
	quotaGateRetryAfterMax = 3600 * time.Second
)

// QuotaExhaustedError 携带对外 429 契约所需的重试与原因信息。
// Bound=true 表示续链 preserveBinding=true 场景（reason=quota_gate_bound）。
type QuotaExhaustedError struct {
	Bound      bool
	RetryAfter time.Duration
}

// Error 返回类型化配额耗尽错误描述，区分续链绑定账号与整池耗尽场景。
func (e *QuotaExhaustedError) Error() string {
	if e.Bound {
		return "antigravity bound account quota exhausted"
	}
	return "antigravity account quota exhausted"
}

// Is 使 errors.Is(err, ErrQuotaExhausted) 对两种原因都成立。
func (e *QuotaExhaustedError) Is(target error) bool { return target == ErrQuotaExhausted }

// quotaGateRetryAfter 把阻断到期时刻折算成对外的 Retry-After：无可信到期时刻取默认
// 60s，超出上限则钳制到 3600s（Spec §6.2 #2）。
func quotaGateRetryAfter(resetAt time.Time, now time.Time) time.Duration {
	if resetAt.IsZero() || !resetAt.After(now) {
		return quotaGateRetryAfterDefault
	}
	remaining := resetAt.Sub(now)
	if remaining > quotaGateRetryAfterMax {
		return quotaGateRetryAfterMax
	}
	return remaining
}

// antigravityAccountQuotaGateBlock 是自锁封装的「是否被硬门禁阻断 + 阻断到期时刻」，
// 供锁外的候选集合求值使用；与选号复用的是同一个谓词，不存在第二套判定口径。
func antigravityAccountQuotaGateBlock(acc *Account) (bool, time.Time) {
	if acc == nil {
		return false, time.Time{}
	}
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	exhausted, _, resetAt := acc.IsAntigravityQuotaExhaustedLocked(time.Now())
	if !exhausted {
		return false, time.Time{}
	}
	return true, resetAt
}

// antigravityQuotaGateCandidateScope 判定账号是否仍在候选集合内：管理面停用（禁用 /
// 调度暂停 / 智能暂停）的账号本就不是候选，既不构成配额阻断，也不构成混合原因。
func antigravityQuotaGateCandidateScope(acc *Account) bool {
	if acc == nil {
		return false
	}
	if atomic.LoadInt32(&acc.Disabled) != 0 {
		return false
	}
	if atomic.LoadInt32(&acc.DispatchPaused) != 0 || atomic.LoadInt32(&acc.IntelligencePaused) != 0 {
		return false
	}
	return true
}

// AntigravityQuotaGateExhaustion 判定「候选集合是否全部因配额硬门禁被阻断」：命中时
// 返回类型化的 *QuotaExhaustedError（Bound 表示续链 preserveBinding 场景，RetryAfter
// 取被阻断候选中最早的可信到期时刻），否则返回 nil。
//
// 严格的保守口径（Spec §6.2 #2）：
//   - 只要还有一个候选未被门禁阻断（可选、并发满、鉴权失败、冷却、非 Antigravity…），
//     都属于「混合原因」，返回 nil，调用方保持既有 503 / 无可用账号语义；
//   - 续链（preserveBinding=true）的候选集合就是绑定的那个账号，绑定关系不因阻断改变；
//   - 只做一次只读求值，调用方不得再据此二次推断 reason。
func (s *Store) AntigravityQuotaGateExhaustion(sessionKey string, apiKeyID int64, exclude map[int64]bool, filter AccountFilter, preserveBinding bool) *QuotaExhaustedError {
	if s == nil {
		return nil
	}
	now := time.Now()
	filter = s.withUsableEgressFilter(filter)

	var candidates []*Account
	if preserveBinding {
		accountID, ok := s.SessionAffinityAccountID(sessionKey)
		if !ok || accountID == 0 || (exclude != nil && exclude[accountID]) {
			return nil
		}
		s.mu.RLock()
		acc := s.lookupByIDLocked(accountID)
		s.mu.RUnlock()
		if acc == nil || !s.accountAllowedForAPIKey(acc, apiKeyID) {
			return nil
		}
		if filter != nil && !filter(acc) {
			return nil
		}
		candidates = []*Account{acc}
	} else {
		for _, acc := range s.accountSnapshotAccounts() {
			if acc == nil || (exclude != nil && exclude[acc.DBID]) {
				continue
			}
			if !antigravityQuotaGateCandidateScope(acc) {
				continue
			}
			if !s.accountAllowedForAPIKey(acc, apiKeyID) {
				continue
			}
			if filter != nil && !filter(acc) {
				continue
			}
			candidates = append(candidates, acc)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	var earliestReset time.Time
	for _, acc := range candidates {
		blocked, resetAt := antigravityAccountQuotaGateBlock(acc)
		if !blocked {
			return nil // 混合原因：不是「全部因配额阻断」
		}
		if resetAt.IsZero() {
			continue
		}
		if earliestReset.IsZero() || resetAt.Before(earliestReset) {
			earliestReset = resetAt
		}
	}
	return &QuotaExhaustedError{Bound: preserveBinding, RetryAfter: quotaGateRetryAfter(earliestReset, now)}
}

// IsAntigravityQuotaExhaustedLocked 是纯内存只读的硬门禁谓词：基于已投影到
// Account.AntigravityQuota 的 V2 快照，判断该账号此刻是否应因配额耗尽被阻断调度。
//
// 严格约束（Spec §3.3）：本方法不得进行任何 I/O，也不得派生 goroutine；调用方需
// 自行保证在持有账号锁的临界区内调用（Locked 语义）。
//
// 返回：
//   - exhausted：是否阻断；
//   - reason：阻断原因（known_zero / stale_zero），未阻断时为空串；
//   - resetAt：阻断的到期时刻（可信 resetAt 或 TTL 兜底），未阻断时为零值。
func (a *Account) IsAntigravityQuotaExhaustedLocked(now time.Time) (bool, string, time.Time) {
	if a == nil || a.UpstreamType != UpstreamAntigravity {
		return false, "", time.Time{}
	}
	return antigravityQuotaSnapshotGateBlock(&a.AntigravityQuota, now)
}

// antigravityQuotaSnapshotGateBlock 是硬门禁谓词的唯一实现体：只依赖一份 V2 配额快照
// 与当前时钟，不读 Account 的其他字段。调度门禁与展示侧（管理端 /state）都经由它求值，
// 避免出现第二套判定口径（Spec §3.3）。
func antigravityQuotaSnapshotGateBlock(snapshot *AntigravityQuotaSnapshot, now time.Time) (bool, string, time.Time) {
	// 止血开关：若系统设置关闭门禁，立即放行（Spec §8.1，不影响已落库快照）
	if !ConfiguredAntigravitySettings().GetQuotaGateEnabled() {
		return false, "", time.Time{}
	}
	if snapshot == nil {
		return false, "", time.Time{}
	}
	// legacy 快照（无 schema_version=2）：旧数据显式 0 不可信，绝不硬封（Spec §3.1）
	if snapshot.SchemaVersion != 2 {
		return false, "", time.Time{}
	}
	// 元数据可信条件（Spec §3.1）：applied_seq > 0、observed_at 非零且不在未来；
	// 缺失即 unknown，不构成阻断依据。
	meta, ok := snapshot.SourceMetadata["groups"]
	if !ok || meta.AppliedSeq <= 0 || meta.ObservedAt.IsZero() || meta.ObservedAt.After(now) {
		return false, "", time.Time{}
	}

	for _, g := range snapshot.Groups {
		for _, b := range g.Buckets {
			w, ok := NormalizeAntigravityWindow(b.Window)
			if !ok || !b.FractionPresent || b.RemainingFraction == nil {
				continue // 白名单外 / 无 presence / 无数值 → unknown，不阻断
			}
			if *b.RemainingFraction != 0.0 {
				continue // known_positive（含极小正数）与解析层未收敛的非法值一律不阻断
			}
			resetAt, hasReset := ValidateResetTime(b.ResetTime, b.ObservedAt, w)
			ttlDeadline := b.ObservedAt.Add(time.Duration(b.TTLSeconds) * time.Second)
			if hasReset {
				if now.Before(resetAt) {
					if now.After(ttlDeadline) {
						// 超龄已知零（stale）：观测年龄超 TTL 但 resetAt 未到，
						// 保持阻断并标记 stale，进入优先补查，绝不静默放行（Spec §3.3）
						return true, "stale_zero", resetAt
					}
					return true, "known_zero", resetAt // 阻断持续到可信 resetAt
				}
				// resetAt 已过：窗口滚动转 unknown，仅解除本桶，不清其他有效零桶
				continue
			}
			// 无可信 resetAt：TTL 兜底阻断，避免永久封禁
			if now.Before(ttlDeadline) {
				return true, "known_zero", ttlDeadline
			}
		}
	}
	return false, "", time.Time{}
}

// AntigravityQuotaGateView 是硬门禁的只读视图，供管理端展示四态与来源元数据
// （Spec §3.3「管理端按来源显示事实过期」、§9 管理层落点）。它不参与任何调度决策，
// 判定与调度侧共用 antigravityQuotaSnapshotGateBlock 同一函数。
type AntigravityQuotaGateView struct {
	GateEnabled    bool
	SchemaVersion  int
	Blocked        bool
	Reason         string
	Stale          bool
	ResetAt        time.Time
	SourceMetadata map[string]SourceMeta
}

// EvaluateAntigravityQuotaGate 对一份持久化配额快照做与硬门禁完全同口径的只读求值。
// 调用方无需持有 Account 锁，因此管理端可以直接对数据库行里的快照观测门禁状态。
func EvaluateAntigravityQuotaGate(snapshot *AntigravityQuotaSnapshot, now time.Time) AntigravityQuotaGateView {
	view := AntigravityQuotaGateView{GateEnabled: ConfiguredAntigravitySettings().GetQuotaGateEnabled()}
	if snapshot != nil {
		view.SchemaVersion = snapshot.SchemaVersion
		if len(snapshot.SourceMetadata) > 0 {
			view.SourceMetadata = make(map[string]SourceMeta, len(snapshot.SourceMetadata))
			for key, meta := range snapshot.SourceMetadata {
				view.SourceMetadata[key] = meta
			}
		}
	}
	view.Blocked, view.Reason, view.ResetAt = antigravityQuotaSnapshotGateBlock(snapshot, now)
	view.Stale = view.Blocked && view.Reason == "stale_zero"
	return view
}
