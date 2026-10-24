package database

// Antigravity 配额来源（source）的短事务 CAS 协议（Spec §4.1、验收 #28/#36/#39）。
//
// 本文件与 auth 包解耦：配额事实以 credentials JSON 的 `antigravity_quota`
// （字符串形式的 JSON 快照）与 `antigravity_quota_epoch`（失效纪元）两个键持久化，
// 其中 `antigravity_quota` 的**落库形态恒为 JSON 字符串**：mutator 在内存里把它当成
// map 就地合并只是工作形态，写库前一律由 normalizeAntigravityQuotaCredentialForWrite
// 序列化回字符串——全仓库读取端都经 `AccountRow.GetCredential` 取文本，写成嵌套
// JSON Object 会让内存硬门禁 fail-open、清除逻辑静默跳过 facts 清空。
// 所有写入都在**短事务 + 行锁**内完成，事务中不联网：
//   - 预约：逐 source 递增 `source_metadata.<source>.current_attempt_seq`；
//   - 提交：Latest-Success-Wins，仅要求 `mySeq > applied_seq` 且 gen/scope/epoch 匹配；
//   - 清除：同事务删除 source 事实与定时器并递增 epoch，构成在途观测的失效屏障；
//   - 轮换：UpdateCAS 的受控 carryforward 让同身份例行换票不洗新已落库的配额事实。
//
// 序号写入不推进 `credential_generation`（预约不是凭据轮换），因此预约与提交
// 可以在同一代际上前后发生。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	// antigravityQuotaCredentialKey 是配额快照在 credentials JSON 中的键（明文存储，
	// 触发器可直接 json_extract 比较）。
	antigravityQuotaCredentialKey = "antigravity_quota"
	// antigravityQuotaEpochCredentialKey 是配额事实失效纪元（与 generation 独立）。
	antigravityQuotaEpochCredentialKey = "antigravity_quota_epoch"
	// antigravityQuotaPermanentRefreshErrorKey 是 RT 谱系断裂的永久失败标记。
	antigravityQuotaPermanentRefreshErrorKey = "antigravity_permanent_refresh_error"

	antigravityUpstreamType = "antigravity"

	antigravityQuotaKeySchemaVersion  = "schema_version"
	antigravityQuotaKeySourceMetadata = "source_metadata"
	antigravityQuotaKeyQuotaGroups    = "quota_groups"
	antigravityQuotaKeyAICredits      = "ai_credits"

	antigravitySourceMetaAppliedSeq        = "applied_seq"
	antigravitySourceMetaCurrentAttemptSeq = "current_attempt_seq"
	antigravitySourceMetaLastAttemptAt     = "last_attempt_at"
	antigravitySourceMetaLastSuccessAt     = "last_success_at"

	// antigravityQuotaTimerKindPrefix 是配额流定时器在 maintenance_jobs 中的 job_kind 前缀。
	antigravityQuotaTimerKindPrefix = "antigravity_quota"

	// antigravityRTConflictAuditEventType / antigravityRTConflictAuditSource 是 RT 冲突
	// 分支 (c)（身份隔离）写入 account_events 的审计事件类型与来源。
	antigravityRTConflictAuditEventType = "antigravity_rt_conflict_isolated"
	antigravityRTConflictAuditSource    = "system"
)

// RTConflictOutcome 是 RT 轮换冲突按 reload 结果分流后的处置分支（Spec §4.1 #5）。
type RTConflictOutcome int

const (
	// RTConflictTrustWinner durable 行已含更新的 access token：信任 winner，丢弃本次
	// token 与全部 source 观测，绝不回写。
	RTConflictTrustWinner RTConflictOutcome = iota + 1
	// RTConflictReplayCredentialOnly durable 行仍等于预轮换凭据且身份未变：在调用方
	// 仍持 family lease 的前提下重放一次仅含凭据字段的 UpdateCAS，不携带任何 source 观测。
	RTConflictReplayCredentialOnly
	// RTConflictIsolateIdentity 身份谱系无法证明连续：禁止把本次 RT 写入当前身份，
	// 落永久失败标记并写审计。
	RTConflictIsolateIdentity
)

// String 返回 RTConflictOutcome 分流决策的枚举文本表示。
func (o RTConflictOutcome) String() string {
	switch o {
	case RTConflictTrustWinner:
		return "trust_winner"
	case RTConflictReplayCredentialOnly:
		return "replay_credential_only"
	case RTConflictIsolateIdentity:
		return "isolate_identity"
	default:
		return "unknown"
	}
}

// antigravityRTReplayRaceHook 仅供本包测试注入：HandleAntigravityRTConflict 分支 (b)
// 重放前调用一次，用于确定性地模拟"重放窗口内被同身份 winner 抢先写入"。生产恒为 nil。
var antigravityRTReplayRaceHook func(accountID int64)

// antigravityQuotaRowTx 是短事务内锁定的账号行。
type antigravityQuotaRowTx struct {
	tx         *sql.Tx
	doc        map[string]any
	generation int64
	familyID   string
}

// mutateAntigravityQuotaCredentials 在单个短事务内锁定账号行（PG 用 FOR UPDATE，
// SQLite 依赖 withSQLiteWriteLock 串行化），把 mutator 对 credentials 文档的修改写回，
// **不**推进 credential_generation。mutator 返回 proceed=false 时不写入（fence 拒绝）。
func (db *DB) mutateAntigravityQuotaCredentials(ctx context.Context, accountID int64, mutator func(*antigravityQuotaRowTx) (bool, error)) error {
	if db == nil || db.conn == nil || accountID <= 0 {
		return errors.New("antigravity quota mutation requires a live account id")
	}
	return db.withSQLiteWriteLock(ctx, func() error {
		tx, beginErr := db.conn.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		defer tx.Rollback()
		query := `SELECT credentials, credential_generation, COALESCE(credential_family_id,'') FROM accounts WHERE id=$1 AND status <> 'deleted'`
		if !db.isSQLite() {
			query += ` FOR UPDATE`
		}
		var raw any
		var generation int64
		var familyID string
		if scanErr := tx.QueryRowContext(ctx, query, accountID).Scan(&raw, &generation, &familyID); scanErr != nil {
			return scanErr
		}
		doc := decodeCredentials(raw)
		if doc == nil {
			doc = map[string]any{}
		}
		row := &antigravityQuotaRowTx{tx: tx, doc: doc, generation: generation, familyID: strings.TrimSpace(familyID)}
		proceed, mutateErr := mutator(row)
		if mutateErr != nil || !proceed {
			return mutateErr
		}
		// 落库形态栅栏：antigravity_quota 在持久化文档中**恒为 JSON 字符串**（既有规范）。
		// mutator 为了方便就地合并会把它变成 map（内存工作形态），必须在 marshal 之前统一
		// 序列化回字符串，否则落库会变成嵌套 JSON Object：`AccountRow.GetCredential` 对
		// map 返回 ""，内存硬门禁会 fail-open，清除逻辑也会静默跳过 facts 清空。
		if normalizeErr := normalizeAntigravityQuotaCredentialForWrite(row.doc); normalizeErr != nil {
			return normalizeErr
		}
		encoded, marshalErr := json.Marshal(encryptSensitiveCredentials(row.doc))
		if marshalErr != nil {
			return marshalErr
		}
		updateQuery := `UPDATE accounts SET credentials=$1, updated_at=CURRENT_TIMESTAMP WHERE id=$2 AND credential_generation=$3`
		if !db.isSQLite() {
			updateQuery = `UPDATE accounts SET credentials=$1::jsonb, updated_at=NOW() WHERE id=$2 AND credential_generation=$3`
		}
		if _, execErr := tx.ExecContext(ctx, updateQuery, encoded, accountID, generation); execErr != nil {
			return execErr
		}
		return tx.Commit()
	})
}

// ReserveAntigravityQuotaSource 在该 source 自己的子请求联网前预约一枚序号：短事务内
// 锁定账号行，校验未删除、expectedGen 匹配、expectedScope 非空时身份必须相等（为空表示
// 调用方不做身份校验），仅递增该 source 的 `current_attempt_seq` 并记录真实尝试时间，
// 返回序号与其绑定的 quota_epoch。
// 未取得执行资格的调用方不得调用本函数空耗序号（Spec §4.1 #1）。
func (db *DB) ReserveAntigravityQuotaSource(ctx context.Context, accountID int64, source string, expectedGen int64, expectedScope string) (int64, int64, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return 0, 0, errors.New("antigravity quota reservation requires a source name")
	}
	expectedScope = strings.TrimSpace(expectedScope)
	var seq, epoch int64
	err := db.mutateAntigravityQuotaCredentials(ctx, accountID, func(row *antigravityQuotaRowTx) (bool, error) {
		if row.generation != expectedGen {
			return false, fmt.Errorf("antigravity quota reservation rejected: generation fence account=%d expected=%d current=%d", accountID, expectedGen, row.generation)
		}
		// scope fence：expectedScope 非空时 durable 身份必须与之完全相等，否则拒绝；
		// expectedScope 为空表示调用方不做身份校验（本函数不替调用方兜底身份，只认
		// 它给的期望值）。只认自己身份的调用方（如 groups summary）必须在 expectedScope
		// 为空时自行放弃，不得传空串放行。
		if expectedScope != "" && row.familyID != expectedScope {
			return false, fmt.Errorf("antigravity quota reservation rejected: identity scope fence account=%d expected=%s current=%q", accountID, expectedScope, row.familyID)
		}
		meta := antigravitySourceMetadataForWrite(row.doc, source)
		next, _ := int64FromJSONValue(meta[antigravitySourceMetaCurrentAttemptSeq])
		next++
		meta[antigravitySourceMetaCurrentAttemptSeq] = next
		meta[antigravitySourceMetaLastAttemptAt] = time.Now().UTC().Format(time.RFC3339)
		seq = next
		epoch = antigravityQuotaEpochFromDocument(row.doc)
		return true, nil
	})
	if err != nil {
		return 0, 0, err
	}
	return seq, epoch, nil
}

// CommitAntigravityQuotaSource 以 Latest-Success-Wins 契约提交一个 source 的成功观测：
// 短事务内复验 epoch/gen 以及（expectedScope 非空时的）身份，并要求 `mySeq > applied_seq`；
// **不得**要求 `current_attempt_seq == mySeq`（后发失败不得否决先发成功，验收 #28）。任一
// fence 不匹配只返回 applied=false 且不报错——它是写入栅栏，不是网络去重证明。
func (db *DB) CommitAntigravityQuotaSource(ctx context.Context, accountID int64, source string, mySeq int64, epoch int64, expectedGen int64, expectedScope string, snapshotJSON string) (bool, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return false, errors.New("antigravity quota commit requires a source name")
	}
	incoming := map[string]any{}
	if trimmed := strings.TrimSpace(snapshotJSON); trimmed != "" {
		if err := json.Unmarshal([]byte(trimmed), &incoming); err != nil {
			return false, fmt.Errorf("decode antigravity quota snapshot for source %s: %w", source, err)
		}
		if incoming == nil {
			incoming = map[string]any{}
		}
	}
	expectedScope = strings.TrimSpace(expectedScope)
	applied := false
	err := db.mutateAntigravityQuotaCredentials(ctx, accountID, func(row *antigravityQuotaRowTx) (bool, error) {
		if row.generation != expectedGen {
			return false, nil
		}
		if expectedScope != "" && row.familyID != expectedScope {
			return false, nil
		}
		if epoch != antigravityQuotaEpochFromDocument(row.doc) {
			return false, nil
		}
		appliedSeq, _ := int64FromJSONValue(antigravitySourceMetadataEntryForRead(row.doc, source)[antigravitySourceMetaAppliedSeq])
		if mySeq <= appliedSeq {
			return false, nil
		}
		successAt := time.Now().UTC().Format(time.RFC3339)
		facts := antigravityQuotaFactsForWrite(row.doc)
		antigravityMergeQuotaFacts(facts, incoming, source, mySeq, successAt)
		// 这里只维护内存工作形态（map）；落库前的统一规范化会把 facts 序列化回 JSON 字符串，
		// 因此提交路径不再各自 marshal。
		row.doc[antigravityQuotaCredentialKey] = facts
		applied = true
		return true, nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// ClearAntigravityQuotaSourcesCAS 清除该账号全部配额 source 事实与定时器，并在
// **同一短事务内**递增 `antigravity_quota_epoch`（Spec §4.1 失效纪元、验收 #36）。
// 凭据、models 等目录分区与 credential generation 均保持不变，因此清除后仍在途的
// 观测（无论其 mySeq 多大）提交时都会因 epoch 不匹配被拒。facts 的解析同时兼容 JSON
// 字符串与 JSON 对象两种形态：即使数据曾被写成对象（历史脏数据），也必须彻底清空事实并
// 推进 epoch，绝不允许因类型断言失败而静默残留。
func (db *DB) ClearAntigravityQuotaSourcesCAS(ctx context.Context, accountID int64) (int64, error) {
	newEpoch := int64(0)
	err := db.mutateAntigravityQuotaCredentials(ctx, accountID, func(row *antigravityQuotaRowTx) (bool, error) {
		if _, err := row.tx.ExecContext(ctx,
			`DELETE FROM maintenance_jobs WHERE entity_id=$1 AND job_kind LIKE $2`,
			accountID, antigravityQuotaTimerKindPrefix+"%"); err != nil {
			return false, err
		}
		if facts, ok := antigravityQuotaFactsFromDocument(row.doc); ok {
			// 只删除配额 source 事实；models / 目录分区由目录流自行维护。
			delete(facts, antigravityQuotaKeyQuotaGroups)
			delete(facts, antigravityQuotaKeySourceMetadata)
			delete(facts, antigravityQuotaKeyAICredits)
			if antigravityQuotaFactsHasContent(facts) {
				// 仍承载其他内容：以 map 活引用保留，序列化回 JSON 字符串由落库前的
				// 规范化统一负责。
				row.doc[antigravityQuotaCredentialKey] = facts
			} else {
				row.doc[antigravityQuotaCredentialKey] = ""
			}
		}
		// 非字符串且非对象的非法形态（理论上不存在）由落库前的规范化按清空写法兜底。
		newEpoch = antigravityQuotaEpochFromDocument(row.doc) + 1
		row.doc[antigravityQuotaEpochCredentialKey] = newEpoch
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	return newEpoch, nil
}

// GetAntigravityQuotaEpoch 只读解析 credentials JSON 内嵌的失效纪元，缺省 0。
func (db *DB) GetAntigravityQuotaEpoch(ctx context.Context, accountID int64) (int64, error) {
	if db == nil || db.conn == nil || accountID <= 0 {
		return 0, nil
	}
	var raw any
	if err := db.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id=$1`, accountID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return antigravityQuotaEpochFromDocument(decodeCredentials(raw)), nil
}

// ApplyAntigravityCarryforwardOnUpdateCAS 是 UpdateAccountCredentialsCAS 的 Antigravity
// 受控 carryforward（Spec §4.1 代际变迁规则 #1）：例行同身份换票不得把已成功落库的配额
// 事实洗新，也不得递增失效纪元——严格保留原 observed_at/reset/ttl/applied_seq 与
// known_zero 状态，因此已确认的零额度门禁不会因为一次 token 轮换而解除。
//
// 该逻辑按 `upstream_type == "antigravity"` 条件化，Grok/Codex 走原路径完全不受影响。
// 调用方显式管理 `antigravity_quota`（写入新观测，或身份断裂时按既有策略清空）时，
// 本函数一律不覆盖其值；身份/family 变化时同样不搬运。
func ApplyAntigravityCarryforwardOnUpdateCAS(merged map[string]any, prevRaw string) {
	if merged == nil {
		return
	}
	if upstream, _ := merged["upstream_type"].(string); !strings.EqualFold(strings.TrimSpace(upstream), antigravityUpstreamType) {
		return
	}
	trimmed := strings.TrimSpace(prevRaw)
	if trimmed == "" {
		return
	}
	var prev map[string]any
	if err := json.Unmarshal([]byte(trimmed), &prev); err != nil || prev == nil {
		return
	}
	prevFamily := strings.TrimSpace(credentialStringFromMap(prev, "credential_family_id"))
	nextFamily := strings.TrimSpace(credentialStringFromMap(merged, "credential_family_id"))
	if prevFamily != "" && nextFamily != "" && prevFamily != nextFamily {
		return
	}
	if _, ok := merged[antigravityQuotaEpochCredentialKey]; !ok {
		if prevEpoch, found := int64FromJSONValue(prev[antigravityQuotaEpochCredentialKey]); found {
			merged[antigravityQuotaEpochCredentialKey] = prevEpoch
		}
	}
	if _, ok := merged[antigravityQuotaCredentialKey]; ok {
		return
	}
	if prevFacts := strings.TrimSpace(credentialStringFromMap(prev, antigravityQuotaCredentialKey)); prevFacts != "" {
		merged[antigravityQuotaCredentialKey] = prevFacts
	}
}

// HandleAntigravityRTConflict 在凭据 CAS 失败后按 **reload 结果** 三分支互斥分流
// （Spec §4.1 #5、验收 #39）。调用方约定：仍持有同一 family lease。
// 分支之外不得自行发明第四种覆盖写入，任何分支都绝不顺带接纳来源不匹配的配额观测。
func (db *DB) HandleAntigravityRTConflict(ctx context.Context, accountID int64, expectedGen int64, preRotationCredentials string, rotated map[string]any) (RTConflictOutcome, error) {
	if db == nil || db.conn == nil || accountID <= 0 {
		return 0, errors.New("antigravity RT conflict handling requires a live account id")
	}
	var pre map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(preRotationCredentials)), &pre); err != nil || pre == nil {
		return 0, fmt.Errorf("decode pre-rotation credentials: %w", err)
	}
	// reload 持久化行：不复用本方内存态。
	state, err := db.reloadAntigravityRTDurableState(ctx, accountID)
	if err != nil {
		return 0, err
	}
	if state == nil {
		log.Printf("antigravity rt-conflict audit: account=%d outcome=isolate_identity reason=account_missing rotated_refresh_token_not_persisted=true", accountID)
		return RTConflictIsolateIdentity, nil
	}
	if reason := antigravityIdentityBreakReason(pre, state.credentials, state.familyID, state.status); reason != "" {
		return db.isolateAntigravityIdentity(ctx, accountID, state.generation, reason)
	}
	// 分支 (a)：durable winner 已完成同身份换票 → 信任 winner，绝不回写。
	if strings.TrimSpace(credentialStringFromMap(state.credentials, "access_token")) != strings.TrimSpace(credentialStringFromMap(pre, "access_token")) {
		return RTConflictTrustWinner, nil
	}
	// 分支 (b) 的前提：expected generation 未变（本方仍是唯一持有已被消费 RT 的一方）。
	if expectedGen > 0 && state.generation != expectedGen {
		return RTConflictTrustWinner, nil
	}
	credentialUpdates := antigravityCredentialOnlyUpdates(rotated)
	if len(credentialUpdates) == 0 {
		// 本次没有可落库的凭据字段，等价于无可写事实：信任 durable 状态。
		return RTConflictTrustWinner, nil
	}
	// 仅供包内测试注入的竞态模拟钩子（生产恒为 nil）：在重放窗口内模拟同身份 winner
	// 抢先写入，用于确定性地验证失败复验的归类。
	if antigravityRTReplayRaceHook != nil {
		antigravityRTReplayRaceHook(accountID)
	}
	_, applied, replayErr := db.UpdateAccountCredentialsCAS(ctx, accountID, state.generation, credentialUpdates)
	if replayErr == nil && applied {
		return RTConflictReplayCredentialOnly, nil
	}
	// 重放失败先复验 durable 状态：generation 被推进或 access_token 已变，都说明同一
	// 身份上已有 winner 抢先写入（本方只是输掉了这次 CAS），属于分支 (a) 的语义，
	// 必须信任 winner，绝不可误报身份谱系断裂并落隔离标记。
	if recheck, recheckErr := db.reloadAntigravityRTDurableState(ctx, accountID); recheckErr != nil {
		return 0, recheckErr
	} else if recheck != nil {
		generationAdvanced := recheck.generation != state.generation
		tokenChanged := strings.TrimSpace(credentialStringFromMap(recheck.credentials, "access_token")) !=
			strings.TrimSpace(credentialStringFromMap(pre, "access_token"))
		if generationAdvanced || tokenChanged {
			return RTConflictTrustWinner, nil
		}
	}
	reason := "credential-only replay rejected by generation fence"
	if replayErr != nil {
		reason = "credential-only replay failed: " + replayErr.Error()
	}
	return db.isolateAntigravityIdentity(ctx, accountID, state.generation, reason)
}

// antigravityRTDurableState 是 RT 冲突处理 reload 到的持久化状态。
type antigravityRTDurableState struct {
	credentials map[string]any
	generation  int64
	familyID    string
	status      string
}

// reloadAntigravityRTDurableState 读取账号的持久化凭据行；账号不存在时返回 (nil, nil)。
func (db *DB) reloadAntigravityRTDurableState(ctx context.Context, accountID int64) (*antigravityRTDurableState, error) {
	var raw any
	var generation int64
	var familyColumn string
	var status string
	err := db.conn.QueryRowContext(ctx,
		`SELECT credentials, credential_generation, COALESCE(credential_family_id,''), status FROM accounts WHERE id=$1`,
		accountID).Scan(&raw, &generation, &familyColumn, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	doc := decodeCredentials(raw)
	family := strings.TrimSpace(familyColumn)
	if family == "" {
		family = strings.TrimSpace(credentialStringFromMap(doc, "credential_family_id"))
	}
	return &antigravityRTDurableState{credentials: doc, generation: generation, familyID: family, status: status}, nil
}

// antigravityIdentityBreakReason 返回身份谱系断裂的原因；可验证连续时返回空串。
func antigravityIdentityBreakReason(pre, durable map[string]any, durableFamily, status string) string {
	if strings.EqualFold(strings.TrimSpace(status), "deleted") {
		return "account deleted"
	}
	if strings.TrimSpace(credentialStringFromMap(durable, antigravityQuotaPermanentRefreshErrorKey)) != "" {
		return "account already carries a permanent refresh error (isolated)"
	}
	preFamily := strings.TrimSpace(credentialStringFromMap(pre, "credential_family_id"))
	if preFamily != "" && durableFamily != "" && preFamily != durableFamily {
		return fmt.Sprintf("credential family changed from %s to %s", preFamily, durableFamily)
	}
	for _, key := range []string{"account_id", "email", "project_id"} {
		prevValue := strings.TrimSpace(credentialStringFromMap(pre, key))
		durableValue := strings.TrimSpace(credentialStringFromMap(durable, key))
		if prevValue == "" || durableValue == "" || prevValue == durableValue {
			continue
		}
		return fmt.Sprintf("identity scope field %s changed", key)
	}
	return ""
}

// isolateAntigravityIdentity 落永久失败标记并写审计（分支 (c)）：禁止把本次 RT 写入
// 当前身份，需管理员重新建立身份。
func (db *DB) isolateAntigravityIdentity(ctx context.Context, accountID, durableGen int64, reason string) (RTConflictOutcome, error) {
	message := "Antigravity refresh token consumed upstream could not be persisted: " + reason + "; administrative identity sync required"
	_, applied, err := db.UpdateAccountCredentialsCAS(ctx, accountID, durableGen, map[string]any{
		antigravityQuotaPermanentRefreshErrorKey: message,
	})
	log.Printf("antigravity rt-conflict audit: account=%d outcome=isolate_identity reason=%q rotated_refresh_token_not_persisted=true marker_applied=%v", accountID, reason, applied)
	// 与日志并行落一条可查询的账号事件，供后台审计与告警消费；写审计失败只降低
	// 可观测性，不改变分流结论（隔离结论已由上面的标记写入给出）。
	if auditErr := db.InsertAccountEvent(ctx, accountID, antigravityRTConflictAuditEventType, antigravityRTConflictAuditSource); auditErr != nil {
		log.Printf("antigravity rt-conflict audit event failed: account=%d type=%s err=%v", accountID, antigravityRTConflictAuditEventType, auditErr)
	}
	if err != nil {
		return RTConflictIsolateIdentity, err
	}
	return RTConflictIsolateIdentity, nil
}

// antigravityCredentialOnlyUpdates 过滤出仅含凭据字段的 updates（分支 (b) 的重放不得
// 携带任何 source 观测）。
func antigravityCredentialOnlyUpdates(rotated map[string]any) map[string]any {
	allowed := []string{
		"upstream_type", "access_token", "refresh_token", "id_token", "expires_at",
		"oauth_client_key", "antigravity_client_id", "antigravity_client_secret", "oauth_scope",
	}
	filtered := make(map[string]any, len(allowed))
	if rotated == nil {
		return filtered
	}
	for _, key := range allowed {
		if value, ok := rotated[key]; ok {
			filtered[key] = value
		}
	}
	return filtered
}

// antigravityQuotaEpochFromDocument 解析 credentials 文档内嵌的失效纪元，缺省 0。
func antigravityQuotaEpochFromDocument(doc map[string]any) int64 {
	if doc == nil {
		return 0
	}
	value, _ := int64FromJSONValue(doc[antigravityQuotaEpochCredentialKey])
	return value
}

// antigravityQuotaFactsHasContent 判断配额事实是否还承载实质内容（只剩 schema_version
// 之类的骨架时视为已清空，与既有 `antigravity_quota = ""` 的清空写法一致）。
func antigravityQuotaFactsHasContent(facts map[string]any) bool {
	if facts == nil {
		return false
	}
	for key := range facts {
		if key != antigravityQuotaKeySchemaVersion {
			return true
		}
	}
	return false
}

// antigravityQuotaFactsFromDocument 只读解析 credentials 文档内的配额事实，兼容两种形态：
// JSON 字符串（落库规范形态）与 map（内存工作形态 / 历史脏数据）。第二个返回值为 false
// 表示文档内没有可解析的对象事实；本函数不改写文档。
func antigravityQuotaFactsFromDocument(doc map[string]any) (map[string]any, bool) {
	if doc == nil {
		return nil, false
	}
	switch typed := doc[antigravityQuotaCredentialKey].(type) {
	case map[string]any:
		if typed == nil {
			return nil, false
		}
		return typed, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return nil, false
		}
		var facts map[string]any
		if err := json.Unmarshal([]byte(trimmed), &facts); err != nil || facts == nil {
			return nil, false
		}
		return facts, true
	}
	return nil, false
}

// normalizeAntigravityQuotaCredentialForWrite 是 credentials 落库前的形态栅栏：
// `antigravity_quota` 持久化后**恒为 JSON 字符串**（全仓库读取端一律经
// `AccountRow.GetCredential` 取文本，它对非字符串返回 ""，会让内存硬门禁 fail-open）。
// mutator 就地合并时把它保留成 map 只是内存工作形态，这里统一序列化回字符串；既非字符串
// 也非对象的非法形态按既有清空写法（空串）兜底，绝不让读取端拿到非字符串。
func normalizeAntigravityQuotaCredentialForWrite(doc map[string]any) error {
	if doc == nil {
		return nil
	}
	value, present := doc[antigravityQuotaCredentialKey]
	if !present {
		return nil
	}
	switch typed := value.(type) {
	case string:
		return nil
	case map[string]any:
		if typed == nil {
			doc[antigravityQuotaCredentialKey] = ""
			return nil
		}
		encoded, err := json.Marshal(typed)
		if err != nil {
			return fmt.Errorf("encode antigravity quota facts as JSON string: %w", err)
		}
		doc[antigravityQuotaCredentialKey] = string(encoded)
		return nil
	default:
		doc[antigravityQuotaCredentialKey] = ""
		return nil
	}
}

// antigravityQuotaFactsForWrite 返回 credentials 文档内的配额事实对象，缺失时按 V2
// 契约创建骨架。返回的 map 是文档内的活引用（内存工作形态），调用方就地修改即可，
// 落库前的规范化负责把它序列化回 JSON 字符串。
func antigravityQuotaFactsForWrite(doc map[string]any) map[string]any {
	if facts, ok := antigravityQuotaFactsFromDocument(doc); ok {
		if _, isMap := doc[antigravityQuotaCredentialKey].(map[string]any); !isMap {
			// 文档内还是字符串形态：换成解析后的活引用 map，调用方就地修改才会生效。
			doc[antigravityQuotaCredentialKey] = facts
		}
		return facts
	}
	if doc == nil {
		return map[string]any{antigravityQuotaKeySchemaVersion: 2}
	}
	facts := map[string]any{antigravityQuotaKeySchemaVersion: 2}
	doc[antigravityQuotaCredentialKey] = facts
	return facts
}

// antigravitySourceMetadataEntryForRead 只读地定位 credentials 文档内本 source 的元数据，
// 不创建任何对象（用于提交栅栏判定，拒绝路径不得改写文档）。
func antigravitySourceMetadataEntryForRead(doc map[string]any, source string) map[string]any {
	facts, ok := antigravityQuotaFactsFromDocument(doc)
	if !ok {
		return map[string]any{}
	}
	metadata, _ := facts[antigravityQuotaKeySourceMetadata].(map[string]any)
	entry, _ := metadata[source].(map[string]any)
	if entry == nil {
		return map[string]any{}
	}
	return entry
}

// antigravitySourceMetadataForWrite 返回 credentials 文档内 facts.source_metadata.<source>
// 的活引用。
func antigravitySourceMetadataForWrite(doc map[string]any, source string) map[string]any {
	return antigravitySourceMetadataEntryForWrite(antigravityQuotaFactsForWrite(doc), source)
}

// antigravitySourceMetadataEntryForWrite 在配额事实对象内取（或建）本 source 的元数据。
func antigravitySourceMetadataEntryForWrite(facts map[string]any, source string) map[string]any {
	metadata, _ := facts[antigravityQuotaKeySourceMetadata].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		facts[antigravityQuotaKeySourceMetadata] = metadata
	}
	entry, _ := metadata[source].(map[string]any)
	if entry == nil {
		entry = map[string]any{}
		metadata[source] = entry
	}
	return entry
}

// antigravityMergeQuotaFacts 把本 source 的成功观测合并进既有事实：只覆盖本次返回的
// 分组/桶与自己的 source_metadata，未返回的旧桶保留原时标，其他来源保持不变；随后把
// applied_seq 推进到 mySeq 并写入成功时标。
func antigravityMergeQuotaFacts(facts map[string]any, incoming map[string]any, source string, mySeq int64, successAt string) {
	if facts == nil || incoming == nil {
		return
	}
	for key, value := range incoming {
		switch key {
		case antigravityQuotaKeyQuotaGroups:
			facts[key] = antigravityMergeQuotaGroups(facts[key], value)
		case antigravityQuotaKeySourceMetadata:
			antigravityMergeSourceMetadata(facts, value, source)
		default:
			facts[key] = value
		}
	}
	entry := antigravitySourceMetadataEntryForWrite(facts, source)
	appliedSeq, _ := int64FromJSONValue(entry[antigravitySourceMetaAppliedSeq])
	if mySeq > appliedSeq {
		entry[antigravitySourceMetaAppliedSeq] = mySeq
	}
	entry[antigravitySourceMetaLastSuccessAt] = successAt
}

// antigravityMergeQuotaGroups 按 display_name 合并分组、按 window 合并桶：incoming
// 未返回的旧桶原样保留（含其原 observed_at），incoming 返回的桶整体覆盖。
func antigravityMergeQuotaGroups(prevValue, incomingValue any) any {
	incoming, ok := incomingValue.([]any)
	if !ok || len(incoming) == 0 {
		// 未返回任何分组：既有事实原样保留。
		return prevValue
	}
	prevGroups, _ := prevValue.([]any)
	merged := make([]any, 0, len(prevGroups)+len(incoming))
	index := make(map[string]int, len(prevGroups)+len(incoming))
	position := 0
	for _, item := range prevGroups {
		group, _ := item.(map[string]any)
		if group == nil {
			continue
		}
		key := antigravityGroupIdentity(group, position)
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(group))
		position++
	}
	for _, item := range incoming {
		group, _ := item.(map[string]any)
		if group == nil {
			continue
		}
		key := antigravityGroupIdentity(group, position)
		position++
		if at, exists := index[key]; exists {
			target, _ := merged[at].(map[string]any)
			if target == nil {
				continue
			}
			for groupKey, groupValue := range group {
				if groupKey == "buckets" {
					continue
				}
				target[groupKey] = groupValue
			}
			target["buckets"] = antigravityMergeBuckets(target["buckets"], group["buckets"])
			continue
		}
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(group))
	}
	return merged
}

// antigravityMergeBuckets 按 window 合并桶；incoming 未返回的旧桶保留原时标。
func antigravityMergeBuckets(prevValue, incomingValue any) any {
	incoming, ok := incomingValue.([]any)
	if !ok || len(incoming) == 0 {
		return prevValue
	}
	prevBuckets, _ := prevValue.([]any)
	merged := make([]any, 0, len(prevBuckets)+len(incoming))
	index := make(map[string]int, len(prevBuckets)+len(incoming))
	position := 0
	for _, item := range prevBuckets {
		bucket, _ := item.(map[string]any)
		if bucket == nil {
			continue
		}
		key := antigravityBucketIdentity(bucket, position)
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(bucket))
		position++
	}
	for _, item := range incoming {
		bucket, _ := item.(map[string]any)
		if bucket == nil {
			continue
		}
		key := antigravityBucketIdentity(bucket, position)
		position++
		if at, exists := index[key]; exists {
			merged[at] = cloneJSONObject(bucket)
			continue
		}
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(bucket))
	}
	return merged
}

// antigravityMergeSourceMetadata 只合并本 source 的元数据，其他来源保持不变（Spec §4.1 #2）。
func antigravityMergeSourceMetadata(facts map[string]any, incomingValue any, source string) {
	incoming, _ := incomingValue.(map[string]any)
	if incoming == nil {
		return
	}
	incomingEntry, _ := incoming[source].(map[string]any)
	if incomingEntry == nil {
		return
	}
	entry := antigravitySourceMetadataEntryForWrite(facts, source)
	for key, value := range incomingEntry {
		if key == antigravitySourceMetaAppliedSeq || key == antigravitySourceMetaCurrentAttemptSeq {
			// 序号由预约与提交栅栏拥有，不接受快照文本回退。
			if existing, ok := int64FromJSONValue(entry[key]); ok {
				if next, nextOK := int64FromJSONValue(value); !nextOK || next < existing {
					continue
				}
			}
		}
		entry[key] = value
	}
}

// antigravityGroupIdentity 计算配额组在聚合比对时的稳定标识字符串。
func antigravityGroupIdentity(group map[string]any, position int) string {
	for _, key := range []string{"display_name", "group_id", "name"} {
		if value := strings.TrimSpace(credentialStringFromMap(group, key)); value != "" {
			return "g:" + strings.ToLower(value)
		}
	}
	return fmt.Sprintf("#g%d", position)
}

// antigravityBucketIdentity 计算单个配额桶在合并与局部覆盖时的稳定键名。
func antigravityBucketIdentity(bucket map[string]any, position int) string {
	for _, key := range []string{"window", "bucket_id"} {
		if value := strings.TrimSpace(credentialStringFromMap(bucket, key)); value != "" {
			return "b:" + strings.ToLower(value)
		}
	}
	return fmt.Sprintf("#b%d", position)
}

// cloneJSONObject 浅拷贝 JSON 键值对映射对象。
func cloneJSONObject(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
