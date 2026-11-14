package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// usage_logs 明细保留：按时间（MaxAge）与估算体积（MaxBytes）裁掉最旧的行，任一条件满足即清理。
//
// 安全边界：
//   - 只删除总统计（usage_stats_rollup_state）与小时汇总（usage_log_hourly_state）都已消费过的行，
//     汇总数据不受影响；任一汇总未就绪时本轮跳过。
//   - API Key 的窗口限额（RPM/RPD、5h/日/7d/30d 的 cost/token、scope 限额）直接扫描明细计算，
//     最长已配置窗口内的行永远保留，避免限额被“清零”。
//   - 保留最近三个已结束小时及当前小时的完整明细，避免小时核对器用残缺明细覆盖汇总。
//   - 保留最高 ID 的行，MAX(id) 不变，不会触发小时汇总的整表重建。
//   - 多实例（蓝绿重叠）用 PG advisory lock 串行，分批删除避免长事务。
//
// MaxBytes 按“最近行的平均行宽 × 索引系数”估算逻辑体积，不读物理文件大小：DELETE 后表文件
// 不会缩小，用物理大小判断会把所有行都删掉。物理大小在 autovacuum 回收后稳定在上限附近。

const (
	usageLogRetentionLockKey   int64 = 0x636f646578756c72 // "codexulr"
	usageLogRetentionBatchSize       = 5000
	usageLogRetentionSampleRow       = 2000
	// 旧号池实测：usage_logs 总大小 / 堆大小 ≈ 14GB / 8.7GB，含 6 个索引与 TOAST。
	usageLogIndexOverhead = 1.65
	// 堆元组头（23B + 对齐）与行指针（4B）。
	usageLogTupleOverhead = 28
)

// ID 只限定汇总水位和体积裁剪范围，不能替代 created_at 的保留条件。
// $2 是 Key 窗口与小时核对窗口中较早的起点；$3/$4 控制年龄清理，$5 为体积 cutoff。
const usageLogRetentionDeleteSQL = `DELETE FROM usage_logs WHERE id IN
	(SELECT id FROM usage_logs
	 WHERE id <= $1 AND id < (SELECT MAX(id) FROM usage_logs)
	   AND created_at < $2
	   AND (($3 AND created_at < $4) OR id <= $5)
	 ORDER BY id LIMIT $6)`

// UsageLogRetentionPolicy 为 0 的维度表示不按该维度清理。
type UsageLogRetentionPolicy struct {
	MaxAge   time.Duration
	MaxBytes int64
}

// Enabled 判断是否配置了任一清理维度。
func (p UsageLogRetentionPolicy) Enabled() bool {
	return p.MaxAge > 0 || p.MaxBytes > 0
}

// UsageLogPruneResult 描述一轮清理。Skipped 非空时本轮未删除任何行。
type UsageLogPruneResult struct {
	Deleted           int64
	Batches           int
	CutoffID          int64 // 本轮删除候选的 ID 上界，实际删除仍须满足时间保护条件。
	SafeMaxID         int64
	KeepWindow        time.Duration
	EstimatedRowBytes int64
	Skipped           string
}

// apiKeyLimitsKeepWindow 返回该 Key 的窗口限额需要回看的最长时间；没有窗口限额时为 0。
// 累计额度（quota_*）、模型请求额度存于独立计数表，不依赖明细。
func apiKeyLimitsKeepWindow(l APIKeyLimits) time.Duration {
	var keep time.Duration
	use := func(active bool, window time.Duration) {
		if active && window > keep {
			keep = window
		}
	}
	use(l.RPM > 0, time.Minute)
	use(l.RPD > 0 || l.CostLimitDaily > 0 || l.TokenLimitDaily > 0, 24*time.Hour)
	use(l.CostLimit5h > 0 || l.TokenLimit5h > 0, 5*time.Hour)
	use(l.CostLimit7d > 0 || l.TokenLimit7d > 0, 7*24*time.Hour)
	use(l.CostLimit30d > 0 || l.TokenLimit30d > 0, 30*24*time.Hour)
	for _, s := range l.ScopeLimits {
		use(s.Cost5h > 0 || s.Token5h > 0, 5*time.Hour)
		use(s.Cost1d > 0 || s.Token1d > 0 || s.Requests1d > 0, 24*time.Hour)
		use(s.Cost7d > 0 || s.Token7d > 0, 7*24*time.Hour)
		use(s.Cost30d > 0 || s.Token30d > 0, 30*24*time.Hour)
	}
	return keep
}

// usageLogLimitKeepWindow 返回所有 API Key 中最长的窗口限额。
func usageLogLimitKeepWindow(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (time.Duration, error) {
	rows, err := q.QueryContext(ctx, `SELECT limits::text FROM api_keys WHERE limits IS NOT NULL AND limits::text NOT IN ('', '{}', 'null')`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var keep time.Duration
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		var limits APIKeyLimits
		if err := json.Unmarshal([]byte(raw), &limits); err != nil {
			// 无法解析时按最长窗口保守保留，宁可少删。
			return 30 * 24 * time.Hour, nil
		}
		if w := apiKeyLimitsKeepWindow(limits); w > keep {
			keep = w
		}
	}
	return keep, rows.Err()
}

// usageLogRowBudget 把体积上限换算成可保留的行数。
func usageLogRowBudget(maxBytes int64, avgRowBytes float64) (int64, int64) {
	perRow := int64((avgRowBytes + usageLogTupleOverhead) * usageLogIndexOverhead)
	if perRow < 1 {
		perRow = 1
	}
	return maxBytes / perRow, perRow
}

// PruneUsageLogs 执行一轮清理。仅支持 PostgreSQL；SQLite 部署返回 Skipped。
func (db *DB) PruneUsageLogs(ctx context.Context, policy UsageLogRetentionPolicy, now time.Time) (UsageLogPruneResult, error) {
	var result UsageLogPruneResult
	if !policy.Enabled() {
		result.Skipped = "disabled"
		return result, nil
	}
	if db.isSQLite() {
		result.Skipped = "sqlite"
		return result, nil
	}
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	var locked bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, usageLogRetentionLockKey).Scan(&locked); err != nil {
		return result, err
	}
	if !locked {
		result.Skipped = "another instance is pruning"
		return result, nil
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, usageLogRetentionLockKey)

	if err := conn.QueryRowContext(ctx, `SELECT LEAST(
		COALESCE((SELECT last_log_id FROM usage_stats_rollup_state WHERE id = 1 AND initialized = 1), 0),
		COALESCE((SELECT last_log_id FROM usage_log_hourly_state WHERE id = 1 AND ready = 1 AND version = $1), 0))`,
		usageHourlyRollupVersion).Scan(&result.SafeMaxID); err != nil {
		return result, fmt.Errorf("读取汇总进度失败: %w", err)
	}
	if result.SafeMaxID <= 0 {
		result.Skipped = "rollups not ready"
		return result, nil
	}
	if result.KeepWindow, err = usageLogLimitKeepWindow(ctx, conn); err != nil {
		return result, fmt.Errorf("读取 API Key 限额失败: %w", err)
	}

	keepBefore := now.UTC().Truncate(time.Hour).Add(-usageHourlyVerifyHours * time.Hour)
	if keyBefore := now.Add(-result.KeepWindow); keyBefore.Before(keepBefore) {
		keepBefore = keyBefore
	}
	var sizeCut int64
	if policy.MaxBytes > 0 {
		var avg sql.NullFloat64
		if err := conn.QueryRowContext(ctx, `SELECT AVG(pg_column_size(u.*)) FROM
			(SELECT * FROM usage_logs ORDER BY id DESC LIMIT $1) u`, usageLogRetentionSampleRow).Scan(&avg); err != nil {
			return result, err
		}
		if avg.Valid {
			var budget int64
			budget, result.EstimatedRowBytes = usageLogRowBudget(policy.MaxBytes, avg.Float64)
			err := conn.QueryRowContext(ctx, `SELECT id FROM usage_logs ORDER BY id DESC OFFSET $1 LIMIT 1`, budget).Scan(&sizeCut)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return result, err
			}
		}
	}
	result.CutoffID = min(sizeCut, result.SafeMaxID)
	if policy.MaxAge > 0 {
		result.CutoffID = result.SafeMaxID
	}
	if result.CutoffID <= 0 {
		return result, nil
	}
	for ctx.Err() == nil {
		res, err := conn.ExecContext(ctx, usageLogRetentionDeleteSQL,
			result.CutoffID, keepBefore, policy.MaxAge > 0, now.Add(-policy.MaxAge), sizeCut, usageLogRetentionBatchSize)
		if err != nil {
			return result, err
		}
		n, _ := res.RowsAffected()
		result.Deleted += n
		result.Batches++
		if n < usageLogRetentionBatchSize {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	return result, ctx.Err()
}
