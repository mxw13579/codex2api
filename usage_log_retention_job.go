package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codex2api/database"
)

const usageLogRetentionInterval = time.Minute

// usageLogRetentionPolicyFromEnv 读取 usage_logs 明细保留配置，默认关闭：
//   - CODEX_USAGE_LOG_RETENTION_HOURS：保留小时数，例如 48
//   - CODEX_USAGE_LOG_MAX_MB：明细估算体积上限（MiB），例如 1024
//
// 两者任一满足即清理最旧的行。
func usageLogRetentionPolicyFromEnv() (database.UsageLogRetentionPolicy, error) {
	var policy database.UsageLogRetentionPolicy
	parse := func(name string) (int64, error) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return 0, nil
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("%s must be a non-negative integer", name)
		}
		return v, nil
	}
	hours, err := parse("CODEX_USAGE_LOG_RETENTION_HOURS")
	if err != nil {
		return policy, err
	}
	mb, err := parse("CODEX_USAGE_LOG_MAX_MB")
	if err != nil {
		return policy, err
	}
	policy.MaxAge = time.Duration(hours) * time.Hour
	policy.MaxBytes = mb << 20
	return policy, nil
}

// startUsageLogRetention 每分钟清理一次 usage_logs；未配置时不启动。
func startUsageLogRetention(ctx context.Context, db *database.DB, policy database.UsageLogRetentionPolicy) {
	if db == nil || !policy.Enabled() {
		return
	}
	log.Printf("[usage-retention] 已启用：保留 %s，体积上限 %d MiB（任一满足即清理）", policy.MaxAge, policy.MaxBytes>>20)
	run := func() {
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		start := time.Now()
		result, err := db.PruneUsageLogs(runCtx, policy, time.Now())
		if err != nil {
			log.Printf("[usage-retention] 清理失败: %v（已删 %d 行）", err, result.Deleted)
			return
		}
		if result.Deleted > 0 {
			log.Printf("[usage-retention] 删除 %d 行（%d 批，%s）cutoff_id=%d safe_max_id=%d keep_window=%s est_row_bytes=%d",
				result.Deleted, result.Batches, time.Since(start).Round(time.Millisecond),
				result.CutoffID, result.SafeMaxID, result.KeepWindow, result.EstimatedRowBytes)
		}
	}
	go func() {
		ticker := time.NewTicker(usageLogRetentionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}
