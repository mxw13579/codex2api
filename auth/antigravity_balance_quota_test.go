package auth

import (
	"testing"
	"time"
)

// TestComputeQuotaPressure_Scenarios 覆盖配额压力计算的典型场景：双窗口均值、
// absent 省略、models 回退、权威中性分、过期桶兜底与最紧桶选择。
func TestComputeQuotaPressure_Scenarios(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	// 1. 周 0.8, 5h 0.8 -> pressure = (0.2 + 0.2)/2 = 0.2
	w08, f08 := 0.8, 0.8
	snap1 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
					{Window: "5h", RemainingFraction: &f08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap1.ComputeQuotaPressure(now); p != 0.2 {
		t.Fatalf("expected pressure 0.2, got %v", p)
	}

	// 2. 周 0.8, 5h absent -> pressure = 0.2 (absent 省略)
	snap2 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap2.ComputeQuotaPressure(now); p != 0.2 {
		t.Fatalf("expected absent 5h omitted -> pressure 0.2, got %v", p)
	}

	// 3. groups 全无 -> 回退 models
	m06 := 0.6
	snap3 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Models: []AntigravityModelQuota{
			{ModelID: "gemini-2.5-pro", RemainingFraction: &m06, FractionPresent: true},
		},
	}
	if p := snap3.ComputeQuotaPressure(now); p != 0.4 {
		t.Fatalf("expected models fallback pressure 0.4, got %v", p)
	}

	// 4. Spec §7.6 示例 3：周 0.8 + 5h 证实存在但数值不可用 -> (0.2 + 0.5)/2 = 0.35
	//    （中性分 0.5 只给"权威清单证实存在"的窗口；存在性未知仍省略）
	snap4 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {SummaryIntegrity: true},
		},
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
					{Window: "5h", FractionPresent: false}, // 证实存在，数值不可用
				},
			},
		},
	}
	if p := snap4.ComputeQuotaPressure(now); p != 0.35 {
		t.Fatalf("expected Exists window to contribute neutral 0.5 -> pressure 0.35, got %v", p)
	}

	// 5. 桶过期（时效不可用）在权威清单下同样降级为中性 0.5，而不是静默省略
	snap5 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {SummaryIntegrity: true},
		},
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now.Add(-2 * time.Hour), TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap5.ComputeQuotaPressure(now); p != 0.5 {
		t.Fatalf("expected expired bucket to fall back to neutral 0.5, got %v", p)
	}

	// 6. 同窗口多桶取最小剩余（最紧桶决定压力，Spec §7.2）
	f05 := 0.5
	snap6 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &f05, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap6.ComputeQuotaPressure(now); p != 0.5 {
		t.Fatalf("expected tightest weekly bucket (0.5) -> pressure 0.5, got %v", p)
	}
}
