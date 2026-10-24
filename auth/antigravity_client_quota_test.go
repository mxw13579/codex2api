package auth

import (
	"math"
	"testing"
	"time"
)

// TestNormalizeAntigravityWindow 覆盖窗口名规整：白名单映射与非法窗口过滤。
func TestNormalizeAntigravityWindow(t *testing.T) {
	cases := []struct {
		input string
		want  string
		ok    bool
	}{
		{"5h", "5h", true},
		{"FIVE_HOUR", "5h", true},
		{" 5_hour ", "5h", true},
		{"fivehour", "5h", true},
		{"weekly", "weekly", true},
		{"WEEK", "weekly", true},
		{"7d", "weekly", true},
		{"seven_day", "weekly", true},
		{"daily", "", false},
		{"monthly", "", false},
		{"unknown_window", "", false},
	}
	for _, tc := range cases {
		got, ok := NormalizeAntigravityWindow(tc.input)
		if ok != tc.ok || got != tc.want {
			t.Errorf("NormalizeAntigravityWindow(%q) = (%q, %v), want (%q, %v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

// TestValidateResetTime 覆盖配额重置时间的解析、严格未来判定与窗口上限检查。
func TestValidateResetTime(t *testing.T) {
	observed := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	// 合法 5h
	valid5h := observed.Add(4 * time.Hour).Format(time.RFC3339)
	if rt, ok := ValidateResetTime(valid5h, observed, "5h"); !ok || rt.IsZero() {
		t.Fatalf("expected valid 5h reset time, got ok=%v", ok)
	}
	// 越界 5h (>6h)
	invalid5h := observed.Add(8 * time.Hour).Format(time.RFC3339)
	if _, ok := ValidateResetTime(invalid5h, observed, "5h"); ok {
		t.Fatal("expected 8h reset time to fail for 5h window")
	}
	// 早于 observed
	past := observed.Add(-1 * time.Hour).Format(time.RFC3339)
	if _, ok := ValidateResetTime(past, observed, "weekly"); ok {
		t.Fatal("expected past reset time to fail")
	}
}

// TestClassifyAntigravityFraction_FourStates 覆盖剩余额度四态分类：合法正数、归零、未知及越界值处理。
func TestClassifyAntigravityFraction_FourStates(t *testing.T) {
	tiny, zero, neg, over := 0.001, 0.0, -0.5, 1.5
	nan := math.NaN()
	inf := math.Inf(1)
	cases := []struct {
		name    string
		value   *float64
		present bool
		want    string
	}{
		{"tiny positive", &tiny, true, "known_positive"},
		{"exact one", &over, false, "unknown"}, // present=false 永远 unknown
		{"zero with presence", &zero, true, "known_zero"},
		{"missing pointer", nil, true, "unknown"},
		{"absent flag", &zero, false, "unknown"},
		{"negative", &neg, true, "unknown"},
		{"over one", &over, true, "unknown"},
		{"nan", &nan, true, "unknown"},
		{"inf", &inf, true, "unknown"},
	}
	for _, tc := range cases {
		if got := ClassifyAntigravityFraction(tc.value, tc.present); got != tc.want {
			t.Errorf("%s: ClassifyAntigravityFraction = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestApplyQuotaGroupsV2_BadBucketIsolationAndMerge 覆盖 V2 快照合并逻辑：空响应保序、损坏桶隔离与权威整体替换。
func TestApplyQuotaGroupsV2_BadBucketIsolationAndMerge(t *testing.T) {
	oldZero := 0.0
	prevObserved := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	prev := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{{
			DisplayName: "Weekly",
			Buckets: []AntigravityQuotaBucket{{
				BucketID: "w1", Window: "weekly", RemainingFraction: &oldZero, FractionPresent: true,
				ObservedAt: prevObserved, ObservationSeq: 7, TTLSeconds: 3600,
			}},
		}},
	}
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	findBucket := func(snap AntigravityQuotaSnapshot, window string) (AntigravityQuotaBucket, bool) {
		for _, g := range snap.Groups {
			for _, b := range g.Buckets {
				if b.Window == window {
					return b, true
				}
			}
		}
		return AntigravityQuotaBucket{}, false
	}

	// 1. 空/nil groups 不是"窗口不存在"的证据：保留历史快照且不推进成功时标。
	next, advanced := ApplyQuotaGroupsV2(prev, nil, false, now, 3600, 8)
	if advanced {
		t.Fatal("empty response must not advance success")
	}
	if b, ok := findBucket(next, "weekly"); !ok || b.ObservationSeq != 7 || !b.ObservedAt.Equal(prevObserved) {
		t.Fatalf("empty response must retain previous bucket unchanged, got %+v", b)
	}

	// 2. 部分清单中仅返回坏桶：坏桶覆盖为 unknown（绝不复活旧零），未返回旧桶保留原时标。
	incoming := []AntigravityQuotaGroup{{
		DisplayName: "FiveHour",
		Buckets:     []AntigravityQuotaBucket{{BucketID: "h1", Window: "5h", RemainingFraction: nil, FractionPresent: false}},
	}}
	next, advanced = ApplyQuotaGroupsV2(prev, incoming, false, now, 3600, 8)
	if !advanced {
		t.Fatal("non-empty response must advance")
	}
	bad, ok := findBucket(next, "5h")
	if !ok || bad.FractionPresent || bad.RemainingFraction != nil || bad.ObservationSeq != 8 {
		t.Fatalf("bad bucket must be isolated as unknown with new seq, got %+v", bad)
	}
	kept, ok := findBucket(next, "weekly")
	if !ok || kept.ObservationSeq != 7 || !kept.ObservedAt.Equal(prevObserved) {
		t.Fatalf("unreturned previous bucket must keep original timestamps, got %+v", kept)
	}

	// 3. 完整权威清单（summaryIntegrity=true）才允许整体替换未返回桶。
	next, _ = ApplyQuotaGroupsV2(prev, incoming, true, now, 3600, 9)
	if _, ok := findBucket(next, "weekly"); ok {
		t.Fatal("authoritative full list must replace buckets absent from the response")
	}
}
