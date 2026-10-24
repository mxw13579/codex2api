package auth

import (
	"testing"
	"time"
)

// TestComputeBackoff 覆盖连续失败指数退避公式、封顶值计算及大失败次数的溢出防护。
func TestComputeBackoff(t *testing.T) {
	// I=60min: k=1 -> 120m (2h); k=2 -> 240m (4h); 封顶 max(4I, 60m)=240m
	i60 := 60 * time.Minute
	if b1 := ComputeBackoff(i60, 1); b1 != 2*time.Hour {
		t.Fatalf("expected k=1 backoff 2h, got %v", b1)
	}
	if b2 := ComputeBackoff(i60, 2); b2 != 4*time.Hour {
		t.Fatalf("expected k=2 backoff 4h, got %v", b2)
	}
	if b3 := ComputeBackoff(i60, 3); b3 != 4*time.Hour {
		t.Fatalf("expected k=3 capped backoff 4h, got %v", b3)
	}

	// I=3min: max(4I, 60m)=60m. k=1 -> 6m, k=2 -> 12m, k=3 -> 24m, k=4 -> 48m, k=5 -> 60m 封顶
	i3 := 3 * time.Minute
	if b1 := ComputeBackoff(i3, 1); b1 != 6*time.Minute {
		t.Fatalf("expected k=1 backoff 6m, got %v", b1)
	}
	if b5 := ComputeBackoff(i3, 5); b5 != 60*time.Minute {
		t.Fatalf("expected k=5 capped backoff 60m, got %v", b5)
	}

	// 大 k 溢出防护：float64(2^k) 转 time.Duration 会 int64 溢出为负数，
	// 必须始终被封顶在 max(4I, 60min)，且绝不返回负值。
	if b := ComputeBackoff(i60, 22); b != 4*time.Hour {
		t.Fatalf("ComputeBackoff(60m, 22) = %v, want 4h", b)
	}
	if b := ComputeBackoff(i3, 26); b != 60*time.Minute {
		t.Fatalf("ComputeBackoff(3m, 26) = %v, want 60m", b)
	}
	if b := ComputeBackoff(i60, 1024); b != 4*time.Hour {
		t.Fatalf("ComputeBackoff(60m, 1024) = %v, want 4h", b)
	}
	if b := ComputeBackoff(-1, 5); b < 0 {
		t.Fatalf("ComputeBackoff(-1, 5) = %v, want non-negative", b)
	}
	// 非法 interval 且无失败记录时不放大退避（返回原始 interval 语义由调用方兜底）
	if b := ComputeBackoff(-1, 0); b > 0 {
		t.Fatalf("ComputeBackoff(-1, 0) = %v, want <= 0 (no failure)", b)
	}
}

// TestCheckCoverageWindow 覆盖名义槽位覆盖窗 (nominalDue - period, nominalDue] 的半开半闭区间判定。
func TestCheckCoverageWindow(t *testing.T) {
	due := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	period := 60 * time.Minute

	// 11:30 在 (11:00, 12:00] 内 -> 消费
	if !CheckCoverageWindow(due, due.Add(-30*time.Minute), period) {
		t.Fatal("expected 11:30 to be within coverage window of 12:00")
	}
	// 10:59 在 11:00 前 -> 不在覆盖窗
	if CheckCoverageWindow(due, due.Add(-61*time.Minute), period) {
		t.Fatal("expected 10:59 to be outside coverage window")
	}
	// 12:01 超过 due -> 不在名义覆盖窗
	if CheckCoverageWindow(due, due.Add(1*time.Minute), period) {
		t.Fatal("expected 12:01 to be outside coverage window")
	}
}

// TestNominalSlotTime_UniformSpacingAndSharedTheta 覆盖名义槽位的均匀分布计算与共有相位旋转等距性。
func TestNominalSlotTime_UniformSpacingAndSharedTheta(t *testing.T) {
	epoch := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	interval := 60 * time.Minute

	// 1. N=4、theta=0：j=0..3 的槽位在全周期内均匀等距（(j+0.5)*Δ，Δ=I/N=15m）
	//    即 7.5m/22.5m/37.5m/52.5m。
	wantOffsets := []time.Duration{450 * time.Second, 1350 * time.Second, 2250 * time.Second, 3150 * time.Second}
	for j, want := range wantOffsets {
		got := NominalSlotTime(j, 0, interval, 4, epoch, 0)
		if got.Sub(epoch) != want {
			t.Fatalf("N=4 j=%d offset = %v, want %v", j, got.Sub(epoch), want)
		}
	}

	// 2. theta 只整体旋转全部槽位，绝不破坏槽位间 Δ 的均匀恒定间距（Spec §5.3.2）；
	//    mod 周期边界后 j=3 与下一周期 j=0 相接，循环序列仍然等距。
	theta := 7 * time.Minute
	slot := func(j, k int) time.Time { return NominalSlotTime(j, k, interval, 4, epoch, theta) }
	for j := 1; j < 4; j++ {
		if gap := slot(j, 0).Sub(slot(j-1, 0)); gap != 15*time.Minute {
			t.Fatalf("theta must not break uniform spacing, j=%d gap=%v", j, gap)
		}
	}
	if gap := slot(0, 1).Sub(slot(3, 0)); gap != 15*time.Minute {
		t.Fatalf("cyclic spacing across period boundary = %v, want 15m", gap)
	}
	if base0 := slot(0, 0); base0.Sub(NominalSlotTime(0, 0, interval, 4, epoch, 0)) != theta {
		t.Fatal("shared theta must rotate all slots by a constant offset")
	}

	// 3. N=1：Δ=I，槽位仍按公式带 (j+0.5)Δ 偏移（不得退化到无偏移的 epoch+k*I）
	if got := NominalSlotTime(0, 0, interval, 1, epoch, 0); got.Sub(epoch) != 30*time.Minute {
		t.Fatalf("N=1 j=0 offset = %v, want 30m", got.Sub(epoch))
	}

	// 4. 周期推进：T_slot(j,k+1) - T_slot(j,k) == I
	if got := NominalSlotTime(2, 1, interval, 4, epoch, 0).Sub(NominalSlotTime(2, 0, interval, 4, epoch, 0)); got != interval {
		t.Fatalf("period advance = %v, want %v", got, interval)
	}
}
