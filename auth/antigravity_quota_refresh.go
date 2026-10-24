package auth

import (
	"math"
	"time"
)

// ComputeBackoff 计算连续失败退避时长：min(I * 2^k, max(4I, 60min))。
// consecutiveFailures <= 0 时视为无失败，直接返回 interval。
// 采用整数翻倍循环而非 math.Pow：后者在 k 较大时（如 I=60min、k>=22）float64
// 乘积转 time.Duration 会 int64 溢出为负数，绕过封顶判断后调用方 now.Add(backoff)
// 得到过去时间，触发即时重试风暴。
func ComputeBackoff(interval time.Duration, consecutiveFailures int) time.Duration {
	capVal := 4 * interval
	if capVal < 60*time.Minute {
		capVal = 60 * time.Minute
	}
	if consecutiveFailures <= 0 {
		return interval
	}
	if interval <= 0 {
		return capVal
	}
	backoff := interval
	for i := 0; i < consecutiveFailures; i++ {
		if backoff >= capVal || backoff > math.MaxInt64/2 {
			return capVal
		}
		backoff *= 2
	}
	if backoff > capVal {
		return capVal
	}
	return backoff
}

// CheckCoverageWindow 判定 observedAt 是否落在 nominalDue 的名义覆盖窗
// (nominalDue - period, nominalDue] 内。
func CheckCoverageWindow(nominalDue time.Time, observedAt time.Time, period time.Duration) bool {
	windowStart := nominalDue.Add(-period)
	return observedAt.After(windowStart) && !observedAt.After(nominalDue)
}

// NominalSlotTime 按 Spec §5.3.2 公式计算名义槽位：
// T_slot(j,k) = T_epoch + k*I + ((j + 1/2) * Δ + θ) mod I，Δ = I/N。
// 每个名义时间由 (j,k) 独立计算，禁止累加舍入后的 Δ 造成漂移；θ 为单实例
// 周期共有初始相位，只整体旋转槽位，不破坏 Δ 均匀间距。N<=0 视为无资格
// 账号（调度器空闲），退化为 epoch + k*I。N=1 时 Δ=I，(0+0.5)*I + θ 对 I 取模
// 即半周期偏移，与公式一致，不做特判。
func NominalSlotTime(j, k int, interval time.Duration, n int, epoch time.Time, theta time.Duration) time.Time {
	if n <= 0 || interval <= 0 {
		return epoch.Add(time.Duration(k) * interval)
	}
	delta := interval / time.Duration(n)
	// (j + 1/2) * Δ 以整数运算展开（j*Δ + Δ/2），避免 time.Duration(0.5) 截断为 0，
	// 同时保证相邻槽位间距严格等于 Δ、不随 j 累加漂移。
	offset := time.Duration(j)*delta + delta/2 + theta
	offset %= interval
	if offset < 0 {
		offset += interval
	}
	return epoch.Add(time.Duration(k)*interval + offset)
}
