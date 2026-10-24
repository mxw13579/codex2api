package auth

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestRescheduleDebouncer_CoalescesEvents 断言静默窗内连续事件正确合并并在 max_wait 强制结算。
func TestRescheduleDebouncer_CoalescesEvents(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	var runCount int32
	debouncer := NewRescheduleDebouncer(clk, 100*time.Millisecond, 1*time.Second, func() {
		atomic.AddInt32(&runCount, 1)
	})

	// 1. 触发一次，未满 100ms 不执行（deadline 未到，无任何异步路径可触发）
	debouncer.Trigger()
	clk.Advance(50 * time.Millisecond)
	if atomic.LoadInt32(&runCount) != 0 {
		t.Fatal("debouncer executed before quiet window")
	}

	// 2. 持续频繁触发，保持 quiet 窗不断刷新，直到 max_wait (1s) 强制执行。
	// max_wait 到点后 Trigger 在调用栈内同步结算（不依赖 timer goroutine 调度），
	// 因此本断言是确定性的：无论 timer 回调 goroutine 与 Trigger#13 谁先拿锁，
	// 都恰好执行一次（consumeLocked 的 fired != d.timer 合并保护兜底）。
	for i := 0; i < 15; i++ {
		debouncer.Trigger()
		clk.Advance(80 * time.Millisecond)
	}
	if atomic.LoadInt32(&runCount) != 1 {
		t.Fatalf("expected max_wait to trigger exactly 1 execution, got %d", atomic.LoadInt32(&runCount))
	}
	debouncer.Stop()
}

// TestRescheduleDebouncer_StopPreventsExecution 断言防抖器 Stop 后放弃未决事件且不执行回调。
func TestRescheduleDebouncer_StopPreventsExecution(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	var runCount int32
	debouncer := NewRescheduleDebouncer(clk, 100*time.Millisecond, 1*time.Second, func() {
		atomic.AddInt32(&runCount, 1)
	})

	debouncer.Trigger()
	debouncer.Stop()
	clk.Advance(2 * time.Second)
	if atomic.LoadInt32(&runCount) != 0 {
		t.Fatal("stopped debouncer must not execute")
	}
}
