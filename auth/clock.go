package auth

import (
	"sync"
	"time"
)

// Timer 是可注入的定时器抽象，语义对齐 time.Timer 的常用子集。
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Clock 是可注入的时钟抽象，生产路径使用 RealClock，测试路径使用 FakeClock。
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	Since(t time.Time) time.Duration
	Until(t time.Time) time.Duration
	NewTimer(d time.Duration) Timer
}

// RealClock 是基于 time 包的真实时钟实现。
type RealClock struct{}

// Now 返回真实时钟的当前本地时间。
func (RealClock) Now() time.Time { return time.Now() }

// Sleep 挂起当前协程指定时长。
func (RealClock) Sleep(d time.Duration) { time.Sleep(d) }

// Since 返回指定时刻距今流逝的时长。
func (RealClock) Since(t time.Time) time.Duration { return time.Since(t) }

// Until 返回当前时刻距目标时间尚需的时长。
func (RealClock) Until(t time.Time) time.Duration { return time.Until(t) }

// NewTimer 创建并返回基于真实时钟的 Timer 适配器实例。
func (RealClock) NewTimer(d time.Duration) Timer { return &realTimer{Timer: time.NewTimer(d)} }

type realTimer struct {
	*time.Timer
}

// C 返回真实定时器的到期通道。
func (r *realTimer) C() <-chan time.Time {
	return r.Timer.C
}

// DefaultClock 是全局默认时钟，测试中可替换为 FakeClock。
var DefaultClock Clock = RealClock{}

// fakeTimer 是 FakeClock 内部的定时器实现，由 FakeClock.mu 保护。
type fakeTimer struct {
	clk      *FakeClock
	ch       chan time.Time
	deadline time.Time
	active   bool
}

// C 返回假定时器的触发事件通道。
func (ft *fakeTimer) C() <-chan time.Time {
	return ft.ch
}

// Stop 停止假定时器并报告此前是否处于活跃状态。
func (ft *fakeTimer) Stop() bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()
	wasActive := ft.active
	ft.active = false
	return wasActive
}

// Reset 重置假定时器的到期时刻并重新激活。
func (ft *fakeTimer) Reset(d time.Duration) bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()
	wasActive := ft.active
	ft.deadline = ft.clk.now.Add(d)
	ft.active = true
	return wasActive
}

// FakeClock 是确定性测试时钟：时间仅由 Advance 显式推进。
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock 创建以 initial 为起点的假时钟。
func NewFakeClock(initial time.Time) *FakeClock {
	return &FakeClock{now: initial}
}

// Now 返回假时钟记录的当前模拟时刻。
func (fc *FakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.now
}

// Sleep 在假时钟上通过 Advance 步进指定时间。
func (fc *FakeClock) Sleep(d time.Duration) {
	fc.Advance(d)
}

// Since 计算目标时间相对于假时钟当前时刻的时长差。
func (fc *FakeClock) Since(t time.Time) time.Duration {
	return fc.Now().Sub(t)
}

// Until 计算假时钟当前时刻到目标时间的距离。
func (fc *FakeClock) Until(t time.Time) time.Duration {
	return t.Sub(fc.Now())
}

// NewTimer 在假时钟上注册一个新的模拟定时器。
func (fc *FakeClock) NewTimer(d time.Duration) Timer {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	ft := &fakeTimer{
		clk:      fc,
		ch:       make(chan time.Time, 1),
		deadline: fc.now.Add(d),
		active:   true,
	}
	fc.timers = append(fc.timers, ft)
	return ft
}

// Advance 将假时钟时间向前推进 d，并唤醒所有到达 deadline 的活跃定时器。
func (fc *FakeClock) Advance(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = fc.now.Add(d)
	for _, ft := range fc.timers {
		if ft.active && !fc.now.Before(ft.deadline) {
			ft.active = false
			select {
			case ft.ch <- ft.deadline:
			default:
			}
		}
	}
}
