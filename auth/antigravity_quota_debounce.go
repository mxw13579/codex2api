package auth

import (
	"sync"
	"time"
)

// RescheduleDebouncer 实现 Spec §7.5.1 的计划重排防抖（quiet_window=100ms，
// max_wait=1s）。实现要点：
//   - action 一律在 d.mu 之外同步执行：max_wait 到点的 Trigger 在调用栈内结算，
//     测试与调用方在 Trigger 返回后即可观察副作用，不依赖 goroutine 调度；
//   - 每轮 timer 配对一个 stopCh，被合并/停止/Stop 时 close，等待 goroutine
//     经 select 退出，绝不泄漏（原草案 `go func(){ <-t.C() }` 在 Stop 后永久
//     阻塞，属 goroutine 泄漏，已修正）；
//   - timer 回调拿锁后用 consumeLocked(fired) 复核 fired 仍是当前 timer，
//     与并发 Trigger 的合并语义互斥，保证一轮事件恰好执行一次。
type RescheduleDebouncer struct {
	clk         Clock
	quietWindow time.Duration
	maxWait     time.Duration
	action      func()

	mu         sync.Mutex
	timer      Timer
	timerStop  chan struct{}
	firstEvent time.Time
	pending    bool
	stopped    bool
}

// NewRescheduleDebouncer 创建防抖器。clk 为 nil 时回退到 DefaultClock。
func NewRescheduleDebouncer(clk Clock, quiet, max time.Duration, action func()) *RescheduleDebouncer {
	if clk == nil {
		clk = DefaultClock
	}
	return &RescheduleDebouncer{
		clk:         clk,
		quietWindow: quiet,
		maxWait:     max,
		action:      action,
	}
}

// Trigger 记录一次变更事件。账号禁用/删除等必须立即生效的事件不经过本防抖器，
// 由调用方直接执行重排（Spec §7.5.1）。
func (d *RescheduleDebouncer) Trigger() {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	now := d.clk.Now()
	if !d.pending {
		d.pending = true
		d.firstEvent = now
	}

	timeRemaining := d.maxWait - now.Sub(d.firstEvent)
	delay := d.quietWindow
	if timeRemaining < delay {
		delay = timeRemaining
	}
	if delay <= 0 {
		// max_wait 已过：锁内结算，锁外同步执行，调用返回后副作用可见。
		action := d.consumeLocked(nil)
		d.mu.Unlock()
		if action != nil {
			action()
		}
		return
	}

	if d.timerStop != nil {
		close(d.timerStop)
		d.timerStop = nil
	}
	if d.timer != nil {
		d.timer.Stop()
	}
	t := d.clk.NewTimer(delay)
	stopCh := make(chan struct{})
	d.timer = t
	d.timerStop = stopCh
	d.mu.Unlock()

	go func(t Timer, stopCh <-chan struct{}) {
		select {
		case <-t.C():
			d.mu.Lock()
			action := d.consumeLocked(t)
			d.mu.Unlock()
			if action != nil {
				action()
			}
		case <-stopCh:
			// 本轮 timer 已被更新的 Trigger 或 Stop 合并/终止，goroutine 退出。
		}
	}(t, stopCh)
}

// consumeLocked 在持锁状态下结算当前 pending 轮次并返回待执行回调。
// fired 非 nil 时要求它仍是当前 timer，否则说明该轮已被更新的 Trigger 合并。
// 返回 nil 表示无轮次可结算（幂等，重复回调不重复执行）。
func (d *RescheduleDebouncer) consumeLocked(fired Timer) func() {
	if !d.pending {
		return nil
	}
	if fired != nil && fired != d.timer {
		return nil
	}
	d.pending = false
	d.timer = nil
	if d.timerStop != nil {
		close(d.timerStop)
		d.timerStop = nil
	}
	return d.action
}

// Stop 终止防抖器：丢弃 pending 轮次、停止 timer 并释放等待 goroutine。
// 不能只靠 timer.Stop 防竞态（Spec §7.5.3），stopped 标志使后续 Trigger 直接返回。
func (d *RescheduleDebouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopped = true
	if d.timerStop != nil {
		close(d.timerStop)
		d.timerStop = nil
	}
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.pending = false
}
