package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/codex2api/internal/version"
	"github.com/gin-gonic/gin"
)

// healthStoreLockStallThreshold 是 /health 判定账号池疑似死锁的门槛:
// 单次 TryRLock 失败只是正常锁竞争,不能作为重启依据;但连续这么久一次
// 都拿不到读锁,说明有写锁永久持有,实例已无法服务任何请求。此时 /health
// 返回 503,让容器编排的 healthcheck 重启实例——恢复旧版同步 /health
// 死锁时挂起超时带来的自愈能力,同时不误伤高负载下的瞬时竞争。
const healthStoreLockStallThreshold = 30 * time.Second

// healthLockProbe 跟踪 /health 连续拿不到账号池读锁的持续时长。
// 任何一次成功探测都会清零;只有从首次失败起一路无成功才会累计。
type healthLockProbe struct {
	mu           sync.Mutex
	blockedSince time.Time
}

// observe 记录一次探测结果,返回当前连续失败的持续时长(成功时为 0)。
func (p *healthLockProbe) observe(lockAcquired bool, now time.Time) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if lockAcquired {
		p.blockedSince = time.Time{}
		return 0
	}
	if p.blockedSince.IsZero() {
		p.blockedSince = now
	}
	return now.Sub(p.blockedSince)
}

// newHealthHandler 构造 /health:只做非阻塞的尽力统计,避免账号热路径锁竞争拖死 liveness。
// drain_mode_supported/draining 供蓝绿发布脚本判断能否安全发送排空信号。
func newHealthHandler(counts func() (available int, total int, complete bool), probe *healthLockProbe, drain *drainController) gin.HandlerFunc {
	return func(c *gin.Context) {
		available, total, countsComplete := counts()
		blocked := probe.observe(total >= 0, time.Now())
		draining := drain.Draining()
		if blocked >= healthStoreLockStallThreshold {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":               "unavailable",
				"reason":               "account store lock stalled",
				"blocked_seconds":      int(blocked / time.Second),
				"available":            available,
				"total":                total,
				"counts_complete":      countsComplete,
				"drain_mode_supported": drainModeSignalsSupported,
				"draining":             draining,
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"status":               "ok",
			"build_version":        version.Current(),
			"available":            available,
			"total":                total,
			"counts_complete":      countsComplete,
			"drain_mode_supported": drainModeSignalsSupported,
			"draining":             draining,
		})
	}
}
