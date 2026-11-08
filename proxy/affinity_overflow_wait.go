package proxy

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	defaultAffinityOverflowWaitMinBytes = 32 << 10
	maxAffinityOverflowWait             = 10 * time.Second
)

// affinityOverflowWaitForRequest 返回会话绑定账号并发满时，借号前等待绑定账号的时长。
//
//   - CODEX_AFFINITY_OVERFLOW_WAIT_MS：等待毫秒数，未设置或 0 表示不等（原行为：立即借号）。
//   - CODEX_AFFINITY_OVERFLOW_WAIT_MIN_BYTES：请求体小于该字节数时不等，默认 32KiB。
//     小请求可缓存的前缀少，借号损失有限，不值得增加首字延迟。
//
// 未声明 Content-Length 的请求按大请求处理。WebSocket 入站的升级请求没有请求体，不等待。
func affinityOverflowWaitForRequest(c *gin.Context) time.Duration {
	ms := envNonNegativeInt64("CODEX_AFFINITY_OVERFLOW_WAIT_MS", 0)
	if ms <= 0 || c == nil || c.Request == nil {
		return 0
	}
	wait := time.Duration(ms) * time.Millisecond
	if wait > maxAffinityOverflowWait {
		wait = maxAffinityOverflowWait
	}
	minBytes := envNonNegativeInt64("CODEX_AFFINITY_OVERFLOW_WAIT_MIN_BYTES", defaultAffinityOverflowWaitMinBytes)
	if length := c.Request.ContentLength; length >= 0 && length < minBytes {
		return 0
	}
	return wait
}

func envNonNegativeInt64(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}
