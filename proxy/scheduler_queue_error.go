package proxy

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/gin-gonic/gin"
)

const schedulerQueueFullMessage = "Scheduler wait queue is full. Retry after 1 second."
const schedulerSelectionTimeoutMessage = "Account selection timed out. Retry after 1 second."

const (
	// quotaGateReasonPool 是「候选集合全部因 Antigravity 配额硬门禁被阻断」的对外 reason。
	quotaGateReasonPool = "quota_gate"
	// quotaGateReasonBound 是续链（preserveBinding=true）绑定账号被阻断的对外 reason，
	// 与整池耗尽、容量类 503、上游真实 429 在日志与响应体中三者可区分（Spec §6.2 #2）。
	quotaGateReasonBound = "quota_gate_bound"
	// quotaGateDefaultRetryAfterSeconds / quotaGateMaxRetryAfterSeconds 是 Retry-After
	// 的缺省与上下限（Spec §6.2 #2）。
	quotaGateDefaultRetryAfterSeconds = 60
	quotaGateMinRetryAfterSeconds     = 1
	quotaGateMaxRetryAfterSeconds     = 3600
)

func schedulerQueueFullAPIError() *api.APIError {
	return api.NewAPIError(api.ErrCodeServiceUnavailable, schedulerQueueFullMessage, api.ErrorTypeServer)
}

// schedulerQuotaExhaustedMessage 根据阻断原因生成标准化的错误提示文本。
func schedulerQuotaExhaustedMessage(reason string) string {
	return "Antigravity account quota exhausted (" + reason + ")"
}

// schedulerQuotaRetryAfterSeconds 把门禁的 ResetAfter 折算成秒：向上取整保证至少 1s，
// 缺省 60s、上限 3600s（Spec §6.2 #2）。
func schedulerQuotaRetryAfterSeconds(retryAfter time.Duration) int {
	if retryAfter <= 0 {
		return quotaGateDefaultRetryAfterSeconds
	}
	seconds := int(math.Ceil(retryAfter.Seconds()))
	if seconds < quotaGateMinRetryAfterSeconds {
		return quotaGateMinRetryAfterSeconds
	}
	if seconds > quotaGateMaxRetryAfterSeconds {
		return quotaGateMaxRetryAfterSeconds
	}
	return seconds
}

// schedulerQuotaExhaustion 从选号错误提取 429 契约要素（Retry-After 秒数与 reason）。
// reason 只来自类型化错误本身，绝不通过重新求值候选集合推断（Spec §6.2 #2）。
func schedulerQuotaExhaustion(err error) (retryAfterSeconds int, reason string, ok bool) {
	if err == nil || !errors.Is(err, auth.ErrQuotaExhausted) {
		return 0, "", false
	}
	retryAfterSeconds = quotaGateDefaultRetryAfterSeconds
	reason = quotaGateReasonPool
	var quotaErr *auth.QuotaExhaustedError
	if errors.As(err, &quotaErr) {
		reason = quotaGateReasonPool
		if quotaErr.Bound {
			reason = quotaGateReasonBound
		}
		retryAfterSeconds = schedulerQuotaRetryAfterSeconds(quotaErr.RetryAfter)
	}
	return retryAfterSeconds, reason, true
}

func schedulerSelectionAPIError(err error) *api.APIError {
	switch {
	case errors.Is(err, auth.ErrQuotaExhausted):
		_, reason, _ := schedulerQuotaExhaustion(err)
		return api.NewAPIError(api.ErrCodeRateLimitReached, schedulerQuotaExhaustedMessage(reason), api.ErrorTypeRateLimit)
	case errors.Is(err, auth.ErrSchedulerQueueFull):
		return schedulerQueueFullAPIError()
	case errors.Is(err, context.DeadlineExceeded):
		return api.NewAPIError(api.ErrCodeServiceUnavailable, schedulerSelectionTimeoutMessage, api.ErrorTypeServer)
	default:
		return nil
	}
}

// continuousRetryProtocolOf 从请求路径推断对外协议，只服务于没有显式 protocol 参数的
// 出口（native countTokens 与独立的配额 429 出口）。它只判定响应信封，不参与任何
// 配额结论的求值。
func continuousRetryProtocolOf(c *gin.Context) continuousRetryHTTPProtocol {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return continuousRetryProtocolResponses
	}
	path := c.Request.URL.Path
	switch {
	case strings.HasPrefix(path, "/v1beta/"), strings.HasPrefix(path, "/v1alpha/"):
		return continuousRetryProtocolGemini
	case strings.HasSuffix(path, "/messages"), strings.HasSuffix(path, "/count_tokens"):
		return continuousRetryProtocolAnthropic
	case strings.HasSuffix(path, "/chat/completions"):
		return continuousRetryProtocolChat
	default:
		return continuousRetryProtocolResponses
	}
}

// writeSchedulerQuotaExhaustedError 输出 429 + Retry-After + reason。
// reason 取 "quota_gate"（整池耗尽）或 "quota_gate_bound"（续链保留绑定），
// 与容量类 503、上游真实 429（reason=upstream_429）在日志与响应中可区分。
func writeSchedulerQuotaExhaustedError(c *gin.Context, retryAfterSeconds int, reason string) {
	writeSchedulerQuotaExhaustedErrorForProtocol(c, retryAfterSeconds, reason, continuousRetryProtocolOf(c))
}

// writeSchedulerQuotaExhaustedErrorForProtocol 按指定协议信封格式输出 429 配额阻断错误。
func writeSchedulerQuotaExhaustedErrorForProtocol(c *gin.Context, retryAfterSeconds int, reason string, protocol continuousRetryHTTPProtocol) {
	if c == nil {
		return
	}
	if reason == "" {
		reason = quotaGateReasonPool
	}
	if retryAfterSeconds < quotaGateMinRetryAfterSeconds {
		retryAfterSeconds = quotaGateDefaultRetryAfterSeconds
	}
	if retryAfterSeconds > quotaGateMaxRetryAfterSeconds {
		retryAfterSeconds = quotaGateMaxRetryAfterSeconds
	}
	if !c.Writer.Written() {
		c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	message := schedulerQuotaExhaustedMessage(reason)
	switch protocol {
	case continuousRetryProtocolAnthropic:
		sendAnthropicError(c, http.StatusTooManyRequests, "rate_limit_error", message)
	case continuousRetryProtocolChat:
		c.JSON(http.StatusTooManyRequests, gin.H{"error": gin.H{
			"code":    http.StatusTooManyRequests,
			"type":    quotaGateReasonPool,
			"reason":  reason,
			"message": message,
		}})
	case continuousRetryProtocolGemini:
		writeGeminiNativeError(c, http.StatusTooManyRequests, message)
	default:
		c.JSON(http.StatusTooManyRequests, gin.H{"error": gin.H{
			"code":    http.StatusTooManyRequests,
			"type":    quotaGateReasonPool,
			"reason":  reason,
			"message": message,
		}})
	}
}

// Queue overload and selection timeout are local retryable capacity errors.
// Neither should start another quota scan or overwrite committed SSE.
func writeSchedulerQueueError(c *gin.Context, err error, protocol continuousRetryHTTPProtocol) bool {
	apiErr := schedulerSelectionAPIError(err)
	if apiErr == nil {
		return false
	}
	if !claimContinuousRetryTerminal(c, protocol) || c.Request.Context().Err() != nil {
		return true
	}
	// 配额硬门禁是 429 + 真实 Retry-After，与容量类 503（Retry-After 1s）分开出口，
	// 下游据此退避、也不会把配额耗尽当成网关故障（Spec §6.2 #2）。
	if retryAfterSeconds, reason, ok := schedulerQuotaExhaustion(err); ok {
		writeSchedulerQuotaExhaustedErrorForProtocol(c, retryAfterSeconds, reason, protocol)
		return true
	}
	if !c.Writer.Written() {
		c.Header("Retry-After", "1")
	}
	message := apiErr.Message
	switch protocol {
	case continuousRetryProtocolGemini:
		writeGeminiNativeError(c, http.StatusServiceUnavailable, message)
	case continuousRetryProtocolAnthropic:
		if !writeCommittedAnthropicRetryError(c, "overloaded_error", message) {
			sendAnthropicError(c, http.StatusServiceUnavailable, "overloaded_error", message)
		}
	case continuousRetryProtocolChat:
		if !writeCommittedChatRetryError(c, message) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": apiErr})
		}
	default:
		if !writeCommittedResponsesRetryError(c, message) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": apiErr})
		}
	}
	return true
}
