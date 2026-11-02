package proxy

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

// External scheduler mode (issue #831): an API key with
// Limits.ExternalScheduler hands account choice, failover and retries to an
// upstream scheduler. Each execution request names one account; the gateway
// runs it there or fails, and never consults its own scheduling gates.
const (
	// PinnedAccountHeader names the account an external scheduler key runs a
	// request on. The gateway echoes it on responses that reached the account.
	PinnedAccountHeader = "X-Codex2API-Account-ID"
	// pinnedUpstreamStatusHeader carries the upstream status of a failed attempt
	// before protocol-specific rewrites (for example 401 -> 503 pool errors).
	pinnedUpstreamStatusHeader = "X-Codex2API-Upstream-Status"
	// pinnedAccountErrorHeader carries the machine-readable reason a pinned
	// request was refused before or instead of an upstream attempt.
	pinnedAccountErrorHeader = "X-Codex2API-Account-Error"

	contextPinnedAccountID = "pinnedAccountID"

	// maxPinnedAccountAcquisitions bounds same-account re-runs inside one
	// request, such as a credential refresh after an upstream 401. They all run
	// on the pinned account; none of them switches accounts.
	maxPinnedAccountAcquisitions = 3
)

// modelRateLimitGate selects whether request filters also drop accounts whose
// requested model is under an upstream-observed rate limit. Pinned requests
// evaluate capability only and leave that call to the external scheduler.
type modelRateLimitGate bool

const (
	enforceModelRateLimits modelRateLimitGate = true
	ignoreModelRateLimits  modelRateLimitGate = false
)

func (g modelRateLimitGate) blocks(account *auth.Account, model string) bool {
	return bool(g) && model != "" && account.IsModelRateLimited(model)
}

func (g modelRateLimitGate) blocksAntigravity(account *auth.Account, model, wireModel string) bool {
	return bool(g) && antigravityAccountModelRateLimited(account, model, wireModel)
}

func modelRateLimitGateFor(pin *pinnedAccountDispatch) modelRateLimitGate {
	if pin != nil {
		return ignoreModelRateLimits
	}
	return enforceModelRateLimits
}

type externalSchedulerRoute int

const (
	externalSchedulerRouteUnsupported externalSchedulerRoute = iota
	// externalSchedulerRouteNoInference serves catalogs or local estimates and
	// never runs a model on an account, so it needs no pinned account.
	externalSchedulerRouteNoInference
	// externalSchedulerRoutePinned runs on the account in PinnedAccountHeader.
	externalSchedulerRoutePinned
)

// classifyExternalSchedulerRoute matches gin route patterns. Every execution
// route without pinned-account support is refused rather than silently
// scheduled by the gateway.
func classifyExternalSchedulerRoute(method, route string) externalSchedulerRoute {
	switch method {
	case http.MethodPost:
		switch route {
		case "/v1/responses", "/responses", "/backend-api/codex/responses",
			"/v1/chat/completions", "/chat/completions",
			"/v1/messages", "/messages":
			return externalSchedulerRoutePinned
		case "/v1/messages/count_tokens", "/messages/count_tokens",
			"/v1/responses/input_tokens", "/responses/input_tokens",
			"/v1/prompt-filter/newapi/verify":
			return externalSchedulerRouteNoInference
		}
	case http.MethodGet:
		switch route {
		case "/v1/models", "/models", "/backend-api/codex/models":
			return externalSchedulerRouteNoInference
		}
	}
	return externalSchedulerRouteUnsupported
}

// rejectExternalSchedulerIngress applies the external scheduler contract right
// after API key authentication. Other keys ignore PinnedAccountHeader. It
// returns true after writing a rejection.
func rejectExternalSchedulerIngress(c *gin.Context, row *database.APIKeyRow) bool {
	if c == nil || c.Request == nil || row == nil || !row.Limits.ExternalScheduler {
		return false
	}
	route := c.FullPath()
	switch classifyExternalSchedulerRoute(c.Request.Method, route) {
	case externalSchedulerRouteNoInference:
		return false
	case externalSchedulerRoutePinned:
		raw := strings.TrimSpace(c.GetHeader(PinnedAccountHeader))
		if raw == "" {
			writeExternalSchedulerIngressError(c, route, http.StatusBadRequest, "pinned_account_required",
				"This API key uses external scheduler mode: set "+PinnedAccountHeader+" to the account that should run the request")
			return true
		}
		id, ok := parsePinnedAccountID(raw)
		if !ok {
			writeExternalSchedulerIngressError(c, route, http.StatusBadRequest, "pinned_account_invalid",
				PinnedAccountHeader+" must be a positive decimal account ID")
			return true
		}
		c.Set(contextPinnedAccountID, id)
		return false
	}
	writeExternalSchedulerIngressError(c, route, http.StatusForbidden, "external_scheduler_endpoint_unsupported",
		"This endpoint does not support pinned accounts yet, so external scheduler API keys cannot use it")
	return true
}

func parsePinnedAccountID(raw string) (int64, bool) {
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func writeExternalSchedulerIngressError(c *gin.Context, route string, status int, code, message string) {
	c.Header(pinnedAccountErrorHeader, code)
	if route == "/v1/messages" || route == "/messages" {
		writeAnthropicCodedError(c, status, code, message)
		return
	}
	writeOpenAICodedError(c, status, code, message)
}

func pinnedAccountIDFromContext(c *gin.Context) (int64, bool) {
	if c == nil {
		return 0, false
	}
	value, exists := c.Get(contextPinnedAccountID)
	if !exists {
		return 0, false
	}
	id, ok := value.(int64)
	return id, ok && id > 0
}

// pinnedAccountDispatch replaces scheduler selection for one pinned request.
// The first acquisition reports why the account cannot run the request. Later
// attempts re-run on the same account only when the handler did not exclude
// it; an exclusion means the handler wanted to fail over, which is the
// external scheduler's decision.
type pinnedAccountDispatch struct {
	accountID    int64
	acquisitions int
	failure      string
	message      string
}

func pinnedAccountDispatchForRequest(c *gin.Context) *pinnedAccountDispatch {
	id, ok := pinnedAccountIDFromContext(c)
	if !ok {
		return nil
	}
	return &pinnedAccountDispatch{accountID: id}
}

const (
	pinnedFailureScopeBudget   = "scope_budget_exhausted"
	pinnedFailureAttemptFailed = "attempt_failed"
)

// next acquires the pinned account for one attempt. filter must hold only
// capability checks. A nil result ends the request; writeFailure reports why.
func (p *pinnedAccountDispatch) next(c *gin.Context, store *auth.Store, filter auth.AccountFilter, exclusions *retryAccountExclusions) *auth.Account {
	if p.acquisitions > 0 && (exclusions.ForSelection()[p.accountID] || p.acquisitions >= maxPinnedAccountAcquisitions) {
		p.failure = pinnedFailureAttemptFailed
		return nil
	}
	p.acquisitions++
	account, failure := store.AcquirePinnedAccount(p.accountID, requestAPIKeyID(c), filter)
	if account == nil {
		p.failure = string(failure)
		return nil
	}
	// Scope budgets and scope concurrency are limits configured on this key,
	// not scheduling decisions, so they still apply to the pinned account.
	if gate := scopeBudgetGateFromContext(c); gate != nil && !gate.filter(nil)(account) {
		store.Release(account)
		p.failure = pinnedFailureScopeBudget
		p.message = gate.exhaustedMessage()
		return nil
	}
	c.Header(PinnedAccountHeader, strconv.FormatInt(account.ID(), 10))
	return account
}

func (p *pinnedAccountDispatch) failureResponse() (status int, code, message, retryAfter string) {
	id := p.accountID
	switch p.failure {
	case string(auth.PinnedAccountNotFound):
		return http.StatusNotFound, "pinned_account_not_found",
			fmt.Sprintf("Account %d does not exist or is not available to this API key", id), ""
	case string(auth.PinnedAccountUnsupported):
		return http.StatusBadRequest, "pinned_account_unsupported",
			fmt.Sprintf("Account %d cannot serve this request: check the model, the endpoint, and any conversation state it depends on", id), ""
	case string(auth.PinnedAccountDisabled):
		return http.StatusServiceUnavailable, "pinned_account_disabled",
			fmt.Sprintf("Account %d is disabled by an administrator", id), ""
	case string(auth.PinnedAccountCredentialRefreshing):
		return http.StatusServiceUnavailable, "pinned_account_credential_refreshing",
			fmt.Sprintf("Account %d is refreshing its upstream credential; retry shortly", id), "2"
	case string(auth.PinnedAccountEgressUnavailable):
		return http.StatusServiceUnavailable, "pinned_account_egress_unavailable",
			fmt.Sprintf("Account %d has no usable proxy route", id), ""
	case pinnedFailureScopeBudget:
		message := p.message
		if message == "" {
			message = fmt.Sprintf("This API key's scope limit for account %d is exhausted", id)
		}
		return http.StatusTooManyRequests, "pinned_account_scope_budget_exhausted", message, ""
	case pinnedFailureAttemptFailed:
		return http.StatusBadGateway, "pinned_account_attempt_failed",
			fmt.Sprintf("The attempt on account %d failed; the gateway does not retry on another account", id), ""
	default:
		return http.StatusServiceUnavailable, "pinned_account_credential_unavailable",
			fmt.Sprintf("Account %d has no usable upstream credential", id), ""
	}
}

// writeFailure ends a pinned request that could not run, or re-run, on its account.
func (p *pinnedAccountDispatch) writeFailure(c *gin.Context, protocol continuousRetryHTTPProtocol, isStream bool) {
	status, code, message, retryAfter := p.failureResponse()
	c.Header(pinnedAccountErrorHeader, code)
	if retryAfter != "" {
		c.Header("Retry-After", retryAfter)
	}
	switch protocol {
	case continuousRetryProtocolAnthropic:
		if isStream && writeCommittedAnthropicRetryError(c, mapHTTPStatusToAnthropicError(status), message) {
			return
		}
		if !claimContinuousRetryTerminal(c, protocol) {
			return
		}
		writeAnthropicCodedError(c, status, code, message)
	case continuousRetryProtocolChat:
		if isStream && writeCommittedChatRetryError(c, message) {
			return
		}
		if !claimContinuousRetryTerminal(c, protocol) {
			return
		}
		writeOpenAICodedError(c, status, code, message)
	default:
		if isStream && writeCommittedResponsesRetryError(c, message) {
			return
		}
		if !claimContinuousRetryTerminal(c, protocol) {
			return
		}
		writeOpenAICodedError(c, status, code, message)
	}
}

// retryLimit turns off a gateway retry budget: retrying the same account or
// failing over is the external scheduler's decision.
func (p *pinnedAccountDispatch) retryLimit(limit int) int {
	if p == nil {
		return limit
	}
	return 0
}

// continuousRetryPolicy turns off continuous retry, which would otherwise keep
// a request waiting on the pool after a failure.
func (p *pinnedAccountDispatch) continuousRetryPolicy(policy database.ContinuousRetryPolicy) database.ContinuousRetryPolicy {
	if p == nil {
		return policy
	}
	return database.NormalizeContinuousRetryPolicy(database.ContinuousRetryPolicy{})
}

// markPinnedUpstreamResponse exposes a failed attempt's upstream status and
// Retry-After before any protocol rewrite of the downstream response.
func markPinnedUpstreamResponse(c *gin.Context, resp *http.Response) {
	if resp == nil {
		return
	}
	if _, ok := pinnedAccountIDFromContext(c); !ok {
		return
	}
	c.Header(pinnedUpstreamStatusHeader, strconv.Itoa(resp.StatusCode))
	if retryAfter := normalizedRetryAfter(resp.Header.Get("Retry-After")); retryAfter != "" {
		c.Header("Retry-After", retryAfter)
	}
}

func writeOpenAICodedError(c *gin.Context, status int, code, message string) {
	errType := ErrorTypeInvalidRequest
	switch {
	case status == http.StatusTooManyRequests:
		errType = "rate_limit_error"
	case status >= http.StatusInternalServerError:
		errType = ErrorTypeServerError
	}
	c.JSON(status, gin.H{
		"error": gin.H{
			"message": message,
			"type":    errType,
			"code":    code,
		},
	})
}

func writeAnthropicCodedError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{
		"type": "error",
		"error": gin.H{
			"type":    mapHTTPStatusToAnthropicError(status),
			"message": message,
			"code":    code,
		},
	})
}
