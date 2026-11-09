package proxy

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	errorCodeTierPoolUnavailable = "tier_pool_unavailable"
	errorCodeServiceTierConflict = "service_tier_conflict"
)

type ultrafastRequestKey struct{}
type ultrafastContextLostKey struct{}

func isUltrafastTier(tier string) bool {
	return strings.EqualFold(strings.TrimSpace(tier), "ultrafast")
}

// Keep the original intent separate from the mutable per-attempt billing tier.
// Installing false as well as true makes this state local to each WS turn.
func prepareUltrafastRequest(c *gin.Context, body []byte) ([]byte, *Error) {
	normalized, ultra, err := normalizeUltrafastBody(body)
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ultrafastRequestKey{}, ultra))
	return normalized, err
}

func normalizeUltrafastBody(body []byte) ([]byte, bool, *Error) {
	snake := gjson.GetBytes(body, "service_tier").String()
	camel := gjson.GetBytes(body, "serviceTier").String()
	ultra := isUltrafastTier(snake) || isUltrafastTier(camel)
	if !ultra {
		return body, false, nil
	}
	if (strings.TrimSpace(snake) != "" && !isUltrafastTier(snake)) ||
		(strings.TrimSpace(camel) != "" && !isUltrafastTier(camel)) {
		return body, true, serviceTierConflict("Conflicting service_tier and serviceTier values")
	}
	body, _ = sjson.SetBytes(body, "service_tier", "ultrafast")
	body, _ = sjson.DeleteBytes(body, "serviceTier")
	return body, true, nil
}

func requestedUltrafast(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	ultra, _ := ctx.Value(ultrafastRequestKey{}).(bool)
	return ultra
}

// Do not use auth.NormalizePlanType: it deliberately folds ProMax into Pro.
// Relay labels cannot establish the plan of the eventual upstream account.
func accountSupportsUltrafast(account *auth.Account) bool {
	if account == nil || account.IsRelayStyle() {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(account.GetPlanType())) {
	case "promax", "pro_max", "pro-max":
		return true
	default:
		return false
	}
}

func ultrafastAccountFilter(ctx context.Context, base auth.AccountFilter) auth.AccountFilter {
	if !requestedUltrafast(ctx) {
		return base
	}
	return func(account *auth.Account) bool {
		return accountSupportsUltrafast(account) && (base == nil || base(account))
	}
}

// Only distinguish a structurally absent plan here. Busy/cooling/exhausted
// ProMax accounts still go through the existing scheduler and error handling.
func (h *Handler) ultrafastPoolError(c *gin.Context) *Error {
	if !requestedUltrafast(c.Request.Context()) || pinnedAccountDispatchForRequest(c) != nil {
		return nil
	}
	for _, account := range h.store.Accounts() {
		if accountSupportsUltrafast(account) && h.store.APIKeyAllowsAccount(requestAPIKeyID(c), account) {
			return nil
		}
	}
	return &Error{Code: errorCodeTierPoolUnavailable, Type: ErrorTypeServerError,
		HTTPStatus: http.StatusServiceUnavailable,
		Message:    "当前 API Key 可用范围内没有支持 ultrafast 的 ProMax 账号"}
}

func serviceTierConflict(message string) *Error {
	return &Error{Code: errorCodeServiceTierConflict, Type: ErrorTypeInvalidRequest,
		HTTPStatus: http.StatusBadRequest, Message: message}
}

// Check after Payload rules, and again at the send boundary. This also covers
// retained HTTP leases, compact and internal follow-up requests. The caller
// owns/relinquishes the lease; the executor never releases it a second time.
func guardUltrafastUpstream(ctx context.Context, account *auth.Account, body []byte) *Error {
	return guardUltrafastTier(ctx, account, extractServiceTier(body))
}

func guardUltrafastTier(ctx context.Context, account *auth.Account, tier string) *Error {
	finalUltra := isUltrafastTier(tier)
	if requestedUltrafast(ctx) && !finalUltra {
		return serviceTierConflict("An explicit ultrafast request cannot be downgraded by translation or Payload rules")
	}
	if finalUltra && ctx != nil {
		if lost, _ := ctx.Value(ultrafastContextLostKey{}).(bool); lost {
			return serviceTierConflict("ultrafast requires complete continuation input; history recovery failed")
		}
	}
	if finalUltra && !accountSupportsUltrafast(account) {
		return serviceTierConflict("ultrafast requires a ProMax account; the selected account is not eligible")
	}
	return nil
}

func serviceTierError(err error) *Error {
	var tierErr *Error
	if errors.As(err, &tierErr) && (tierErr.Code == errorCodeServiceTierConflict || tierErr.Code == errorCodeTierPoolUnavailable) {
		return tierErr
	}
	return nil
}

func serviceTierAPIError(err *Error) *api.APIError {
	kind := api.ErrorTypeInvalidRequest
	if err.HTTPStatus == http.StatusServiceUnavailable {
		kind = api.ErrorTypeServer
	}
	return api.NewAPIError(api.ErrorCode(err.Code), err.Message, kind)
}

func sendServiceTierError(c *gin.Context, err *Error) {
	protocol := continuousRetryProtocolResponses
	if strings.HasSuffix(c.Request.URL.Path, "/messages") {
		protocol = continuousRetryProtocolAnthropic
	} else if strings.HasSuffix(c.Request.URL.Path, "/chat/completions") {
		protocol = continuousRetryProtocolChat
	}
	if !claimContinuousRetryTerminal(c, protocol) {
		return
	}
	if strings.HasSuffix(c.Request.URL.Path, "/messages") {
		if writeCommittedAnthropicRetryError(c, "invalid_request_error", err.Message) {
			return
		}
		kind := "invalid_request_error"
		if err.HTTPStatus == http.StatusServiceUnavailable {
			kind = "overloaded_error"
		}
		sendAnthropicError(c, err.HTTPStatus, kind, err.Message)
		return
	}
	if strings.HasSuffix(c.Request.URL.Path, "/chat/completions") {
		if writeCommittedChatRetryError(c, err.Message) {
			return
		}
	} else if writeCommittedResponsesRetryError(c, err.Message) {
		return
	}
	c.JSON(err.HTTPStatus, err.ToGinH())
}
