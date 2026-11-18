package proxy

import (
	"errors"
	"log"
	"net/http"

	"github.com/codex2api/api"
	"github.com/gin-gonic/gin"
)

// responseContextAccountContextKey 记录 WebSocket 轮次当前尝试的账号 ID，
// 供写错误帧时的"上下文不可用"日志使用；未选号时为空（日志记 0）。
const responseContextAccountContextKey = "response_context_account_id"

const (
	responseContextTransportHTTP = "http"
	responseContextTransportWS   = "ws"
)

// logResponseContextUnavailable 在向客户端返回"上下文不可用"（409/503）时打一行固定格式的
// 容器日志，便于按原因码 grep。不记录 previous_response_id、请求内容、API Key 或用户标识。
func logResponseContextUnavailable(c *gin.Context, transport string, status int, reason string, accountID int64) {
	endpoint := ""
	if c != nil && c.Request != nil && c.Request.URL != nil {
		endpoint = c.Request.URL.Path
	}
	if reason == "" {
		reason = "unknown"
	}
	log.Printf("[ResponseContext] unavailable status=%d reason=%s endpoint=%s transport=%s account=%d",
		status, reason, endpoint, transport, accountID)
}

// responseContextUnavailableReason 识别本地生成的"上下文不可用"错误（responsesWSContextUnavailable
// 及其 nativeResponsesWSContextError 改码后的形式），返回原因码。上游透传的
// previous_response_not_found 不带 previous_response_id 明细，不在此列。
func responseContextUnavailableReason(apiErr *api.APIError) (string, bool) {
	if apiErr == nil {
		return "", false
	}
	switch apiErr.Code {
	case api.ErrCodeResponseContextUnavailable, api.ErrorCode("previous_response_not_found"), api.ErrCodeServiceUnavailable:
	default:
		return "", false
	}
	var details []api.ErrorDetail
	switch typed := apiErr.Details.(type) {
	case []api.ErrorDetail:
		details = typed
	case api.ErrorDetail:
		details = []api.ErrorDetail{typed}
	}
	for _, detail := range details {
		if detail.Field == "previous_response_id" && detail.Message != "" {
			return detail.Message, true
		}
	}
	return "", false
}

// logResponsesWSContextUnavailable 在 WebSocket 错误帧是"上下文不可用"时记录日志。
func logResponsesWSContextUnavailable(c *gin.Context, apiErr *api.APIError) {
	reason, ok := responseContextUnavailableReason(apiErr)
	if !ok {
		return
	}
	var accountID int64
	if c != nil {
		accountID = c.GetInt64(responseContextAccountContextKey)
	}
	logResponseContextUnavailable(c, responseContextTransportWS, responsesWSErrorStatus(apiErr), reason, accountID)
}

// logHTTPResponsesContinuationLost 在 HTTP 入站请求以 ResponsesContinuationLostError
// （409 Previous response context is unavailable）结束时记录原因码。
func logHTTPResponsesContinuationLost(c *gin.Context, err error, accountID int64) {
	var lost *ResponsesContinuationLostError
	if !errors.As(err, &lost) {
		return
	}
	logResponseContextUnavailable(c, responseContextTransportHTTP, http.StatusConflict, lost.Reason, accountID)
}
