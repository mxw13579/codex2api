package proxy

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const liveMaxCreateAttempts = 4

type liveCreateState struct {
	handler    *Handler
	client     *gin.Context
	request    *liveCallRequest
	releaseKey func()
	keyHeld    bool
	apiKey     string
	filter     func(*auth.Account) bool
	excluded   map[int64]bool
	last       liveCreateResult
}

// LiveCreate 选中账号后通过共享服务生成证明，并冻结成功建连的出站结果。
func (h *Handler) LiveCreate(c *gin.Context) {
	if !liveRequireAllowLive(c) {
		return
	}
	request, err := parseLiveCallRequest(c)
	if err != nil {
		api.SendError(c, api.NewAPIError(api.ErrCodeInvalidRequest, err.Error(), api.ErrorTypeInvalidRequest))
		return
	}
	if h.enforceAPIKeyLimitsAndReply(c, "") {
		return
	}
	releaseKey, ok := h.acquireAPIKeyConcurrency(c)
	if !ok {
		return
	}
	state := &liveCreateState{
		handler: h, client: c, request: request, releaseKey: releaseKey,
		apiKey:   strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")),
		filter:   applyAffinityGroupRouting(c, resolveRequestSessionIdentity(c.Request.Header, request.Session), liveAccountFilter),
		excluded: make(map[int64]bool),
	}
	defer state.releaseUnheldKey()
	state.run()
}

func (s *liveCreateState) releaseUnheldKey() {
	if !s.keyHeld && s.releaseKey != nil {
		s.releaseKey()
	}
}

func (s *liveCreateState) run() {
	for attempt := 0; attempt < liveMaxCreateAttempts; attempt++ {
		account := s.handler.store.NextExcludingWithFilter(requestAPIKeyID(s.client), s.excluded, s.filter)
		if account == nil {
			s.reply(s.last)
			return
		}
		result := s.handler.createUpstreamLiveCall(s.client.Request.Context(), liveCreateInput{
			Account: account, Request: s.request, Downstream: s.client.Request.Header, APIKey: s.apiKey,
		})
		if result.created != nil {
			s.save(account, result.created)
			return
		}
		s.excluded[account.ID()] = true
		s.handler.store.Release(account)
		if s.handleFailure(result) {
			return
		}
	}
	s.reply(s.last)
}

func (s *liveCreateState) handleFailure(result liveCreateResult) bool {
	if s.client.Request.Context().Err() != nil {
		return true
	}
	if result.local {
		status := StatusCodeFromError(result.err)
		if status == 0 {
			status = http.StatusServiceUnavailable
		}
		api.SendErrorWithStatus(s.client, api.NewAPIError(api.ErrCodeServiceUnavailable, result.err.Error(), api.ErrorTypeServer), status)
		return true
	}
	if result.status >= http.StatusBadRequest && result.status < http.StatusInternalServerError && result.status != http.StatusTooManyRequests {
		s.reply(result)
		return true
	}
	if result.status != 0 {
		s.last = result
	}
	return false
}

func (s *liveCreateState) reply(result liveCreateResult) {
	if result.status == 0 {
		api.SendError(s.client, api.ErrServiceUnavailable)
		return
	}
	contentType := result.contentType
	if contentType == "" {
		contentType = "application/json"
	}
	s.client.Data(result.status, contentType, result.body)
}

func (s *liveCreateState) save(account *auth.Account, created *liveCallCreated) {
	record := s.record(account, created)
	if err := s.handler.liveCalls().save(s.client.Request.Context(), record, account, s.releaseKey); err != nil {
		s.handler.store.Release(account)
		log.Printf("live: save call mapping: %v", err)
		api.SendErrorWithStatus(s.client, api.NewAPIError(api.ErrCodeServiceUnavailable, "Live call mapping unavailable", api.ErrorTypeServer), http.StatusServiceUnavailable)
		return
	}
	s.keyHeld = true
	s.client.Header("Location", liveSidebandLocation(s.client.FullPath(), created.callID))
	s.client.Data(http.StatusOK, "application/sdp", created.sdp)
}

func (s *liveCreateState) record(account *auth.Account, created *liveCallCreated) *liveCallRecord {
	now := time.Now()
	trace := snapshotUpstreamTrace(s.client.Request.Context())
	return &liveCallRecord{
		RequestID: trace.RequestID, UpstreamRequestID: trace.UpstreamRequestID,
		UpstreamProxyID: trace.Proxy.ID, UpstreamProxyName: trace.Proxy.Name,
		CallID: created.callID, CallHash: hashLiveCallID(created.callID),
		AccountID: account.ID(), APIKeyID: requestAPIKeyID(s.client), LeaseID: uuid.NewString(),
		Model: liveSessionModel(s.request.Session), CreatedAt: now, ExpiresAt: now.Add(liveMaxSessionDuration),
		Controller: liveControllerPending, AttestationCiphertext: created.attestationCiphertext,
		OutboundIdentity: created.identity, UserAgent: s.client.GetHeader("User-Agent"),
		ClientIP: s.client.ClientIP(), InboundEndpoint: liveInboundEndpoint(s.client),
	}
}
