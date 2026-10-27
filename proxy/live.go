package proxy

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/codex2api/proxy/attestation"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	defaultLiveMaxSessionDuration = time.Hour
	liveLeaseRefreshInterval      = 20 * time.Second
	liveUpstreamBodyLimit         = 2 << 20
	liveAttestationHeader         = attestation.HeaderName
	liveCallCacheNamespace        = "live_call"
	liveAccountLeaseNamespace     = "live_acct"
	liveKeyLeaseNamespace         = "live_key"

	liveControllerPending  = "pending"
	liveControllerObserver = "observer"
	liveControllerProxy    = "proxy"
	liveControllerClosed   = "closed"
)

var (
	chatGPTLiveCallsURL = "https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas"
	// Codex v3 的 sideband 使用独立端点，仍携带建连账号的 OAuth 和 account ID。
	liveSidebandUpstreamBaseURL = "wss://api.openai.com/v1/live"
	// liveCallsURLForTest 仅测试替换上游 POST URL。
	liveCallsURLForTest    = ""
	liveMaxSessionDuration = defaultLiveMaxSessionDuration
)

var liveModelAliases = []string{"gpt-live", "gpt-live-1", "gpt-live-1-mini"}

type liveCallRequest struct {
	SDP     string          `json:"sdp"`
	Session json.RawMessage `json:"session"`
}

type liveCallRecord struct {
	CallID                string                `json:"call_id"`
	CallHash              string                `json:"call_hash"`
	AccountID             int64                 `json:"account_id"`
	APIKeyID              int64                 `json:"api_key_id"`
	LeaseID               string                `json:"lease_id"`
	Model                 string                `json:"model"`
	CreatedAt             time.Time             `json:"created_at"`
	ExpiresAt             time.Time             `json:"expires_at"`
	Controller            string                `json:"controller"`
	ControllerOwner       string                `json:"controller_owner,omitempty"`
	AttestationCiphertext string                `json:"attestation_ciphertext,omitempty"`
	OutboundIdentity      *liveOutboundIdentity `json:"outbound_identity,omitempty"`
	UserAgent             string                `json:"user_agent,omitempty"`
	ClientIP              string                `json:"client_ip,omitempty"`
	InboundEndpoint       string                `json:"inbound_endpoint,omitempty"`
	RequestID             string                `json:"request_id,omitempty"`
	UpstreamRequestID     string                `json:"upstream_request_id,omitempty"`
	UpstreamProxyID       int64                 `json:"upstream_proxy_id,omitempty"`
	UpstreamProxyName     string                `json:"upstream_proxy_name,omitempty"`
	UsageLogged           bool                  `json:"usage_logged,omitempty"`
}

func liveAccountFilter(account *auth.Account) bool {
	return account != nil && !account.IsRelayStyle()
}

func isLiveModelName(model string) bool {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "gpt-live", "gpt-live-1", "gpt-live-1-mini":
		return true
	default:
		return false
	}
}

func liveModelExplicitlyDenied(id string, deny []string) bool {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return false
	}
	for _, item := range deny {
		if strings.ToLower(strings.TrimSpace(item)) == id {
			return true
		}
	}
	return false
}

func hashLiveCallID(callID string) string {
	sum := sha256.Sum256([]byte(callID))
	return fmt.Sprintf("%x", sum[:])
}

func liveSessionModel(session json.RawMessage) string {
	model := strings.TrimSpace(gjson.GetBytes(session, "model").String())
	if model == "" {
		return "gpt-live"
	}
	return model
}

func ValidateLiveCallRequest(request *liveCallRequest) error {
	if request == nil || strings.TrimSpace(request.SDP) == "" {
		return errors.New("sdp is required")
	}
	if len(request.Session) == 0 || !json.Valid(request.Session) {
		return errors.New("session must be valid JSON")
	}
	var sessionObject map[string]json.RawMessage
	if err := json.Unmarshal(request.Session, &sessionObject); err != nil || sessionObject == nil {
		return errors.New("session must be a JSON object")
	}
	return nil
}

func liveCallIDFromLocation(location string) (string, error) {
	location = strings.TrimSpace(location)
	if location == "" {
		return "", errors.New("live upstream response has no Location")
	}
	parsed, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parse live Location: %w", err)
	}
	callID := strings.TrimSpace(path.Base(strings.TrimSuffix(parsed.Path, "/")))
	if callID == "" || callID == "." || callID == "codex" {
		return "", errors.New("live upstream Location has no call id")
	}
	return callID, nil
}

func liveSidebandLocation(fullPath, callID string) string {
	prefix := "/v1/live/"
	if strings.HasPrefix(fullPath, "/backend-api/codex/") {
		prefix = "/backend-api/codex/"
	}
	return prefix + url.PathEscape(callID)
}

func liveRequireAllowLive(c *gin.Context) bool {
	row := apiKeyRowFromContext(c)
	if row != nil && row.Limits.AllowLive {
		return true
	}
	api.SendErrorWithStatus(c, api.NewAPIError(
		api.ErrCodeInsufficientScope,
		"ChatGPT Live is not enabled for this API key",
		api.ErrorTypePermission,
	), http.StatusForbidden)
	return false
}

func parseLiveCallRequest(c *gin.Context) (*liveCallRequest, error) {
	contentType := strings.ToLower(c.GetHeader("Content-Type"))
	if strings.HasPrefix(contentType, "multipart/form-data") {
		request := &liveCallRequest{
			SDP:     c.PostForm("sdp"),
			Session: json.RawMessage(c.PostForm("session")),
		}
		if err := ValidateLiveCallRequest(request); err != nil {
			return nil, err
		}
		return request, nil
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, liveUpstreamBodyLimit+1))
	if err != nil {
		return nil, errors.New("failed to read request body")
	}
	if len(raw) > liveUpstreamBodyLimit {
		return nil, errors.New("request body is too large")
	}
	var request liveCallRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, errors.New("request body must be valid JSON")
	}
	if err := ValidateLiveCallRequest(&request); err != nil {
		return nil, err
	}
	return &request, nil
}

func liveInboundEndpoint(c *gin.Context) string {
	if c == nil {
		return "/v1/live"
	}
	if path := strings.TrimSpace(c.FullPath()); path != "" {
		return path
	}
	if c.Request != nil {
		return c.Request.URL.Path
	}
	return "/v1/live"
}
