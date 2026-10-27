package proxy

import (
	"net/http"
	"strings"
	"time"

	"github.com/codex2api/auth"
	"github.com/google/uuid"
)

type liveOutboundIdentity struct {
	UserAgent  string `json:"user_agent"`
	Version    string `json:"version"`
	Originator string `json:"originator"`
}

type liveUpstreamHeaderInput struct {
	Account     *auth.Account
	Downstream  http.Header
	APIKey      string
	Identity    *liveOutboundIdentity
	Attestation *string
}

func liveIdentityFromHeaders(headers http.Header) *liveOutboundIdentity {
	return &liveOutboundIdentity{
		UserAgent: headers.Get("User-Agent"), Version: headers.Get("Version"),
		Originator: headers.Get("Originator"),
	}
}

func (identity *liveOutboundIdentity) apply(headers http.Header) {
	// 显式空 UA 抑制 Go 默认身份，其余空值恢复为未发送。
	headers.Set("User-Agent", identity.UserAgent)
	for name, value := range map[string]string{"Version": identity.Version, "Originator": identity.Originator} {
		if value == "" {
			headers.Del(name)
			continue
		}
		headers.Set(name, value)
	}
}

func (h *Handler) resolveLiveIdentity(input liveUpstreamHeaderInput) (*liveOutboundIdentity, error) {
	if input.Identity != nil {
		return input.Identity, nil
	}
	identity, err := ResolveCodexOutboundClientIdentity(CodexClientIdentityInput{
		Account: input.Account, APIKey: input.APIKey, DeviceConfig: h.deviceCfg, Headers: input.Downstream,
	})
	if err != nil {
		return nil, err
	}
	originator := CodexOriginatorForGeneratedUserAgent(identity.UserAgent)
	if !identity.Generated && IsCodexOfficialClientByHeaders("", input.Downstream.Get("Originator")) {
		originator = input.Downstream.Get("Originator")
	}
	return &liveOutboundIdentity{UserAgent: identity.UserAgent, Version: identity.Version, Originator: originator}, nil
}

func applyLiveAuthentication(headers http.Header, account *auth.Account) {
	if account.IsCodexAgentIdentity() {
		if assertion, err := account.BuildCodexAgentAssertion(time.Now()); err == nil {
			headers.Set("Authorization", assertion)
			return
		}
	}
	headers.Set("Authorization", "Bearer "+account.GetAccessToken())
}

func (h *Handler) applyLiveUpstreamHeaders(req *http.Request, input liveUpstreamHeaderInput) error {
	if req == nil || input.Account == nil {
		return nil
	}
	identity, err := h.resolveLiveIdentity(input)
	if err != nil {
		return err
	}
	applyLiveAuthentication(req.Header, input.Account)
	identity.apply(req.Header)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/sdp")
	if accountID := input.Account.EffectiveAccountID(); accountID != "" {
		req.Header.Set("chatgpt-account-id", accountID)
	}
	applyCodexAllowedForwardHeaders(req, input.Downstream)
	ApplyCodexFingerprintHeaders(req.Header, input.Account, input.Downstream)
	applyAccountCustomHeaders(req, input.Account)
	if input.Identity != nil {
		input.Identity.apply(req.Header)
	}
	req.Header.Set("OpenAI-Alpha", "quicksilver=v2")
	req.Header.Del("OpenAI-Beta")
	for _, name := range []string{"session-id", "thread-id"} {
		if strings.TrimSpace(req.Header.Get(name)) == "" {
			req.Header.Set(name, uuid.NewString())
		}
	}
	return ApplyCodexAttestation(req.Context(), CodexAttestationInput{
		Headers: req.Header, Account: input.Account, FrozenValue: input.Attestation,
	})
}
