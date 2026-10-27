package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy/attestation"
)

const liveCreateTimeout = 45 * time.Second

type liveCallCreated struct {
	sdp                   []byte
	callID                string
	attestationCiphertext string
	identity              *liveOutboundIdentity
}

type liveCreateInput struct {
	Account    *auth.Account
	Request    *liveCallRequest
	Downstream http.Header
	APIKey     string
}

type liveCreateResult struct {
	created     *liveCallCreated
	status      int
	contentType string
	body        []byte
	err         error
	local       bool
}

func (h *Handler) createUpstreamLiveCall(ctx context.Context, input liveCreateInput) liveCreateResult {
	reqCtx, cancel := context.WithTimeout(ctx, liveCreateTimeout)
	defer cancel()
	req, err := h.newUpstreamLiveRequest(reqCtx, input)
	if err != nil {
		return liveCreateResult{err: err, local: true}
	}
	ciphertext, err := encryptLiveAttestation(req.Header.Get(attestation.HeaderName), h.liveAttestationSecret())
	if err != nil {
		return liveCreateResult{err: errors.New("failed to protect the Live attestation"), local: true}
	}
	created := &liveCallCreated{attestationCiphertext: ciphertext, identity: liveIdentityFromHeaders(req.Header)}
	proxyURL := ""
	if h.store != nil {
		proxyURL = h.store.ResolveProxyForAccount(input.Account)
	}
	resp, err := getCodexMaintenanceClient(input.Account, proxyURL).Do(req)
	if err != nil {
		return liveCreateResult{err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	return decodeLiveCreateResponse(resp, created)
}

func (h *Handler) newUpstreamLiveRequest(ctx context.Context, input liveCreateInput) (*http.Request, error) {
	body, err := json.Marshal(input.Request)
	if err != nil {
		return nil, err
	}
	endpoint := chatGPTLiveCallsURL
	if liveCallsURLForTest != "" {
		endpoint = liveCallsURLForTest
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	err = h.applyLiveUpstreamHeaders(req, liveUpstreamHeaderInput{
		Account: input.Account, Downstream: input.Downstream, APIKey: input.APIKey,
	})
	return req, err
}

func decodeLiveCreateResponse(resp *http.Response, created *liveCallCreated) liveCreateResult {
	result := liveCreateResult{status: resp.StatusCode, contentType: resp.Header.Get("Content-Type")}
	result.body, result.err = io.ReadAll(io.LimitReader(resp.Body, liveUpstreamBodyLimit+1))
	if result.err != nil || len(result.body) > liveUpstreamBodyLimit {
		result.status, result.body = http.StatusBadGateway, nil
		result.err = errors.New("failed to read bounded Live upstream response")
		return result
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		log.Printf("live: upstream create %d: %s", resp.StatusCode, truncateForLog(result.body, 512))
		result.err = fmt.Errorf("live upstream status %d", resp.StatusCode)
		return result
	}
	created.callID, result.err = liveCallIDFromLocation(resp.Header.Get("Location"))
	if result.err != nil {
		result.status, result.body = http.StatusBadGateway, nil
		return result
	}
	created.sdp, result.created = result.body, created
	return result
}
