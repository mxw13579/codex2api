package proxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/security"
	"github.com/tidwall/gjson"
)

const (
	upstreamCookieHeaderLimit     = 64 << 10
	upstreamCookieTraceMaxBytes   = 512 << 10
	upstreamCookieTraceMaxEntries = 128
)

type upstreamCookieModelKey struct{}
type upstreamCookieContextKey struct{}

// Collection follows the persisted admin setting, independently of file logs.
func upstreamCookieLoggingEnabled() bool {
	// This runs for every stream frame; the stored settings are already
	// normalized, so avoid reparsing the client-identity config on this path.
	settings, ok := runtimeSettings.Load().(RuntimeSettings)
	return ok && settings.CodexUpstreamCookieLogEnabled
}

// WithUpstreamCookieLog starts an independent request buffer. The empty buffer
// also allows enabling diagnostics while a downstream WS connection is open.
func WithUpstreamCookieLog(ctx context.Context) context.Context {
	return context.WithValue(ctx, upstreamCookieContextKey{}, &upstreamCookieCollector{})
}

func upstreamCookieCollectorFromContext(ctx context.Context) *upstreamCookieCollector {
	if ctx == nil {
		return nil
	}
	collector, _ := ctx.Value(upstreamCookieContextKey{}).(*upstreamCookieCollector)
	return collector
}

// SnapshotUpstreamCookieLog freezes collected events for an existing usage row.
// It never drains the buffer: a final success can include earlier silent 429s.
func SnapshotUpstreamCookieLog(ctx context.Context) string {
	return upstreamCookieCollectorFromContext(ctx).snapshot()
}

type upstreamOAILBCookie struct {
	Source    string `json:"source"`
	Value     string `json:"value"`
	Host      string `json:"host,omitempty"`
	IssuedAt  int64  `json:"iat,omitempty"`
	ExpiresAt int64  `json:"exp,omitempty"`
}

type upstreamCookieEntry struct {
	Time               string                `json:"time"`
	Event              string                `json:"event"`
	Transport          string                `json:"transport"`
	AccountID          int64                 `json:"account_id"`
	RequestID          string                `json:"request_id,omitempty"`
	AttemptID          string                `json:"attempt_id,omitempty"`
	ConnectionID       string                `json:"connection_id,omitempty"`
	Model              string                `json:"model,omitempty"`
	HTTPStatus         int                   `json:"http_status,omitempty"`
	Status             int                   `json:"status,omitempty"`
	Cookie             []string              `json:"cookie"`
	SetCookie          []string              `json:"set_cookie"`
	OAILB              []upstreamOAILBCookie `json:"__oailb,omitempty"`
	UnifiedHost        string                `json:"unified_host,omitempty"`
	UnifiedSource      string                `json:"unified_source,omitempty"`
	CookieTruncated    bool                  `json:"cookie_truncated,omitempty"`
	CFRay              string                `json:"cf_ray,omitempty"`
	HandshakeCFRay     string                `json:"handshake_cf_ray,omitempty"`
	UpstreamRequestID  string                `json:"upstream_request_id,omitempty"`
	HandshakeRequestID string                `json:"handshake_request_id,omitempty"`
	ResponseID         string                `json:"response_id,omitempty"`
	ErrorCode          string                `json:"error_code,omitempty"`
	ErrorType          string                `json:"error_type,omitempty"`
	ProxyID            int64                 `json:"proxy_id,omitempty"`
	ProxyName          string                `json:"proxy_name,omitempty"`
}

// UpstreamCookieTrace freezes the actual outbound cookie and response cookies.
// WS connection snapshots are immutable; each turn gets its own trace, so a
// pooled handshake is not mistaken for this turn's request metadata.
type UpstreamCookieTrace struct {
	mu        sync.Mutex
	entry     upstreamCookieEntry
	terminal  bool
	collector *upstreamCookieCollector
}

func cookieLogRequestID(ctx context.Context) string {
	if a := upstreamTraceFromContext(ctx); a != nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.requestID
	}
	return ""
}

// NewUpstreamCookieTrace records even failed handshakes, before error conversion
// can discard their headers. Only Codex accounts are included.
func NewUpstreamCookieTrace(ctx context.Context, account *auth.Account, transport, connectionID string, requestHeaders http.Header, resp *http.Response) *UpstreamCookieTrace {
	if !upstreamCookieLoggingEnabled() || account == nil {
		return nil
	}
	account.Mu().RLock()
	upstreamType := strings.ToLower(strings.TrimSpace(account.UpstreamType))
	account.Mu().RUnlock()
	if upstreamType != "" && upstreamType != "codex" {
		return nil
	}
	e := upstreamCookieEntry{Transport: transport, AccountID: account.ID(), RequestID: cookieLogRequestID(ctx), ConnectionID: connectionID}
	if ctx != nil {
		e.Model, _ = ctx.Value(upstreamCookieModelKey{}).(string)
		e.Model = cookieLogValue(e.Model)
	}
	if a := upstreamTraceFromContext(ctx); a != nil {
		a.mu.Lock()
		if a.current != nil && a.current.accountID == account.ID() {
			e.ProxyID, e.ProxyName = a.current.proxy.ID, a.current.proxy.Name
		}
		a.mu.Unlock()
	}
	var truncated bool
	e.Cookie, e.CookieTruncated = boundedCookieHeaders(requestHeaders.Values("Cookie"))
	responseHeaders := http.Header{}
	if resp != nil {
		e.HTTPStatus, responseHeaders = resp.StatusCode, resp.Header
	}
	e.SetCookie, truncated = boundedCookieHeaders(responseHeaders.Values("Set-Cookie"))
	e.CookieTruncated = e.CookieTruncated || truncated
	e.CFRay = cookieLogValue(responseHeaders.Get("Cf-Ray"))
	for _, name := range []string{"X-Request-Id", "Request-Id", "X-Openai-Request-Id", "X-Oai-Request-Id"} {
		if value := responseHeaders.Get(name); value != "" {
			e.UpstreamRequestID = cookieLogValue(value)
			break
		}
	}
	request := &http.Request{Header: http.Header{"Cookie": e.Cookie}}
	response := &http.Response{Header: http.Header{"Set-Cookie": e.SetCookie}}
	for _, source := range []struct {
		name    string
		cookies []*http.Cookie
	}{{"request", request.Cookies()}, {"response", response.Cookies()}} {
		for _, cookie := range source.cookies {
			if cookie.Name != "__oailb" {
				continue
			}
			decoded := decodeUpstreamOAILB(cookie.Value)
			decoded.Source = source.name
			e.OAILB = append(e.OAILB, decoded)
			// An invalid/cleared response cookie must not inherit an old request route.
			e.UnifiedHost, e.UnifiedSource = decoded.Host, source.name
		}
	}
	if transport == "websocket" {
		if e.ConnectionID == "" {
			e.ConnectionID = NewUpstreamSessionUUID()
		}
		e.Event = "websocket_handshake"
		e.HandshakeCFRay, e.CFRay = e.CFRay, ""
		e.HandshakeRequestID, e.UpstreamRequestID = e.UpstreamRequestID, ""
	} else {
		e.Event, e.AttemptID = "http_response", NewUpstreamSessionUUID()
	}
	e.Status = e.HTTPStatus
	collector := upstreamCookieCollectorFromContext(ctx)
	t := &UpstreamCookieTrace{entry: e}
	collector.append(e)
	if transport != "websocket" {
		t.collector = collector
	}
	// A pooled connection must not retain the first request's buffer/context.
	return t
}

func boundedCookieHeaders(values []string) ([]string, bool) {
	out := make([]string, 0, min(len(values), 64))
	remaining := upstreamCookieHeaderLimit
	for i, value := range values {
		if i >= 64 {
			return out, true
		}
		if len(value) > remaining {
			out = append(out, value[:remaining])
			return out, true
		}
		out = append(out, value)
		remaining -= len(value)
	}
	return out, false
}

// Decode only the routing fields. This is an unverified JWT claim, not proof
// that the named host handled inference, and is never used for routing.
func decodeUpstreamOAILB(value string) upstreamOAILBCookie {
	out := upstreamOAILBCookie{Value: value}
	if len(value) > 16<<10 {
		return out
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return out
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return out
	}
	var claims struct {
		Host      string `json:"host"`
		IssuedAt  int64  `json:"iat"`
		ExpiresAt int64  `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return out
	}
	out.Host, out.IssuedAt, out.ExpiresAt = cookieLogValue(claims.Host), claims.IssuedAt, claims.ExpiresAt
	return out
}

func cookieLogValue(value string) string {
	return security.SafeTruncate(security.MaskSensitiveData(value), 256)
}

// BeginTurn uses the stored handshake cookies, not headers newly assembled for
// a reused connection. The gateway request ID is refreshed for every turn.
func (t *UpstreamCookieTrace) BeginTurn(ctx context.Context, attemptID, model string) *UpstreamCookieTrace {
	if t == nil || !upstreamCookieLoggingEnabled() {
		return nil
	}
	t.mu.Lock()
	e := t.entry
	t.mu.Unlock()
	e.Event, e.RequestID, e.AttemptID, e.Model = "websocket_turn", cookieLogRequestID(ctx), attemptID, cookieLogValue(model)
	e.Status = 0
	collector := upstreamCookieCollectorFromContext(ctx)
	collector.append(e)
	return &UpstreamCookieTrace{entry: e, collector: collector}
}

// ObserveEvent runs before callbacks can suppress a retryable error. Both
// successes and semantic 429s are emitted, including errors on pooled sockets.
func (t *UpstreamCookieTrace) ObserveEvent(payload []byte) {
	if t == nil || !upstreamCookieLoggingEnabled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.terminal {
		return
	}
	event := gjson.GetBytes(payload, "type").String()
	if event == "codex.response.metadata" {
		for key, value := range gjson.GetBytes(payload, "headers").Map() {
			switch strings.ToLower(key) {
			case "cf-ray":
				t.entry.CFRay = cookieLogValue(value.String())
			case "x-request-id", "request-id", "x-openai-request-id", "x-oai-request-id":
				t.entry.UpstreamRequestID = cookieLogValue(value.String())
			}
		}
		return
	}
	switch event {
	case "response.completed", "response.done", "response.incomplete", "response.failed", "error":
	default:
		return
	}
	payload = NormalizeResponsesTerminalEvent(payload)
	event = gjson.GetBytes(payload, "type").String()
	if event != "response.completed" && event != "response.incomplete" && event != "response.failed" && event != "error" {
		return
	}
	e := t.entry
	e.Event = event
	e.Status = http.StatusOK
	if event == "response.failed" || event == "error" {
		e.Status = responseFailedStatusCode(payload)
	}
	if e.Model == "" {
		e.Model = cookieLogValue(gjson.GetBytes(payload, "response.model").String())
	}
	e.ResponseID = cookieLogValue(gjson.GetBytes(payload, "response.id").String())
	for _, prefix := range []string{"response.error", "response.status_details.error", "error"} {
		if value := gjson.GetBytes(payload, prefix+".code").String(); e.ErrorCode == "" {
			e.ErrorCode = cookieLogValue(value)
		}
		if value := gjson.GetBytes(payload, prefix+".type").String(); e.ErrorType == "" {
			e.ErrorType = cookieLogValue(value)
		}
	}
	t.terminal = true
	t.collector.append(e)
}

type upstreamCookieEventObserver interface {
	observeUpstreamCookieEvent([]byte)
}

func forwardUpstreamCookieEvent(body io.Reader, payload []byte) {
	if observer, ok := body.(upstreamCookieEventObserver); ok {
		observer.observeUpstreamCookieEvent(payload)
	}
}

type upstreamCookieBody struct {
	io.ReadCloser
	trace *UpstreamCookieTrace
}

func (b *upstreamCookieBody) observeUpstreamCookieEvent(payload []byte) {
	b.trace.ObserveEvent(payload)
}

// Buffers are bounded per downstream request, including retries across accounts.
// Serialized entries are immutable, so later events cannot change a usage row
// that is already queued for an asynchronous database write.
type upstreamCookieCollector struct {
	mu        sync.Mutex
	entries   []json.RawMessage
	bytes     int
	truncated bool
}

func (c *upstreamCookieCollector) append(entry upstreamCookieEntry) {
	if c == nil || !upstreamCookieLoggingEnabled() {
		return
	}
	entry.Time = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= upstreamCookieTraceMaxEntries || c.bytes+len(data)+1 > upstreamCookieTraceMaxBytes {
		c.truncated = true
		return
	}
	c.entries = append(c.entries, data)
	c.bytes += len(data) + 1
}

func (c *upstreamCookieCollector) snapshot() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) == 0 && !c.truncated {
		return ""
	}
	entries := c.entries
	if entries == nil {
		entries = []json.RawMessage{}
	}
	data, _ := json.Marshal(struct {
		Entries   []json.RawMessage `json:"entries"`
		Truncated bool              `json:"truncated"`
	}{entries, c.truncated})
	return string(data)
}
