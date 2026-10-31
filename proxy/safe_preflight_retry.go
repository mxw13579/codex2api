package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/codex2api/auth"
	"github.com/codex2api/security"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/http2"
)

// This file deliberately has no Handler loop, terminal renderer, billing or
// account-health implementation. Those remain owned by Handler.Responses.
const (
	safePreflightSSEMaxEnvelopeBytes = 1 << 20
	safePreflightPendingMax          = 64 << 10
	safePreflightMaxAttempts         = 3
	safePreflightRetryWindow         = 60 * time.Second
)

var (
	errSafePreflightSSEEnvelope = errors.New("unsupported safe preflight SSE envelope")
	errSafePreflightSSETooLarge = errors.New("safe preflight SSE envelope exceeds limit")
	errSafePreflightSSERead     = errors.New("safe preflight SSE read failed")
	errSafePreflightLocal       = errors.New("safe preflight local guard rejected request")
)

type safePreflightSSEFrame struct {
	event                       string
	data, wire                  []byte
	envelopeChecked, hasComment bool
}

func (f safePreflightSSEFrame) isRateLimits() bool {
	return f.envelopeChecked && !f.hasComment && isSafePreflightRateLimitsFrame(f.event, f.data)
}

// The standalone reader stays bounded. Once protocol commit permanently closes
// replay, the Handler resumes the legacy reader on the SAME buffered reader.
// This preserves read-ahead bytes, ordinary SSE fields/EOF-tail compatibility,
// terminal normalization and generation capacity without widening the whitelist.
func readSafePreflightSSE(body io.Reader, onUnsafe func(), observe func(safePreflightSSEFrame) bool) error {
	return readSafePreflightSSEWithCommit(body, onUnsafe, observe, nil)
}

func readSafePreflightSSEWithCommit(body io.Reader, onUnsafe func(), observe func(safePreflightSSEFrame) bool, committed func() bool) error {
	if body == nil || onUnsafe == nil || observe == nil {
		return errSafePreflightSSEEnvelope
	}
	r := bufio.NewReaderSize(body, 4096)
	var frame safePreflightSSEFrame
	var dataLines [][]byte
	eventSeen := false
	unsafe := func(err error) error { onUnsafe(); return err }
	for {
		if committed != nil && committed() {
			return ReadSSEStreamWithEvent(r, func(event string, data []byte) bool {
				return observe(safePreflightSSEFrame{event: event, data: data})
			})
		}
		remaining := safePreflightSSEMaxEnvelopeBytes - len(frame.wire)
		line, readErr := readSafePreflightSSELine(r, remaining)
		if errors.Is(readErr, errSafePreflightSSETooLarge) {
			return unsafe(readErr)
		}
		if len(line) == 0 && readErr != nil {
			if len(frame.wire) != 0 {
				return unsafe(io.ErrUnexpectedEOF)
			}
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return errSafePreflightSSERead
		}
		if len(line) == 0 || line[len(line)-1] != '\n' {
			return unsafe(io.ErrUnexpectedEOF)
		}
		frame.wire = append(frame.wire, line...)
		line = line[:len(line)-1]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if !utf8.Valid(line) || bytes.IndexByte(line, '\r') >= 0 || bytes.IndexByte(line, 0) >= 0 {
			return unsafe(errSafePreflightSSEEnvelope)
		}
		if len(line) == 0 {
			if len(dataLines) != 0 {
				frame.envelopeChecked = true
				frame.data = bytes.Join(dataLines, []byte{'\n'})
				if !observe(frame) {
					return nil
				}
			} else if eventSeen {
				return unsafe(errSafePreflightSSEEnvelope)
			}
			frame, dataLines, eventSeen = safePreflightSSEFrame{}, nil, false
			continue
		}
		if line[0] == ':' {
			frame.hasComment = true
			continue
		}
		field, value, ok := bytes.Cut(line, []byte{':'})
		if !ok {
			return unsafe(errSafePreflightSSEEnvelope)
		}
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "event":
			if eventSeen {
				return unsafe(errSafePreflightSSEEnvelope)
			}
			eventSeen, frame.event = true, string(value)
		case "data":
			dataLines = append(dataLines, value)
		default:
			return unsafe(errSafePreflightSSEEnvelope)
		}
	}
}

func readSafePreflightSSELine(r *bufio.Reader, remaining int) ([]byte, error) {
	var line []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(part) > remaining-len(line) {
			return nil, errSafePreflightSSETooLarge
		}
		line = append(line, part...)
		if err != bufio.ErrBufferFull {
			return line, err
		}
	}
}

type safePreflightContextKey struct{}

// Public state is request-scoped, never reset on retry. Only sent/revoked are
// attempt-scoped. The send permit is consumed AFTER all blocking quota work.
// No credential, request bytes or identity digest may enter diagnostics.
type safePreflightState struct {
	mu                                                         sync.Mutex
	started                                                    time.Time
	downstream                                                 context.Context
	attempts                                                   int
	staged, pinned, sent, revoked                              bool
	replayUnsafe, protocolCommitted, writeFailed, localFailure bool
	digest                                                     [sha256.Size]byte
	rootCAs                                                    *x509.CertPool
	accountID                                                  int64
	dispatchPolicy                                             auth.DispatchPolicy
	sessionID                                                  string
	admit                                                      func(*auth.Account, string, string) bool
	// Request-scoped and monotonic; observation/usage/commit are NOT delivery.
	// These record complete local terminal bytes and a returned http.Flusher.Flush,
	// not a transport error acknowledgement or proof the client parsed them.
	// Keep failure and success evidence distinct: neither can bless the other.
	successTerminalWrittenAndFlushed bool
	failedTerminalWrittenAndFlushed  bool
}

func safePreflightFromContext(ctx context.Context) *safePreflightState {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(safePreflightContextKey{}).(*safePreflightState)
	return s
}

func (s *safePreflightState) lockReplay() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.replayUnsafe = true
	s.mu.Unlock()
}
func (s *safePreflightState) localError() {
	s.mu.Lock()
	s.replayUnsafe, s.localFailure = true, true
	s.mu.Unlock()
}
func (s *safePreflightState) retryUnsafe() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replayUnsafe || s.writeFailed || s.localFailure
}
func (s *safePreflightState) committed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.protocolCommitted
}
func (s *safePreflightState) commit() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.replayUnsafe, s.protocolCommitted = true, true
	s.mu.Unlock()
}
func (s *safePreflightState) noteTerminalWrittenAndFlushed(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeFailed || s.localFailure {
		return
	}
	switch {
	case isResponsesSuccessTerminalEvent(event):
		s.successTerminalWrittenAndFlushed = true
	case event == "response.failed" || event == "error":
		s.failedTerminalWrittenAndFlushed = true
	}
}
func (s *safePreflightState) canStart(ctx context.Context) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return ctx.Err() == nil && !s.replayUnsafe && !s.writeFailed && !s.localFailure &&
		s.attempts < safePreflightMaxAttempts && time.Since(s.started) < safePreflightRetryWindow
}
func (s *safePreflightState) begin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = false
	if s.replayUnsafe || s.localFailure || s.writeFailed || s.attempts >= safePreflightMaxAttempts ||
		time.Since(s.started) >= safePreflightRetryWindow || s.downstream != nil && s.downstream.Err() != nil {
		s.revoked, s.localFailure = true, true
		return
	}
	s.attempts++
	s.sent, s.revoked = false, false
}
func (s *safePreflightState) pin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.staged || !s.sent || s.replayUnsafe || s.writeFailed || s.localFailure {
		return false
	}
	s.pinned = true
	return true
}
func (s *safePreflightState) pinnedAccount() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pinned {
		return s.accountID
	}
	return 0
}
func (s *safePreflightState) wasSent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent
}

// Eligibility is snapshotted at ingress, before stripping tools/continuations.
// No filter, cookie observer or telemetry setting is disabled for this mode.
func (h *Handler) safePreflightPrepared(c *gin.Context, raw []byte, stream bool, settings RuntimeSettings) bool {
	cfg := h.promptFilterConfigForRequest(c)
	return settings.CodexSafePreflightRetry && stream && !settings.CodexForceWebsocket &&
		!h.shouldUseWebsocketForHTTP() && !settings.CodexContinueThinking && !settings.OverflowAutoCompact && !settings.ContinuousRetryPolicy.Enabled &&
		!autoCompactOverflowEnabled(c) && !(cfg.Enabled && cfg.Advanced.Output.Enabled) &&
		h.getMaxRetries() >= 0 && h.getMaxRateLimitRetries() >= 0 &&
		c.GetHeader(codexTurnStateHeader) == "" && c.GetHeader(codexTurnMetadataHeader) == "" &&
		!codexResponsesLiteRequested(raw, c.Request.Header) && safePreflightRequestBody(raw)
}

func safePreflightRoute(ctx context.Context, account *auth.Account, proxyURL string) bool {
	if account == nil || account.IsRelayStyle() || account.IsCodexAgentIdentity() || resinCarriesEgress(account) ||
		codexTransportModeFromEnv() != codexTransportModeStandard || pipelineFromContext(ctx) != nil {
		return false
	}
	if bound, ok := AccountBoundEgress(ctx); ok {
		proxyURL = bound
	}
	// V1 requires an explicit HTTP(S) proxy. No env proxy, direct, Resin or
	// uTLS route is replaced by a different client to make it eligible.
	if strings.TrimSpace(proxyURL) == "" {
		return false
	}
	u, err := security.ParseProxyURL(proxyURL)
	if err != nil {
		return true
	} // activate fail-closed validation, NOT legacy direct fallback
	return u.Scheme == "http" || u.Scheme == "https"
}

func safePreflightExplicitProxy(raw string) (*url.URL, error) {
	u, err := security.ParseProxyURL(raw)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errSafePreflightLocal
	}
	return u, nil
}

// Protocol rejection belongs in guard, not in this eligibility predicate:
// an unknown TLSNextProto map must fail closed, not opt into the legacy sender.
func safePreflightSupportedClient(client *http.Client) bool {
	if client == nil {
		return false
	}
	t, ok := client.Transport.(*http.Transport)
	if !ok || t == nil || t.Proxy == nil || t.DialTLS != nil || t.DialTLSContext != nil || t.GetProxyConnectHeader != nil || t.OnProxyConnectResponse != nil {
		return false
	}
	if c := t.TLSClientConfig; c != nil {
		if c.InsecureSkipVerify || c.VerifyConnection != nil || c.VerifyPeerCertificate != nil || c.GetClientCertificate != nil || len(c.Certificates) != 0 {
			return false
		}
	}
	return true
}

func safePreflightAttemptEligible(ctx context.Context, account *auth.Account, body []byte, proxyURL string, websocket bool) bool {
	if websocket || !safePreflightRequestBody(body) || !safePreflightRoute(ctx, account, proxyURL) {
		return false
	}
	if bound, ok := AccountBoundEgress(ctx); ok {
		proxyURL = bound
	}
	if _, err := safePreflightExplicitProxy(proxyURL); err != nil {
		return true
	}
	return safePreflightSupportedClient(ResolveCodexRequestEgress(ctx, account, CodexBaseURL+"/responses", proxyURL, false).Client())
}

// Only the known x/net/http2 ConfigureTransports callbacks are accepted. The
// prototype is local configuration only: no request, dial or callback execution.
// Code IDs classify implementations, NOT closure/pool identity. In particular,
// a callback with this ID is never permission to use its captured original pool.
var safePreflightHTTP2Callbacks = sync.OnceValue(func() map[string]uintptr {
	t := &http.Transport{}
	h2, err := http2.ConfigureTransports(t)
	if err != nil || h2 == nil || t.TLSNextProto[http2.NextProtoTLS] == nil {
		return nil // a changed library implementation needs explicit review
	}
	ids := make(map[string]uintptr, len(t.TLSNextProto))
	for name, callback := range t.TLSNextProto {
		if callback == nil {
			return nil
		}
		ids[name] = reflect.ValueOf(callback).Pointer()
	}
	return ids
})

// V1 supports the production ConfigureTransports configuration and explicit H1
// transports, not custom protocol policy/HTTP2Config hooks or implicit bundled
// H2 initialization. The latter can mutate the source during Clone. Production
// newCodexStandardTransport explicitly configures x/net H2 and remains eligible.
func safePreflightProtocol(t *http.Transport) (string, error) {
	if t.Protocols != nil || t.HTTP2 != nil {
		return "", errSafePreflightLocal
	}
	h2 := len(t.TLSNextProto) != 0
	if h2 {
		known := safePreflightHTTP2Callbacks()
		if len(known) == 0 || len(t.TLSNextProto) != len(known) {
			return "", errSafePreflightLocal
		}
		for name, callback := range t.TLSNextProto {
			if callback == nil || known[name] == 0 || reflect.ValueOf(callback).Pointer() != known[name] {
				return "", errSafePreflightLocal
			}
		}
	} else if t.TLSNextProto == nil && (t.ForceAttemptHTTP2 || t.TLSClientConfig == nil && t.Dial == nil && t.DialContext == nil) {
		return "", errSafePreflightLocal
	}
	var hasH2, hasH1 bool
	if c := t.TLSClientConfig; c != nil {
		for _, protocol := range c.NextProtos {
			switch protocol {
			case "h2":
				hasH2 = true
			case "http/1.1":
				hasH1 = true
			default:
				return "", errSafePreflightLocal
			}
		}
	}
	if h2 {
		if !hasH2 || !hasH1 {
			return "", errSafePreflightLocal
		}
		return "x/net/http2", nil
	}
	if hasH2 {
		return "", errSafePreflightLocal
	}
	return "http/1.1", nil
}

type safePreflightSendContextKey struct{}

type safePreflightSendConfig struct {
	transport *http.Transport
	h2        *http2.Transport
	pool      http2.ClientConnPool
	callbacks map[string]func(string, *tls.Conn) http.RoundTripper
}

// This descriptor covers the actual owned sender, not the opaque configuration
// captured by the old pool. Only the production H2 defaults + keepalive are used.
func (c *safePreflightSendConfig) http2Config() ([]any, error) {
	h2 := c.h2
	if h2 == nil {
		return nil, nil
	}
	// The known ConfigureTransports noDialClientConnPool is comparable. Check
	// the type first, so an arbitrary custom pool cannot cause a comparison panic.
	if c.pool == nil || reflect.TypeOf(h2.ConnPool) != reflect.TypeOf(c.pool) || !reflect.TypeOf(c.pool).Comparable() || h2.ConnPool != c.pool ||
		h2.DialTLS != nil || h2.DialTLSContext != nil || h2.TLSClientConfig != nil || h2.CountError != nil ||
		h2.AllowHTTP || h2.DisableCompression || h2.StrictMaxConcurrentStreams || h2.MaxHeaderListSize != 0 ||
		h2.MaxReadFrameSize != 0 || h2.MaxDecoderHeaderTableSize != 0 || h2.MaxEncoderHeaderTableSize != 0 ||
		h2.IdleConnTimeout != 0 || h2.WriteByteTimeout != 0 {
		return nil, errSafePreflightLocal
	}
	return []any{h2.ReadIdleTimeout, h2.PingTimeout}, nil
}

// Hash the final send values, including genuine attestation produced by the
// ordinary header builder. Attestation is never frozen, minted or replayed here.
// The transport descriptor is CONFIGURATION evidence, not a physical egress IP
// proof. The actual Proxy callback is also checked by the cloned transport.
func (s *safePreflightState) guard(req *http.Request, body []byte, model string, account *auth.Account, egress CodexEgress, client *http.Client, consume bool) error {
	if req == nil || account == nil || egress.ViaResin() ||
		req.Header.Get(codexTurnStateHeader) != "" || req.Header.Get(codexTurnMetadataHeader) != "" ||
		!safePreflightRoute(req.Context(), account, egress.DialProxyURL) || !safePreflightSupportedClient(client) {
		return errSafePreflightLocal
	}
	// Recheck exactly the dispatch policy used by the shared scheduler. Spark
	// has its own quota window; ordinary account usage is not its health gate.
	available := account.IsAvailable()
	if s.dispatchPolicy == auth.DispatchPolicySpark {
		available = account.SparkDispatchEligible()
	}
	if !available {
		return errSafePreflightLocal
	}
	proxy, err := safePreflightExplicitProxy(egress.DialProxyURL)
	if err != nil {
		return err
	}
	if s.admit != nil && !s.admit(account, model, egress.DialProxyURL) {
		return errSafePreflightLocal
	}
	// Admission may have blocked. Validate protocol callbacks again here, rather
	// than interpreting an unknown map as ordinary H1 or disabling the feature.
	if !safePreflightSupportedClient(client) {
		return errSafePreflightLocal
	}
	t := client.Transport.(*http.Transport)
	protocol, err := safePreflightProtocol(t)
	if err != nil {
		return err
	}
	var h2Config []any
	if protocol == "x/net/http2" {
		h2Config = []any{codexHTTP2ReadIdleTimeout, codexHTTP2PingTimeout}
	}
	if consume {
		owned, _ := req.Context().Value(safePreflightSendContextKey{}).(*safePreflightSendConfig)
		if owned == nil || owned.transport != t || (owned.h2 != nil) != (protocol == "x/net/http2") || req.GetBody != nil {
			return errSafePreflightLocal
		}
		h2Config, err = owned.http2Config()
		if err != nil {
			return err
		}
		// Even a replacement closure with a KNOWN code ID may capture another
		// pool. Restore only our newly configured callbacks, never the source's.
		// Clone does not inherit altProto; ConfigureTransports registered our
		// independent no-dial H2 pool there as well (x/net v0.60.0).
		t.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper, len(owned.callbacks))
		for name, callback := range owned.callbacks {
			t.TLSNextProto[name] = callback
		}
	}
	// Function identities only detect local hook replacement; they do not prove
	// DNS/IP constancy. Pool/closure instance addresses MUST NOT pin retries.
	transportConfig := []any{fmt.Sprintf("%p", t.DialContext), fmt.Sprintf("%p", t.Dial), t.ProxyConnectHeader,
		protocol, h2Config, t.ForceAttemptHTTP2, t.DisableCompression, t.DisableKeepAlives, t.ResponseHeaderTimeout, t.TLSHandshakeTimeout,
		t.MaxResponseHeaderBytes, t.ExpectContinueTimeout, t.MaxConnsPerHost, t.MaxIdleConns, t.MaxIdleConnsPerHost,
		t.IdleConnTimeout, t.ReadBufferSize, t.WriteBufferSize, client.Timeout}
	var rootPool *x509.CertPool
	if tls := t.TLSClientConfig; tls != nil {
		rootPool = tls.RootCAs
		var roots [][]byte
		if tls.RootCAs != nil {
			roots = tls.RootCAs.Subjects()
		}
		transportConfig = append(transportConfig, tls.ServerName, tls.MinVersion, tls.MaxVersion, tls.NextProtos, tls.CipherSuites, tls.CurvePreferences, roots,
			tls.Renegotiation, tls.SessionTicketsDisabled, fmt.Sprintf("%p", tls.ClientSessionCache))
	}
	identity := struct {
		ID, Generation                 int64
		Method, URL, Host, Proxy, Mode string
		Headers, Trailers              http.Header
		Length                         int64
		Transfer                       []string
		Body                           []byte
		Transport                      []any
	}{account.ID(), account.GetCredentialGeneration(), req.Method, req.URL.String(), req.Host, proxy.String(), codexTransportModeFromEnv(),
		req.Header, req.Trailer, req.ContentLength, req.TransferEncoding, body, transportConfig}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return errSafePreflightLocal
	}
	digest := sha256.Sum256(encoded)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replayUnsafe || s.localFailure || s.writeFailed || s.revoked || s.sent || s.attempts < 1 || s.attempts > safePreflightMaxAttempts ||
		req.Context().Err() != nil || s.downstream != nil && s.downstream.Err() != nil || time.Since(s.started) >= safePreflightRetryWindow ||
		s.pinned && (!s.staged || s.digest != digest || !safePreflightRootsEqual(s.rootCAs, rootPool)) {
		return errSafePreflightLocal
	}
	s.digest, s.accountID, s.staged = digest, account.ID(), true
	if consume {
		s.sent = true
		s.rootCAs = nil
		if rootPool != nil {
			s.rootCAs = rootPool.Clone()
		}
	}
	return nil
}

func safePreflightRootsEqual(a, b *x509.CertPool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(b)
}

// Clone client/TLS/dial settings, but NEVER inherit an H2 sender. In x/net
// v0.60.0 ConfigureTransports installs closures capturing its t2/connPool;
// Transport.Clone copies those closures, not their pool. Discard them before
// registering a new pool with the same production keepalive policy.
func safePreflightClient(client *http.Client, req *http.Request, proxy *url.URL) (*http.Client, func(), error) {
	if req == nil || proxy == nil || !safePreflightSupportedClient(client) {
		return nil, nil, errSafePreflightLocal
	}
	source := client.Transport.(*http.Transport)
	protocol, err := safePreflightProtocol(source)
	if err != nil {
		return nil, nil, err
	}
	clone := *client
	t := source.Clone()
	t.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	if t.TLSClientConfig != nil {
		// tls.Config.Clone shares these slices. Detach them before configuring
		// ALPN and before any final guard; no original-pool configuration changes.
		t.TLSClientConfig.NextProtos = slices.Clone(t.TLSClientConfig.NextProtos)
		t.TLSClientConfig.CipherSuites = slices.Clone(t.TLSClientConfig.CipherSuites)
		t.TLSClientConfig.CurvePreferences = slices.Clone(t.TLSClientConfig.CurvePreferences)
		if t.TLSClientConfig.RootCAs != nil {
			t.TLSClientConfig.RootCAs = t.TLSClientConfig.RootCAs.Clone()
		}
	}
	owned := &safePreflightSendConfig{transport: t}
	if protocol == "x/net/http2" {
		owned.h2, err = http2.ConfigureTransports(t)
		if err != nil || owned.h2 == nil {
			return nil, nil, errSafePreflightLocal // no silent H1 fallback
		}
		owned.h2.ReadIdleTimeout = codexHTTP2ReadIdleTimeout
		owned.h2.PingTimeout = codexHTTP2PingTimeout
		owned.pool = owned.h2.ConnPool
	}
	owned.callbacks = make(map[string]func(string, *tls.Conn) http.RoundTripper, len(t.TLSNextProto))
	for name, callback := range t.TLSNextProto {
		owned.callbacks[name] = callback
	}
	resolve := t.Proxy
	expectedProxy := proxy.String()
	t.Proxy = func(r *http.Request) (*url.URL, error) {
		actual, err := resolve(r)
		if err != nil || actual == nil || actual.String() != expectedProxy {
			// net/http resolves Proxy before getConn/dial. This is a proven
			// local rejection, not an upstream attempt for usage/request counts.
			// Keep the one-shot permit revoked even though no packet was sent.
			if state := safePreflightFromContext(r.Context()); state != nil {
				state.mu.Lock()
				state.sent = false
				state.revoked, state.localFailure, state.replayUnsafe = true, true, true
				state.mu.Unlock()
			}
			return nil, errSafePreflightLocal
		}
		return actual, nil
	}
	clone.Transport = t
	clone.Jar = nil
	clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	// x/net v0.60.0 shouldRetryRequest cannot replay a written nonempty POST
	// without GetBody. The unread errClientConnUnusable case is not a replay.
	req.GetBody = nil
	*req = *req.WithContext(context.WithValue(req.Context(), safePreflightSendContextKey{}, owned))
	cleanup := func() {
		// Explicitly close OUR H2 pool, independent of net/http's lazy internal
		// h2transport discovery. Neither operation touches the source's pool.
		if owned.h2 != nil {
			owned.h2.CloseIdleConnections()
		}
		t.CloseIdleConnections()
	}
	return &clone, cleanup, nil
}

type safePreflightResponseBody struct {
	io.ReadCloser
	closeIdle func()
}

func (b *safePreflightResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.closeIdle()
	return err
}
func (b *safePreflightResponseBody) observeUpstreamCookieEvent(data []byte) {
	if o, ok := b.ReadCloser.(upstreamCookieEventObserver); ok {
		o.observeUpstreamCookieEvent(data)
	}
}

// The existing keepalive executor may abandon a worker. Revoke its permit and
// join it before the Handler can release the account/scope or start a new one.
// The upstream context still uses the ordinary five-second usage drain.
func (s *safePreflightState) execute(ctx context.Context, cancel context.CancelFunc, execute func() (*http.Response, error)) (*http.Response, error) {
	if s == nil {
		return executeHTTPWithContinuousRetryKeepalive(ctx, execute)
	}
	s.mu.Lock()
	rejected := s.revoked || s.localFailure || s.writeFailed || s.replayUnsafe || ctx.Err() != nil ||
		s.downstream != nil && s.downstream.Err() != nil || time.Since(s.started) >= safePreflightRetryWindow
	s.mu.Unlock()
	if rejected {
		return nil, errSafePreflightLocal
	}
	done := make(chan struct{})
	var workerErr error
	resp, err := executeHTTPWithContinuousRetryKeepalive(ctx, func() (*http.Response, error) {
		defer close(done)
		response, executeErr := execute()
		workerErr = executeErr
		return response, executeErr
	})
	s.mu.Lock()
	s.revoked = true
	wasSent := s.sent
	s.mu.Unlock()
	if err != nil && !wasSent {
		cancel()
	}
	// An in-flight send retains the original drain context. A heartbeat write
	// failure cancels its downstream parent; that parent's existing five-second
	// timer, not a new experiment timeout, bounds the join.
	<-done
	// The join publishes the worker's final quota result even if keepalive
	// returned early. Preserve that error, not an abandoned response/body;
	// the wrapper still owns its cleanup. Handler keeps downstream 499 priority.
	if apiKeyModelRequestError(workerErr) != nil {
		err = workerErr
	}
	return resp, err
}

// Called exclusively by the Handler/read-pump writer goroutine (and under its
// downstreamMu for frames). The keepalive has the same writer ownership. A
// partial write closes the replay window and cancels downstream writes while
// preserving the existing upstream drain context.
func (s *safePreflightState) write(c *gin.Context, wire []byte) error {
	if c.Request.Context().Err() != nil {
		return context.Canceled
	}
	n, err := c.Writer.Write(wire)
	if err == nil && n != len(wire) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.failedWrite(c)
		return err
	}
	c.Writer.Flush()
	return nil
}
func (s *safePreflightState) failedWrite(c *gin.Context) {
	s.mu.Lock()
	s.writeFailed, s.replayUnsafe = true, true
	s.mu.Unlock()
	if k, ok := continuousRetryKeepaliveForContext(c.Request.Context()).(*requestContinuousRetryKeepalive); ok && k.cancel != nil {
		k.cancel(io.ErrClosedPipe)
	}
}

// writeResponsesStreamBreakEvent checks the post-content synthetic terminal at
// its actual stream-writer boundary. Earlier buffered bytes count toward the
// required total, but scanner output, a partial write or an unflushed buffer is
// never evidence of complete delivery. The guarded writer owns short/error and
// prewrite-cancel checks. No upstream terminal/usage/payload is manufactured.
func (s *safePreflightState) writeResponsesStreamBreakEvent(w *streamFlushWriter) error {
	payload := responsesStreamBreakPayload(time.Now().Unix())
	before := w.deliveredBytes()
	wanted := int64(w.buffer.Len()) + int64(len(sseDataPrefix)) + int64(len(payload)) + int64(len(sseDataSuffix))
	unfiltered := w.outputScanner == nil && len(w.scanCarry) == 0
	if err := w.WriteSSEData(payload); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if unfiltered && w.deliveredBytes()-before == wanted && w.buffer.Len() == 0 &&
		w.outputScanner == nil && len(w.scanCarry) == 0 && w.flusher != nil {
		// Flush has returned. A cancellation during that completed local
		// write/flush must not erase it; this is still not a client ACK.
		s.noteTerminalWrittenAndFlushed("response.failed")
	}
	return nil
}

type safePreflightWriter struct {
	state  *safePreflightState
	c      *gin.Context
	writer io.Writer
}

func (w safePreflightWriter) Write(p []byte) (int, error) {
	// A coalesced frame may have been accepted before cancellation, but must
	// not be written later by the terminal/final flush during the usage drain.
	if err := w.c.Request.Context().Err(); err != nil {
		return 0, err
	}
	n, err := w.writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.state.failedWrite(w.c)
	}
	return n, err
}
func (s *safePreflightState) installKeepalive(c *gin.Context) {
	if k, ok := continuousRetryKeepaliveForContext(c.Request.Context()).(*requestContinuousRetryKeepalive); ok {
		k.mu.Lock()
		k.write = func() error {
			setSSEStreamHeaders(c, "text/event-stream")
			return s.write(c, []byte(continuousRetryKeepaliveComment))
		}
		k.mu.Unlock()
	}
}

// safePreflightFinalFailureWriter checks ONLY the selected final failure render.
// It delegates Gin's status/header bookkeeping and the shared renderer's wire
// format. It is not a renderer, a second serve loop, or a client ACK mechanism.
// The Handler owns it synchronously after the read pump has joined.
type safePreflightFinalFailureWriter struct {
	gin.ResponseWriter
	state            *safePreflightState
	c                *gin.Context
	err              error
	written, flushed int64
}

func (s *safePreflightState) captureFinalFailure(c *gin.Context) *safePreflightFinalFailureWriter {
	s.lockReplay() // The selected final render can never reopen retry admission.
	w := &safePreflightFinalFailureWriter{ResponseWriter: c.Writer, state: s, c: c}
	c.Writer = w
	return w
}

func (w *safePreflightFinalFailureWriter) beforeWrite() error {
	if w.err != nil {
		return w.err
	}
	w.state.mu.Lock()
	failed := w.state.writeFailed
	w.state.mu.Unlock()
	if failed {
		w.err = io.ErrClosedPipe
	} else {
		w.err = w.c.Request.Context().Err()
	}
	if w.err == nil {
		w.state.commit()
	}
	return w.err
}

func (w *safePreflightFinalFailureWriter) checkedWrite(n, want int, err error) (int, error) {
	if err == nil && n != want {
		err = io.ErrShortWrite
	}
	if err != nil {
		// Keep opaque transport errors out of final usage/access diagnostics.
		w.err = io.ErrClosedPipe
		w.state.failedWrite(w.c)
	} else {
		w.written += int64(n)
	}
	return n, err
}

func (w *safePreflightFinalFailureWriter) Write(p []byte) (int, error) {
	if err := w.beforeWrite(); err != nil {
		return 0, err
	}
	n, err := w.ResponseWriter.Write(p)
	return w.checkedWrite(n, len(p), err)
}

func (w *safePreflightFinalFailureWriter) WriteString(p string) (int, error) {
	if err := w.beforeWrite(); err != nil {
		return 0, err
	}
	n, err := w.ResponseWriter.WriteString(p)
	return w.checkedWrite(n, len(p), err)
}

func (w *safePreflightFinalFailureWriter) Flush() {
	if w.err != nil || w.written == 0 {
		return // Never flush a failed/short write as though it were delivery.
	}
	w.ResponseWriter.Flush()
	w.flushed = w.written
	// Do not reread ctx here: a complete local write followed by a returned
	// void Flush remains evidence even if cancellation arrived during Flush.
}

func (w *safePreflightFinalFailureWriter) finish(outcome streamOutcome) streamOutcome {
	w.c.Writer = w.ResponseWriter
	w.state.mu.Lock()
	failed := w.state.writeFailed
	complete := !failed && w.err == nil && w.written > 0 && w.flushed == w.written
	if complete {
		// Unlike forward delivery, this also covers the SELECTED local failure
		// and stream break: neither needs a genuine upstream failure payload.
		w.state.failedTerminalWrittenAndFlushed = true
	}
	w.state.mu.Unlock()
	if failed || w.err != nil {
		return classifyStreamOutcome(nil, nil, io.ErrClosedPipe, false)
	}
	if complete {
		return outcome // Preserve semantic 502/598/etc., never manufacture 200.
	}
	// No local delivery evidence: a renderer that skipped a canceled request
	// must still become 499 before usage/access/ordinary health finalization.
	return safePreflightOverlay(w.state, w.c, outcome, nil, nil)
}

// Only prepares HTTP framing; the shared error renderer owns the one terminal.
func (s *safePreflightState) prepareFailure(c *gin.Context, status int) {
	if s == nil || c.Request.Context().Err() != nil || c.Writer.Written() {
		return
	}
	if status < 400 || status >= 598 {
		status = http.StatusBadGateway
	}
	c.Writer.Header().Del(codexTurnStateHeader)
	setSSEStreamHeaders(c, "text/event-stream")
	c.Status(status)
	c.Writer.WriteHeaderNow()
}

// Raw envelope classification happens before normalization; ordinary events
// then undergo EXACTLY the legacy terminal normalization and cookie observer.
// The synchronous ack in the shared pump keeps observation ahead of retries.
func (s *safePreflightState) read(ctx context.Context, body io.Reader, callback func(safePreflightSSEFrame, string, []byte) bool) error {
	type observation struct {
		frame  safePreflightSSEFrame
		unsafe bool
	}
	err := readStreamWithContinuousRetryKeepalive(ctx, func(yield func(observation) bool) error {
		return readSafePreflightSSEWithCommit(body, func() { yield(observation{unsafe: true}) }, func(f safePreflightSSEFrame) bool {
			return yield(observation{frame: f})
		}, s.committed)
	}, func(o observation) bool {
		if o.unsafe {
			s.localError()
			return true
		}
		f := o.frame
		if bytes.Equal(f.data, []byte("[DONE]")) {
			s.localError()
			return false
		}
		event, data := f.event, f.data
		if !f.isRateLimits() {
			event, data = normalizeUpstreamResponsesTerminalEvent(event, data)
		}
		if observer, ok := body.(upstreamCookieEventObserver); ok {
			observer.observeUpstreamCookieEvent(data)
		}
		return callback(f, event, data)
	})
	s.mu.Lock()
	local := s.localFailure
	s.mu.Unlock()
	if local {
		return errSafePreflightLocal
	}
	return err
}

// Strict duplicate-aware JSON is used ONLY for bounded precommit evidence.
func safePreflightJSON(data []byte) (map[string]any, bool) {
	if len(data) == 0 || len(data) > safePreflightSSEMaxEnvelopeBytes || !utf8.Valid(data) {
		return nil, false
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var value func(int) (any, bool)
	value = func(depth int) (any, bool) {
		if depth > 16 {
			return nil, false
		}
		t, err := d.Token()
		if err != nil {
			return nil, false
		}
		delim, container := t.(json.Delim)
		if !container {
			return t, true
		}
		switch delim {
		case '{':
			m := map[string]any{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, false
				}
				key, ok := k.(string)
				if !ok {
					return nil, false
				}
				if _, exists := m[key]; exists {
					return nil, false
				}
				v, ok := value(depth + 1)
				if !ok {
					return nil, false
				}
				m[key] = v
			}
			end, err := d.Token()
			return m, err == nil && end == json.Delim('}')
		case '[':
			a := []any{}
			for d.More() {
				v, ok := value(depth + 1)
				if !ok {
					return nil, false
				}
				a = append(a, v)
			}
			end, err := d.Token()
			return a, err == nil && end == json.Delim(']')
		default:
			return nil, false
		}
	}
	v, ok := value(0)
	if !ok {
		return nil, false
	}
	m, ok := v.(map[string]any)
	_, err := d.Token()
	return m, ok && err == io.EOF
}
func safePreflightKeys(m map[string]any, allowed string) bool {
	for k := range m {
		found := false
		for _, name := range strings.Fields(allowed) {
			if k == name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func safePreflightEmpty(v any) bool {
	if v == nil {
		return true
	}
	a, ok := v.([]any)
	return ok && len(a) == 0
}
func safePreflightRequestBody(body []byte) bool {
	m, ok := safePreflightJSON(body)
	if !ok || !safePreflightKeys(m, "model input instructions stream store reasoning reasoning_effort service_tier prompt_cache_key include text parallel_tool_calls tools") || !safePreflightEmpty(m["tools"]) {
		return false
	}
	if v := m["reasoning"]; v != nil {
		r, ok := v.(map[string]any)
		if !ok || !safePreflightKeys(r, "effort summary") {
			return false
		}
		for _, value := range r {
			if value != nil {
				if _, ok := value.(string); !ok {
					return false
				}
			}
		}
	}
	if v := m["include"]; v != nil {
		items, ok := v.([]any)
		if !ok {
			return false
		}
		for _, item := range items {
			if item != "reasoning.encrypted_content" {
				return false
			}
		}
	}
	if v := m["text"]; v != nil {
		text, ok := v.(map[string]any)
		if !ok || !safePreflightKeys(text, "format verbosity") {
			return false
		}
		if value := text["verbosity"]; value != nil {
			if _, ok := value.(string); !ok {
				return false
			}
		}
		if value := text["format"]; value != nil {
			format, ok := value.(map[string]any)
			if !ok || !safePreflightKeys(format, "type") || (format["type"] != "text" && format["type"] != "json_object") {
				return false
			}
		}
	}
	if _, ok := m["input"].(string); ok {
		return true
	}
	items, ok := m["input"].([]any)
	if !ok {
		return false
	}
	for _, v := range items {
		item, ok := v.(map[string]any)
		if !ok || !safePreflightKeys(item, "type role content") {
			return false
		}
		if typ, exists := item["type"]; exists && typ != "message" {
			return false
		}
		switch item["role"] {
		case "user", "assistant", "system", "developer":
		default:
			return false
		}
		if _, ok := item["content"].(string); ok {
			continue
		}
		parts, ok := item["content"].([]any)
		if !ok {
			return false
		}
		for _, part := range parts {
			p, ok := part.(map[string]any)
			if !ok || !safePreflightKeys(p, "type text") || (p["type"] != "input_text" && p["type"] != "output_text") {
				return false
			}
			if _, ok := p["text"].(string); !ok {
				return false
			}
		}
	}
	return true
}

// Never erase a present malformed raw control field into event-only fallback.
func safePreflightRawEvent(m map[string]any, fallback string) (string, bool) {
	raw, present := m["type"]
	if !present {
		return fallback, fallback != "" // Event-only compatibility is ABSENT-only.
	}
	typ, ok := raw.(string)
	return typ, ok && typ != ""
}

// Full ordinary Response echoes are private, not public-safe. Scalar request
// settings and understood nested echo objects cannot execute. Output, tool
// declarations, continuation IDs, unknown nested control fields close the window.
func safePreflightPrivateFrame(m map[string]any, event string) bool {
	originalEvent, ok := safePreflightRawEvent(m, event)
	return ok && safePreflightPrivateFrameWithEvent(m, originalEvent, event)
}

// originalEvent comes from the raw envelope, before terminal normalization. In
// particular an event-only response.done must not be mistaken for raw failed.
func safePreflightPrivateFrameWithEvent(m map[string]any, originalEvent, event string) bool {
	// Validate before metadata/error fast paths. A present invalid/unknown
	// raw type cannot borrow the explicit SSE event or a normalized alias.
	typ, ok := safePreflightRawEvent(m, originalEvent)
	if !ok || typ != originalEvent {
		return false
	}
	if originalEvent != event {
		if event != "response.failed" {
			return false
		}
		switch originalEvent {
		case "response.done", "response.completed", "response.incomplete":
		default:
			return false
		}
	}
	if event == codexResponseMetadataFrameType || event == responseMetadataFrameType {
		return safePreflightPrivateHeaders(m)
	}
	if !safePreflightKeys(m, "type sequence_number response error") {
		return false
	}
	if n, exists := m["sequence_number"]; exists {
		if _, ok := n.(json.Number); !ok {
			return false
		}
	}
	if event == "error" {
		_, hasResponse := m["response"]
		return !hasResponse && safePreflightErrorObject(m["error"])
	}
	if m["error"] != nil {
		return false
	}
	r, ok := m["response"].(map[string]any)
	if !ok || !safePreflightKeys(r, "id object created_at status model output error usage incomplete_details tools instructions max_output_tokens max_tool_calls parallel_tool_calls tool_choice temperature top_p presence_penalty frequency_penalty reasoning text truncation metadata store background service_tier previous_response_id prompt_cache_key prompt_cache_retention safety_identifier user completed_at") ||
		!safePreflightEmpty(r["output"]) || !safePreflightEmpty(r["tools"]) || r["previous_response_id"] != nil || !safePreflightZeroOutputUsage(r["usage"]) {
		return false
	}
	for _, key := range []string{"id", "object", "model", "instructions", "truncation", "service_tier", "prompt_cache_key", "prompt_cache_retention", "safety_identifier", "user"} {
		if v := r[key]; v != nil {
			if _, ok := v.(string); !ok {
				return false
			}
		}
	}
	for _, key := range []string{"created_at", "completed_at", "max_output_tokens", "max_tool_calls", "temperature", "top_p", "presence_penalty", "frequency_penalty"} {
		if v := r[key]; v != nil {
			if _, ok := v.(json.Number); !ok {
				return false
			}
		}
	}
	for _, key := range []string{"store", "parallel_tool_calls", "background"} {
		if v := r[key]; v != nil {
			if _, ok := v.(bool); !ok {
				return false
			}
		}
	}
	if r["background"] == true {
		return false
	}
	if v := r["tool_choice"]; v != nil && v != "auto" && v != "none" {
		return false
	}
	if v := r["reasoning"]; v != nil {
		v, ok := v.(map[string]any)
		if !ok || !safePreflightKeys(v, "effort summary") {
			return false
		}
		for _, value := range v {
			if value != nil {
				if _, ok := value.(string); !ok {
					return false
				}
			}
		}
	}
	if v := r["text"]; v != nil {
		v, ok := v.(map[string]any)
		if !ok || !safePreflightKeys(v, "format verbosity") {
			return false
		}
		if verbosity := v["verbosity"]; verbosity != nil {
			if _, ok := verbosity.(string); !ok {
				return false
			}
		}
		if format := v["format"]; format != nil {
			f, ok := format.(map[string]any)
			if !ok || !safePreflightKeys(f, "type") || (f["type"] != "text" && f["type"] != "json_object") {
				return false
			}
		}
	}
	if v := r["metadata"]; v != nil {
		v, ok := v.(map[string]any)
		if !ok {
			return false
		}
		for _, value := range v {
			if _, ok := value.(string); !ok {
				return false
			}
		}
	}
	if v := r["incomplete_details"]; v != nil {
		v, ok := v.(map[string]any)
		if !ok || !safePreflightKeys(v, "reason") {
			return false
		}
		if _, ok := v["reason"].(string); !ok {
			return false
		}
	}
	if event == "response.failed" {
		return safePreflightPrivateFailedStatus(originalEvent, r["status"], event) && safePreflightErrorObject(r["error"])
	}
	if event == "response.incomplete" {
		return r["status"] == "incomplete" && r["error"] == nil
	}
	return (event == "response.created" || event == "response.in_progress") &&
		(r["status"] == nil || r["status"] == "queued" || r["status"] == "in_progress") && r["error"] == nil && r["incomplete_details"] == nil
}

// Only these raw event/status combinations may remain private after failure
// normalization. Missing/null status is explicitly compatible, but only inside
// a known terminal with the understood error/output/usage checks above. A
// structured error does NOT make an unknown status safe for replay.
func safePreflightPrivateFailedStatus(originalEvent string, status any, normalizedEvent string) bool {
	if normalizedEvent != "response.failed" {
		return false
	}
	switch originalEvent {
	case "response.failed", "response.done", "response.completed", "response.incomplete":
	default:
		return false
	}
	switch value := status.(type) {
	case nil:
		return true // Absence and explicit null carry no unknown control data.
	case string:
		value = strings.ToLower(strings.TrimSpace(value))
		switch value {
		case "failed", "error", "cancelled", "canceled":
			return true
		case "completed", "incomplete":
			// The legacy normalizer explicitly maps these terminal aliases to
			// failed when error evidence exists, retaining the ORIGINAL status.
			// Raw response.failed has no such normalization/compatibility rule.
			return originalEvent != "response.failed"
		default:
			code, err := strconv.Atoi(value)
			return err == nil && code >= 400 && code < 600
		}
	case json.Number:
		// Keep the original lexeme: decimal/exponent spellings and fractions
		// rounded into range are not conservative private-replay evidence.
		_, ok := safePreflightSchemaInteger(value, 400, 599)
		return ok
	default:
		return false // Objects, arrays, booleans and unknown value types.
	}
}

// Only documented, inert transport/quota headers may stay PRIVATE before
// commit. This does not authorize early publication, nor manufacture turn-state
// metadata when HTTP headers have already been committed by rate/heartbeat.
func safePreflightPrivateHeaders(m map[string]any) bool {
	if !safePreflightKeys(m, "type sequence_number headers") {
		return false
	}
	if n, exists := m["sequence_number"]; exists {
		if _, ok := n.(json.Number); !ok {
			return false
		}
	}
	headers, ok := m["headers"].(map[string]any)
	if !ok || len(headers) == 0 || len(headers) > 32 {
		return false
	}
	seen := make(map[string]bool, len(headers))
	for key, value := range headers {
		name := strings.ToLower(key)
		if seen[name] {
			return false
		}
		seen[name] = true
		if !isCodexUsageFrameHeader(name) {
			switch name {
			case "x-codex-turn-state", "x-models-etag", "x-request-id", "cf-ray":
			default:
				return false
			}
		}
		values := []any{value}
		if a, ok := value.([]any); ok {
			values = a
		}
		if len(values) == 0 || len(values) > 4 {
			return false
		}
		for _, v := range values {
			text, ok := v.(string)
			if !ok || len(text) > 16<<10 || strings.ContainsAny(text, "\r\n\x00") {
				return false
			}
		}
	}
	return true
}

// Input/cache accounting alone is not execution evidence. The shared empty
// incomplete repair can retry only when every output/reasoning counter is zero.
func safePreflightZeroOutputUsage(v any) bool {
	if v == nil {
		return true
	}
	m, ok := v.(map[string]any)
	if !ok || !safePreflightKeys(m, "input_tokens output_tokens total_tokens input_tokens_details output_tokens_details") {
		return false
	}
	if m["output_tokens"] != json.Number("0") {
		return false
	}
	for _, k := range []string{"input_tokens", "total_tokens"} {
		if n := m[k]; n != nil {
			if _, ok := n.(json.Number); !ok {
				return false
			}
		}
	}
	for key, allowed := range map[string]string{"input_tokens_details": "cached_tokens", "output_tokens_details": "reasoning_tokens"} {
		if v := m[key]; v != nil {
			details, ok := v.(map[string]any)
			if !ok || !safePreflightKeys(details, allowed) {
				return false
			}
			for _, n := range details {
				if _, ok := n.(json.Number); !ok {
					return false
				}
				if key == "output_tokens_details" && n != json.Number("0") {
					return false
				}
			}
		}
	}
	return true
}

func safePreflightErrorObject(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 || !safePreflightKeys(m, "code type message param status_code") {
		return false
	}
	for k, v := range m {
		if k == "status_code" {
			if _, ok := v.(json.Number); !ok {
				return false
			}
			continue
		}
		if v != nil {
			if _, ok := v.(string); !ok {
				return false
			}
		}
	}
	return true
}

// Only observation changes replay eligibility. It must precede ANY shared
// buffering/candidate/suppression decision. Successful terminals commit too.
func (s *safePreflightState) observe(f safePreflightSSEFrame, event string, data []byte) {
	if s == nil || s.retryUnsafe() || f.isRateLimits() {
		return
	}
	m, ok := safePreflightJSON(f.data)
	if !ok || f.hasComment {
		s.lockReplay()
		return
	}
	typ, ok := safePreflightRawEvent(m, f.event)
	if !ok || f.event != "" && f.event != typ {
		s.lockReplay()
		return
	}
	// Inspect raw fields AND raw event/status semantics. Only explicitly known
	// combinations may use the legacy done/completed-with-failure mapping.
	if !safePreflightPrivateFrameWithEvent(m, typ, normalizedUpstreamSSEEventType(event, data)) {
		s.lockReplay()
	}
}

func safePreflightOverlay(s *safePreflightState, c *gin.Context, outcome streamOutcome, readErr, writeErr error) streamOutcome {
	if s == nil {
		return outcome
	}
	s.mu.Lock()
	delivered := s.successTerminalWrittenAndFlushed && outcome.logStatusCode == http.StatusOK ||
		s.failedTerminalWrittenAndFlushed && outcome.logStatusCode >= http.StatusBadRequest
	writeFailed := s.writeFailed
	s.mu.Unlock()
	// Actual downstream write errors always win, even over a local semantic
	// failure or deadline. Keep opaque writer errors out of diagnostics.
	if writeFailed || writeErr != nil {
		return classifyStreamOutcome(nil, nil, io.ErrClosedPipe, false)
	}
	// The Handler supplies the SEMANTIC outcome, never a provisional terminal
	// 200. Delivery also covers the synthetic in-stream break and selected
	// local failures: neither requires a genuine upstream failurePayload.
	// Preserve the actual EOF/readErr/local classification, not a blanket 598.
	// Drain-only observation is not delivery. A shared final renderer still
	// checks its own write/flush before accounting and ordinary health.
	ctxErr := continuousRetryContextError(c.Request.Context())
	if ctxErr != nil && !delivered {
		return classifyStreamOutcome(ctxErr, nil, nil, false)
	}
	if errors.Is(readErr, errSafePreflightLocal) {
		return streamOutcome{logStatusCode: http.StatusBadGateway, failureKind: "local", failureMessage: errSafePreflightLocal.Error(), terminalLocal: true}
	}
	return outcome
}
