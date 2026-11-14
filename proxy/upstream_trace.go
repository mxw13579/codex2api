package proxy

import (
	"context"
	"net/http"
	"sync"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/codex2api/security"
	"github.com/gin-gonic/gin"
)

type upstreamTraceContextKey struct{}
type upstreamTraceAttempt struct {
	accountID int64
	requestID string
	proxy     auth.ProxyAuditLabel
	// upstreamTurnState 是上游响应回带的 X-Codex-Turn-State，空串表示没有。
	upstreamTurnState string
}

type upstreamTraceSnapshot struct {
	RequestID           string
	accountID           int64
	UpstreamRequestID   string
	Proxy               auth.ProxyAuditLabel
	InjectedTurnState   string
	UpstreamTurnState   string
	UpstreamCookieTrace string
}

func snapshotUpstreamTrace(ctx context.Context, retainCookie bool) upstreamTraceSnapshot {
	a := upstreamTraceFromContext(ctx)
	if a == nil {
		return upstreamTraceSnapshot{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	result := upstreamTraceSnapshot{RequestID: a.requestID}
	if retainCookie {
		result.UpstreamCookieTrace = SnapshotUpstreamCookieLog(ctx)
	}
	if a.current != nil {
		result.accountID = a.current.accountID
		result.UpstreamRequestID = a.current.requestID
		result.Proxy = a.current.proxy
		result.UpstreamTurnState = a.current.upstreamTurnState
	}
	return result
}

func (s upstreamTraceSnapshot) apply(input *database.UsageLogInput) {
	input.RequestID = s.RequestID
	input.UpstreamCookieTrace = s.UpstreamCookieTrace
	if s.accountID == input.AccountID {
		input.UpstreamRequestID = s.UpstreamRequestID
		input.UpstreamProxyID = s.Proxy.ID
		input.UpstreamProxyName = s.Proxy.Name
		input.InjectedTurnState = s.InjectedTurnState
		input.UpstreamTurnState = s.UpstreamTurnState
	}
}

type upstreamTraceAudit struct {
	mu        sync.Mutex
	requestID string
	store     *auth.Store
	current   *upstreamTraceAttempt
}

func upstreamTraceFromContext(ctx context.Context) *upstreamTraceAudit {
	if ctx == nil {
		return nil
	}
	a, _ := ctx.Value(upstreamTraceContextKey{}).(*upstreamTraceAudit)
	return a
}

func attachUpstreamTrace(c *gin.Context, store *auth.Store) {
	if c == nil || c.Request == nil {
		return
	}
	a := &upstreamTraceAudit{requestID: NewUpstreamSessionUUID(), store: store}
	c.Request = c.Request.WithContext(context.WithValue(WithUpstreamCookieLog(c.Request.Context()), upstreamTraceContextKey{}, a))
	c.Header("X-Codex2API-Request-ID", a.requestID)
}

func resetUpstreamRequestTrace(c *gin.Context) {
	if c == nil || c.Request == nil {
		return
	}
	if a := upstreamTraceFromContext(c.Request.Context()); a != nil {
		a.mu.Lock()
		a.requestID = NewUpstreamSessionUUID()
		a.current = nil
		a.mu.Unlock()
	}
	c.Request = c.Request.WithContext(WithUpstreamCookieLog(c.Request.Context()))
}

func resetUpstreamAttemptTrace(ctx context.Context) {
	if a := upstreamTraceFromContext(ctx); a != nil {
		a.mu.Lock()
		a.current = nil
		a.mu.Unlock()
	}
}

func beginUpstreamTrace(ctx context.Context, account *auth.Account, proxyURL string, ws bool) func(*http.Response) {
	recordUpstreamAttempt(ctx)
	a := upstreamTraceFromContext(ctx)
	if a == nil || account == nil {
		return func(*http.Response) {}
	}
	boundProxy, accountBound := AccountBoundEgress(ctx)
	if accountBound {
		proxyURL = boundProxy
	}
	label := a.store.ProxyAuditForURL(proxyURL)
	if !accountBound && ws && proxyURL == "" {
		label = auth.ProxyAuditLabel{Name: "unknown"}
	}
	if !accountBound && resinCarriesEgress(account) {
		label = auth.ProxyAuditLabel{Name: "resin"}
	}
	label.Name = security.MaskSensitiveData(label.Name)
	attempt := &upstreamTraceAttempt{accountID: account.ID(), proxy: label}
	a.mu.Lock()
	a.current = attempt
	a.mu.Unlock()
	header := account.GetUpstreamRequestIDHeader()
	return func(resp *http.Response) {
		if resp == nil || ws {
			return
		} // A WS handshake ID is not a per-turn ID; WS turn state arrives per frame, see ObserveCodexTurnStateFrame.
		turnState := observedCodexTurnState(resp.Header.Get(codexTurnStateHeader))
		id := upstreamResponseRequestID(resp.Header, header)
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.current == attempt {
			attempt.requestID = id
			if turnState != "" {
				attempt.upstreamTurnState = turnState
			}
		}
	}
}

// noteUpstreamTurnState 把上游回带的 turn state 记到当前尝试上；WS 路径逐帧调用，
// 后到的值覆盖先到的。
func noteUpstreamTurnState(ctx context.Context, state string) {
	a := upstreamTraceFromContext(ctx)
	if a == nil || state == "" {
		return
	}
	a.mu.Lock()
	if a.current != nil {
		a.current.upstreamTurnState = state
	}
	a.mu.Unlock()
}

func doTracedUpstreamRequest(client *http.Client, req *http.Request, account *auth.Account, proxyURL string, diagnosticBody ...[]byte) (*http.Response, error) {
	requestID := cookieLogRequestID(req.Context()) // freeze before a later turn can reset the shared trace
	record := beginUpstreamTrace(req.Context(), account, proxyURL, false)
	resp, err := client.Do(req)
	record(resp)
	sentRequest := req
	if resp != nil && resp.Request != nil {
		sentRequest = resp.Request
		if resp.StatusCode >= 400 && len(diagnosticBody) > 0 && !sameUpstreamDiagnosticRequest(req, sentRequest) {
			// 真正重定向可能改变正文；HTTP/2 内部的浅拷贝不是重定向。
			diagnosticBody = nil
		}
	}
	if trace := NewUpstreamCookieTrace(req.Context(), account, "http", "", sentRequest.Header, resp); trace != nil && resp != nil && resp.Body != nil {
		resp.Body = &upstreamCookieBody{ReadCloser: resp.Body, trace: trace}
	}
	if diagnostic := wrapUpstreamHTTPDiagnostic(sentRequest, resp, account, diagnosticBody...); diagnostic != nil {
		// No caller can read this response yet. Bind correlation to this exact
		// response, not the mutable current attempt consulted at log-write time.
		diagnostic.diagnostic.RequestID = requestID
	}
	return resp, err
}

// sameUpstreamDiagnosticRequest accepts net/http's internal request copies,
// including setupRewindBody on a reused HTTP/2 connection. The client records a
// real redirect in Request.Response, even when it redirects to the same URL.
// Keep checking wire metadata too; never attach the first-hop plaintext to a
// changed destination, method, or encoding. This does not read either Body.
func sameUpstreamDiagnosticRequest(original, sent *http.Request) bool {
	if original == nil || sent == nil {
		return false
	}
	if original == sent {
		return true
	}
	return original.Response == sent.Response &&
		original.URL != nil && sent.URL != nil && original.URL.String() == sent.URL.String() &&
		original.Method == sent.Method && original.Host == sent.Host &&
		original.ContentLength == sent.ContentLength &&
		original.Header.Get("Content-Encoding") == sent.Header.Get("Content-Encoding")
}

func populateUpstreamTrace(c *gin.Context, input *database.UsageLogInput, retainCookie bool) {
	if c == nil || c.Request == nil || input == nil {
		return
	}
	if input.RequestID != "" {
		return
	} // Hidden continuation rounds carry their own snapshot.
	a := upstreamTraceFromContext(c.Request.Context())
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	input.RequestID = a.requestID
	if retainCookie {
		input.UpstreamCookieTrace = SnapshotUpstreamCookieLog(c.Request.Context())
	}
	if current := a.current; current != nil && current.accountID == input.AccountID {
		input.UpstreamRequestID = current.requestID
		input.UpstreamProxyID = current.proxy.ID
		input.UpstreamProxyName = current.proxy.Name
		input.UpstreamTurnState = current.upstreamTurnState
	}
}
