package proxy

// All traffic is synthetic and loopback-only. Success uses a real standard
// Transport configured by the production constructor, TLS with a private root,
// an explicit CONNECT proxy, and a real EnableHTTP2 server. No fake RoundTripper
// is used. The existing H1 acceptance harness supplies only Handler/store setup;
// its upstream is forbidden here. These tests must not run in parallel.
import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/database"
)

type spHTTP2Fixture struct {
	h         *spAcceptanceHarness
	client    *http.Client
	transport *http.Transport
	proxyURL  *url.URL
	oldStream *spAcceptanceGate
	mu        sync.Mutex
	remotes   []string
	closed    map[string]bool
}

func spHTTP2NewFixture(t *testing.T, retries int, db *database.DB, script func(http.ResponseWriter, *http.Request, int)) *spHTTP2Fixture {
	t.Helper()
	var forbiddenH1 atomic.Bool
	h := spAcceptanceNewHarness(t, retries, db, func(w http.ResponseWriter, _ *http.Request, _ int) {
		forbiddenH1.Store(true)
		http.Error(w, "synthetic H1 route forbidden", http.StatusBadGateway)
	})
	t.Cleanup(func() {
		if forbiddenH1.Load() {
			t.Error("HTTP2 fixture reached the forbidden H1 upstream")
		}
	})
	f := &spHTTP2Fixture{h: h, oldStream: spAcceptanceNewGate(), closed: make(map[string]bool)}
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 || r.TLS == nil || r.TLS.NegotiatedProtocol != "h2" || r.Host != "chatgpt.com" {
			h.badRoute.Store(true)
			http.Error(w, "synthetic non-H2 route rejected", http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Synthetic-Connection", r.RemoteAddr)
		switch r.URL.Path {
		case "/synthetic-warm", "/synthetic-old-check":
			_, _ = io.WriteString(w, "synthetic-idle")
			return
		case "/synthetic-old-active":
			spAcceptanceFlush(w, "synthetic-active\n")
			if f.oldStream.wait(r.Context()) {
				_, _ = io.WriteString(w, "synthetic-survived\n")
			}
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/backend-api/codex/responses" || r.Header.Get("Upgrade") != "" {
			h.badRoute.Store(true)
			http.Error(w, "synthetic response route rejected", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			h.badRoute.Store(true)
			http.Error(w, "synthetic body rejected", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.remotes = append(f.remotes, r.RemoteAddr)
		f.mu.Unlock()
		h.mu.Lock()
		h.wire = append(h.wire, spAcceptanceWireRequest{r.Header.Clone(), body, r.Host, r.URL.RequestURI()})
		h.mu.Unlock()
		n := int(h.calls.Add(1))
		w.Header().Set("Content-Type", "text/event-stream")
		script(w, r, n)
	}))
	upstream.EnableHTTP2 = true
	upstream.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			f.mu.Lock()
			f.closed[conn.RemoteAddr().String()] = true
			f.mu.Unlock()
		}
	}
	upstream.StartTLS()
	t.Cleanup(upstream.Close)
	target, err := url.Parse(upstream.URL)
	if err != nil || !net.ParseIP(target.Hostname()).IsLoopback() {
		t.Fatal("invalid synthetic TLS loopback address")
	}

	var tunnels sync.Map
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != "chatgpt.com:443" {
			h.badRoute.Store(true)
			http.Error(w, "synthetic CONNECT route rejected", http.StatusForbidden)
			return
		}
		// Never resolve or dial the public CONNECT authority.
		server, err := net.DialTimeout("tcp", target.Host, 2*time.Second)
		if err != nil {
			http.Error(w, "synthetic target unavailable", http.StatusBadGateway)
			return
		}
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			server.Close()
			return
		}
		tunnels.Store(client, server)
		defer func() {
			client.Close()
			server.Close()
			tunnels.Delete(client)
		}()
		h.connects.Add(1)
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		if buffered.Flush() != nil {
			return
		}
		done := make(chan struct{})
		go func() {
			_, _ = io.Copy(server, buffered)
			server.Close()
			close(done)
		}()
		_, _ = io.Copy(client, server)
		client.Close()
		<-done
	}))
	t.Cleanup(func() {
		tunnels.Range(func(k, v any) bool {
			k.(net.Conn).Close()
			v.(net.Conn).Close()
			return true
		})
		proxyServer.Close()
	})
	f.proxyURL, err = url.Parse(proxyServer.URL)
	if err != nil || !net.ParseIP(f.proxyURL.Hostname()).IsLoopback() {
		t.Fatal("invalid synthetic CONNECT loopback address")
	}
	h.proxyURL = proxyServer.URL
	h.account.Mu().Lock()
	h.account.ProxyURL = proxyServer.URL
	h.account.Mu().Unlock()
	// Use the actual production constructor, including ConfigureTransports and
	// keepalive, BEFORE prewarming the original H2 pool. Only fixture routing,
	// trust and session-cache isolation differ; no TLS verification is disabled.
	var ok bool
	f.transport, ok = newCodexStandardTransport(proxyServer.URL).(*http.Transport)
	if !ok || f.transport.TLSNextProto["h2"] == nil {
		t.Fatal("production standard transport did not configure HTTP2")
	}
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	f.transport.TLSClientConfig.RootCAs = roots
	f.transport.TLSClientConfig.ServerName = "example.com"
	f.transport.TLSClientConfig.MinVersion = tls.VersionTLS12
	f.transport.TLSClientConfig.ClientSessionCache = tls.NewLRUClientSessionCache(8)
	f.transport.Proxy = func(r *http.Request) (*url.URL, error) {
		if state := safePreflightFromContext(r.Context()); state != nil {
			state.mu.Lock()
			attempt := state.attempts
			state.mu.Unlock()
			h.mu.Lock()
			h.sends = append(h.sends, spAcceptanceSend{state, attempt, CodexTurnStateAffinityKeyFromContext(r.Context()), proxyServer.URL})
			h.mu.Unlock()
		}
		return f.proxyURL, nil
	}
	f.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || address != f.proxyURL.Host {
			h.badRoute.Store(true)
			return nil, errors.New("synthetic transport denied non-proxy dial")
		}
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, address)
	}
	f.client = &http.Client{Transport: f.transport, Timeout: 10 * time.Second}
	for _, prefix := range []string{"", "account-bound|"} {
		key := prefix + clientPoolKey(h.account, proxyServer.URL, codexTransportModeStandard)
		previous, existed := clientPool.Load(key)
		entry := &poolEntry{client: f.client, createdAt: time.Now().UnixNano()}
		entry.touch()
		clientPool.Store(key, entry)
		t.Cleanup(func() {
			clientPool.Delete(key)
			if existed {
				clientPool.Store(key, previous)
			}
		})
	}
	t.Cleanup(f.transport.CloseIdleConnections)
	t.Cleanup(f.oldStream.open)
	return f
}

func (f *spHTTP2Fixture) originalRequest(t *testing.T, path string) (*http.Response, string) {
	t.Helper()
	resp, err := f.client.Get("https://chatgpt.com" + path)
	if err != nil {
		t.Fatal("synthetic original H2 request failed (details redacted)")
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 || resp.TLS == nil || resp.TLS.NegotiatedProtocol != "h2" || len(resp.TLS.VerifiedChains) == 0 {
		t.Fatal("original request did not use authenticated TLS with real HTTP2")
	}
	remote := resp.Header.Get("X-Synthetic-Connection")
	if remote == "" {
		t.Fatal("no synthetic upstream connection identity")
	}
	return resp, remote
}

func (f *spHTTP2Fixture) warm(t *testing.T) string {
	t.Helper()
	resp, remote := f.originalRequest(t, "/synthetic-warm")
	if spAcceptanceReadRest(t, resp.Body) != "synthetic-idle" {
		t.Fatal("original H2 pool did not prewarm")
	}
	resp.Body.Close()
	return remote
}

func (f *spHTTP2Fixture) waitClosed(t *testing.T, remote string) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		f.mu.Lock()
		closed := f.closed[remote]
		f.mu.Unlock()
		if closed {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("safe sender cleanup left its own idle H2 connection open")
		}
	}
}

func TestSafePreflightHTTP2IsolatedRetryAndCleanup(t *testing.T) {
	gate := spAcceptanceNewGate()
	f := spHTTP2NewFixture(t, 1, nil, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			spAcceptanceFlush(w, spAcceptanceRate)
			if gate.wait(r.Context()) {
				spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
			}
			return
		}
		spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_h2_retry"))
	})
	t.Cleanup(gate.open)
	oldRemote := f.warm(t)
	active, activeRemote := f.originalRequest(t, "/synthetic-old-active")
	if activeRemote != oldRemote {
		t.Fatal("fixture did not keep an in-flight stream on the prewarmed original pool")
	}
	_, reader, _ := f.h.request(t)
	prefix := spAcceptanceReadEnvelope(t, reader)
	state := f.h.provePath(t, 1)
	if prefix != spAcceptanceRate || state == nil || state.pinnedAccount() != f.h.account.ID() {
		t.Fatal("real downstream rate publication did not pin the H2 request")
	}
	gate.open()
	wire := prefix + spAcceptanceReadRest(t, reader)
	f.h.wait(t)
	f.h.provePath(t, 2)
	f.h.assertStableIdentity(t)
	spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_h2_retry")
	f.mu.Lock()
	remotes := append([]string(nil), f.remotes...)
	f.mu.Unlock()
	if len(remotes) != 2 || remotes[0] == oldRemote || remotes[1] == oldRemote || remotes[0] == remotes[1] {
		t.Fatal("safe sends reused an original or previous-attempt H2 pool")
	}
	for _, remote := range remotes {
		f.waitClosed(t, remote)
	}
	f.mu.Lock()
	oldClosed := f.closed[oldRemote]
	f.mu.Unlock()
	if oldClosed {
		t.Error("safe cleanup closed the original in-flight H2 connection")
	}
	f.oldStream.open()
	if spAcceptanceReadRest(t, active.Body) != "synthetic-active\nsynthetic-survived\n" {
		t.Error("original in-flight stream did not survive safe cleanup")
	}
	active.Body.Close()
	check, checkRemote := f.originalRequest(t, "/synthetic-old-check")
	spAcceptanceReadRest(t, check.Body)
	check.Body.Close()
	if checkRemote != oldRemote || f.h.connects.Load() != 3 {
		t.Error("cleanup affected the old pool or safe sends bypassed their explicit CONNECT")
	}
}

func TestSafePreflightHTTP2CallbackReplacementAcrossQuota(t *testing.T) {
	for _, stage := range []string{"before_retry_quota", "during_retry_quota"} {
		t.Run(stage, func(t *testing.T) {
			path, db, quota := spSharedEdgesQuota(t)
			gate := spAcceptanceNewGate()
			f := spHTTP2NewFixture(t, 1, nil, func(w http.ResponseWriter, r *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceRate)
				if gate.wait(r.Context()) {
					spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
				}
			})
			t.Cleanup(gate.open)
			spSharedEdgesAttachQuota(t, f.h, quota)
			f.warm(t)
			_, reader, _ := f.h.request(t)
			prefix := spAcceptanceReadEnvelope(t, reader)
			if prefix != spAcceptanceRate {
				t.Fatal("callback replacement lacked a genuine rate publication barrier")
			}
			state := f.h.provePath(t, 1)
			connects := f.h.connects.Load()
			release := func() {}
			if stage == "during_retry_quota" {
				release = spSharedEdgesHoldSQLiteWriter(t, path)
				gate.open()
				spSharedEdgesWaitQuota(t, f.h, state, quota, db)
			}
			// The first safe send owns an independent map, and the second either
			// has not begun or is blocked in real quota storage. No config reader
			// overlaps this source-map mutation. The wrapper only counts a bug;
			// it cannot produce a fake successful response.
			original := f.transport.TLSNextProto["h2"]
			var callbackCalls atomic.Int32
			f.transport.TLSNextProto["h2"] = func(authority string, conn *tls.Conn) http.RoundTripper {
				callbackCalls.Add(1)
				return original(authority, conn)
			}
			gate.open()
			release()
			wire := prefix + spAcceptanceReadRest(t, reader)
			f.h.wait(t)
			f.h.provePath(t, 1)
			if callbackCalls.Load() != 0 || f.h.connects.Load() != connects || state.wasSent() {
				t.Error("replacement H2 callback escaped rejection after quota admission")
			}
			spAcceptanceTerminal(t, wire, "response.failed", "")
		})
	}
}

func TestSafePreflightHTTP2FinalSenderCallbacks(t *testing.T) {
	for _, replacement := range []string{"known_old_pool", "unknown_callback", "unknown_protocol", "protocol_policy", "http2_config", "h2_custom_callback", "h2_pool", "alpn"} {
		t.Run(replacement, func(t *testing.T) {
			f := spHTTP2NewFixture(t, 0, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_final_sender"))
			})
			oldRemote := f.warm(t)
			state := &safePreflightState{started: time.Now(), downstream: context.Background()}
			state.begin()
			ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), safePreflightContextKey{}, state), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, CodexBaseURL+"/responses", strings.NewReader(spAcceptanceBody))
			if err != nil {
				t.Fatal("could not construct synthetic H2 request")
			}
			egress := CodexEgress{Kind: CodexEgressProxy, URL: req.URL.String(), DialProxyURL: f.proxyURL.String()}
			body := []byte(spAcceptanceBody)
			if err := state.guard(req, body, "gpt-5.5", f.h.account, egress, f.client, false); err != nil {
				t.Fatal("pre-quota guard rejected known production H2 configuration")
			}
			client, cleanup, err := safePreflightClient(f.client, req, f.proxyURL)
			if err != nil {
				t.Fatal("could not isolate the known production H2 transport")
			}
			defer cleanup()
			owned, _ := req.Context().Value(safePreflightSendContextKey{}).(*safePreflightSendConfig)
			if owned == nil || owned.h2 == nil || owned.h2.ReadIdleTimeout != codexHTTP2ReadIdleTimeout || owned.h2.PingTimeout != codexHTTP2PingTimeout {
				t.Fatal("isolated H2 sender did not preserve the production keepalive policy")
			}
			if req.GetBody != nil {
				t.Error("nonempty POST retained transport-level replay capability")
			}
			transport := client.Transport.(*http.Transport)
			var callbackCalls atomic.Int32
			unknown := func(authority string, conn *tls.Conn) http.RoundTripper {
				callbackCalls.Add(1)
				return f.transport.TLSNextProto["h2"](authority, conn)
			}
			switch replacement {
			case "known_old_pool":
				transport.TLSNextProto["h2"] = f.transport.TLSNextProto["h2"]
			case "unknown_callback":
				transport.TLSNextProto["h2"] = unknown
			case "unknown_protocol":
				transport.TLSNextProto["synthetic-unknown"] = unknown
			case "protocol_policy":
				transport.Protocols = new(http.Protocols)
				transport.Protocols.SetHTTP1(true)
			case "http2_config":
				transport.HTTP2 = &http.HTTP2Config{SendPingTimeout: time.Second}
			case "h2_custom_callback":
				owned.h2.CountError = func(string) { callbackCalls.Add(1) }
			case "h2_pool":
				owned.h2.ConnPool = nil
			case "alpn":
				transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
			}
			err = state.guard(req, body, "gpt-5.5", f.h.account, egress, client, true)
			if replacement != "known_old_pool" {
				if !errors.Is(err, errSafePreflightLocal) || state.wasSent() || f.h.calls.Load() != 0 || f.h.connects.Load() != 1 || callbackCalls.Load() != 0 {
					t.Error("final guard failed to reject changed protocol configuration without sending")
				}
				return
			}
			if err != nil {
				t.Fatal("final guard failed to restore its own known H2 sender")
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal("isolated final H2 send failed (details redacted)")
			}
			spAcceptanceReadRest(t, resp.Body)
			resp.Body.Close()
			cleanup()
			remote := resp.Header.Get("X-Synthetic-Connection")
			if resp.ProtoMajor != 2 || resp.TLS == nil || len(resp.TLS.VerifiedChains) == 0 || remote == "" || remote == oldRemote || callbackCalls.Load() != 0 || f.h.connects.Load() != 2 {
				t.Error("final sender executed a replaced old-pool callback or lost real verified H2")
			}
			f.waitClosed(t, remote)
			check, checkRemote := f.originalRequest(t, "/synthetic-old-check")
			spAcceptanceReadRest(t, check.Body)
			check.Body.Close()
			if checkRemote != oldRemote || f.h.connects.Load() != 2 {
				t.Error("safe cleanup closed the original idle H2 pool")
			}
		})
	}
}
