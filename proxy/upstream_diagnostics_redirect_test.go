package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// These fixtures contain only synthetic data. Never include bodies in failures.
const diagnosticRedirectFirstJSON = `{"model":"gpt-5","service_tier":"default","reasoning":{"effort":"low"},"stream":false,"input":[{"role":"user","content":"synthetic-first"}]}`
const diagnosticRedirectFinalJSON = `{"model":"gpt-6","service_tier":"priority","reasoning":{"effort":"high"},"stream":true,"input":[{"role":"user","content":"synthetic-final"},{"role":"user","content":"synthetic-next"}],"tools":[{"type":"function","name":"synthetic_tool"}],"instructions":"synthetic"}`
const diagnosticRedirectResponseJSON = `{"error":{"type":"invalid_request_error","code":"unsupported_value","message":"synthetic failure"}}`

type diagnosticRedirectBodyProbe struct {
	reads, closes int
}

func (p *diagnosticRedirectBodyProbe) Read([]byte) (int, error) {
	p.reads++
	return 0, io.EOF
}

func (p *diagnosticRedirectBodyProbe) Close() error {
	p.closes++
	return nil
}

func TestSameUpstreamDiagnosticRequestBoundaries(t *testing.T) {
	cases := []struct {
		name   string
		change func(original, sent *http.Request)
		want   bool
	}{
		{name: "shallow copy", want: true},
		{name: "equal independent metadata", want: true, change: func(original, sent *http.Request) {
			urlCopy := *original.URL
			sent.URL = &urlCopy
			sent.Header = original.Header.Clone()
		}},
		{name: "same response association", want: true, change: func(original, sent *http.Request) {
			original.Response = &http.Response{StatusCode: http.StatusTemporaryRedirect}
			sent.Response = original.Response
		}},
		{name: "method", change: func(_, sent *http.Request) { sent.Method = http.MethodGet }},
		{name: "host", change: func(_, sent *http.Request) { sent.Host = "other.invalid" }},
		{name: "content length", change: func(_, sent *http.Request) { sent.ContentLength++ }},
		{name: "content encoding", change: func(_, sent *http.Request) {
			sent.Header = sent.Header.Clone()
			sent.Header.Set("Content-Encoding", "br")
		}},
		{name: "url path", change: func(_, sent *http.Request) {
			urlCopy := *sent.URL
			urlCopy.Path = "/other"
			sent.URL = &urlCopy
		}},
		{name: "url query", change: func(_, sent *http.Request) {
			urlCopy := *sent.URL
			urlCopy.RawQuery = "synthetic=2"
			sent.URL = &urlCopy
		}},
		{name: "nil sent url", change: func(_, sent *http.Request) { sent.URL = nil }},
		{name: "nil original url", change: func(original, _ *http.Request) { original.URL = nil }},
		{name: "redirect to identical url", change: func(_, sent *http.Request) {
			sent.Response = &http.Response{StatusCode: http.StatusTemporaryRedirect}
		}},
		{name: "different response association", change: func(original, sent *http.Request) {
			original.Response = &http.Response{StatusCode: http.StatusTemporaryRedirect}
			sent.Response = &http.Response{StatusCode: http.StatusTemporaryRedirect}
		}},
		{name: "removed response association", change: func(original, _ *http.Request) {
			original.Response = &http.Response{StatusCode: http.StatusTemporaryRedirect}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := diagnosticRedirectRequest(t, "http://upstream.invalid/v1/responses?synthetic=1", []byte(diagnosticRedirectFirstJSON))
			original.Header.Set("Content-Encoding", "gzip")
			probe := &diagnosticRedirectBodyProbe{}
			original.Body = probe
			getBodyCalls := 0
			original.GetBody = func() (io.ReadCloser, error) {
				getBodyCalls++
				return probe, nil
			}
			if !sameUpstreamDiagnosticRequest(original, original) {
				t.Fatal("original request rejected")
			}
			if sameUpstreamDiagnosticRequest(nil, original) || sameUpstreamDiagnosticRequest(original, nil) || sameUpstreamDiagnosticRequest(nil, nil) {
				t.Fatal("nil request accepted")
			}
			sent := *original
			if tc.change != nil {
				tc.change(original, &sent)
			}
			if got := sameUpstreamDiagnosticRequest(original, &sent); got != tc.want {
				t.Errorf("same request = %t, want %t", got, tc.want)
			}
			if probe.reads != 0 || probe.closes != 0 || getBodyCalls != 0 {
				t.Fatal("request comparison touched Body or GetBody")
			}
		})
	}
}

func diagnosticRedirectLogging(t *testing.T) {
	t.Helper()
	t.Setenv("LOG_DISABLED", "false")
	t.Setenv("LOG_DIR", t.TempDir())
	previous := upstreamDiagnosticLogger
	writer := &upstreamDiagnosticWriter{}
	upstreamDiagnosticLogger = writer
	t.Cleanup(func() {
		drained := writer.close(time.Second)
		upstreamDiagnosticLogger = previous
		if !drained {
			t.Error("diagnostic writer did not drain")
		}
	})
}

func diagnosticRedirectRequest(t *testing.T, url string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal("could not construct synthetic request")
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

func diagnosticRedirectGzip(t *testing.T, plaintext []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err := writer.Write(plaintext); err != nil {
		t.Fatal("could not compress synthetic request")
	}
	if err := writer.Close(); err != nil {
		t.Fatal("could not finish synthetic compressed request")
	}
	return out.Bytes()
}

// Called by server handlers: report failures without terminating their goroutines.
func diagnosticRedirectCheckWire(t *testing.T, req *http.Request, method, path, encoding string, want []byte) {
	t.Helper()
	if req.Method != method || req.URL.Path != path || req.Header.Get("Content-Encoding") != encoding || req.ContentLength != int64(len(want)) {
		t.Error("unexpected wire request metadata")
	}
	body, err := io.ReadAll(req.Body)
	if err != nil || !bytes.Equal(body, want) {
		t.Error("wire request body changed")
	}
}

func diagnosticRedirectFailure(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = io.WriteString(w, diagnosticRedirectResponseJSON)
}

func diagnosticRedirectRead(t *testing.T, resp *http.Response, want upstreamRequestSummary) {
	t.Helper()
	defer resp.Body.Close()
	body, ok := resp.Body.(*upstreamDiagnosticBody)
	if !ok || resp.StatusCode != http.StatusBadRequest {
		t.Fatal("final failure was not wrapped for diagnostics")
	}
	if body.diagnostic.Request != want {
		t.Error("diagnostic summary does not belong to the final request")
	}
	if body.diagnostic.ReadBytes != 0 || len(body.prefix) != 0 || body.emitted {
		t.Error("diagnostics consumed response bytes before the caller")
	}
	// A partial read must not cause read-ahead or change the caller's byte stream.
	prefix := make([]byte, 7)
	if _, err := io.ReadFull(body, prefix); err != nil {
		t.Fatal("partial response read failed")
	}
	if body.diagnostic.ReadBytes != int64(len(prefix)) || body.emitted {
		t.Error("diagnostics changed partial-read semantics")
	}
	rest, err := io.ReadAll(body)
	if err != nil || string(append(prefix, rest...)) != diagnosticRedirectResponseJSON {
		t.Error("diagnostics changed the final response body")
	}
	if !body.diagnostic.BodyComplete || body.diagnostic.ReadBytes != int64(len(diagnosticRedirectResponseJSON)) || body.diagnostic.Request != want {
		t.Error("final diagnostic lost request ownership or response read metadata")
	}
	if err := body.Close(); err != nil {
		t.Error("closing the response failed")
	}
}

func TestUpstreamDiagnosticRedirect302DropsPlaintext(t *testing.T) {
	diagnosticRedirectLogging(t)
	plaintext := []byte(diagnosticRedirectFirstJSON)
	var requests, redirects, getBodyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			diagnosticRedirectCheckWire(t, r, http.MethodPost, "/start", "", plaintext)
			w.Header().Set("Location", "/final")
			w.WriteHeader(http.StatusFound)
		case 2:
			diagnosticRedirectCheckWire(t, r, http.MethodGet, "/final", "", nil)
			diagnosticRedirectFailure(w)
		default:
			t.Error("unexpected extra network request")
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	req := diagnosticRedirectRequest(t, server.URL+"/start", plaintext)
	getBody := req.GetBody
	req.GetBody = func() (io.ReadCloser, error) { getBodyCalls.Add(1); return getBody() }
	client := server.Client()
	client.Timeout = 5 * time.Second
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		redirects.Add(1)
		if len(via) != 1 || via[0] != req || next.Method != http.MethodGet || next.Response == nil || next.Response.StatusCode != http.StatusFound {
			t.Error("302 redirect callback metadata changed")
		}
		return nil
	}
	resp, err := doTracedUpstreamRequest(client, req, nil, "", plaintext)
	if err != nil {
		t.Fatal("loopback redirect request failed")
	}
	diagnosticRedirectRead(t, resp, upstreamRequestSummary{Omitted: "not_replayable"})
	if requests.Load() != 2 || redirects.Load() != 1 || getBodyCalls.Load() != 0 {
		t.Fatal("302 diagnostics added requests, redirect callbacks, or body replays")
	}
}

func TestUpstreamDiagnosticRedirect307SameURLEncodedBody(t *testing.T) {
	diagnosticRedirectLogging(t)
	plaintext := []byte(diagnosticRedirectFirstJSON)
	encoded := diagnosticRedirectGzip(t, plaintext)
	var requests, redirects, getBodyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		diagnosticRedirectCheckWire(t, r, http.MethodPost, "/same", "gzip", encoded)
		switch requests.Add(1) {
		case 1:
			w.Header().Set("Location", "/same")
			w.WriteHeader(http.StatusTemporaryRedirect)
		case 2:
			diagnosticRedirectFailure(w)
		default:
			t.Error("unexpected extra network request")
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	req := diagnosticRedirectRequest(t, server.URL+"/same", encoded)
	// Redirect requests use the URL host fallback; do the same on the first hop
	// so Host cannot accidentally hide a missing Response-association check.
	req.Host = ""
	req.Header.Set("Content-Encoding", "gzip")
	getBody := req.GetBody
	req.GetBody = func() (io.ReadCloser, error) { getBodyCalls.Add(1); return getBody() }
	client := server.Client()
	client.Timeout = 5 * time.Second
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		redirects.Add(1)
		if len(via) != 1 || via[0] != req || next.Response == nil || next.Response.StatusCode != http.StatusTemporaryRedirect || next.URL.String() != req.URL.String() || next.Method != req.Method || next.Host != req.Host || next.ContentLength != req.ContentLength || next.Header.Get("Content-Encoding") != req.Header.Get("Content-Encoding") {
			t.Error("same-URL redirect fixture changed wire metadata")
		}
		return nil
	}
	resp, err := doTracedUpstreamRequest(client, req, nil, "", plaintext)
	if err != nil {
		t.Fatal("loopback same-URL redirect failed")
	}
	diagnosticRedirectRead(t, resp, upstreamRequestSummary{Omitted: "encoded_body"})
	if requests.Load() != 2 || redirects.Load() != 1 || getBodyCalls.Load() != 1 {
		t.Fatal("307 diagnostics added requests, redirect callbacks, or encoded-body replays")
	}
}

func TestUpstreamDiagnosticRedirect307RewrittenBody(t *testing.T) {
	diagnosticRedirectLogging(t)
	final := []byte(diagnosticRedirectFinalJSON)
	// Preserve length as well as URL/method/host/encoding: only Response marks
	// this as a redirect. The first-hop summary still describes different JSON.
	first := []byte(diagnosticRedirectFirstJSON + strings.Repeat(" ", len(final)-len(diagnosticRedirectFirstJSON)))
	var requests, redirects, firstReplays, finalReplays atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			diagnosticRedirectCheckWire(t, r, http.MethodPost, "/same", "", first)
			w.Header().Set("Location", "/same")
			w.WriteHeader(http.StatusTemporaryRedirect)
		case 2:
			diagnosticRedirectCheckWire(t, r, http.MethodPost, "/same", "", final)
			diagnosticRedirectFailure(w)
		default:
			t.Error("unexpected extra network request")
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	req := diagnosticRedirectRequest(t, server.URL+"/same", first)
	req.Host = "" // Match net/http's URL host fallback on the redirect.
	getBody := req.GetBody
	req.GetBody = func() (io.ReadCloser, error) { firstReplays.Add(1); return getBody() }
	client := server.Client()
	client.Timeout = 5 * time.Second
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		redirects.Add(1)
		if len(via) != 1 || via[0] != req || next.Response == nil || next.Response.StatusCode != http.StatusTemporaryRedirect || next.URL.String() != req.URL.String() || next.Method != req.Method || next.Host != req.Host || next.ContentLength != req.ContentLength || next.Header.Get("Content-Encoding") != req.Header.Get("Content-Encoding") {
			t.Error("rewritten redirect fixture changed wire metadata")
		}
		if next.Body != nil {
			if err := next.Body.Close(); err != nil {
				return err
			}
		}
		next.Body = io.NopCloser(bytes.NewReader(final))
		next.GetBody = func() (io.ReadCloser, error) {
			finalReplays.Add(1)
			return io.NopCloser(bytes.NewReader(final)), nil
		}
		next.ContentLength = int64(len(final))
		return nil
	}
	resp, err := doTracedUpstreamRequest(client, req, nil, "", first)
	if err != nil {
		t.Fatal("loopback rewritten redirect failed")
	}
	diagnosticRedirectRead(t, resp, upstreamRequestSummary{
		Bytes: len(final), Model: "gpt-6", ServiceTier: "priority", ReasoningEffort: "high",
		Stream: true, InputType: "array", InputItems: 2, Tools: 1, HasInstructions: true,
	})
	if requests.Load() != 2 || redirects.Load() != 1 || firstReplays.Load() != 1 || finalReplays.Load() != 1 {
		t.Fatal("rewritten redirect changed network or independent body-replay counts")
	}
}

func TestUpstreamDiagnosticRedirectCheckRedirectPolicy(t *testing.T) {
	policyError := errors.New("synthetic redirect policy rejection")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "custom error", err: policyError},
		{name: "use last response", err: http.ErrUseLastResponse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diagnosticRedirectLogging(t)
			plaintext := []byte(diagnosticRedirectFirstJSON)
			var requests, redirects, getBodyCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) != 1 {
					t.Error("rejected redirect caused an extra network request")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				diagnosticRedirectCheckWire(t, r, http.MethodPost, "/start", "", plaintext)
				w.Header().Set("Location", "/must-not-send")
				w.WriteHeader(http.StatusTemporaryRedirect)
				_, _ = io.WriteString(w, diagnosticRedirectResponseJSON)
			}))
			defer server.Close()
			req := diagnosticRedirectRequest(t, server.URL+"/start", plaintext)
			getBody := req.GetBody
			req.GetBody = func() (io.ReadCloser, error) { getBodyCalls.Add(1); return getBody() }
			client := server.Client()
			client.Timeout = 5 * time.Second
			client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
				redirects.Add(1)
				if len(via) != 1 || via[0] != req || next.Response == nil || next.Response.StatusCode != http.StatusTemporaryRedirect {
					t.Error("redirect policy callback metadata changed")
				}
				return tc.err
			}
			resp, err := doTracedUpstreamRequest(client, req, nil, "", plaintext)
			if tc.err == http.ErrUseLastResponse {
				if err != nil {
					t.Fatal("ErrUseLastResponse no longer returns a successful client result")
				}
			} else if !errors.Is(err, policyError) {
				t.Fatal("custom CheckRedirect error identity was lost")
			}
			if resp == nil || resp.Body == nil {
				t.Fatal("redirect response was lost")
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusTemporaryRedirect || !sameUpstreamDiagnosticRequest(req, resp.Request) {
				t.Error("rejected redirect no longer returns the first-hop response")
			}
			if _, ok := resp.Body.(*upstreamDiagnosticBody); ok {
				t.Error("non-error HTTP status was wrapped for diagnostics")
			}
			data, readErr := io.ReadAll(resp.Body)
			if tc.err == http.ErrUseLastResponse {
				if readErr != nil || string(data) != diagnosticRedirectResponseJSON {
					t.Error("ErrUseLastResponse body is no longer readable unchanged")
				}
			} else if readErr == nil || len(data) != 0 {
				t.Error("custom-error response no longer preserves net/http's closed body")
			}
			if requests.Load() != 1 || redirects.Load() != 1 || getBodyCalls.Load() != 1 {
				t.Fatal("redirect policy changed requests, callbacks, or standard 307 replay count")
			}
		})
	}
}

func TestUpstreamDiagnosticHTTP2ReuseKeepsPlaintext(t *testing.T) {
	diagnosticRedirectLogging(t)
	plaintext := []byte(diagnosticRedirectFirstJSON)
	encoded := diagnosticRedirectGzip(t, plaintext)
	var requests, getBodyCalls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) > 2 {
			t.Error("HTTP/2 diagnostics caused an extra network request")
		}
		if r.ProtoMajor != 2 {
			t.Error("fixture did not negotiate HTTP/2")
		}
		diagnosticRedirectCheckWire(t, r, http.MethodPost, "/same", "gzip", encoded)
		diagnosticRedirectFailure(w)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	// httptest's client trusts only its test certificate; TLS verification stays on.
	// Match production: no Client.Timeout; a request deadline bounds the fixture.
	// Explicit x/net HTTP/2 setup exercises the reused alternate RoundTripper,
	// rather than merely Client.Timeout's unrelated request-copy behavior.
	client := server.Client()
	if enableCodexHTTP2KeepAlive(client.Transport.(*http.Transport)) == nil {
		t.Fatal("could not configure production HTTP/2 transport")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	warm := diagnosticRedirectRequest(t, server.URL+"/same", encoded).WithContext(ctx)
	warm.Header.Set("Content-Encoding", "gzip")
	warmResp, err := client.Do(warm)
	if err != nil {
		t.Fatal("HTTP/2 warm-up failed")
	}
	_, readErr := io.Copy(io.Discard, warmResp.Body)
	closeErr := warmResp.Body.Close()
	if readErr != nil || closeErr != nil || warmResp.ProtoMajor != 2 {
		t.Fatal("HTTP/2 warm-up did not complete")
	}
	req := diagnosticRedirectRequest(t, server.URL+"/same", encoded)
	req.Header.Set("Content-Encoding", "gzip")
	getBody := req.GetBody
	req.GetBody = func() (io.ReadCloser, error) { getBodyCalls.Add(1); return getBody() }
	var reused atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused.Store(info.Reused) },
	}))
	resp, err := doTracedUpstreamRequest(client, req, nil, "", plaintext)
	if err != nil {
		t.Fatal("reused HTTP/2 request failed")
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 || !reused.Load() {
		t.Fatal("fixture did not reuse the HTTP/2 connection")
	}
	if resp.Request == nil || resp.Request == req || resp.Request.Response != nil || !sameUpstreamDiagnosticRequest(req, resp.Request) {
		t.Fatal("fixture did not exercise an equivalent internal request copy")
	}
	diagnosticRedirectRead(t, resp, upstreamRequestSummary{
		Bytes: len(plaintext), Model: "gpt-5", ServiceTier: "default", ReasoningEffort: "low", InputType: "array", InputItems: 1,
	})
	if requests.Load() != 2 || getBodyCalls.Load() != 0 {
		t.Fatal("internal-copy diagnostics added network requests or reopened the encoded body")
	}
}
