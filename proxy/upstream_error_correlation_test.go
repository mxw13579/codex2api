package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/gin-gonic/gin"
)

func TestUpstreamErrorLogLinksOriginalBodyAndDiagnostic(t *testing.T) {
	diagnosticRedirectLogging(t)
	previous := badRequestLogger
	logger := &fileLogger{path: "bad_request.log"}
	badRequestLogger = logger
	t.Cleanup(func() { logger.close(); badRequestLogger = previous })
	const upstreamID = "ad7c8bc3-819d-41be-962e-532d122a6710"
	const originalError = `{"detail":"Bad Request"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Request-ID", upstreamID)
		w.Header().Set("CF-Ray", "1234abcd-IAD")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, originalError)
	}))
	defer server.Close()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	requestBody := []byte(`{"model":"gpt-6.1-sol","reasoning":{"effort":"low"},"input":"DO_NOT_LOG_THE_PROMPT"}`)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, server.URL, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := doTracedUpstreamRequest(server.Client(), req, &auth.Account{DBID: 41}, "", requestBody)
	if err != nil {
		t.Fatal(err)
	}
	body, err := readUpstreamErrorBody(c.Request.Context(), resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != originalError {
		t.Fatal("original upstream error changed")
	}
	logUpstreamError("/v1/responses", resp.StatusCode, "gpt-6.1-sol", 41, body, resp)
	if !upstreamDiagnosticLogger.close(time.Second) {
		t.Fatal("diagnostic writer did not drain")
	}
	logged, err := os.ReadFile(filepath.Join(errorLogDir(), "upstream_errors.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var d upstreamHTTPDiagnostic
	if err := json.Unmarshal(bytes.TrimSpace(logged), &d); err != nil {
		t.Fatal(err)
	}
	rawLog, err := os.ReadFile(filepath.Join(errorLogDir(), "bad_request.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"\nRequest-ID: " + d.RequestID + "\n",
		"\nUpstream-Request-ID: " + upstreamID + "\n",
		"\nCF-Ray: 1234abcd-IAD\n",
		"Response:\n" + originalError,
	} {
		if !strings.Contains(string(rawLog), want) {
			t.Error("body log lost a correlation field or the original response")
		}
	}
	if d.RequestID == "" || d.UpstreamRequestID != upstreamID || d.CFRay != "1234abcd-IAD" || d.Request.Model != "gpt-6.1-sol" || d.Request.ReasoningEffort != "low" || !d.BodyComplete || d.ReadBytes != 24 {
		t.Fatal("diagnostic failed to preserve actual request/response metadata")
	}
	if bytes.Contains(rawLog, []byte("DO_NOT_LOG")) || bytes.Contains(logged, []byte("DO_NOT_LOG")) {
		t.Fatal("request prompt was copied into logs")
	}
	if bytes.Contains(logged, []byte("Bad Request")) {
		t.Fatal("raw error was unnecessarily duplicated into diagnostic JSONL")
	}
}

func TestUpstreamErrorLogCorrelationIsolation(t *testing.T) {
	t.Setenv("LOG_DISABLED", "false")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	ctx := c.Request.Context()
	account := &auth.Account{DBID: 4}
	makeResponse := func(id, ray string) *http.Response {
		req := httptest.NewRequest(http.MethodPost, "http://localhost/", nil).WithContext(ctx)
		resp := &http.Response{StatusCode: 400, Header: http.Header{"X-Request-Id": {id}, "Cf-Ray": {ray}}, Body: io.NopCloser(strings.NewReader(`{"detail":"Bad Request"}`))}
		wrapped := wrapUpstreamHTTPDiagnostic(req, resp, account)
		wrapped.emit = func(upstreamHTTPDiagnostic) {}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	first := makeResponse(upstreamResponseTestUUID, "1234-IAD")
	firstRequestID := cookieLogRequestID(ctx)
	// B starts on the same context/account; then A's body is logged late.
	resetUpstreamRequestTrace(c)
	record := beginUpstreamTrace(ctx, account, "", false)
	second := makeResponse("later-attempt", "later-ray")
	record(second)
	got := upstreamErrorLogCorrelation(first, 4)
	if !strings.Contains(got, "Request-ID: "+firstRequestID+"\n") || !strings.Contains(got, "Upstream-Request-ID: "+upstreamResponseTestUUID+"\n") || !strings.Contains(got, "CF-Ray: 1234-IAD\n") || strings.Contains(got, "later") {
		t.Fatal("late body was correlated with a newer attempt or WS turn")
	}
	if other := upstreamErrorLogCorrelation(first, 5); strings.Contains(other, "Upstream-") || strings.Contains(other, "CF-Ray") || !strings.HasPrefix(other, "Request-ID:") {
		t.Fatal("error correlation borrowed another account's upstream identifiers")
	}
	first.Body = &safePreflightResponseBody{ReadCloser: first.Body, closeIdle: func() {}}
	if upstreamErrorLogCorrelation(first, 4) != got {
		t.Fatal("safe preflight wrapper lost the response-owned snapshot")
	}
	resetUpstreamAttemptTrace(ctx)
	if upstreamErrorLogCorrelation(first, 4) != got {
		t.Fatal("reset changed an already returned response's metadata")
	}
	unknown := &http.Response{Header: http.Header{"X-Request-Id": {"unverified-handshake"}}, Body: io.NopCloser(strings.NewReader("unknown"))}
	if upstreamErrorLogCorrelation(nil, 4) != "" || upstreamErrorLogCorrelation(unknown, 4) != "" {
		t.Fatal("missing response snapshot should not invent correlation or use a handshake ID")
	}
}

func TestUpstreamErrorLogCorrelationValidatesIDs(t *testing.T) {
	for _, bad := range []string{"Bearer SECRET", "bad\r\nForged: header", "sk-abcdefghijklmnopqrstuv", strings.Repeat("a", 129)} {
		resp := &http.Response{Body: &upstreamDiagnosticBody{diagnostic: upstreamHTTPDiagnostic{AccountID: 1, RequestID: bad, UpstreamRequestID: bad, CFRay: bad}}}
		if upstreamErrorLogCorrelation(resp, 1) != "" {
			t.Fatal("unsafe identifier entered the body log")
		}
	}
}

type errorLogMetadataTransport func(*http.Request) (*http.Response, error)

func (f errorLogMetadataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestUpstreamErrorLogFreezesRequestIDBeforeSend(t *testing.T) {
	t.Setenv("LOG_DISABLED", "false")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	originalID := cookieLogRequestID(c.Request.Context())
	client := &http.Client{Transport: errorLogMetadataTransport(func(req *http.Request) (*http.Response, error) {
		resetUpstreamRequestTrace(c)
		return &http.Response{StatusCode: 400, Header: http.Header{"X-Request-Id": {upstreamResponseTestUUID}}, Request: req, Body: io.NopCloser(strings.NewReader("synthetic error"))}, nil
	})}
	req, _ := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, "http://localhost/", nil)
	resp, err := doTracedUpstreamRequest(client, req, &auth.Account{DBID: 4}, "")
	if err != nil {
		t.Fatal(err)
	}
	if cookieLogRequestID(c.Request.Context()) == originalID || !strings.Contains(upstreamErrorLogCorrelation(resp, 4), "Request-ID: "+originalID+"\n") {
		t.Fatal("later turn reset overwrote in-flight attempt's request ID")
	}
	// The fixture's body is in memory; suppress file emission before closing it.
	resp.Body.(*upstreamDiagnosticBody).emit = func(upstreamHTTPDiagnostic) {}
	resp.Body.Close()
}

func TestUpstreamServerErrorLogCorrelation(t *testing.T) {
	t.Setenv("LOG_DISABLED", "false")
	t.Setenv("LOG_DIR", t.TempDir())
	previous := serverErrorLogger
	logger := &fileLogger{path: "server_error.log"}
	serverErrorLogger = logger
	t.Cleanup(func() { logger.close(); serverErrorLogger = previous })
	resp := &http.Response{Body: &upstreamDiagnosticBody{diagnostic: upstreamHTTPDiagnostic{AccountID: 4, RequestID: "local-request", UpstreamRequestID: upstreamResponseTestUUID, CFRay: "1234-IAD"}}}
	logUpstreamError("/v1/responses", 500, "gpt-6.1-sol", 4, []byte(`{"error":{"message":"synthetic upstream failure"}}`), resp)
	logged, err := os.ReadFile(filepath.Join(errorLogDir(), "server_error.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(logged, []byte("Upstream-Request-ID: "+upstreamResponseTestUUID+"\n")) || !bytes.Contains(logged, []byte("synthetic upstream failure")) {
		t.Fatal("5xx body log lost response-owned correlation or upstream text")
	}
}
