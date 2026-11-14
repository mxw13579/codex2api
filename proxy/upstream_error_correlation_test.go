package proxy

import (
	"bytes"
	"context"
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
	"github.com/codex2api/config"
	"github.com/codex2api/database"
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
	attempt := &encryptedContentAttempt{memory: &encryptedContentMemory{now: time.Now}}
	attempt.observeResponse(resp, requestBody)
	if _, ok := resp.Body.(*encryptedErrorObserver); !ok {
		t.Fatal("fixture did not install the encrypted-content observer")
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
	originalCorrelation := upstreamErrorLogCorrelation(first, 4)
	attempt := &encryptedContentAttempt{memory: &encryptedContentMemory{now: time.Now}}
	attempt.observeResponse(first, []byte(`{"input":"synthetic request"}`))
	if upstreamErrorLogCorrelation(first, 4) != originalCorrelation {
		t.Fatal("encrypted-content observer lost response-owned metadata")
	}
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
	// Spooling closes A after B has started, but must keep A's correlation.
	pipeline := &ImagePipeline{slots: make(chan struct{}, 1), dir: t.TempDir()}
	defer pipeline.Close()
	metadata := upstreamErrorMetadataFromBody(first.Body)
	file, err := pipeline.collectResponse(ctx, first.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer removePipelineFile(file)
	first.Body = &pipelineResponseReader{File: file, err: pipeline.responseReadError, errorMetadata: metadata}
	if upstreamErrorLogCorrelation(first, 4) != got {
		t.Fatal("image pipeline lost the original response's metadata")
	}
	body, err := io.ReadAll(first.Body)
	if err != nil || string(body) != `{"detail":"Bad Request"}` {
		t.Fatal("image spooling changed the error body")
	}
	if other := upstreamErrorLogCorrelation(first, 5); strings.Contains(other, "Upstream-") || strings.Contains(other, "CF-Ray") {
		t.Fatal("image pipeline borrowed another account's identifiers")
	}
	first.Body = &safePreflightResponseBody{ReadCloser: first.Body, closeIdle: func() {}}
	if upstreamErrorLogCorrelation(first, 4) != got {
		t.Fatal("safe preflight wrapper lost the response-owned snapshot")
	}
	resetUpstreamAttemptTrace(ctx)
	if upstreamErrorLogCorrelation(first, 4) != got {
		t.Fatal("reset changed an already returned response's metadata")
	}
	unknown := &http.Response{StatusCode: 400, Header: http.Header{"X-Request-Id": {"unverified-handshake"}}, Body: &struct{ io.ReadCloser }{first.Body}}
	if upstreamErrorLogCorrelation(nil, 4) != "" || upstreamErrorLogCorrelation(unknown, 4) != "" {
		t.Fatal("unknown wrapper should not be unwrapped or borrow a header/current attempt")
	}
	attempt.observeResponse(unknown, nil)
	if upstreamErrorLogCorrelation(unknown, 4) != "" {
		t.Fatal("encrypted-content observer bypassed an unknown wrapper")
	}
	unknown.Body = &pipelineResponseReader{File: file, errorMetadata: upstreamErrorMetadataFromBody(unknown.Body)}
	if upstreamErrorLogCorrelation(unknown, 4) != "" {
		t.Fatal("image pipeline invented metadata for an unknown wrapper")
	}
}

func TestUpstreamErrorLogQueuedImageCorrelation(t *testing.T) {
	diagnosticRedirectLogging(t)
	previousLogger := badRequestLogger
	logger := &fileLogger{path: "bad_request.log"}
	badRequestLogger = logger
	t.Cleanup(func() { logger.close(); badRequestLogger = previousLogger })
	previousSettings, previousResin := CurrentRuntimeSettings(), resinCfg.Load()
	previousPipeline, previousExecution := processImagePipeline.Load(), processImageGate.Load()
	t.Cleanup(func() {
		ApplyRuntimeSettings(previousSettings)
		SetResinConfig(previousResin)
		processImagePipeline.Store(previousPipeline)
		processImageGate.Store(previousExecution)
	})
	UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings {
		s.CodexTelemetryEnabled = false
		s.CodexForceWebsocket = false
		s.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
		return s
	})
	const originalError = `{"detail":"Bad Request"}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Request-ID", upstreamResponseTestUUID)
		w.Header().Set("CF-Ray", "image-fixture-ray")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, originalError)
	}))
	defer upstream.Close()
	SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "image-correlation-fixture"})
	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 1, TestConcurrency: 1, MaxRetries: 0})
	defer store.Stop()
	store.AddAccount(&auth.Account{DBID: 4, AccessToken: "synthetic-token", AccountID: "image-correlation-fixture", PlanType: "plus"})
	h := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
	ConfigureImagePipeline(1)
	ConfigureImageExecutionLimit(1)
	executionCtx, releaseExecution, err := AcquireImageExecution(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer releaseExecution()
	ctx, pipeline, err := NewImagePipeline(executionCtx, t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()
	_, status, err := h.GenerateQueuedImage(ctx, func() ([]byte, error) {
		return []byte(`{"model":"gpt-image-2","prompt":"synthetic image"}`), nil
	}, false, nil)
	if status != http.StatusBadRequest || err == nil {
		t.Fatalf("queued image status=%d err=%v, want synthetic upstream 400", status, err)
	}
	if !upstreamDiagnosticLogger.close(time.Second) {
		t.Fatal("diagnostic writer did not drain")
	}
	diagnostics, err := os.ReadFile(filepath.Join(errorLogDir(), "upstream_errors.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic upstreamHTTPDiagnostic
	if err := json.Unmarshal(bytes.TrimSpace(diagnostics), &diagnostic); err != nil {
		t.Fatal("expected one synthetic image diagnostic")
	}
	logged, err := os.ReadFile(filepath.Join(errorLogDir(), "bad_request.log"))
	if err != nil {
		t.Fatal(err)
	}
	if diagnostic.RequestID == "" || !bytes.Contains(logged, []byte("Request-ID: "+diagnostic.RequestID+"\n")) ||
		!bytes.Contains(logged, []byte("Upstream-Request-ID: "+upstreamResponseTestUUID+"\n")) ||
		!bytes.Contains(logged, []byte("CF-Ray: image-fixture-ray\n")) || !bytes.Contains(logged, []byte("Response:\n"+originalError)) {
		t.Fatal("queued image spooling lost response-owned correlation or changed the error body")
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
