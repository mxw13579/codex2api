package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

type diagnosticFailingReader struct{ sent bool }

func (r *diagnosticFailingReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.ErrUnexpectedEOF
	}
	r.sent = true
	return copy(p, `{"detail":"partial`), errors.New("SECRET proxy://user:password@host")
}
func (*diagnosticFailingReader) Close() error { return nil }

func TestUpstreamDiagnosticBodyObservesWithoutChangingReads(t *testing.T) {
	cases := []struct {
		name          string
		body          io.ReadCloser
		limited       bool
		wantComplete  bool
		wantTruncated bool
		wantError     string
	}{
		{"small json", io.NopCloser(strings.NewReader(`{"detail":"Bad Request"}`)), false, true, false, ""},
		{"large", io.NopCloser(strings.NewReader(strings.Repeat("x", upstreamDiagnosticBodyLimit+1))), false, true, true, ""},
		{"caller limit", io.NopCloser(strings.NewReader(strings.Repeat("x", 64))), true, false, false, ""},
		{"read error", &diagnosticFailingReader{}, false, false, false, "read_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var entries []upstreamHTTPDiagnostic
			body := &upstreamDiagnosticBody{ReadCloser: tc.body, emit: func(d upstreamHTTPDiagnostic) { entries = append(entries, d) }}
			var reader io.Reader = body
			if tc.limited {
				reader = io.LimitReader(body, 4)
			}
			data, err := io.ReadAll(reader)
			if tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), "SECRET")) {
				t.Fatalf("original read error changed: %v", err)
			}
			if tc.wantError == "" && err != nil {
				t.Fatal(err)
			}
			body.Close()
			body.Close()
			if len(entries) != 1 {
				t.Fatalf("emitted %d times", len(entries))
			}
			d := entries[0]
			if d.ReadBytes != int64(len(data)) || d.BodyComplete != tc.wantComplete || d.BodyCaptureTruncated != tc.wantTruncated || d.ReadError != tc.wantError {
				t.Fatalf("diagnostic: %+v", d)
			}
			if tc.limited && !d.ClosedEarly {
				t.Fatal("early close lost")
			}
			encoded, _ := json.Marshal(d)
			if strings.Contains(string(encoded), "SECRET") || strings.Contains(string(encoded), "Bad Request") || strings.Contains(string(encoded), "password") {
				t.Fatal("raw error content leaked")
			}
		})
	}
}

func TestUpstreamDiagnosticReadLimit(t *testing.T) {
	for _, size := range []int{maxUpstreamErrorBodyBytes, maxUpstreamErrorBodyBytes + 1, maxUpstreamErrorBodyBytes * 2} {
		var got upstreamHTTPDiagnostic
		body := &upstreamDiagnosticBody{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("x", size))), emit: func(d upstreamHTTPDiagnostic) { got = d }}
		data, err := readUpstreamErrorBody(context.Background(), body)
		body.Close()
		if len(data) != maxUpstreamErrorBodyBytes {
			t.Fatal(len(data))
		}
		if size > maxUpstreamErrorBodyBytes {
			if !errors.Is(err, errUpstreamErrorBodyTooLarge) || got.BodyComplete || got.ReadBytes != maxUpstreamErrorBodyBytes+1 {
				t.Fatalf("limit not recorded: %v %+v", err, got)
			}
		} else if err != nil || !got.BodyComplete {
			t.Fatalf("exact limit was treated as truncation: %v %+v", err, got)
		}
	}
}

func TestUpstreamDiagnosticFinalRequestAndIDs(t *testing.T) {
	t.Setenv("LOG_DISABLED", "false")
	dir := t.TempDir()
	t.Setenv("LOG_DIR", dir)
	previous := upstreamDiagnosticLogger
	upstreamDiagnosticLogger = &upstreamDiagnosticWriter{}
	t.Cleanup(func() { upstreamDiagnosticLogger.close(time.Second); upstreamDiagnosticLogger = previous })
	requestID := "ad7c8bc3-819d-41be-962e-532d122a6710"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Openai-Request-Id", requestID)
		w.Header().Set("Cf-Ray", "1234abcd-IAD")
		w.Header().Set("Content-Type", "application/json; boundary=SECRET")
		w.Header().Set("Set-Cookie", "SECRET_COOKIE")
		w.WriteHeader(400)
		io.WriteString(w, `{"detail":"Bad Request","input":"SECRET_RESPONSE"}`)
	}))
	defer server.Close()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	// 模拟压缩/改写：显式传入的是最终明文，不能误读压缩字节或原始入站参数。
	finalBody := []byte(`{"model":"gpt-6.1-sol","service_tier":"ultrafast","reasoning":{"effort":"xhigh"},"stream":true,"input":[{"content":"SECRET_PROMPT"}],"instructions":"SECRET_SYSTEM","previous_response_id":"SECRET_ID","tools":[{"name":"SECRET_TOOL","parameters":{"secret":true}}]}`)
	req, _ := http.NewRequestWithContext(c.Request.Context(), "POST", server.URL+"/?key=SECRET_URL", bytes.NewReader([]byte("not JSON compressed bytes")))
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("Authorization", "Bearer SECRET_AUTH")
	resp, err := doTracedUpstreamRequest(server.Client(), req, &auth.Account{DBID: 42}, "http://SECRET_PROXY", finalBody)
	if err != nil {
		t.Fatal(err)
	}
	data, err := readUpstreamErrorBody(c.Request.Context(), resp.Body)
	resp.Body.Close()
	if err != nil || !strings.Contains(string(data), "Bad Request") {
		t.Fatal("changed upstream body")
	}
	if !upstreamDiagnosticLogger.close(time.Second) {
		t.Fatal("diagnostic writer did not drain")
	}
	logged, err := os.ReadFile(filepath.Join(dir, "upstream_errors.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logged), "SECRET") || strings.Contains(string(logged), "not JSON compressed") {
		t.Fatalf("sensitive data leaked: %s", logged)
	}
	var d upstreamHTTPDiagnostic
	if err := json.Unmarshal(bytes.TrimSpace(logged), &d); err != nil {
		t.Fatal(err)
	}
	if d.RequestID == "" || d.UpstreamRequestID != requestID || d.CFRay != "1234abcd-IAD" || d.AccountID != 42 || d.Status != 400 || d.ContentType != "application/json" {
		t.Fatalf("metadata lost: %+v", d)
	}
	if d.Request.Model != "gpt-6.1-sol" || d.Request.ServiceTier != "ultrafast" || d.Request.ReasoningEffort != "xhigh" || d.Request.InputItems != 1 || d.Request.Tools != 1 || !d.Request.HasPreviousResponse || !d.BodyComplete {
		t.Fatalf("wrong final body: %+v", d)
	}
}

func TestUpstreamDiagnosticMetadataDoesNotBypassRedaction(t *testing.T) {
	for _, value := range []string{"ad7c8bc3-819d-41be-962e-532d122a6710", "AD7C8BC3-819D-41BE-962E-532D122A6710", "UnlabelledSecretToken12345678", "sk-abcdefghijklmnopqrstuv", "Bearer SECRET"} {
		for _, kind := range []string{"model", "error"} {
			if got := diagnosticBodyLabel(value, kind); got == value || (got != "" && !strings.HasPrefix(got, "sha256:")) {
				t.Fatalf("body label exposed secret: %q", got)
			}
		}
	}
	if diagnosticBodyLabel("gpt-6.1-sol", "model") != "gpt-6.1-sol" || diagnosticBodyLabel("unsupported_value", "error") != "unsupported_value" {
		t.Fatal("known label was lost")
	}
}

func TestUpstreamDiagnosticCanceledClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var got upstreamHTTPDiagnostic
	body := &upstreamDiagnosticBody{ReadCloser: io.NopCloser(strings.NewReader("pending")), ctx: ctx, emit: func(d upstreamHTTPDiagnostic) { got = d }}
	cancel()
	body.Close()
	if got.ReadError != "canceled" || !got.ClosedEarly || got.BodyComplete {
		t.Fatalf("cancellation lost: %+v", got)
	}
}

func TestUpstreamDiagnosticOnlyFailuresAndBoundedSummary(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	req.GetBody = func() (io.ReadCloser, error) {
		t.Fatal("success or disabled logging must not inspect request body")
		return nil, nil
	}
	t.Setenv("LOG_DISABLED", "false")
	body := io.NopCloser(strings.NewReader("ok"))
	resp := &http.Response{StatusCode: 200, Body: body}
	wrapUpstreamHTTPDiagnostic(req, resp, nil)
	if resp.Body != body {
		t.Fatal("success body wrapped")
	}
	t.Setenv("LOG_DISABLED", "true")
	resp.StatusCode = 400
	wrapUpstreamHTTPDiagnostic(req, resp, nil)
	if resp.Body != body {
		t.Fatal("disabled logging body wrapped")
	}
	if got := summarizeUpstreamRequest([]byte(strings.Repeat("x", upstreamDiagnosticRequestLimit+1))); got.Omitted != "too_large" {
		t.Fatal(got)
	}
	if got := summarizeUpstreamRequest([]byte(`{"model":"Bearer SECRET","service_tier":"SECRET","reasoning":{"effort":"SECRET"}}`)); strings.Contains(got.Model, "SECRET") || got.ServiceTier != "" || got.ReasoningEffort != "" {
		t.Fatal(got)
	}
	if diagnosticReadError(context.Canceled) != "canceled" || diagnosticReadError(context.DeadlineExceeded) != "deadline_exceeded" || diagnosticReadError(io.ErrUnexpectedEOF) != "unexpected_eof" {
		t.Fatal("wrong read classification")
	}
}
