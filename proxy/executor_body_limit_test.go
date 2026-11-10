package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHandlersBoundUpstreamErrorBodies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const marker = "must-not-read-tail"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"`+strings.Repeat("x", 2*maxUpstreamErrorBodyBytes)+marker+`"}}`)
	}))
	defer upstream.Close()
	store := newOpenAIResponsesRelayStore(upstream.URL)
	defer store.Stop()
	h := NewHandler(store, nil, nil, nil)
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/responses/compact"} {
		t.Run(path, func(t *testing.T) {
			body := `{"model":"gpt-4.1-direct","input":"hello","stream":false}`
			if path == "/v1/chat/completions" {
				body = `{"model":"gpt-4.1-direct","messages":[{"role":"user","content":"hello"}],"stream":false}`
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			switch path {
			case "/v1/responses":
				h.Responses(c)
			case "/v1/chat/completions":
				h.ChatCompletions(c)
			default:
				h.ResponsesCompact(c)
			}
			if recorder.Code != http.StatusBadRequest || strings.Contains(recorder.Body.String(), marker) || recorder.Body.Len() > maxUpstreamErrorBodyBytes+1024 {
				t.Fatalf("unbounded or unexpected error response: status=%d bytes=%d", recorder.Code, recorder.Body.Len())
			}
		})
	}
}

func TestReadUpstreamErrorBodyBoundsPrefix(t *testing.T) {
	input := strings.NewReader(strings.Repeat("x", 2*maxUpstreamErrorBodyBytes))
	data, err := readUpstreamErrorBody(context.Background(), input)
	if !errors.Is(err, errUpstreamErrorBodyTooLarge) || len(data) != maxUpstreamErrorBodyBytes || input.Len() != maxUpstreamErrorBodyBytes-1 {
		t.Fatalf("len=%d remaining=%d err=%v", len(data), input.Len(), err)
	}
	data, err = readUpstreamErrorBody(context.Background(), strings.NewReader(`{"error":{"code":"restricted"}}`))
	if err != nil || string(data) != `{"error":{"code":"restricted"}}` {
		t.Fatalf("small error changed: %q err=%v", data, err)
	}
}

func TestReadAllLimitedRejectsTruncatedSuccessJSON(t *testing.T) {
	data, err := readAllLimitedWithContinuousRetryKeepalive(context.Background(), strings.NewReader(`{"padding":"123456789"}`), 8)
	if err == nil || data != nil {
		t.Fatalf("oversized success was not rejected: %q err=%v", data, err)
	}
}

func TestCodexAccessRestrictedPeekPreservesBody(t *testing.T) {
	body := []byte(`{"error":{"code":"codex_access_restricted","message":"restricted"}}`)
	resp := &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	if !isCodexAccessRestrictedResponse(resp) {
		t.Fatal("expected codex_access_restricted to be detected")
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("restored body = %q, want %q", got, body)
	}
}

func TestCodexAccessRestrictedPeekDoesNotBufferWholeBody(t *testing.T) {
	prefix := `{"error":{"code":"other","padding":"`
	suffix := `"}}`
	padding := strings.Repeat("x", 2<<20)
	body := []byte(prefix + padding + suffix)
	resp := &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	if isCodexAccessRestrictedResponse(resp) {
		t.Fatal("non-restricted error must not be detected")
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("restored body length = %d, want %d", len(got), len(body))
	}
}
