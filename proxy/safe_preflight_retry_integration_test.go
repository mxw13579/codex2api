package proxy

// These are loopback raw-observer and dormant-flag regression tests, NOT
// acceptance tests for safe-preflight retries. Handler is intentionally not
// wired to the experiment until final-send identity and terminal ownership are
// proven. No synthetic fixture here establishes real OAuth/model behavior.

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

func safePreflightLoopbackClient(t *testing.T) *http.Client {
	t.Helper()
	// Never consult environment proxy settings in synthetic fixtures.
	transport := &http.Transport{Proxy: nil}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}
}

func TestSafePreflightRawObserverLoopbackReadBarrier(t *testing.T) {
	const first = "event: codex.rate_limits\r\ndata: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"primary\":{\"used_percent\":10}}}\r\n\r\n"
	const last = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"synthetic-response\"}}\n\n"
	continueUpstream := make(chan struct{})
	upstreamFinished := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(continueUpstream) }) }
	defer release()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, first)
		w.(http.Flusher).Flush()
		select {
		case <-continueUpstream:
			_, _ = io.WriteString(w, last)
		case <-r.Context().Done():
		}
		close(upstreamFinished)
	}))
	t.Cleanup(upstream.Close)
	client := safePreflightLoopbackClient(t)
	observerResult := make(chan error, 1)
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL, nil)
		if err != nil {
			observerResult <- err
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			observerResult <- err
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "text/event-stream")
		var writeErr error
		err = readSafePreflightSSE(resp.Body, func() {}, func(f safePreflightSSEFrame) bool {
			// This fixture only echoes validated raw envelopes, with no retry
			// or metadata-safety assertion. It is not a Handler adapter.
			n, err := w.Write(f.wire)
			if err == nil && n != len(f.wire) {
				err = io.ErrShortWrite
			}
			writeErr = err
			if err != nil {
				return false
			}
			w.(http.Flusher).Flush()
			return true
		})
		if writeErr != nil {
			err = writeErr
		}
		observerResult <- err
	}))
	t.Cleanup(downstream.Close)
	// Release a waiting upstream before either server's cleanup on failure.
	t.Cleanup(release)

	resp, err := client.Get(downstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	var received bytes.Buffer
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		received.WriteString(line)
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	if received.String() != first {
		t.Fatal("downstream did not receive the original complete envelope")
	}
	select {
	case <-upstreamFinished:
		t.Fatal("source finished before the downstream read barrier")
	default:
	}
	release()
	rest, err := io.ReadAll(r)
	if err != nil || string(rest) != last {
		t.Fatalf("remaining stream changed: err=%v", err)
	}
	select {
	case err := <-observerResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("raw observer did not finish")
	}
}

func TestSafePreflightDormantFlagPreservesRealHandlerRelay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	previous := CurrentRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previous) })
	const stream = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"synthetic-relay\"}}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic-output\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"synthetic-relay\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, stream)
	}))
	t.Cleanup(upstream.Close)
	store := auth.NewStore(nil, nil, &database.SystemSettings{
		MaxConcurrency: 1, TestConcurrency: 1,
		MaxRetries: 0, MaxRateLimitRetries: 0,
	})
	t.Cleanup(store.Stop)
	store.AddAccount(&auth.Account{
		DBID: 1, UpstreamType: auth.UpstreamOpenAIResponses,
		BaseURL: upstream.URL, APIKey: "synthetic-test-only",
		Models: []string{"gpt-4.1-direct"}, PlanType: "api",
	})
	handler := NewHandler(store, nil, nil, nil)
	router := gin.New()
	router.POST("/v1/responses", handler.Responses)
	downstream := httptest.NewServer(router)
	t.Cleanup(downstream.Close)
	client := safePreflightLoopbackClient(t)
	var baseline []byte
	for _, enabled := range []bool{false, true} {
		settings := DefaultRuntimeSettings()
		settings.CodexSafePreflightRetry = enabled
		settings.CodexPreflightSSEPassthrough = false
		ApplyRuntimeSettings(settings)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, downstream.URL+"/v1/responses", strings.NewReader(
			`{"model":"gpt-4.1-direct","input":"synthetic-input","stream":true}`,
		))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		if readErr != nil || resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("synthetic-output")) {
			t.Fatalf("relay regression: status=%d err=%v", resp.StatusCode, readErr)
		}
		if !enabled {
			baseline = body
		} else if !bytes.Equal(baseline, body) {
			t.Fatal("dormant experiment changed existing relay output")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d, want exactly one per logical request", calls.Load())
	}
}
