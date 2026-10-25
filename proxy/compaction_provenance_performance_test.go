package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/cache"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

// Count writes, not just cache entries: overwriting one digest for every copy of
// an output_item.done/completed pair would otherwise look like deduplication.
type compactionPerformanceCache struct {
	cache.TokenCache
	mu     sync.Mutex
	writes map[string]int
}

func (c *compactionPerformanceCache) SetRuntime(ctx context.Context, namespace, key string, value json.RawMessage, ttl time.Duration) error {
	if namespace == compactionProvenanceCacheNamespace {
		c.mu.Lock()
		c.writes[key]++
		c.mu.Unlock()
	}
	return c.TokenCache.SetRuntime(ctx, namespace, key, value, ttl)
}

func (c *compactionPerformanceCache) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, count := range c.writes {
		total += count
	}
	return total
}

// Same synthetic native-Codex/Resin loopback setup as
// newContinuousRetryLocalNativeStore, accepting testing.TB for benchmarks too.
// No DB, credential files, real upstream, or websocket transport is involved.
func newCompactionPerformanceHandler(tb testing.TB, upstreamURL string, continuous bool) (*Handler, *compactionPerformanceCache) {
	tb.Helper()
	gin.SetMode(gin.TestMode)
	previousSettings, previousResin := CurrentRuntimeSettings(), GetResinConfig()
	previousKeepalive := continuousRetryKeepaliveInterval
	tb.Cleanup(func() {
		ApplyRuntimeSettings(previousSettings)
		SetResinConfig(previousResin)
		continuousRetryKeepaliveInterval = previousKeepalive
	})
	settings := DefaultRuntimeSettings()
	settings.CodexForceWebsocket = false
	settings.CodexContinueThinking = false
	settings.CodexTelemetryEnabled = false
	settings.CodexRequestCompression = false
	settings.StreamFlushPolicy = StreamFlushPolicyImmediate
	settings.ContinuousRetryPolicy = database.ContinuousRetryPolicy{
		Enabled: continuous, StatusCodes: []int{http.StatusServiceUnavailable},
	}
	ApplyRuntimeSettings(settings)
	continuousRetryKeepaliveInterval = 0
	SetResinConfig(&ResinConfig{BaseURL: upstreamURL, PlatformName: compactionPerformanceResinPlatform})

	store := auth.NewStore(nil, nil, &database.SystemSettings{
		MaxConcurrency: 2, TestConcurrency: 1, TestModel: "gpt-5.5", MaxRetries: 0, MaxRateLimitRetries: 0,
	})
	tb.Cleanup(store.Stop)
	store.SetAffinityMode(auth.AffinityModeOff)
	store.SetRetryIntervalMS(0)
	account := &auth.Account{DBID: 701, AccessToken: "synthetic-access", AccountID: "synthetic-account", PlanType: "pro"}
	account.SetSchedulerPriority(100)
	store.AddAccount(account)
	tb.Cleanup(func() { recyclePooledClient(account, "") })

	tracking := &compactionPerformanceCache{TokenCache: cache.NewMemory(1), writes: make(map[string]int)}
	tb.Cleanup(func() { _ = tracking.Close() })
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
	handler.SetRuntimeCache(tracking)
	return handler, tracking
}

type compactionPerformanceWriter struct {
	*httptest.ResponseRecorder
	beforeWrite func()
	beforeFlush func()
	writeErr    error
	written     int
}

func (w *compactionPerformanceWriter) Write(data []byte) (int, error) {
	if w.beforeWrite != nil {
		w.beforeWrite()
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	n, err := w.ResponseRecorder.Write(data)
	w.written += n
	return n, err
}

func (w *compactionPerformanceWriter) Flush() {
	if w.beforeFlush != nil {
		w.beforeFlush()
	}
	w.ResponseRecorder.Flush()
}

func runCompactionPerformanceRequest(ctx context.Context, handler *Handler, writer http.ResponseWriter, stream bool) {
	body := fmt.Sprintf(`{"model":"gpt-5.5","input":"synthetic fixture","store":false,"stream":%t}`, stream)
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")
	handler.Responses(c)
}

func compactionPerformanceItem(state string) string {
	return fmt.Sprintf(`{"type":"response.output_item.done","output_index":0,"item":{"type":"compaction","encrypted_content":%q}}`, state)
}

func compactionPerformanceCompleted(state string) string {
	output := "[]"
	if state != "" {
		output = fmt.Sprintf(`[{"type":"compaction","encrypted_content":%q}]`, state)
	}
	return fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_compaction_performance","status":"completed","output":%s,"usage":{"input_tokens":1,"output_tokens":1}}}`, output)
}

const (
	compactionPerformanceResinPlatform = "compaction-performance-fixture"
	compactionPerformanceResponsesPath = "/" + compactionPerformanceResinPlatform + "/https/chatgpt.com/backend-api/codex/responses"
)

// Telemetry jobs queued by earlier tests resolve the global Resin URL when they
// are sent, even with telemetry disabled here. Admit only this fixture's exact
// POST target and synthetic identities before counting or consuming an attempt.
func isCompactionPerformanceResponseRequest(r *http.Request) bool {
	if r.Method != http.MethodPost || r.URL.RequestURI() != compactionPerformanceResponsesPath {
		return false
	}
	switch r.Header.Get("X-Resin-Account") {
	case "701":
		return r.Header.Get("Chatgpt-Account-Id") == "synthetic-account" &&
			r.Header.Get("Authorization") == "Bearer synthetic-access"
	case "702":
		return r.Header.Get("Chatgpt-Account-Id") == "synthetic-second-account" &&
			r.Header.Get("Authorization") == "Bearer synthetic-second-access"
	default:
		return false
	}
}

func newCompactionPerformanceSequenceSSEServer(tb testing.TB, attempts [][]string) (*httptest.Server, *atomic.Int32) {
	tb.Helper()
	if len(attempts) == 0 {
		tb.Fatal("SSE fixture requires at least one attempt")
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isCompactionPerformanceResponseRequest(r) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		attempt := int(calls.Add(1)) - 1
		if attempt >= len(attempts) {
			attempt = len(attempts) - 1
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range attempts[attempt] {
			if _, err := io.WriteString(w, "data: "+event+"\n\n"); err != nil {
				return
			}
		}
	}))
	tb.Cleanup(server.Close)
	return server, &calls
}

func TestCompactionPerformanceSSEFixturesIgnoreUnrelatedRequests(t *testing.T) {
	failed := `{"type":"response.failed","response":{"status":"failed","status_code":503,"error":{"code":"server_error","message":"synthetic isolation failure"}}}`
	winner := compactionPerformanceCompleted("synthetic-isolation-winner")
	small, large := compactionPerformanceDelta(8), compactionPerformanceDelta(32)
	textResponse := string(small) + string(small) + string(large) + "data: " + compactionPerformanceCompleted("") + "\n\n"
	for _, fixture := range []struct {
		name  string
		start func(testing.TB) (*httptest.Server, *atomic.Int32)
		want  [2]string
	}{
		{
			name: "sequence",
			start: func(tb testing.TB) (*httptest.Server, *atomic.Int32) {
				return newCompactionPerformanceSequenceSSEServer(tb, [][]string{{failed}, {winner}})
			},
			want: [2]string{"data: " + failed + "\n\n", "data: " + winner + "\n\n"},
		},
		{
			name: "text",
			start: func(tb testing.TB) (*httptest.Server, *atomic.Int32) {
				return newCompactionPerformanceTextServer(tb, small, 2, large, 1)
			},
			want: [2]string{textResponse, textResponse},
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			upstream, calls := fixture.start(t)
			client := upstream.Client()
			client.Timeout = 5 * time.Second
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			request := func(method, path string, headers map[string]string) (int, string, string) {
				t.Helper()
				// Always address this loopback server; target hosts appear only in
				// the Resin path. No telemetry worker or global config is needed.
				req, err := http.NewRequest(method, upstream.URL+path, strings.NewReader(`{}`))
				if err != nil {
					t.Fatal("could not create loopback fixture request")
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Resin-Account", "701")
				req.Header.Set("Chatgpt-Account-Id", "synthetic-account")
				req.Header.Set("Authorization", "Bearer synthetic-access")
				for name, value := range headers {
					req.Header.Set(name, value)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal("loopback fixture request failed")
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal("could not read loopback fixture response")
				}
				return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
			}
			unrelated := []struct {
				name, method, path string
				headers            map[string]string
			}{
				{"analytics", http.MethodPost, "/" + compactionPerformanceResinPlatform + "/https/chatgpt.com/backend-api/codex/analytics-events/events", nil},
				{"metrics", http.MethodPost, "/" + compactionPerformanceResinPlatform + "/https/ab.chatgpt.com/otlp/v1/metrics", nil},
				{"bare_responses", http.MethodPost, "/responses", nil},
				{"other_platform", http.MethodPost, "/other-fixture/https/chatgpt.com/backend-api/codex/responses", nil},
				{"other_host", http.MethodPost, "/" + compactionPerformanceResinPlatform + "/https/synthetic.invalid/backend-api/codex/responses", nil},
				{"compact", http.MethodPost, compactionPerformanceResponsesPath + "/compact", nil},
				{"query", http.MethodPost, compactionPerformanceResponsesPath + "?unrelated=1", nil},
				{"method", http.MethodGet, compactionPerformanceResponsesPath, nil},
				{"missing_identity", http.MethodPost, compactionPerformanceResponsesPath, map[string]string{"X-Resin-Account": ""}},
				{"other_resin_account", http.MethodPost, compactionPerformanceResponsesPath, map[string]string{"X-Resin-Account": "799"}},
				{"other_account_id", http.MethodPost, compactionPerformanceResponsesPath, map[string]string{"Chatgpt-Account-Id": "synthetic-unrelated-account"}},
				{"other_authorization", http.MethodPost, compactionPerformanceResponsesPath, map[string]string{"Authorization": "Bearer synthetic-unrelated-access"}},
			}
			// Inject noise before, between, and after the two business attempts,
			// synchronously: correctness must not depend on worker scheduling.
			for completed := 0; completed <= len(fixture.want); completed++ {
				for _, noise := range unrelated {
					status, contentType, body := request(noise.method, noise.path, noise.headers)
					if status != http.StatusNotFound || contentType == "text/event-stream" || body != "" {
						t.Fatalf("unrelated %s request received fixture data: status=%d bytes=%d", noise.name, status, len(body))
					}
					if got := calls.Load(); got != int32(completed) {
						t.Fatalf("unrelated %s request changed business calls to %d, want %d", noise.name, got, completed)
					}
				}
				if completed == len(fixture.want) {
					break
				}
				var headers map[string]string
				if completed == 1 {
					headers = map[string]string{
						"X-Resin-Account":    "702",
						"Chatgpt-Account-Id": "synthetic-second-account",
						"Authorization":      "Bearer synthetic-second-access",
					}
				}
				status, contentType, body := request(http.MethodPost, compactionPerformanceResponsesPath, headers)
				if status != http.StatusOK || contentType != "text/event-stream" || body != fixture.want[completed] {
					t.Fatalf("business attempt %d returned unexpected SSE sequence: status=%d bytes=%d", completed+1, status, len(body))
				}
				if got := calls.Load(); got != int32(completed+1) {
					t.Fatalf("business calls = %d, want %d", got, completed+1)
				}
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("business calls = %d, want 2", got)
			}
		})
	}
}

func TestCodexHTTPCompactionProvenanceAttemptCommit(t *testing.T) {
	const failedState = "synthetic-failed-compaction"
	const winnerState = "synthetic-winning-compaction"
	for _, stream := range []bool{true, false} {
		for _, scenario := range []string{"success", "retry", "failed_terminal", "write_failure", "cancel_at_write"} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, scenario), func(t *testing.T) {
				winner := []string{
					compactionPerformanceItem(winnerState),
					compactionPerformanceItem(winnerState),
					compactionPerformanceCompleted(winnerState),
				}
				attempts := [][]string{winner}
				if scenario == "retry" {
					attempts = [][]string{{
						compactionPerformanceItem(failedState),
						`{"type":"response.output_text.delta","delta":"private failed attempt"}`,
						`{"type":"response.failed","response":{"status":"failed","status_code":503,"error":{"code":"server_error","message":"synthetic retry"}}}`,
					}, winner}
				} else if scenario == "failed_terminal" {
					attempts = [][]string{{
						compactionPerformanceItem(failedState),
						`{"type":"response.failed","response":{"status":"failed","status_code":400,"error":{"code":"invalid_request_error","message":"synthetic terminal failure"}}}`,
					}}
				}
				upstream, calls := newCompactionPerformanceSequenceSSEServer(t, attempts)
				handler, tracking := newCompactionPerformanceHandler(t, upstream.URL, true)
				wantAccount := int64(701)
				if scenario == "retry" {
					second := &auth.Account{DBID: 702, AccessToken: "synthetic-second-access", AccountID: "synthetic-second-account", PlanType: "pro"}
					handler.store.AddAccount(second)
					t.Cleanup(func() { recyclePooledClient(second, "") })
					wantAccount = second.ID()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				writer := &compactionPerformanceWriter{ResponseRecorder: httptest.NewRecorder()}
				writeAttempts := 0
				writer.beforeWrite = func() {
					writeAttempts++
					if tracking.writeCount() != 0 {
						t.Error("provenance was published before downstream commit finished")
					}
					if scenario == "cancel_at_write" {
						cancel()
					}
				}
				writer.beforeFlush = func() {
					if tracking.writeCount() != 0 {
						t.Error("provenance was published before downstream flush finished")
					}
				}
				if scenario == "write_failure" {
					writer.writeErr = io.ErrClosedPipe
				}
				runCompactionPerformanceRequest(ctx, handler, writer, stream)
				wantCalls := int32(len(attempts))
				if calls.Load() != wantCalls {
					t.Fatalf("upstream attempts = %d, want %d", calls.Load(), wantCalls)
				}
				if _, ok, err := tracking.GetRuntime(context.Background(), compactionProvenanceCacheNamespace, compactionContentDigest(failedState)); err != nil || ok {
					t.Fatalf("failed attempt published provenance: found=%t err=%v", ok, err)
				}
				success := scenario == "success" || scenario == "retry"
				if !success {
					if tracking.writeCount() != 0 {
						t.Fatal("failed/canceled attempt published provenance")
					}
					if scenario == "cancel_at_write" && (writeAttempts == 0 || ctx.Err() != context.Canceled) {
						t.Fatal("cancellation hook did not run")
					}
					if scenario == "write_failure" && (writeAttempts == 0 || writer.written != 0) {
						t.Fatal("downstream write failure was not exercised")
					}
					return
				}
				if writer.Code != http.StatusOK || writer.written == 0 || !strings.Contains(writer.Body.String(), `"status":"completed"`) {
					t.Fatalf("winner was not committed: status=%d bytes=%d", writer.Code, writer.written)
				}
				if stream && !writer.Flushed {
					t.Fatal("winner was published without flushing the streaming commit")
				}
				if strings.Contains(writer.Body.String(), failedState) || strings.Contains(writer.Body.String(), "private failed attempt") {
					t.Fatal("failed attempt escaped private replay")
				}
				if got := tracking.writeCount(); got != 1 {
					t.Fatalf("duplicate compaction publications = %d, want exactly one digest", got)
				}
				raw, ok, err := tracking.GetRuntime(context.Background(), compactionProvenanceCacheNamespace, compactionContentDigest(winnerState))
				if err != nil || !ok {
					t.Fatalf("committed winner provenance: found=%t err=%v", ok, err)
				}
				record, err := decodeCompactionProvenanceRecord(raw)
				if err != nil || record.AccountID != wantAccount || record.CompatibilityDomain != nativeCodexCompactionDomain {
					t.Fatalf("winner account/domain mismatch: account=%d domain=%q err=%v", record.AccountID, record.CompatibilityDomain, err)
				}
				if bytes.Contains(raw, []byte(winnerState)) {
					t.Fatal("provenance cache retained opaque content")
				}
			})
		}
	}
}

func TestCodexHTTPCompactionProvenanceReplayFailure(t *testing.T) {
	for _, stage := range []string{"write_limit", "commit_read"} {
		t.Run(stage, func(t *testing.T) {
			const state = "synthetic-replay-failure-compaction"
			upstream, calls := newCompactionPerformanceSequenceSSEServer(t, [][]string{{compactionPerformanceItem(state), compactionPerformanceCompleted(state)}})
			handler, tracking := newCompactionPerformanceHandler(t, upstream.URL, true)
			commitRead := false
			handler.continuousRetryReplayFactory = func() *continuousRetryReplay {
				if stage == "write_limit" {
					return newContinuousRetryReplayWithLimits(0, 1)
				}
				replay := newContinuousRetryReplayWithLimits(0, 1<<20)
				replay.beforeReadForTest = func(r *continuousRetryReplay) {
					commitRead = true
					if r.file == nil {
						t.Error("fixture did not create a file-backed replay")
						return
					}
					_ = r.file.Close()
				}
				return replay
			}
			writer := &compactionPerformanceWriter{ResponseRecorder: httptest.NewRecorder()}
			runCompactionPerformanceRequest(context.Background(), handler, writer, true)
			if calls.Load() != 1 || writer.Code != http.StatusInternalServerError {
				t.Fatalf("local replay failure: attempts=%d status=%d", calls.Load(), writer.Code)
			}
			if stage == "commit_read" && !commitRead {
				t.Fatal("replay commit failure was not exercised")
			}
			if tracking.writeCount() != 0 || strings.Contains(writer.Body.String(), state) {
				t.Fatal("failed replay published provenance or private compaction state")
			}
		})
	}
}

func TestCodexHTTPCompactionProvenanceDisabledRetryUnchanged(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			const state = "synthetic-immediate-compaction"
			upstream, _ := newCompactionPerformanceSequenceSSEServer(t, [][]string{{compactionPerformanceItem(state), compactionPerformanceItem(state), compactionPerformanceCompleted(state)}})
			handler, tracking := newCompactionPerformanceHandler(t, upstream.URL, false)
			writer := &compactionPerformanceWriter{ResponseRecorder: httptest.NewRecorder(), writeErr: io.ErrClosedPipe}
			runCompactionPerformanceRequest(context.Background(), handler, writer, stream)
			// Legacy behavior records as frames arrive, even if the later write fails.
			if got := tracking.writeCount(); got != 3 {
				t.Fatalf("non-continuous publications = %d, want unchanged per-frame recording", got)
			}
		})
	}
}

func compactionPerformanceDelta(size int) []byte {
	return []byte(`data: {"type":"response.output_text.delta","delta":"` + strings.Repeat("x", size) + "\"}\n\n")
}

func newCompactionPerformanceTextServer(tb testing.TB, small []byte, smallCount int, large []byte, largeCount int) (*httptest.Server, *atomic.Int32) {
	tb.Helper()
	var calls atomic.Int32
	terminal := "data: " + compactionPerformanceCompleted("") + "\n\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isCompactionPerformanceResponseRequest(r) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, batch := range []struct {
			frame []byte
			count int
		}{{small, smallCount}, {large, largeCount}} {
			for i := 0; i < batch.count; i++ {
				if _, err := w.Write(batch.frame); err != nil {
					return
				}
			}
		}
		_, _ = io.WriteString(w, terminal)
	}))
	tb.Cleanup(server.Close)
	return server, &calls
}

func TestCodexHTTPCompactionProvenanceTextDeltaRetention(t *testing.T) {
	// 16 MiB of large deltas plus thousands of small deltas. Measure while the
	// real handler is still live, at the first downstream write, not after it
	// returns and drops the old [][]byte. The streaming replay must have spilled;
	// the non-streaming path has no replay and must not retain delta history.
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			small, large := compactionPerformanceDelta(128), compactionPerformanceDelta(256<<10)
			const smallCount, largeCount = 4096, 64
			upstream, calls := newCompactionPerformanceTextServer(t, small, smallCount, large, largeCount)
			handler, tracking := newCompactionPerformanceHandler(t, upstream.URL, true)
			spilled := false
			handler.continuousRetryReplayFactory = func() *continuousRetryReplay {
				replay := newContinuousRetryReplay()
				replay.beforeReadForTest = func(r *continuousRetryReplay) {
					spilled = r.file != nil && r.memory.Cap() == 0
				}
				return replay
			}
			writer := &compactionPerformanceWriter{ResponseRecorder: httptest.NewRecorder()}
			writer.Body = nil // Do not let the downstream fixture retain the stream.
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			var retained int64
			measured := false
			writer.beforeWrite = func() {
				if measured {
					return
				}
				measured = true
				runtime.GC()
				var during runtime.MemStats
				runtime.ReadMemStats(&during)
				retained = int64(during.HeapAlloc) - int64(before.HeapAlloc)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCompactionPerformanceRequest(ctx, handler, writer, stream)
			if calls.Load() != 1 || writer.Code != http.StatusOK || !measured || writer.written == 0 {
				t.Fatalf("did not reach successful commit: calls=%d status=%d measured=%t bytes=%d", calls.Load(), writer.Code, measured, writer.written)
			}
			if stream && (!spilled || writer.written < smallCount*len(small)+largeCount*len(large)) {
				t.Fatal("fixture did not commit the full file-backed streaming replay")
			}
			if !stream && writer.written > 64<<10 {
				t.Fatal("non-streaming fixture unexpectedly returned delta history")
			}
			// Allow parser/transport buffers and incidental allocations, but not a
			// second in-heap copy of the 16+ MiB stream alongside the disk replay.
			if retained > 8<<20 {
				t.Fatalf("text-only attempt retained %d bytes at commit; want <8 MiB without provenance frame copies", retained)
			}
			if tracking.writeCount() != 0 {
				t.Fatal("text-only attempt published compaction provenance")
			}
			t.Logf("text-only attempt retained %d bytes at commit (stream=%t)", retained, stream)
		})
	}
}

func TestCompactionProvenanceTextDeltaInspectionDoesNotAllocate(t *testing.T) {
	frame := bytes.TrimSuffix(bytes.TrimPrefix(compactionPerformanceDelta(1<<20), []byte("data: ")), []byte("\n\n"))
	var digests compactionProvenanceDigests
	allocs := testing.AllocsPerRun(32, func() { digests.addPayload(frame) })
	if digests != nil || allocs != 0 {
		t.Fatalf("plain text inspection retained/allocated provenance: digests=%d allocations=%g", len(digests), allocs)
	}
}

// End-to-end allocations include SSE parsing and HTTP replay, not just the
// already-existing digest helper. Compare B/op before/after for both paths;
// use -benchtime=1x on memory-constrained machines. All traffic is loopback.
func BenchmarkCodexHTTPCompactionProvenanceTextDeltas(b *testing.B) {
	for _, stream := range []bool{true, false} {
		for _, shape := range []struct {
			name  string
			size  int
			count int
		}{{"many_small", 128, 4096}, {"large_spilled", 256 << 10, 64}} {
			b.Run(fmt.Sprintf("stream=%t/%s", stream, shape.name), func(b *testing.B) {
				frame := compactionPerformanceDelta(shape.size)
				upstream, _ := newCompactionPerformanceTextServer(b, frame, shape.count, nil, 0)
				handler, tracking := newCompactionPerformanceHandler(b, upstream.URL, true)
				b.ReportAllocs()
				b.SetBytes(int64(len(frame) * shape.count))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					writer := &compactionPerformanceWriter{ResponseRecorder: httptest.NewRecorder()}
					writer.Body = nil
					runCompactionPerformanceRequest(context.Background(), handler, writer, stream)
					if writer.Code != http.StatusOK || writer.written == 0 {
						b.Fatal("fixture did not commit the response")
					}
				}
				b.StopTimer()
				if tracking.writeCount() != 0 {
					b.Fatal("text-only benchmark published compaction provenance")
				}
			})
		}
	}
}
