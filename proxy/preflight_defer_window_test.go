package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

const (
	pdwRateLimits = "event: codex.rate_limits\n" +
		`data: {"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"primary":{"used_percent":1,"window_minutes":300}}}` + "\n\n"
	pdwOverload = "event: error\n" +
		`data: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded."}}` + "\n\n" +
		"event: response.failed\n" +
		`data: {"type":"response.failed","response":{"id":"resp_overloaded","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded."}}}` + "\n\n"
	pdwUnsupported = "event: response.failed\n" +
		`data: {"type":"response.failed","response":{"id":"resp_unsupported","status":"failed","status_code":400,"error":{"type":"invalid_request_error","code":"unsupported_value","message":"Unsupported value: 'none' is not supported with the 'gpt-5.6-sol' model."}}}` + "\n\n"
	pdwSuccess = "event: response.output_text.delta\n" +
		`data: {"type":"response.output_text.delta","delta":"recovered-ok"}` + "\n\n" +
		"event: response.completed\n" +
		`data: {"type":"response.completed","response":{"id":"resp_recovered","object":"response","status":"completed","model":"gpt-5.6-sol",` +
		`"output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"recovered-ok","annotations":[]}]}],` +
		`"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}` + "\n\n"
)

type pdwAttempt func(w http.ResponseWriter)

func pdwFrames(frames ...string) pdwAttempt {
	return func(w http.ResponseWriter) {
		for _, frame := range frames {
			_, _ = io.WriteString(w, frame)
		}
	}
}

// runPreflightDeferCase 以前置元数据透传开启、强制 HTTP 上游的方式跑一次 /v1/responses 流式请求。
func runPreflightDeferCase(t *testing.T, windowMS string, attempts ...pdwAttempt) (*httptest.ResponseRecorder, int32) {
	t.Helper()
	recorder := httptest.NewRecorder()
	calls := runPreflightDeferCaseWithWriter(t, recorder, windowMS, attempts...)
	return recorder, calls
}

func runPreflightDeferCaseWithWriter(t *testing.T, writer http.ResponseWriter, windowMS string, attempts ...pdwAttempt) int32 {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Setenv("CODEX_PREFLIGHT_PASSTHROUGH_DEFER_MS", windowMS)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		n := int(calls.Add(1))
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if n <= len(attempts) {
			attempts[n-1](w)
			return
		}
		_, _ = io.WriteString(w, pdwSuccess)
	}))
	t.Cleanup(server.Close)

	previousRuntime, previousResin := CurrentRuntimeSettings(), resinCfg.Load()
	t.Cleanup(func() {
		ApplyRuntimeSettings(previousRuntime)
		resinCfg.Store(previousResin)
	})
	runtime := previousRuntime
	runtime.CodexForceWebsocket = false
	runtime.CodexPreflightSSEPassthrough = true
	runtime.StreamFlushPolicy = StreamFlushPolicyImmediate
	runtime.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	ApplyRuntimeSettings(runtime)
	SetResinConfig(&ResinConfig{BaseURL: server.URL, PlatformName: "test"})

	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 4, MaxRetries: 3, MaxRateLimitRetries: 3})
	t.Cleanup(store.Stop)
	store.AddAccount(&auth.Account{DBID: 915101, AccessToken: "tok-a", Status: auth.StatusReady})
	store.AddAccount(&auth.Account{DBID: 915102, AccessToken: "tok-b", Status: auth.StatusReady})
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)

	c, _ := gin.CreateTestContext(writer)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses",
		bytes.NewBufferString(`{"model":"gpt-5.6-sol","input":"hi","stream":true}`)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")
	handler.Responses(c)
	return calls.Load()
}

// 窗口内：元数据之后紧跟降载，必须透明换号重试，客户端看不到降载。
func TestPreflightDeferWindowRetriesOverloadAfterMetadata(t *testing.T) {
	rec, calls := runPreflightDeferCase(t, "300", pdwFrames(pdwRateLimits, pdwOverload))
	body := rec.Body.String()
	if calls != 2 {
		t.Fatalf("upstream calls = %d, want 2 (overload retried); body=%s", calls, body)
	}
	if rec.Code != http.StatusOK || !strings.Contains(body, "recovered-ok") || strings.Contains(body, "overloaded") {
		t.Fatalf("retry not transparent: status=%d body=%s", rec.Code, body)
	}
}

// 对照：不配置窗口时保持原透传行为（元数据已写出，降载不重试）。
func TestPreflightDeferWindowDisabledKeepsLegacyPassthrough(t *testing.T) {
	rec, calls := runPreflightDeferCase(t, "", pdwFrames(pdwRateLimits, pdwOverload))
	if calls != 1 {
		t.Fatalf("upstream calls = %d, want 1 (legacy passthrough behavior)", calls)
	}
	if !strings.Contains(rec.Body.String(), "codex.rate_limits") {
		t.Fatalf("legacy passthrough must forward metadata: %s", rec.Body.String())
	}
}

// 窗口内：元数据之后是确定性错误，不重试，按真实 HTTP 状态码返回。
func TestPreflightDeferWindowReturnsDeterministicErrorAsHTTPStatus(t *testing.T) {
	rec, calls := runPreflightDeferCase(t, "300", pdwFrames(pdwRateLimits, pdwUnsupported))
	if calls != 1 {
		t.Fatalf("upstream calls = %d, want 1 (deterministic error must not retry)", calls)
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unsupported_value") {
		t.Fatalf("upstream error detail lost: %s", rec.Body.String())
	}
}

// 窗口内：元数据之后正常输出，元数据随正文一起写出。
func TestPreflightDeferWindowForwardsMetadataWithContent(t *testing.T) {
	rec, calls := runPreflightDeferCase(t, "300", pdwFrames(pdwRateLimits, pdwSuccess))
	body := rec.Body.String()
	if calls != 1 || rec.Code != http.StatusOK {
		t.Fatalf("calls=%d status=%d body=%s", calls, rec.Code, body)
	}
	meta, text := strings.Index(body, "codex.rate_limits"), strings.Index(body, "recovered-ok")
	if meta < 0 || text < 0 || meta > text {
		t.Fatalf("metadata must be delivered before content: %s", body)
	}
}

// Flush observations are copied on the writer goroutine. Tests never read the
// recorder concurrently with the handler/timer, and do not need polling sleeps
// to decide whether metadata reached the downstream before content.
type pdwObservedRecorder struct {
	*httptest.ResponseRecorder
	flushed chan string
}

func (w *pdwObservedRecorder) Flush() {
	w.ResponseRecorder.Flush()
	w.flushed <- w.Body.String()
}

func TestPreflightDeferWindowRestoresPassthroughForLaterMetadata(t *testing.T) {
	recorder := &pdwObservedRecorder{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan string, 32)}
	awaitFlush := func(marker string) bool {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		for {
			select {
			case body := <-recorder.flushed:
				if strings.Contains(body, marker) {
					return true
				}
			case <-timer.C:
				t.Errorf("metadata %q remained buffered before content", marker)
				return false
			}
		}
	}
	attempt := func(w http.ResponseWriter) {
		_, _ = io.WriteString(w, pdwRateLimits)
		w.(http.Flusher).Flush()
		if !awaitFlush("codex.rate_limits") {
			return
		}
		// Expiry must not turn lifecycle events into immediate passthrough.
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_lifecycle_after_expiry\"}}\n\n")
		w.(http.Flusher).Flush()
		quiet := time.NewTimer(50 * time.Millisecond)
	waitLifecycle:
		for {
			select {
			case body := <-recorder.flushed:
				if strings.Contains(body, "resp_lifecycle_after_expiry") {
					t.Error("expiry changed lifecycle buffering")
				}
			case <-quiet.C:
				break waitLifecycle
			}
		}
		// Each of these two post-expiry frames must flush on its own. No
		// content or terminal is sent until both have reached the downstream.
		for _, marker := range []string{"pdw_after_expiry_one", "pdw_after_expiry_two"} {
			_, _ = io.WriteString(w, "data: {\"type\":\"codex.response.metadata\",\"marker\":\""+marker+"\"}\n\n")
			w.(http.Flusher).Flush()
			if !awaitFlush(marker) {
				return
			}
		}
		_, _ = io.WriteString(w, pdwSuccess)
	}
	calls := runPreflightDeferCaseWithWriter(t, recorder, "30", attempt)
	body := recorder.Body.String()
	if calls != 1 || recorder.Code != http.StatusOK || !strings.Contains(body, "recovered-ok") {
		t.Fatalf("calls=%d status=%d, want one successful streaming attempt", calls, recorder.Code)
	}
	if strings.Count(body, "pdw_after_expiry_one") != 1 || strings.Count(body, "pdw_after_expiry_two") != 1 || strings.Count(body, "resp_lifecycle_after_expiry") != 1 {
		t.Fatal("metadata/lifecycle frames were lost or duplicated")
	}
}

// 窗口到期：定时器写出元数据（恢复透传首响），之后的降载与原透传一致，不再重试。
func TestPreflightDeferWindowFlushesMetadataWhenWindowExpires(t *testing.T) {
	stall := func(w http.ResponseWriter) {
		_, _ = io.WriteString(w, pdwRateLimits)
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, pdwOverload)
	}
	rec, calls := runPreflightDeferCase(t, "50", stall)
	if calls != 1 {
		t.Fatalf("upstream calls = %d, want 1 (metadata already flushed by timer)", calls)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "codex.rate_limits") {
		t.Fatalf("timer did not flush metadata: status=%d body=%s", rec.Code, rec.Body.String())
	}
}
