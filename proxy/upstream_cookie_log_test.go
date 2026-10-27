package proxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

func cookieLogTestSetup(t *testing.T) context.Context {
	t.Helper()
	previous := CurrentRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previous) })
	UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings {
		s.CodexUpstreamCookieLogEnabled = true
		return s
	})
	return WithUpstreamCookieLog(context.Background())
}
func cookieLogTestToken(host string) string {
	b, _ := json.Marshal(map[string]any{"host": host, "iat": 123, "exp": 456})
	return "eyJhbGciOiJFUzI1NiJ9." + base64.RawURLEncoding.EncodeToString(b) + ".signature"
}
func cookieLogTestRead(t *testing.T, raw string) []upstreamCookieEntry {
	t.Helper()
	var envelope struct {
		Entries []upstreamCookieEntry `json:"entries"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Entries
}

func TestUpstreamCookieHTTPRealAndSemantic429(t *testing.T) {
	for _, status := range []int{200, 429} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			cookieLogTestSetup(t)
			token := cookieLogTestToken("chat.gateway.unified-23.api.openai.com")
			payload := `{"type":"response.failed","response":{"id":"resp-limited","error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded"}}}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Cookie") != "__cflb=outbound" {
					t.Error("request cookie changed")
				}
				w.Header().Add("Set-Cookie", "__oailb="+token+"; Path=/; HttpOnly")
				w.Header().Add("Set-Cookie", "__cflb=inbound; Path=/")
				w.Header().Set("Cf-Ray", "ray-http")
				w.WriteHeader(status)
				if status == 200 {
					io.WriteString(w, "data: "+payload+"\n\n")
				} else {
					io.WriteString(w, `{"error":{"code":"rate_limit_error"}}`)
				}
			}))
			defer server.Close()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
			attachUpstreamTrace(c, nil)
			ctx := context.WithValue(c.Request.Context(), upstreamCookieModelKey{}, "gpt-6.1-sol")
			req, _ := http.NewRequestWithContext(ctx, "POST", server.URL, nil)
			req.Header.Set("Cookie", "__cflb=outbound")
			req.Header.Set("Authorization", "Bearer must-not-log-authorization")
			resp, err := doTracedUpstreamRequest(server.Client(), req, &auth.Account{DBID: 4286}, "http://user:must-not-log-proxy@proxy.invalid")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if status == 200 {
				if err := ReadSSEStream(resp.Body, func(data []byte) bool {
					if string(data) != payload {
						t.Errorf("body mutated: %s", data)
					}
					return false
				}); err != nil {
					t.Fatal(err)
				}
			}
			input := &database.UsageLogInput{AccountID: 4286}
			populateUpstreamTrace(c, input)
			entries := cookieLogTestRead(t, input.UpstreamCookieTrace)
			want := 1
			if status == 200 {
				want = 2
			}
			if len(entries) != want {
				t.Fatalf("entries=%d want=%d", len(entries), want)
			}
			last := entries[len(entries)-1]
			if last.Status != 429 || last.HTTPStatus != status || last.AccountID != 4286 || last.UnifiedHost != "chat.gateway.unified-23.api.openai.com" || last.Model != "gpt-6.1-sol" {
				t.Fatalf("entry=%+v", last)
			}
			if last.RequestID != input.RequestID || last.RequestID == "" || last.AttemptID == "" || len(last.SetCookie) != 2 || last.Cookie[0] != "__cflb=outbound" || last.OAILB[0].Value != token {
				t.Fatal("cookie correlation lost")
			}
			if status == 200 && (last.ErrorCode != "turn_rate_limit_exceeded" || last.Event != "response.failed" || entries[0].AttemptID != last.AttemptID) {
				t.Fatal("semantic failure lost")
			}
			if strings.Contains(input.UpstreamCookieTrace, "must-not-log") {
				t.Fatal("unrelated authentication data logged")
			}
		})
	}
}

func TestUpstreamCookiePooledTurnCorrelation(t *testing.T) {
	cookieLogTestSetup(t)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	reqHeader := http.Header{"Cookie": {"__cflb=actually-sent"}}
	resp := &http.Response{StatusCode: 101, Header: http.Header{"Set-Cookie": {"__oailb=" + cookieLogTestToken("chat.gateway.unified-189.api.openai.com")}, "Cf-Ray": {"handshake-ray"}, "X-Request-Id": {"handshake-id"}}}
	trace := NewUpstreamCookieTrace(c.Request.Context(), &auth.Account{DBID: 4283}, "websocket", "connection-one", reqHeader, resp)
	if trace.collector != nil {
		t.Fatal("pooled connection retains first request buffer")
	}
	reqHeader.Set("Cookie", "__cflb=not-sent-on-reuse")
	resp.Header.Set("Set-Cookie", "__oailb=not-actually-observed")
	first := trace.BeginTurn(c.Request.Context(), "turn-one", "gpt-6.1-sol")
	first.ObserveEvent([]byte(`{"type":"codex.response.metadata","headers":{"cf-ray":"turn-one-ray","x-request-id":"turn-one-id"}}`))
	first.ObserveEvent([]byte(`{"type":"response.completed","response":{"id":"resp-ok"}}`))
	firstEntries := cookieLogTestRead(t, SnapshotUpstreamCookieLog(c.Request.Context()))
	resetUpstreamRequestTrace(c)
	second := trace.BeginTurn(c.Request.Context(), "turn-two", "gpt-6.1-sol")
	second.ObserveEvent([]byte(`{"type":"error","status_code":429,"error":{"code":"turn_rate_limit_exceeded"}}`))
	second.ObserveEvent([]byte(`{"type":"response.failed"}`))
	secondEntries := cookieLogTestRead(t, SnapshotUpstreamCookieLog(c.Request.Context()))
	if len(firstEntries) != 3 || len(secondEntries) != 2 {
		t.Fatal("request buffers mixed or duplicate terminal")
	}
	one, two := firstEntries[2], secondEntries[1]
	if one.Status != 200 || two.Status != 429 || one.RequestID == two.RequestID || one.AttemptID == two.AttemptID {
		t.Fatal("turns mixed")
	}
	if one.CFRay != "turn-one-ray" || one.UpstreamRequestID != "turn-one-id" || two.CFRay != "" || two.UpstreamRequestID != "" {
		t.Fatal("per-turn metadata inherited across turns")
	}
	for _, e := range append(firstEntries, secondEntries...) {
		if e.ConnectionID != "connection-one" || e.UnifiedHost != "chat.gateway.unified-189.api.openai.com" || e.Cookie[0] != "__cflb=actually-sent" || e.HandshakeCFRay != "handshake-ray" || e.HandshakeRequestID != "handshake-id" {
			t.Fatal("handshake snapshot mutated")
		}
	}
}

func TestUpstreamCookieDisabledAndProviderScope(t *testing.T) {
	ctx := cookieLogTestSetup(t)
	// File logging settings and the old environment flag do not override admin.
	t.Setenv("LOG_DISABLED", "true")
	t.Setenv("CODEX_UPSTREAM_COOKIE_LOG_ENABLED", "true")
	for _, test := range []struct {
		enabled bool
		account *auth.Account
	}{{false, &auth.Account{DBID: 1}}, {true, &auth.Account{DBID: 1, UpstreamType: "claude"}}} {
		UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings { s.CodexUpstreamCookieLogEnabled = test.enabled; return s })
		if tr := NewUpstreamCookieTrace(ctx, test.account, "http", "", nil, &http.Response{StatusCode: 200}); tr != nil {
			t.Fatal("disabled/provider scope ignored")
		}
	}
	if SnapshotUpstreamCookieLog(ctx) != "" {
		t.Fatal("unexpected collection")
	}
	trace := NewUpstreamCookieTrace(ctx, &auth.Account{DBID: 1}, "http", "", nil, &http.Response{StatusCode: 200})
	if trace == nil || len(cookieLogTestRead(t, SnapshotUpstreamCookieLog(ctx))) != 1 {
		t.Fatal("admin setting incorrectly follows file logs")
	}
	UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings { s.CodexUpstreamCookieLogEnabled = false; return s })
	trace.ObserveEvent([]byte(`{"type":"response.completed"}`))
	if len(cookieLogTestRead(t, SnapshotUpstreamCookieLog(ctx))) != 1 || trace.BeginTurn(ctx, "late-turn", "") != nil {
		t.Fatal("hot disable did not stop new collection")
	}
	var missing *UpstreamCookieTrace
	missing.BeginTurn(ctx, "", " ").ObserveEvent([]byte(`{"type":"response.completed"}`))
}

func TestUpstreamCookieMalformedJWTAndBoundedHeaders(t *testing.T) {
	for _, value := range []string{"invalid", "a.***.z", "a." + base64.RawURLEncoding.EncodeToString([]byte(`{"host":42}`)) + ".z", strings.Repeat("x", 20<<10)} {
		got := decodeUpstreamOAILB(value)
		if got.Host != "" || got.Value != value {
			t.Fatal("malformed JWT accepted")
		}
	}
	ctx := cookieLogTestSetup(t)
	tr := NewUpstreamCookieTrace(ctx, &auth.Account{DBID: 1}, "http", "", http.Header{"Cookie": {strings.Repeat("x", upstreamCookieHeaderLimit+10)}}, &http.Response{StatusCode: 429, Header: http.Header{"Set-Cookie": {"__oailb=invalid; HttpOnly", "__cflb=value; Path=/"}}})
	if tr == nil {
		t.Fatal("missing trace")
	}
	e := cookieLogTestRead(t, SnapshotUpstreamCookieLog(ctx))[0]
	if !e.CookieTruncated || len(e.Cookie[0]) != upstreamCookieHeaderLimit || e.UnifiedHost != "" || e.OAILB[0].Value != "invalid" {
		t.Fatal("unexpected bounded snapshot")
	}
	if headers, truncated := boundedCookieHeaders(make([]string, 65)); len(headers) != 64 || !truncated {
		t.Fatal("header count not bounded")
	}
}

func TestUpstreamCookieConcurrentCollectionBounds(t *testing.T) {
	ctx := cookieLogTestSetup(t)
	collector := upstreamCookieCollectorFromContext(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.append(upstreamCookieEntry{Event: "fixture"})
			_ = collector.snapshot()
		}()
	}
	wg.Wait()
	if len(cookieLogTestRead(t, collector.snapshot())) != 20 {
		t.Fatal("concurrent records lost")
	}
	for _, size := range []int{1, upstreamCookieHeaderLimit} {
		bounded := &upstreamCookieCollector{}
		for i := 0; i < upstreamCookieTraceMaxEntries+1; i++ {
			bounded.append(upstreamCookieEntry{Cookie: []string{strings.Repeat("x", size)}})
		}
		raw := bounded.snapshot()
		if !json.Valid([]byte(raw)) || len(raw) > upstreamCookieTraceMaxBytes+64 || !strings.Contains(raw, `"truncated":true`) {
			t.Fatal("request collection not bounded valid JSON")
		}
	}
}

func TestUpstreamCookieWrappedHTTPStream(t *testing.T) {
	for _, event := range []string{
		`{"type":"response.completed","response":{"id":"fixture-ok"}}`,
		`{"type":"response.failed","response":{"error":{"code":"turn_rate_limit_exceeded"}}}`,
	} {
		ctx := cookieLogTestSetup(t)
		trace := NewUpstreamCookieTrace(ctx, &auth.Account{DBID: 1}, "http", "", nil, &http.Response{StatusCode: 200})
		base := &upstreamCookieBody{ReadCloser: io.NopCloser(strings.NewReader("data: " + event + "\n\n")), trace: trace}
		// Exercise the real wrappers without dispatching simulated telemetry.
		attempt := &codexTelemetryAttempt{}
		attempt.done.Do(func() {})
		body := &encryptedErrorObserver{ReadCloser: &codexTelemetryBody{ReadCloser: base, attempt: attempt}, stream: true, record: func([]byte) {}}
		if err := ReadSSEStream(body, func(data []byte) bool {
			if string(data) != event {
				t.Error("wrapped bytes changed")
			}
			if len(cookieLogTestRead(t, SnapshotUpstreamCookieLog(ctx))) != 2 {
				t.Error("terminal not captured before callback")
			}
			return false
		}); err != nil {
			t.Fatal(err)
		}
		entries := cookieLogTestRead(t, SnapshotUpstreamCookieLog(ctx))
		if entries[1].Status != 200 && entries[1].Status != 429 {
			t.Fatal("wrapped terminal outcome missing")
		}
	}
}

func TestUpstreamCookieAdminRequestsAndImageSpooling(t *testing.T) {
	parent := cookieLogTestSetup(t)
	upstreamCookieCollectorFromContext(parent).append(upstreamCookieEntry{Event: "parent-only"})
	previousResin := resinCfg.Load()
	t.Cleanup(func() { resinCfg.Store(previousResin); ConfigureImagePipeline(0); ConfigureImageExecutionLimit(0) })
	UpdateRuntimeSettings(func(s RuntimeSettings) RuntimeSettings {
		s.CodexTelemetryEnabled = false
		s.CodexForceWebsocket = false
		s.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
		return s
	})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = readUpstreamRequestBody(r)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Set-Cookie", "__oailb="+cookieLogTestToken("fixture.unified.example")+"; Path=/; HttpOnly")
		io.WriteString(w, `data: {"type":"response.completed","response":{"id":"fixture-response","status":"completed","output":[{"type":"image_generation_call","result":"`+tinyPNGBase64+`","output_format":"png"}]}}`+"\n\n")
	}))
	defer upstream.Close()
	SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "cookie-fixture"})
	db, err := database.New("sqlite", filepath.Join(t.TempDir(), "admin-cookies.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := auth.NewStore(db, nil, &database.SystemSettings{MaxConcurrency: 2, TestConcurrency: 1})
	t.Cleanup(store.Stop)
	store.AddAccount(&auth.Account{DBID: 1, AccessToken: "synthetic-token", AccountID: "cookie-fixture", PlanType: "plus"})
	h := NewHandler(store, db, &config.Config{AllowAnonymousV1: true}, nil)
	body := []byte(`{"model":"gpt-image-2","prompt":"synthetic image","images":[{"image_url":"data:image/png;base64,` + tinyPNGBase64 + `"}]}`)
	for _, generate := range []func(context.Context, []byte, *database.APIKeyRow, bool) ([]byte, int, error){h.GenerateImageOnceForAdmin, h.GenerateImageEditForAdmin} {
		if _, status, err := generate(parent, body, nil, false); err != nil || status != 200 {
			t.Fatalf("admin image status=%d err=%v", status, err)
		}
	}
	ConfigureImagePipeline(1)
	ConfigureImageExecutionLimit(1)
	executionCtx, releaseExecution, err := AcquireImageExecution(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseExecution()
	jobCtx, pipeline, err := NewImagePipeline(executionCtx, t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()
	pipeline.Output = func(context.Context, QueuedImageResult) error { return nil }
	if _, status, err := h.GenerateQueuedImage(jobCtx, func() ([]byte, error) { return body, nil }, false, nil); err != nil || status != 200 {
		t.Fatalf("queued image status=%d err=%v", status, err)
	}
	if status, _ := h.ExecuteInternalResponse(parent, []byte(`{"model":"gpt-5.5","input":"synthetic internal request"}`)); status != 200 {
		t.Fatalf("internal response status=%d", status)
	}
	db.FlushUsageLogs()
	logs, err := db.ListRecentUsageLogs(parent, 10)
	if err != nil || len(logs) != 4 {
		t.Fatalf("admin usage rows=%d err=%v", len(logs), err)
	}
	ids := map[string]bool{}
	for _, log := range logs {
		raw, err := db.GetUsageLogUpstreamCookies(parent, log.ID)
		if err != nil {
			t.Fatal(err)
		}
		entries := cookieLogTestRead(t, raw)
		if len(entries) != 2 || entries[1].Event != "response.completed" || entries[1].UnifiedHost != "fixture.unified.example" || entries[1].RequestID != log.RequestID {
			t.Fatal("admin request lost its isolated cookie diagnostics")
		}
		if log.RequestID == "" || ids[log.RequestID] {
			t.Fatal("admin requests share request identity")
		}
		ids[log.RequestID] = true
	}
	if entries := cookieLogTestRead(t, SnapshotUpstreamCookieLog(parent)); len(entries) != 1 || entries[0].Event != "parent-only" {
		t.Fatal("child requests modified parent diagnostics")
	}
}

func TestUpstreamCookieSilentRetryFinalUsageAndSnapshot(t *testing.T) {
	cookieLogTestSetup(t)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	attachUpstreamTrace(c, nil)
	ctx := c.Request.Context()
	failed := NewUpstreamCookieTrace(ctx, &auth.Account{DBID: 1}, "http", "", nil, &http.Response{StatusCode: 200})
	failed.ObserveEvent([]byte(`{"type":"response.failed","response":{"error":{"code":"turn_rate_limit_exceeded"}}}`))
	frozen := snapshotUpstreamTrace(ctx)
	// The ordinary attempt reset must not discard a silently suppressed 429.
	resetUpstreamAttemptTrace(ctx)
	success := NewUpstreamCookieTrace(ctx, &auth.Account{DBID: 2}, "http", "", nil, &http.Response{StatusCode: 200})
	success.ObserveEvent([]byte(`{"type":"response.completed"}`))
	db, err := database.New("sqlite", filepath.Join(t.TempDir(), "cookie-usage.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	h := &Handler{db: db}
	input := &database.UsageLogInput{AccountID: 2, StatusCode: 200}
	h.logUsageForRequest(c, input)
	db.FlushUsageLogs()
	logs, err := db.ListRecentUsageLogs(ctx, 10)
	if err != nil || len(logs) != 1 || logs[0].StatusCode != 200 {
		t.Fatal("final usage log not saved")
	}
	raw, err := db.GetUsageLogUpstreamCookies(ctx, logs[0].ID)
	if err != nil || raw != input.UpstreamCookieTrace {
		t.Fatal("request cookies not saved with usage log")
	}
	entries := cookieLogTestRead(t, raw)
	if len(entries) != 4 || entries[1].Status != 429 || entries[1].AccountID != 1 || entries[3].Status != 200 || entries[3].AccountID != 2 {
		t.Fatal("successful usage row lost silent retries or their accounts")
	}
	hiddenRound := &database.UsageLogInput{AccountID: 1}
	frozen.apply(hiddenRound)
	populateUpstreamTrace(c, hiddenRound)
	if len(cookieLogTestRead(t, hiddenRound.UpstreamCookieTrace)) != 2 {
		t.Fatal("continuation snapshot overwritten by later request events")
	}
	resetUpstreamRequestTrace(c)
	// Even a late terminal from the previous request cannot enter the new one.
	success.ObserveEvent([]byte(`{"type":"response.completed"}`))
	if SnapshotUpstreamCookieLog(c.Request.Context()) != "" {
		t.Fatal("new downstream request inherited cookies")
	}
}
