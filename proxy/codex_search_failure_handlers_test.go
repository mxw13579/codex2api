package proxy

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

// These are consumer integration tests, not hand-written response.failed
// fixtures: run the real hosted-search bridge first, then serve its unmodified
// wire through loopback HTTP to the real handler/executor and temporary SQLite.
// Search/model callbacks and identities are entirely synthetic. No public
// search service or account authentication is involved; no production hook is added.
func TestCodexSearchFailureHandlers(t *testing.T) {
	for _, tc := range []struct {
		route  string
		stream bool
		status int
	}{
		{"responses", true, http.StatusTooManyRequests},
		{"responses", false, http.StatusBadRequest},
		{"chat", true, http.StatusBadRequest},
		{"chat", false, http.StatusTooManyRequests},
		{"messages", true, http.StatusUnauthorized},
		{"messages", false, http.StatusTooManyRequests},
	} {
		t.Run(fmt.Sprintf("%s/stream=%t/status=%d", tc.route, tc.stream, tc.status), func(t *testing.T) {
			wire := codexSearchFailureHandlerWire(t, tc.status, true)
			h, db, key, calls := newCodexSearchFailureHandlerFixture(t, []string{wire})
			recorder := invokeCodexSearchFailureHandler(t, h, key, tc.route, tc.stream)
			assertCodexSearchFailureHandlerResponse(t, recorder, tc.route, tc.stream, tc.status)
			if calls.Load() != 1 {
				t.Errorf("handler attempts=%d, want 1", calls.Load())
			}
			assertCodexSearchFailureHandlerAccounting(t, db, key.ID, tc.route, tc.stream, tc.status, 1)
		})
	}
}

// A hidden round has already spent real tokens even when no public body has
// committed. Retrying must not discard that usage when the attempt-local usage
// variable is reset. These assertions deliberately require preservation rather
// than accepting the current missing-retry-accounting behavior or skipping it.
func TestCodexSearchFailureHandlersRetryRetainsUsage(t *testing.T) {
	for _, tc := range []struct {
		route  string
		stream bool
		text   bool
	}{
		{"responses", false, true},
		{"chat", false, true},
		{"messages", false, true},
		// Chat does not expose hosted-search items as content. This exercises
		// its pre-content retry gate without removing bridge-produced frames.
		{"chat", true, false},
	} {
		t.Run(fmt.Sprintf("%s/stream=%t", tc.route, tc.stream), func(t *testing.T) {
			first := codexSearchFailureHandlerWire(t, http.StatusTooManyRequests, tc.text)
			last := codexSearchFailureHandlerWire(t, http.StatusBadRequest, tc.text)
			h, db, key, calls := newCodexSearchFailureHandlerFixture(t, []string{first, last})
			recorder := invokeCodexSearchFailureHandler(t, h, key, tc.route, tc.stream)
			// Both attempts fail. The retry must expose only the last error, not
			// a success, partial buffered output, or the first 429 terminal.
			assertCodexSearchFailureHandlerResponse(t, recorder, tc.route, false, http.StatusBadRequest)
			if strings.Contains(recorder.Body.String(), codexSearchFailureHandlerMessage(http.StatusTooManyRequests)) {
				t.Error("discarded retry terminal leaked downstream")
			}
			if calls.Load() != 2 {
				t.Fatalf("handler attempts=%d, want exactly 2 (429 then 400)", calls.Load())
			}
			assertCodexSearchFailureHandlerAccounting(t, db, key.ID, tc.route, tc.stream, http.StatusBadRequest, 2)
		})
	}
}

func TestCodexSearchFailureNativeWebSocketAccounting(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%t", retry), func(t *testing.T) {
			wires := []string{codexSearchFailureHandlerWire(t, http.StatusBadRequest, true)}
			if retry {
				wires = append([]string{codexSearchFailureHandlerWire(t, http.StatusTooManyRequests, true)}, wires...)
			}
			h, db, key, calls := newCodexSearchFailureHandlerFixture(t, wires)
			if retry {
				// Exercise the existing opt-in replay path, without changing the
				// default streaming behavior or committing the discarded output.
				runtime := CurrentRuntimeSettings()
				runtime.ContinuousRetryPolicy = database.ContinuousRetryPolicy{Enabled: true, StatusCodes: []int{429}, MaxDurationSeconds: 3}
				ApplyRuntimeSettings(runtime)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, stop := dialBadRequestEndpointWS(t, h, ctx, func(c *gin.Context) {
				c.Set(contextAPIKeyID, key.ID)
				c.Set(contextAPIKeyRow, key)
			})
			defer stop()
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.5","service_tier":"priority","input":"synthetic search request"}`)); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
			terminal := false
			for i := 0; i < 64; i++ {
				_, payload, err := conn.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				event := gjson.ParseBytes(payload)
				if event.Get("type").String() == "response.completed" || event.Get("type").String() == "response.incomplete" {
					t.Fatal("hidden HTTP failure became a success terminal")
				}
				if event.Get("type").String() == "response.failed" || event.Get("type").String() == "error" {
					if !strings.Contains(string(payload), codexSearchFailureHandlerMessage(http.StatusBadRequest)) {
						t.Fatal("WebSocket lost final HTTP failure")
					}
					terminal = true
					break
				}
			}
			if !terminal {
				t.Fatal("no failure terminal")
			}
			stop() // Wait for the real turn's final accounting and lease release.
			if int(calls.Load()) != len(wires) {
				t.Fatalf("attempts=%d, want %d", calls.Load(), len(wires))
			}
			assertCodexSearchFailureHandlerAccounting(t, db, key.ID, "responses", true, http.StatusBadRequest, len(wires))
		})
	}
}

func codexSearchFailureHandlerMessage(status int) string {
	return fmt.Sprintf("Synthetic hidden continuation rejection %d", status)
}

func codexSearchFailureHandlerWire(t *testing.T, status int, withText bool) string {
	t.Helper()
	bridge := &anthropicCodexWebSearch{name: "private_fixture_search", model: "gpt-5.5", remaining: 2}
	rounds := make([]*nativeSearchFixture, 2)
	for i := range rounds {
		round := &nativeSearchFixture{id: fmt.Sprintf("resp_synthetic_%d", i), createdAt: int64(1700000000 + i)}
		round.event("response.created", map[string]any{"response": map[string]any{
			"id": round.id, "created_at": round.createdAt, "status": "in_progress", "output": []any{},
		}})
		var output []any
		if withText {
			// Character estimation (~140 output tokens) differs from the real
			// two-round total (80), so overwriting reported usage cannot pass.
			text := fmt.Sprintf("Synthetic model text %d: ", i) + strings.Repeat("x", 192)
			id := fmt.Sprintf("msg_synthetic_%d", i)
			message := nativeSearchMessage(id, text)
			round.item("response.output_item.added", 0, nativeSearchMessage(id))
			round.event("response.output_text.delta", map[string]any{
				"item_id": id, "output_index": 0, "content_index": 0, "delta": text,
			})
			round.item("response.output_item.done", 0, message)
			output = append(output, message)
		}
		call := codexSearchTestCall(bridge.name, fmt.Sprintf("Synthetic query %d", i))
		call["id"], call["call_id"] = fmt.Sprintf("fc_synthetic_%d", i), fmt.Sprintf("call_synthetic_%d", i)
		output = append(output, call)
		round.event("response.completed", map[string]any{"response": map[string]any{
			"id": round.id, "created_at": round.createdAt, "status": "completed", "output": output,
			"model": "gpt-5.5", "service_tier": "default",
			"usage": map[string]any{
				"input_tokens": 100 * (i + 1), "output_tokens": 30 + 20*i, "total_tokens": 130 + 120*i,
				"input_tokens_details":  map[string]any{"cached_tokens": 10 * (i + 1)},
				"output_tokens_details": map[string]any{"reasoning_tokens": 3 + 2*i},
			},
		}})
		rounds[i] = round
	}
	errorType := "invalid_request_error"
	switch status {
	case http.StatusTooManyRequests:
		errorType = "rate_limit_exceeded"
	case http.StatusUnauthorized:
		errorType = "authentication_error"
	}
	rejected := &codexSearchHTTPFailureBody{Reader: strings.NewReader(fmt.Sprintf(
		`{"error":{"code":%q,"type":%q,"message":%q}}`, errorType, errorType, codexSearchFailureHandlerMessage(status)))}
	first := rounds[0].response()
	attachCodexSearchContinuation(first, bridge)
	stored := first.Body.(*codexSearchContextBody)
	opened, searched := 0, 0
	var wire strings.Builder
	err := bridge.run(context.Background(), first, []byte(`{"model":"gpt-5.5","input":[]}`), func(next []byte) (*http.Response, error) {
		opened++
		if !strings.Contains(string(next), "function_call_output") {
			t.Error("hidden continuation omitted the actual search result")
		}
		if opened == 1 {
			return rounds[1].response(), nil
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: rejected}, nil
	}, func(raw []byte) (*CodexAlphaSearchResponse, error) {
		searched++
		return nativeSearchSuccess(raw)
	}, &wire)
	if err != nil {
		t.Fatalf("bridge must emit a failure terminal, not a pipe error: %v", err)
	}
	if opened != 2 || searched != 2 || !rejected.closed || stored.completed || len(stored.replay) != 0 {
		t.Fatalf("bridge calls/searches/closed/cached=%d/%d/%t/%t, want 2/2/true/false", opened, searched, rejected.closed, stored.completed)
	}
	events, terminal := assertNativeSearchContract(t, wire.String(), bridge.responseID, rounds[0].createdAt)
	if terminal.Get("type").String() != "response.failed" || terminal.Get("response.status").String() != "failed" || terminal.Get("response.status_code").Int() != int64(status) {
		t.Fatal("bridge did not preserve the HTTP rejection as its sole failure terminal")
	}
	for path, want := range map[string]int64{
		"input_tokens": 300, "output_tokens": 80, "total_tokens": 380,
		"input_tokens_details.cached_tokens": 30, "output_tokens_details.reasoning_tokens": 8,
	} {
		if got := terminal.Get("response.usage." + path).Int(); got != want {
			t.Fatalf("bridge usage %s=%d, want %d", path, got, want)
		}
	}
	for _, event := range events {
		if event.Get("type").String() == "response.completed" || (!withText && isFirstTokenResult(event)) {
			t.Fatal("hidden completion or unexpected content escaped the bridge")
		}
	}
	return wire.String()
}

func newCodexSearchFailureHandlerFixture(t *testing.T, wires []string) (*Handler, *database.DB, *database.APIKeyRow, *atomic.Int32) {
	t.Helper()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "CODEX2API_TEST_POSTGRES_DSN"} {
		t.Setenv(name, "")
	}
	withPayloadRules(t, `{}`)
	// Restore the auth egress flag as well as the pointer restored by the
	// existing endpoint helper; all runtime changes below are also restored.
	previousResin := GetResinConfig()
	t.Cleanup(func() { SetResinConfig(previousResin) })
	var calls atomic.Int32
	h, accounts := newBadRequestEndpointFixture(t, database.BadRequestRetryPolicy{}, false, len(wires), func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		attempt := int(calls.Add(1))
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/responses") || attempt > len(wires) {
			t.Error("unexpected loopback request or extra handler retry")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, wires[attempt-1])
	})
	settings := CurrentRuntimeSettings()
	settings.CodexSafePreflightRetry = false
	settings.CodexPreflightSSEPassthrough = false
	settings.CodexContinueThinking = false
	settings.BillingTierPolicy = BillingTierPolicyActual
	settings.StreamFlushPolicy = "immediate"
	ApplyRuntimeSettings(settings)
	h.store.SetMaxRateLimitRetries(len(wires) - 1)
	t.Cleanup(func() {
		for _, account := range accounts {
			if account.GetActiveRequests() != 0 {
				t.Error("failed handler retained an account lease")
			}
		}
	})
	db, err := database.New("sqlite", filepath.Join(t.TempDir(), "synthetic-search-failure.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetUsageLogConfig(database.UsageLogModeFull, 100, 60)
	h.db = db
	keyID, err := db.InsertAPIKey(context.Background(), "synthetic search failure", "synthetic-search-failure-key")
	if err != nil {
		t.Fatal(err)
	}
	key, err := db.GetAPIKeyByID(context.Background(), keyID)
	if err != nil {
		t.Fatal(err)
	}
	return h, db, key, &calls
}

func invokeCodexSearchFailureHandler(t *testing.T, h *Handler, key *database.APIKeyRow, route string, stream bool) *httptest.ResponseRecorder {
	t.Helper()
	path, serve := "/v1/responses", h.Responses
	input := `"input":"synthetic search request"`
	switch route {
	case "chat":
		path, serve = "/v1/chat/completions", h.ChatCompletions
		input = `"messages":[{"role":"user","content":"synthetic search request"}]`
	case "messages":
		path, serve = "/v1/messages", h.Messages
		input = `"max_tokens":256,"messages":[{"role":"user","content":"synthetic search request"}]`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(&informationalRecordingWriter{ResponseRecorder: recorder})
	// Tools are deliberately absent: the upstream fixture already ran the
	// real bridge. Do not wrap the projected stream in a second bridge.
	body := fmt.Sprintf(`{"model":"gpt-5.5","stream":%t,"service_tier":"priority",%s}`, stream, input)
	c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(contextAPIKeyID, key.ID)
	c.Set(contextAPIKeyRow, key)
	serve(c)
	return recorder
}

func assertCodexSearchFailureHandlerResponse(t *testing.T, recorder *httptest.ResponseRecorder, route string, visibleStream bool, status int) {
	t.Helper()
	body := recorder.Body.String()
	wantHTTP := status
	if visibleStream {
		wantHTTP = http.StatusOK // Content committed before the failure.
	}
	if recorder.Code != wantHTTP {
		t.Errorf("downstream HTTP status=%d, want %d", recorder.Code, wantHTTP)
	}
	if strings.Contains(body, "[DONE]") || strings.Contains(body, `"type":"response.completed"`) || strings.Contains(body, `"type":"message_stop"`) {
		t.Error("failure fabricated a success terminal")
	}
	var payloads []gjson.Result
	if visibleStream {
		for line := range strings.SplitSeq(body, "\n") {
			if raw, ok := strings.CutPrefix(line, "data: "); ok {
				if !gjson.Valid(raw) {
					t.Fatal("invalid downstream SSE JSON")
				}
				payloads = append(payloads, gjson.Parse(raw))
			}
		}
	} else {
		if !gjson.Valid(body) {
			t.Fatal("expected one JSON error, not an empty 200 or concatenated terminals")
		}
		payloads = append(payloads, gjson.Parse(body))
		if strings.Contains(body, "Synthetic model text") || payloads[0].Get("output").Exists() || payloads[0].Get("choices").Exists() || payloads[0].Get("content").Exists() {
			t.Error("non-streaming failure leaked a partial success object")
		}
	}
	failures, texts := 0, 0
	for i, payload := range payloads {
		kind := payload.Get("type").String()
		if kind == "response.completed" || kind == "response.incomplete" || kind == "message_stop" ||
			payload.Get("choices.0.finish_reason").String() != "" || payload.Get("delta.stop_reason").String() != "" {
			t.Error("failure included a clean finish reason or success event")
		}
		if kind == "response.output_text.delta" || payload.Get("choices.0.delta.content").String() != "" || payload.Get("delta.type").String() == "text_delta" {
			texts++
		}
		errorMessage := payload.Get("error.message").String()
		if kind == "response.failed" {
			errorMessage = payload.Get("response.error.message").String()
			if payload.Get("response.status_code").Int() != int64(status) {
				t.Error("Responses SSE lost the actual rejection status")
			}
		}
		if kind == "response.failed" || payload.Get("error").IsObject() {
			failures++
			if i != len(payloads)-1 || !strings.Contains(errorMessage, codexSearchFailureHandlerMessage(status)) {
				t.Error("failure was not the last event or lost the upstream error message")
			}
			if route == "messages" && payload.Get("error.type").String() != mapHTTPStatusToAnthropicError(status) {
				t.Error("Messages error type lost the upstream HTTP status mapping")
			}
		}
	}
	if failures != 1 || (visibleStream && texts != 2) {
		t.Errorf("downstream failures/text deltas=%d/%d, want one failure and exactly two visible deltas when streaming", failures, texts)
	}
}

func assertCodexSearchFailureHandlerAccounting(t *testing.T, db *database.DB, keyID int64, route string, stream bool, finalStatus, attempts int) {
	t.Helper()
	db.FlushUsageLogs()
	logs, err := db.ListRecentUsageLogs(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/" + route
	if route == "chat" {
		path = "/v1/chat/completions"
	}
	// Unmapped Responses/Chat models leave EffectiveModel empty. Messages
	// translates the model explicitly and does not accept OpenAI service_tier.
	wantEffective, wantRequested := "", "priority"
	if route == "messages" {
		wantEffective, wantRequested = "gpt-5.5", ""
	}
	var input, output, total, cached, reasoning, prompt, completion, finals int
	var accountBilled, userBilled float64
	for _, entry := range logs {
		if entry.Endpoint != path || entry.Stream != stream || entry.ViaWebsocket || entry.APIKeyID != keyID || entry.Model != "gpt-5.5" || entry.EffectiveModel != wantEffective {
			t.Errorf("usage attribution endpoint=%q stream=%t ws=%t key=%d model=%q effective=%q", entry.Endpoint, entry.Stream, entry.ViaWebsocket, entry.APIKeyID, entry.Model, entry.EffectiveModel)
		}
		if entry.ActualServiceTier != "default" || entry.RequestedServiceTier != wantRequested || entry.BillingServiceTier != "default" {
			t.Errorf("failed usage tiers actual/requested/billing=%q/%q/%q, want default/%q/default", entry.ActualServiceTier, entry.RequestedServiceTier, entry.BillingServiceTier, wantRequested)
		}
		if entry.StatusCode == http.StatusOK || entry.StatusCode == logStatusUpstreamStreamBreak {
			t.Errorf("HTTP failure logged as success/generic break: %d", entry.StatusCode)
		}
		if !entry.IsRetryAttempt {
			finals++
			if entry.StatusCode != finalStatus || entry.AttemptIndex != attempts || !strings.Contains(entry.ErrorMessage, codexSearchFailureHandlerMessage(finalStatus)) {
				t.Errorf("final usage status/attempt=%d/%d, want %d/%d with the real upstream message", entry.StatusCode, entry.AttemptIndex, finalStatus, attempts)
			}
		} else if entry.StatusCode != http.StatusTooManyRequests || entry.AttemptIndex != 1 {
			t.Error("discarded attempt has incorrect status/attempt attribution")
		}
		input += entry.InputTokens
		output += entry.OutputTokens
		total += entry.TotalTokens
		cached += entry.CachedTokens
		reasoning += entry.ReasoningTokens
		prompt += entry.PromptTokens
		completion += entry.CompletionTokens
		accountBilled += entry.AccountBilled
		userBilled += entry.UserBilled
	}
	if finals != 1 || len(logs) > attempts {
		t.Errorf("usage logs/final logs=%d/%d, want at most %d logs and exactly one final", len(logs), finals, attempts)
	}
	if input != 300*attempts || output != 80*attempts || total != 380*attempts || cached != 30*attempts || reasoning != 8*attempts || prompt != input || completion != output {
		t.Errorf("real usage lost/estimated/double-counted: input/output/total/cache/reasoning/prompt/completion=%d/%d/%d/%d/%d/%d/%d; want %d/%d/%d/%d/%d with matching legacy counts across %d attempts",
			input, output, total, cached, reasoning, prompt, completion, 300*attempts, 80*attempts, 380*attempts, 30*attempts, 8*attempts, attempts)
	}
	wantCost := float64(attempts) * database.CalculateCost(300, 80, 30, "gpt-5.5", "default")
	if wantCost <= 0 {
		t.Fatal("fixture must have a positive known-model cost")
	}
	if math.Abs(accountBilled-wantCost) > 1e-12 || math.Abs(userBilled-wantCost) > 1e-12 {
		t.Errorf("account/user billed=%g/%g, want %g from real accumulated usage", accountBilled, userBilled, wantCost)
	}
	key, err := db.GetAPIKeyByID(context.Background(), keyID)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(key.QuotaUsed-wantCost) > 1e-12 {
		t.Errorf("persisted synthetic key quota used=%g, want %g (not an estimate or only the last attempt)", key.QuotaUsed, wantCost)
	}
}
