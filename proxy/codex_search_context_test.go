package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

// All upstream traffic in these fixtures is either an in-process executor or
// explicit loopback HTTP. No captured prompts, credentials, or search results.
const searchContextReasoning = `{"type":"reasoning","id":"rs_fixture","summary":[],"encrypted_content":"fixture-opaque"}`
const searchContextExternal = `{"type":"function_call","id":"fc_external","call_id":"external","name":"external","arguments":"{}"}`

func searchContextCall(id string) string {
	return fmt.Sprintf(`{"type":"function_call","id":"fc_%s","call_id":%q,"name":"codex2api_web_search","arguments":"{\"query\":\"synthetic query\"}"}`, id, id)
}

func searchContextMessage(text string) string {
	return fmt.Sprintf(`{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":%q}]}`, text)
}

func searchContextSSE(id string, items ...string) string {
	return fmt.Sprintf("data: {\"type\":\"response.created\",\"response\":{\"id\":%q,\"status\":\"in_progress\",\"output\":[]}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":%q,\"status\":\"completed\",\"output\":[%s],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n", id, id, strings.Join(items, ","))
}

func setupSearchContextLoopback(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	previousSettings, previousSearchURL, previousResin := CurrentRuntimeSettings(), codexAlphaSearchURLForTest, GetResinConfig()
	settings := DefaultRuntimeSettings()
	settings.CodexForceWebsocket = false
	settings.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	ApplyRuntimeSettings(settings)
	resetResponseCacheStateForTest(testResponseCacheConfig())
	t.Cleanup(func() {
		ApplyRuntimeSettings(previousSettings)
		codexAlphaSearchURLForTest = previousSearchURL
		SetResinConfig(previousResin)
		resetResponseCacheForTest()
	})
	search := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"output":[{"type":"message","content":[{"type":"output_text","text":"SEARCH_FIXTURE"}]}]}`)
	}))
	t.Cleanup(search.Close)
	codexAlphaSearchURLForTest = search.URL
}

func searchContextRequest(previous string, parallel, stream, store bool, input string) []byte {
	items := []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"role":"user","content":%q}`, input))}
	if parallel && input == "second" {
		items = []json.RawMessage{json.RawMessage(`{"type":"function_call_output","call_id":"external","output":"EXTERNAL_FIXTURE"}`)}
	}
	body := map[string]any{"model": "gpt-5.5", "input": items, "stream": stream, "store": store,
		"tools": []any{map[string]any{"type": "web_search"}, map[string]any{"type": "function", "name": "external", "parameters": map[string]any{"type": "object", "properties": map[string]any{}}}}}
	if previous != "" {
		body["previous_response_id"] = previous
	}
	if input == "third" {
		// The client may stop declaring hosted search after receiving its result.
		delete(body, "tools")
	}
	raw, _ := json.Marshal(body)
	return raw
}

func searchContextTerminal(t *testing.T, raw []byte, stream bool) gjson.Result {
	t.Helper()
	if !stream {
		return gjson.ParseBytes(raw)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		data := strings.TrimPrefix(line, "data: ")
		if gjson.Get(data, "type").String() == "response.completed" {
			return gjson.Get(data, "response")
		}
	}
	t.Fatal("missing successful search terminal")
	return gjson.Result{}
}

func assertSearchContextPublic(t *testing.T, raw []byte, response gjson.Result) string {
	t.Helper()
	id := response.Get("id").String()
	if !isCodexSearchResponseID(id) {
		t.Fatal("search response did not allocate a logical ID")
	}
	if strings.Contains(string(raw), "codex2api_web_search") || strings.Contains(string(raw), `"function_call_output"`) {
		t.Fatal("public stream exposed private replay")
	}
	return id
}

func assertSearchContextReplay(t *testing.T, body []byte, parallel bool, final bool) {
	t.Helper()
	if gjson.GetBytes(body, "previous_response_id").Exists() {
		t.Fatal("logical search continuation sent a previous ID upstream")
	}
	calls, outputs := map[string]int{}, map[string]int{}
	root, opaque, continued := false, false, false
	for _, item := range gjson.GetBytes(body, "input").Array() {
		switch item.Get("type").String() {
		case "web_search_call":
			t.Fatal("public search projection replaced private history")
		case "function_call":
			calls[item.Get("call_id").String()]++
		case "function_call_output":
			outputs[item.Get("call_id").String()]++
			if strings.HasPrefix(item.Get("call_id").String(), "search_") && !strings.Contains(item.Get("output").String(), "SEARCH_FIXTURE") {
				t.Fatal("private search result was not replayed")
			}
		}
		root = root || item.Get("content").String() == "root"
		opaque = opaque || item.Get("encrypted_content").String() == "fixture-opaque"
		continued = continued || strings.Contains(item.Get("content").Raw, "continued")
	}
	ids := []string{"search_one", "search_two"}
	if parallel {
		ids = []string{"search_one", "external"}
	}
	for _, id := range ids {
		if calls[id] != 1 || outputs[id] != 1 {
			t.Fatalf("call/result pair %q not preserved exactly once: calls=%d outputs=%d", id, calls[id], outputs[id])
		}
	}
	if !root || !opaque || (final && !continued) {
		t.Fatal("multi-turn replay lost the root, encrypted provenance context, or final assistant output")
	}
}

func searchContextScript(parallel bool) [][]string {
	if parallel {
		return [][]string{{searchContextReasoning, searchContextCall("search_one"), searchContextExternal}, {searchContextMessage("continued")}, {searchContextMessage("finished")}}
	}
	return [][]string{{searchContextReasoning, searchContextCall("search_one")}, {searchContextCall("search_two")}, {searchContextMessage("initial answer")}, {searchContextMessage("continued")}, {searchContextMessage("finished")}}
}

func TestCodexSearchContextHTTPHandlers(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("parallel=%t/stream=%t", parallel, stream), func(t *testing.T) {
				setupSearchContextLoopback(t)
				script := searchContextScript(parallel)
				requests := make(chan []byte, len(script))
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body := readUpstreamRequestBody(r)
					n := int(calls.Add(1)) - 1
					if n >= len(script) {
						t.Error("unexpected upstream retry")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					requests <- body
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, searchContextSSE(fmt.Sprintf("resp_real_%d", n), script[n]...))
				}))
				t.Cleanup(upstream.Close)
				SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "search-context-fixture"})
				store := newContinuationCodexStore()
				t.Cleanup(store.Stop)
				handler := NewHandler(store, nil, nil, nil)
				previous := ""
				for turn, input := range []string{"root", "second", "third"} {
					response := invokeResponsesHandler(t, handler.Responses, searchContextRequest(previous, parallel, stream, true, input))
					if response.Code != http.StatusOK {
						t.Fatalf("turn %d status=%d", turn, response.Code)
					}
					terminal := searchContextTerminal(t, response.Body.Bytes(), stream)
					id := terminal.Get("id").String()
					if turn < 2 {
						id = assertSearchContextPublic(t, response.Body.Bytes(), terminal)
					} else if id != fmt.Sprintf("resp_real_%d", len(script)-1) {
						t.Fatal("non-search followup response ID was changed")
					}
					if previous == id {
						t.Fatal("logical IDs were reused across requests")
					}
					previous = id
					if turn == 0 {
						for i := 0; i < len(script)-2; i++ {
							<-requests
						}
					} else {
						assertSearchContextReplay(t, <-requests, parallel, turn == 2)
					}
				}
				if int(calls.Load()) != len(script) {
					t.Fatal("wrong number of real upstream rounds")
				}
			})
		}
	}
}

func searchContextWSTurn(t *testing.T, conn *websocket.Conn, request []byte) []byte {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(request, &body); err != nil {
		t.Fatal(err)
	}
	body["type"] = "response.create"
	if err := conn.WriteJSON(body); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var stream strings.Builder
	for {
		_, event, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		stream.Write(event)
		if strings.Contains(stream.String(), "codex2api_web_search") || strings.Contains(stream.String(), `"function_call_output"`) {
			t.Fatal("WS stream exposed private replay")
		}
		typ := gjson.GetBytes(event, "type").String()
		if typ == "error" || isResponsesTerminalEvent(typ) {
			return event
		}
	}
}

func TestCodexSearchContextWSHandlers(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, persistent := range []bool{false, true} {
			t.Run(fmt.Sprintf("parallel=%t/store=%t", parallel, persistent), func(t *testing.T) {
				setupSearchContextLoopback(t)
				script := searchContextScript(parallel)
				requests := make(chan []byte, len(script))
				var calls atomic.Int32
				fixture := newNativeWSFixture(t, func(_ context.Context, body []byte) (*http.Response, error) {
					n := int(calls.Add(1)) - 1
					if n >= len(script) {
						t.Error("unexpected upstream retry")
						return nativeWSResponse("resp_unexpected"), nil
					}
					requests <- append([]byte(nil), body...)
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(searchContextSSE(fmt.Sprintf("resp_real_%d", n), script[n]...)))}, nil
				})
				conn := fixture.connect(t)
				previous := ""
				for turn, input := range []string{"root", "second", "third"} {
					event := searchContextWSTurn(t, conn, searchContextRequest(previous, parallel, true, persistent, input))
					if gjson.GetBytes(event, "type").String() != "response.completed" {
						t.Fatalf("turn %d did not complete", turn)
					}
					if turn < 2 {
						previous = assertSearchContextPublic(t, event, gjson.GetBytes(event, "response"))
					} else if gjson.GetBytes(event, "response.id").String() != fmt.Sprintf("resp_real_%d", len(script)-1) {
						t.Fatal("non-search followup response ID was changed")
					}
					if turn == 0 {
						for i := 0; i < len(script)-2; i++ {
							<-requests
						}
						_ = conn.Close()
						conn = fixture.connect(t) // Reconnect must use the same complete snapshot.
					} else {
						assertSearchContextReplay(t, <-requests, parallel, turn == 2)
					}
				}
				if int(calls.Load()) != len(script) {
					t.Fatal("logical continuation unexpectedly tried a native previous ID")
				}
			})
		}
	}
}

func seedSearchContext(t *testing.T, owner string, account *auth.Account, opaque bool) string {
	t.Helper()
	bridge := &anthropicCodexWebSearch{}
	response := &http.Response{Body: io.NopCloser(strings.NewReader(""))}
	attachCodexSearchContinuation(response, bridge)
	replay := []json.RawMessage{json.RawMessage(searchContextCall("search_one")), json.RawMessage(`{"type":"function_call_output","call_id":"search_one","output":"SEARCH_FIXTURE"}`)}
	if opaque {
		replay = append(replay, json.RawMessage(searchContextReasoning))
	}
	bridge.onCompleted(replay)
	completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"output":[{"type":"web_search_call"}]}}`, bridge.responseID))
	if !cacheCodexSearchContinuation(response, nativeWSCompletedContext{owner: owner, input: `[{"role":"user","content":"root"}]`, completed: completed, provenance: nativeWSAccountProvenance(account)}) {
		t.Fatal("private completion was not handled")
	}
	return bridge.responseID
}

type searchContextUnavailableBackend struct {
	*recordingResponseContextBackend
}

func (*searchContextUnavailableBackend) GetRuntime(context.Context, string, string) (json.RawMessage, bool, error) {
	return nil, false, errSyntheticBackend
}

func (*searchContextUnavailableBackend) GetRuntimeBounded(context.Context, string, string, int64) ([]byte, bool, error) {
	return nil, false, errSyntheticBackend
}

func TestCodexSearchContextHTTPMissingNeverRelaysLogicalID(t *testing.T) {
	for _, failure := range []string{"missing", "other_owner", "oversize", "entry_bytes", "reconstruction_bytes", "expired", "backend"} {
		t.Run(failure, func(t *testing.T) {
			setupSearchContextLoopback(t)
			t.Setenv("CODEX_WS_CONTINUATION_FAIL_OPEN", "true")
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.WriteString(w, `{"id":"resp_should_not_run","output":[]}`)
			}))
			t.Cleanup(upstream.Close)
			SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "search-context-fixture"})
			store := newContinuationRelayStore(upstream.URL)
			t.Cleanup(store.Stop)
			handler := NewHandler(store, nil, nil, nil)
			owner := "anon"
			if failure == "other_owner" {
				owner = "key:other"
			}
			config := testResponseCacheConfig()
			switch failure {
			case "oversize":
				config.maxItems = 1
			case "entry_bytes":
				config.maxEntryBytes = 1
			case "reconstruction_bytes":
				config.reconstructMaxBytes = 1
			}
			configureResponseCacheForTest(config)
			id := seedSearchContext(t, owner, &auth.Account{DBID: 1}, false)
			if failure == "missing" {
				resetResponseCacheStateForTest(testResponseCacheConfig())
			}
			if failure == "expired" {
				cleanupResponseCacheExpired(time.Now().Add(2 * time.Hour))
			}
			wantStatus := http.StatusConflict
			if failure == "backend" {
				resetResponseCacheStateForTest(testResponseCacheConfig())
				backend := &searchContextUnavailableBackend{newRecordingResponseContextBackend(true)}
				SetResponseContextCache(backend)
				t.Cleanup(func() { _ = backend.TokenCache.Close() })
				wantStatus = http.StatusServiceUnavailable
			}
			response := invokeResponsesHandler(t, handler.Responses, searchContextRequest(id, false, false, true, "second"))
			if response.Code != wantStatus || calls.Load() != 0 {
				t.Fatalf("logical continuation failed open: status=%d upstream=%d", response.Code, calls.Load())
			}
			if strings.Contains(response.Body.String(), id) || strings.Contains(response.Body.String(), "SEARCH_FIXTURE") {
				t.Fatal("context failure disclosed private state")
			}
		})
	}
}

func TestCodexSearchContextWSMissingNeverFailsOpen(t *testing.T) {
	for _, failure := range []string{"missing", "other_session", "oversize", "credential_generation"} {
		t.Run(failure, func(t *testing.T) {
			setupSearchContextLoopback(t)
			t.Setenv("CODEX_WS_CONTINUATION_FAIL_OPEN", "true")
			var calls atomic.Int32
			fixture := newNativeWSFixture(t, func(_ context.Context, _ []byte) (*http.Response, error) {
				calls.Add(1)
				return nativeWSResponse("resp_parallel", searchContextReasoning, searchContextCall("search_one"), searchContextExternal), nil
			})
			if failure == "oversize" {
				config := testResponseCacheConfig()
				config.maxItems = 1
				configureResponseCacheForTest(config)
			}
			conn := fixture.connect(t)
			first := searchContextWSTurn(t, conn, searchContextRequest("", true, true, false, "root"))
			id := assertSearchContextPublic(t, first, gjson.GetBytes(first, "response"))
			// Wait for the real handler commit before simulating loss of L1.
			deadline := time.Now().Add(time.Second)
			for fixture.account.GetActiveRequests() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if fixture.account.GetActiveRequests() != 0 {
				t.Fatal("successful request did not release its lease")
			}
			if failure == "missing" {
				resetResponseCacheStateForTest(testResponseCacheConfig())
			}
			if failure == "credential_generation" {
				fixture.account.Mu().Lock()
				fixture.account.CredentialGeneration++
				fixture.account.Mu().Unlock()
			}
			if failure == "other_session" {
				_ = conn.Close()
				var err error
				conn, _, err = websocket.DefaultDialer.Dial(fixture.url, http.Header{"Session-Id": {"other-fixture-session"}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = conn.Close() })
			}
			terminal := searchContextWSTurn(t, conn, searchContextRequest(id, true, true, false, "second"))
			if gjson.GetBytes(terminal, "type").String() != "error" || calls.Load() != 1 {
				t.Fatal("missing private snapshot was sent upstream despite fail-closed policy")
			}
		})
	}
}

func TestCodexSearchContextSharedProvenanceAndWritePolicy(t *testing.T) {
	resetResponseCacheStateForTest(testResponseCacheConfig())
	t.Cleanup(resetResponseCacheForTest)
	backend := newRecordingResponseContextBackend(true)
	SetResponseContextCache(backend)
	t.Cleanup(func() { _ = backend.TokenCache.Close() })
	account := &auth.Account{DBID: 1, CredentialGeneration: 7}
	id := seedSearchContext(t, "key:1", account, true)
	resetResponseCacheStateForTest(testResponseCacheConfig()) // Keep only the existing shared backend.
	SetResponseContextCache(backend)
	body := searchContextRequest(id, false, false, true, "second")
	source, apiErr := prepareCodexSearchHTTPContinuation(context.Background(), body, "key:1")
	if apiErr != nil || source.previous == nil || source.previous.Source != responseCacheSourceBackend {
		t.Fatal("shared private snapshot was not recovered")
	}
	source.setAccount(account)
	if !strings.Contains(source.Input(), "SEARCH_FIXTURE") || !strings.Contains(source.Input(), "fixture-opaque") {
		t.Fatal("shared snapshot lost private results or encrypted reasoning")
	}
	for _, other := range []*auth.Account{{DBID: 2, CredentialGeneration: 7}, {DBID: 1, CredentialGeneration: 8}} {
		source.setAccount(other)
		if source.Input() != "" || source.err == nil {
			t.Fatal("memoized encrypted replay bypassed provenance")
		}
	}
	if _, apiErr := prepareCodexSearchHTTPContinuation(context.Background(), body, "key:2"); apiErr == nil {
		t.Fatal("shared snapshot crossed owners")
	}
	config := testResponseCacheConfig()
	config.writePolicy = database.ResponseCacheWritePolicyOnDemand
	resetResponseCacheStateForTest(config)
	id = seedSearchContext(t, "key:unqualified", account, false)
	if nativeWSContextLookup(codexSearchContextOwner("key:unqualified", id), id).Kind == responseCacheLookupHit {
		t.Fatal("private namespace bypassed public owner write policy")
	}
	markResponsesWSContinuationCapable("key:no-store", []byte(`{"store":false}`))
	id = seedSearchContext(t, "key:no-store", account, false)
	if nativeWSContextLookup(codexSearchContextOwner("key:no-store", id), id).Kind == responseCacheLookupHit {
		t.Fatal("HTTP store:false unexpectedly granted on-demand persistence")
	}
	markResponsesWSContinuationCapable("key:qualified", []byte(`{"store":true}`))
	id = seedSearchContext(t, "key:qualified", account, false)
	if nativeWSContextLookup(codexSearchContextOwner("key:qualified", id), id).Kind != responseCacheLookupHit {
		t.Fatal("continuation-capable owner was not admitted")
	}
	id = seedSearchContext(t, nativeWSCachePrefix+"store-false", account, false)
	if nativeWSContextLookup(nativeWSCachePrefix+"store-false", id).Kind != responseCacheLookupHit {
		t.Fatal("native store:false lost its internal context mechanism")
	}
}

func TestCodexSearchContextNeverCachesPublicProjection(t *testing.T) {
	resetResponseCacheStateForTest(testResponseCacheConfig())
	t.Cleanup(resetResponseCacheForTest)
	bridge := &anthropicCodexWebSearch{}
	response := &http.Response{Body: io.NopCloser(strings.NewReader(""))}
	attachCodexSearchContinuation(response, bridge)
	completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"output":[{"type":"web_search_call"}]}}`, bridge.responseID))
	snapshot := nativeWSCompletedContext{owner: "anon", input: `[{"role":"user","content":"root"}]`, completed: completed}
	for _, body := range []io.ReadCloser{response.Body, io.NopCloser(strings.NewReader(""))} {
		candidate := &http.Response{Body: body}
		if !cacheCodexSearchContinuation(candidate, snapshot) {
			t.Fatal("missing private callback/wrapper allowed the public cache fallback")
		}
		if nativeWSContextLookup(codexSearchContextOwner("anon", bridge.responseID), bridge.responseID).Kind == responseCacheLookupHit {
			t.Fatal("public-only projection was admitted as complete search context")
		}
	}
}

func TestCodexSearchContextOrdinaryRequestsUnchanged(t *testing.T) {
	setupSearchContextLoopback(t)
	var calls atomic.Int32
	fixture := newNativeWSFixture(t, func(_ context.Context, body []byte) (*http.Response, error) {
		if calls.Add(1) == 2 && gjson.GetBytes(body, "previous_response_id").String() != "resp_plain" {
			t.Error("ordinary native WS continuation was unnecessarily expanded")
		}
		return nativeWSResponse("resp_plain", searchContextMessage("plain")), nil
	})
	conn := fixture.connect(t)
	for _, previous := range []string{"", "resp_plain"} {
		body := []byte(fmt.Sprintf(`{"model":"gpt-5.5","store":false,"previous_response_id":%q,"input":[{"role":"user","content":"plain"}]}`, previous))
		event := searchContextWSTurn(t, conn, body)
		if gjson.GetBytes(event, "response.id").String() != "resp_plain" {
			t.Fatal("ordinary response ID was rewritten")
		}
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, searchContextSSE("resp_plain_http", searchContextMessage("plain")))
	}))
	t.Cleanup(upstream.Close)
	SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "search-context-fixture"})
	store := newContinuationCodexStore()
	t.Cleanup(store.Stop)
	handler := NewHandler(store, nil, nil, nil)
	response := invokeResponsesHandler(t, handler.Responses, []byte(`{"model":"gpt-5.5","store":false,"input":"plain"}`))
	if response.Code != http.StatusOK || gjson.GetBytes(response.Body.Bytes(), "id").String() != "resp_plain_http" || len(getResponseCache("anon", "resp_plain_http")) != 0 {
		t.Fatal("ordinary HTTP message-only response semantics changed")
	}
}
