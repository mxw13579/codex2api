package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"github.com/codex2api/auth"
	"github.com/tidwall/gjson"
)

// These fixtures exercise the committed HTTP wrapper without handlers, sockets,
// shared backends, or credentials. Input construction is outside measurements.
const codexHTTPCacheTool = `{"type":"function_call","id":"fc_fixture","call_id":"call_fixture","name":"fixture","arguments":"{}"}`
const codexHTTPCacheMalformedTool = `{"type":"function_call","call_id":"call_bad","name":"fixture","arguments":"{\"value\":"}`

func codexHTTPCacheInput(size int) string {
	const prefix = `[{"type":"message","role":"user","content":"`
	const suffix = `"}]`
	return prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
}

type codexHTTPCacheRejection struct {
	name      string
	completed []byte
	outputs   []json.RawMessage
	qualified bool
	wantSkip  bool
}

func codexHTTPCacheRejections() []codexHTTPCacheRejection {
	return []codexHTTPCacheRejection{
		{
			name: "on_demand_tool", wantSkip: true,
			completed: []byte(`{"response":{"id":"resp_fixture","output":[` + codexHTTPCacheTool + `]}}`),
		},
		{
			name: "on_demand_fallback", wantSkip: true,
			completed: []byte(`{"response":{"id":"resp_fixture"}}`),
			outputs:   []json.RawMessage{json.RawMessage(codexHTTPCacheTool)},
		},
		{
			name:      "plain_message",
			completed: []byte(`{"response":{"id":"resp_fixture","output":[{"type":"message","role":"assistant","content":"plain"}]}}`),
		},
		{
			name: "qualified_plain_message", qualified: true,
			completed: []byte(`{"response":{"id":"resp_fixture","output":[{"type":"message","role":"assistant","content":"plain"}]}}`),
		},
		{
			name: "hosted_tools", qualified: true,
			completed: []byte(`{"response":{"id":"resp_fixture","output":[{"type":"web_search_call"},{"type":"image_generation_call"}]}}`),
		},
		{
			name: "no_response_id", qualified: true,
			completed: []byte(`{"response":{"output":[` + codexHTTPCacheTool + `]}}`),
			outputs:   []json.RawMessage{json.RawMessage(codexHTTPCacheTool)},
		},
		{
			name: "malformed_tool", qualified: true,
			completed: []byte(`{"response":{"id":"resp_fixture","output":[` + codexHTTPCacheMalformedTool + `]}}`),
			outputs:   []json.RawMessage{json.RawMessage(codexHTTPCacheMalformedTool)},
		},
	}
}

func TestCodexHTTPCacheRejectedInputAllocations(t *testing.T) {
	const owner = "key:http-cache-fixture"
	const runs = 10
	account := &auth.Account{DBID: 101, CredentialGeneration: 1}
	for _, tc := range codexHTTPCacheRejections() {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(resetResponseCacheForTest)
			var smallBytes uint64
			for _, size := range []int{1 << 20, 8 << 20} {
				resetResponseCacheStateForTest(onDemandResponseCacheConfig())
				if tc.qualified {
					markResponseCacheChainOwnerIfOnDemand(owner)
				}
				input := codexHTTPCacheInput(size)
				cacheCommittedCodexHTTPResponse(nil, owner, input, tc.completed, tc.outputs, account)
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				for i := 0; i < runs; i++ {
					cacheCommittedCodexHTTPResponse(nil, owner, input, tc.completed, tc.outputs, account)
				}
				runtime.ReadMemStats(&after)
				bytesPerCall := (after.TotalAlloc - before.TotalAlloc) / runs
				t.Logf("input=%dMiB allocated=%d B/call", size>>20, bytesPerCall)
				// Allow fixed parsing and runtime noise, but not even a 1 MiB copy.
				if bytesPerCall > 64<<10 {
					t.Errorf("%dMiB rejected input allocated %d B/call, want <=64KiB", size>>20, bytesPerCall)
				}
				if size == 1<<20 {
					smallBytes = bytesPerCall
				} else if bytesPerCall > smallBytes+16<<10 {
					t.Errorf("allocation grew with input: 1MiB=%d B/call, 8MiB=%d B/call", smallBytes, bytesPerCall)
				}
				var wantSkipped uint64
				if tc.wantSkip {
					wantSkipped = runs + 1 // Include the warm-up call, exactly once each.
				}
				if stats := GetResponseCacheStats(); stats.Entries != 0 || stats.Bytes != 0 || stats.SkippedWrites != wantSkipped {
					t.Fatalf("rejection stats: entries=%d bytes=%d skipped=%d, want 0/0/%d", stats.Entries, stats.Bytes, stats.SkippedWrites, wantSkipped)
				}
			}
		})
	}
}

func TestCodexHTTPCacheQualifiedToolAndFallback(t *testing.T) {
	const owner = "key:http-cache-fixture"
	input := codexHTTPCacheInput(1 << 20)
	account := &auth.Account{DBID: 101, CredentialGeneration: 1}
	for _, tc := range []struct {
		name      string
		completed []byte
		outputs   []json.RawMessage
	}{
		{
			name:      "terminal_tool",
			completed: []byte(`{"response":{"id":"resp_fixture","output":[` + codexHTTPCacheTool + `]}}`),
			outputs:   []json.RawMessage{json.RawMessage(`{"type":"custom_tool_call","call_id":"unused_fallback","name":"fixture","input":"raw"}`)},
		},
		{
			name:      "missing_terminal_output",
			completed: []byte(`{"response":{"id":"resp_fixture"}}`),
			outputs:   []json.RawMessage{json.RawMessage(codexHTTPCacheTool)},
		},
		{
			name:      "message_terminal_fallback",
			completed: []byte(`{"response":{"id":"resp_fixture","output":[{"type":"message","role":"assistant","content":"plain"}]}}`),
			outputs:   []json.RawMessage{json.RawMessage(codexHTTPCacheTool)},
		},
		{
			name:      "malformed_terminal_valid_fallback",
			completed: []byte(`{"response":{"id":"resp_fixture","output":[` + codexHTTPCacheMalformedTool + `]}}`),
			outputs:   []json.RawMessage{json.RawMessage(codexHTTPCacheTool)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetResponseCacheStateForTest(onDemandResponseCacheConfig())
			t.Cleanup(resetResponseCacheForTest)
			if got := getResponseCache(owner, "resp_missing_fixture"); len(got) != 0 {
				t.Fatal("unexpected initial cache hit")
			}
			cacheCommittedCodexHTTPResponse(nil, owner, input, tc.completed, tc.outputs, account)
			cached := lookupResponseCacheResult(owner, "resp_fixture")
			if cached.Kind != responseCacheLookupHit || len(cached.Items) != 2 {
				t.Fatalf("qualified snapshot: kind=%d items=%d, want hit with input + tool", cached.Kind, len(cached.Items))
			}
			if string(cached.Items[0]) != input[1:len(input)-1] {
				t.Fatal("qualified snapshot lost or changed the large input")
			}
			tool := gjson.ParseBytes(cached.Items[1])
			if tool.Get("call_id").String() != "call_fixture" || tool.Get("arguments").String() != "{}" || tool.Get("id").Exists() {
				t.Fatal("valid tool fallback/normalization changed")
			}
			if stats := GetResponseCacheStats(); stats.Entries != 1 || stats.SkippedWrites != 0 {
				t.Fatalf("qualified write stats: entries=%d skipped=%d", stats.Entries, stats.SkippedWrites)
			}
			cacheCommittedCodexHTTPResponse(nil, "key:other-fixture", input, tc.completed, tc.outputs, account)
			if lookupResponseCacheResult("key:other-fixture", "resp_fixture").Kind == responseCacheLookupHit {
				t.Fatal("qualification or snapshot crossed owners")
			}
			if stats := GetResponseCacheStats(); stats.Entries != 1 || stats.SkippedWrites != 1 {
				t.Fatalf("other-owner rejection: entries=%d skipped=%d, want 1/1", stats.Entries, stats.SkippedWrites)
			}
		})
	}
}

func TestCodexHTTPCacheSearchPrivateReplay(t *testing.T) {
	for _, tc := range []struct {
		name      string
		owner     string
		qualified bool
		wantCache bool
	}{
		{name: "unqualified", owner: "key:search-fixture"},
		{name: "qualified", owner: "key:search-fixture", qualified: true, wantCache: true},
		{name: "native_store_false", owner: nativeWSCachePrefix + "store-false-fixture", wantCache: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetResponseCacheStateForTest(onDemandResponseCacheConfig())
			t.Cleanup(resetResponseCacheForTest)
			if tc.qualified {
				markResponseCacheChainOwnerIfOnDemand(tc.owner)
			}
			account := &auth.Account{DBID: 101, CredentialGeneration: 1}
			bridge := &anthropicCodexWebSearch{}
			response := &http.Response{Body: io.NopCloser(strings.NewReader(""))}
			attachCodexSearchContinuation(response, bridge)
			replay := []json.RawMessage{
				json.RawMessage(searchContextReasoning),
				json.RawMessage(searchContextCall("search_one")),
				json.RawMessage(`{"type":"function_call_output","call_id":"search_one","output":"SEARCH_FIXTURE"}`),
				json.RawMessage(searchContextMessage("private final")),
			}
			bridge.onCompleted(replay)
			completed := []byte(fmt.Sprintf(`{"response":{"id":%q,"output":[{"type":"web_search_call"}]}}`, bridge.responseID))
			// A valid public fallback must never replace the full private tail.
			cacheCommittedCodexHTTPResponse(response, tc.owner, `[{"role":"user","content":"root"}]`, completed, []json.RawMessage{json.RawMessage(codexHTTPCacheTool)}, account)
			lookup := nativeWSContextLookup(codexSearchContextOwner(tc.owner, bridge.responseID), bridge.responseID)
			if !tc.wantCache {
				if lookup.Kind == responseCacheLookupHit || GetResponseCacheStats().SkippedWrites != 1 {
					t.Fatal("search namespace bypassed qualification or counted rejection twice")
				}
				return
			}
			if lookup.Kind != responseCacheLookupHit || len(lookup.Items) != 1+len(replay) {
				t.Fatalf("private search snapshot incomplete: kind=%d items=%d", lookup.Kind, len(lookup.Items))
			}
			if !lookup.nativeProvenance.matches(nativeWSAccountProvenance(account)) {
				t.Fatal("private search snapshot lost account provenance")
			}
			if gjson.GetBytes(lookup.Items[0], "content").String() != "root" {
				t.Fatal("private search snapshot lost its root")
			}
			for i, item := range replay {
				if string(lookup.Items[i+1]) != string(item) {
					t.Fatalf("private replay item %d was lost or replaced by public output", i)
				}
			}
			if nativeWSContextLookup(codexSearchContextOwner("key:other-fixture", bridge.responseID), bridge.responseID).Kind == responseCacheLookupHit {
				t.Fatal("private search snapshot crossed owners")
			}
			if !strings.HasPrefix(tc.owner, nativeWSCachePrefix) && lookupResponseCacheResult(tc.owner, bridge.responseID).Kind == responseCacheLookupHit {
				t.Fatal("search snapshot also entered the public cache namespace")
			}
			body := []byte(fmt.Sprintf(`{"previous_response_id":%q,"input":[{"role":"user","content":"next"}]}`, bridge.responseID))
			source := newResponsesWSReplaySource(body, tc.owner)
			source.setAccount(account)
			if input := source.Input(); source.err != nil || !strings.Contains(input, "SEARCH_FIXTURE") || !strings.Contains(input, "fixture-opaque") || strings.Contains(input, "web_search_call") {
				t.Fatal("private search replay was not recovered intact")
			}
			source.setAccount(&auth.Account{DBID: 101, CredentialGeneration: 2})
			if source.Input() != "" || source.err == nil {
				t.Fatal("private encrypted replay bypassed credential generation")
			}
			if stats := GetResponseCacheStats(); stats.Entries != 1 || stats.SkippedWrites != 0 {
				t.Fatalf("private search write stats: entries=%d skipped=%d", stats.Entries, stats.SkippedWrites)
			}
		})
	}
}

func TestCodexHTTPCacheSearchNeverFallsBackToPublicOutput(t *testing.T) {
	for _, state := range []string{"nil_response", "unwrapped", "incomplete", "mismatched_id"} {
		t.Run(state, func(t *testing.T) {
			resetResponseCacheStateForTest(onDemandResponseCacheConfig())
			t.Cleanup(resetResponseCacheForTest)
			const owner = "key:search-fixture"
			const id = codexSearchResponseIDPrefix + "fixture"
			markResponseCacheChainOwnerIfOnDemand(owner)
			var response *http.Response
			switch state {
			case "unwrapped":
				response = &http.Response{Body: io.NopCloser(strings.NewReader(""))}
			case "incomplete", "mismatched_id":
				body := &codexSearchContextBody{responseID: id, replay: []json.RawMessage{json.RawMessage(codexHTTPCacheTool)}}
				if state == "mismatched_id" {
					body.completed, body.responseID = true, id+"_other"
				}
				response = &http.Response{Body: body}
			}
			completed := []byte(fmt.Sprintf(`{"response":{"id":%q,"output":[%s]}}`, id, codexHTTPCacheTool))
			cacheCommittedCodexHTTPResponse(response, owner, `[{"role":"user","content":"root"}]`, completed, []json.RawMessage{json.RawMessage(codexHTTPCacheTool)}, nil)
			if lookupResponseCacheResult(owner, id).Kind == responseCacheLookupHit || nativeWSContextLookup(codexSearchContextOwner(owner, id), id).Kind == responseCacheLookupHit {
				t.Fatal("unavailable private search context fell back to public tool output")
			}
			if stats := GetResponseCacheStats(); stats.Entries != 0 || stats.SkippedWrites != 0 {
				t.Fatalf("incomplete search stats: entries=%d skipped=%d, want 0/0", stats.Entries, stats.SkippedWrites)
			}
		})
	}
}

func BenchmarkCodexHTTPCacheRejectedInput(b *testing.B) {
	const owner = "key:http-cache-fixture"
	account := &auth.Account{DBID: 101, CredentialGeneration: 1}
	for _, tc := range codexHTTPCacheRejections() {
		for _, size := range []int{1 << 20, 8 << 20} {
			b.Run(fmt.Sprintf("%s/%dMiB", tc.name, size>>20), func(b *testing.B) {
				resetResponseCacheStateForTest(onDemandResponseCacheConfig())
				b.Cleanup(resetResponseCacheForTest)
				if tc.qualified {
					markResponseCacheChainOwnerIfOnDemand(owner)
				}
				input := codexHTTPCacheInput(size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cacheCommittedCodexHTTPResponse(nil, owner, input, tc.completed, tc.outputs, account)
				}
				b.StopTimer()
				var wantSkipped uint64
				if tc.wantSkip {
					wantSkipped = uint64(b.N)
				}
				if stats := GetResponseCacheStats(); stats.Entries != 0 || stats.SkippedWrites != wantSkipped {
					b.Fatalf("rejection stats: entries=%d skipped=%d, want 0/%d", stats.Entries, stats.SkippedWrites, wantSkipped)
				}
			})
		}
	}
}
