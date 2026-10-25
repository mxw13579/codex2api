package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// These fixtures exercise the bridge with in-memory callbacks only. They do not
// select an account, open a transport, or contact the standalone search service.
type nativeSearchFixture struct {
	id        string
	createdAt int64
	events    []string
}

func (f *nativeSearchFixture) event(kind string, fields map[string]any) {
	fields["type"] = kind
	fields["sequence_number"] = len(f.events)
	raw, _ := json.Marshal(fields)
	f.events = append(f.events, string(raw))
}

func (f *nativeSearchFixture) lifecycle(kind string, items ...any) {
	if items == nil {
		items = []any{}
	}
	status := strings.TrimPrefix(kind, "response.")
	if status == "created" {
		status = "in_progress"
	}
	f.event(kind, map[string]any{"response": map[string]any{
		"id": f.id, "object": "response", "created_at": f.createdAt, "status": status,
		"output": items, "model": "fixture-model", "service_tier": "default",
		"usage": map[string]any{"input_tokens": 10, "output_tokens": 5, "total_tokens": 15,
			"input_tokens_details": map[string]any{"cached_tokens": 2}, "output_tokens_details": map[string]any{"reasoning_tokens": 1}},
	}})
}

func (f *nativeSearchFixture) item(kind string, index int, item any) {
	f.event(kind, map[string]any{"output_index": index, "item": item})
}

func (f *nativeSearchFixture) response() *http.Response {
	return codexSearchTestResponse(f.events...)
}

func nativeSearchReasoning(id string, summaries ...string) map[string]any {
	parts := make([]any, 0, len(summaries))
	for _, text := range summaries {
		parts = append(parts, map[string]any{"type": "summary_text", "text": text})
	}
	return map[string]any{"type": "reasoning", "id": id, "summary": parts, "encrypted_content": "fixture-reasoning"}
}

func nativeSearchMessage(id string, texts ...string) map[string]any {
	parts := make([]any, 0, len(texts))
	for _, text := range texts {
		parts = append(parts, map[string]any{"type": "output_text", "text": text, "annotations": []any{}})
	}
	return map[string]any{"type": "message", "id": id, "status": "completed", "role": "assistant", "content": parts}
}

func nativeSearchSuccess([]byte) (*CodexAlphaSearchResponse, error) {
	return &CodexAlphaSearchResponse{StatusCode: http.StatusOK, Body: []byte(`{"output":"Synthetic result","results":[{"url":"https://example.invalid/source","title":"Fixture"}]}`)}, nil
}

// Check the public stream, not just the aggregated output. In particular, every
// emitted item must have a single slot shared by added/done/parts and the final
// array, even when upstream indices and sequence numbers restart each round.
func assertNativeSearchContract(t *testing.T, stream, id string, createdAt int64) ([]gjson.Result, gjson.Result) {
	t.Helper()
	var events []gjson.Result
	var terminal gjson.Result
	added, done := map[int64]string{}, map[int64]bool{}
	ids := map[string]bool{}
	created, terminals := 0, 0
	// Inspect the emitted wire directly: the production SSE reader normalizes
	// terminal events and could otherwise hide an incorrectly emitted type.
	for frame := range strings.SplitSeq(stream, "\n\n") {
		if frame == "" {
			continue
		}
		if !strings.HasPrefix(frame, "data: ") {
			t.Fatalf("unexpected search SSE frame: %q", frame)
		}
		raw := strings.TrimPrefix(frame, "data: ")
		if !gjson.Valid(raw) {
			t.Fatalf("invalid search SSE JSON: %q", raw)
		}
		data := gjson.Parse(raw)
		kind := data.Get("type").String()
		seq := data.Get("sequence_number")
		if seq.Type != gjson.Number || seq.Int() != int64(len(events)) {
			t.Errorf("%s sequence_number = %s, want %d", kind, seq.Raw, len(events))
		}
		events = append(events, data)
		if response := data.Get("response"); response.Exists() {
			if response.Get("id").String() != id || response.Get("created_at").Int() != createdAt {
				t.Errorf("%s changed response identity: %s", kind, response.Raw)
			}
		}
		if responseID := data.Get("response_id"); responseID.Exists() && responseID.String() != id {
			t.Errorf("%s has a foreign response_id: %s", kind, responseID.Raw)
		}
		if kind == "response.created" {
			created++
		}
		if isResponsesTerminalEvent(kind) || kind == "response.cancelled" {
			terminals++
			terminal = data
		}
		index := data.Get("output_index")
		if kind == "response.output_item.added" {
			itemID := data.Get("item.id").String()
			if index.Type != gjson.Number || index.Int() != int64(len(added)) || itemID == "" || ids[itemID] {
				t.Errorf("invalid/reused output slot or identity: %s", data.Raw)
			}
			added[index.Int()] = itemID
			ids[itemID] = true
		}
		if kind == "response.output_item.done" {
			if index.Type != gjson.Number || added[index.Int()] != data.Get("item.id").String() || done[index.Int()] {
				t.Errorf("unpaired or duplicate item done: %s", data.Raw)
			}
			done[index.Int()] = true
		}
		if data.Get("item_id").Exists() {
			if index.Type != gjson.Number || added[index.Int()] != data.Get("item_id").String() {
				t.Errorf("event does not refer to an added item: %s", data.Raw)
			}
		}
		for prefix, key := range map[string]string{
			"response.output_text.": "content_index", "response.content_part.": "content_index",
			"response.refusal.": "content_index", "response.reasoning_summary_": "summary_index",
		} {
			if strings.HasPrefix(kind, prefix) {
				if data.Get(key).Type != gjson.Number || data.Get(key).Int() < 0 || data.Get("item_id").String() == "" {
					t.Errorf("unindexed part event: %s", data.Raw)
				}
			}
		}
	}
	if created != 1 || terminals != 1 || len(events) == 0 || events[len(events)-1].Raw != terminal.Raw {
		t.Fatalf("lifecycle counts: created=%d terminal=%d", created, terminals)
	}
	items := terminal.Get("response.output").Array()
	if len(items) != len(added) || len(done) != len(added) {
		t.Fatalf("output/added/done count mismatch: %d/%d/%d", len(items), len(added), len(done))
	}
	for i, item := range items {
		if item.Get("id").String() != added[int64(i)] || !done[int64(i)] {
			t.Errorf("final output[%d] is not its streamed item: %s", i, item.Raw)
		}
	}
	return events, terminal
}

func TestNativeSearchResponsesForcedChoice(t *testing.T) {
	for _, kind := range []string{"web_search", "web_search_preview"} {
		for _, allowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allowed=%t", kind, allowed), func(t *testing.T) {
				choice := map[string]any{"type": kind}
				if allowed {
					choice = map[string]any{"type": "allowed_tools", "mode": "required", "tools": []any{choice, map[string]any{"type": "function", "name": "Read"}}}
				}
				body, _ := json.Marshal(map[string]any{
					"model": "fixture-model", "input": []any{}, "tool_choice": choice,
					"tools": []any{map[string]any{"type": kind}, map[string]any{"type": "function", "name": "codex2api_web_search"}, map[string]any{"type": "function", "name": "Read"}},
				})
				prepared, _ := PrepareResponsesBody(body)
				out, bridge, err := prepareAnthropicCodexWebSearch(prepared, nil)
				if err != nil || bridge == nil {
					t.Fatalf("prepare failed: bridge=%v err=%v", bridge, err)
				}
				path := "tool_choice"
				if allowed {
					path += ".tools.0"
					if gjson.GetBytes(out, "tool_choice.mode").String() != "required" || gjson.GetBytes(out, "tool_choice.tools.1.name").String() != "Read" {
						t.Fatalf("allowed_tools constraints changed: %s", out)
					}
				}
				if bridge.name == "codex2api_web_search" || gjson.GetBytes(out, path+".type").String() != "function" || gjson.GetBytes(out, path+".name").String() != bridge.name || gjson.GetBytes(out, "tools.0.name").String() != bridge.name {
					t.Fatalf("forced choice does not refer to the replacement: %s", out)
				}
			})
		}
	}
}

func TestNativeSearchResponsesMultiRoundProtocol(t *testing.T) {
	base, bridge, err := prepareAnthropicCodexWebSearch([]byte(`{"model":"fixture-model","input":[],"tools":[{"type":"web_search_preview"}],"tool_choice":{"type":"web_search_preview"}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	rounds := make([]*nativeSearchFixture, 3)
	for i := range rounds {
		rounds[i] = &nativeSearchFixture{id: fmt.Sprintf("resp_%d", i), createdAt: int64(1700000000 + i)}
	}
	for i, round := range rounds[:2] {
		// The backend echoes the private declaration and forced/allowed choice.
		choice := any(map[string]any{"type": "function", "name": bridge.name})
		if i == 1 {
			choice = map[string]any{"type": "allowed_tools", "mode": "required", "tools": []any{choice}}
		}
		round.event("response.created", map[string]any{"response": map[string]any{
			"id": round.id, "created_at": round.createdAt, "status": "in_progress", "output": []any{},
			"tools": []any{map[string]any{"type": "function", "name": bridge.name}}, "tool_choice": choice,
		}})
		round.lifecycle("response.in_progress")
		reasonID := fmt.Sprintf("rs_%d", i)
		reason := nativeSearchReasoning(reasonID, fmt.Sprintf("Thought %d", i))
		round.item("response.output_item.added", 0, nativeSearchReasoning(reasonID))
		round.event("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": reasonID, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
		round.event("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": reasonID, "summary_index": 0, "delta": fmt.Sprintf("Thought %d", i)})
		round.event("response.reasoning_summary_text.done", map[string]any{"output_index": 0, "item_id": reasonID, "summary_index": 0, "text": fmt.Sprintf("Thought %d", i)})
		round.event("response.reasoning_summary_part.done", map[string]any{"output_index": 0, "item_id": reasonID, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": fmt.Sprintf("Thought %d", i)}})
		round.item("response.output_item.done", 0, reason)
		call := codexSearchTestCall(bridge.name, fmt.Sprintf("Synthetic query %d", i))
		call["id"], call["call_id"] = fmt.Sprintf("fc_%d", i), fmt.Sprintf("call_%d", i)
		round.item("response.output_item.added", 1, map[string]any{"id": call["id"], "type": "function_call", "name": bridge.name, "call_id": call["call_id"], "arguments": ""})
		round.event("response.function_call_arguments.delta", map[string]any{"output_index": 1, "delta": call["arguments"]})
		round.event("response.function_call_arguments.done", map[string]any{"output_index": 1, "arguments": call["arguments"]})
		round.item("response.output_item.done", 1, call)
		round.lifecycle("response.completed", reason, call)
	}
	last := rounds[2]
	last.lifecycle("response.created")
	last.lifecycle("response.in_progress")
	message := nativeSearchMessage("msg_answer", "First answer. ", "Second answer.")
	messageAdded := nativeSearchMessage("msg_answer")
	messageAdded["status"] = "in_progress"
	last.item("response.output_item.added", 0, messageAdded)
	for i, part := range message["content"].([]any) {
		text := part.(map[string]any)["text"]
		last.event("response.content_part.added", map[string]any{"output_index": 0, "item_id": "msg_answer", "content_index": i, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
		last.event("response.output_text.delta", map[string]any{"response_id": last.id, "output_index": 0, "item_id": "msg_answer", "content_index": i, "delta": text})
		last.event("response.output_text.done", map[string]any{"output_index": 0, "item_id": "msg_answer", "content_index": i, "text": text})
		last.event("response.content_part.done", map[string]any{"output_index": 0, "item_id": "msg_answer", "content_index": i, "part": part})
	}
	last.item("response.output_item.done", 0, message)
	last.lifecycle("response.completed", message)
	opened, searched := 0, 0
	var stream strings.Builder
	err = bridge.run(context.Background(), rounds[0].response(), base, func(next []byte) (*http.Response, error) {
		opened++
		if opened >= len(rounds) {
			t.Fatal("unexpected continuation")
		}
		if gjson.GetBytes(next, "tool_choice").String() != "auto" || strings.Contains(gjson.GetBytes(next, "input").Raw, `"web_search_call"`) {
			t.Errorf("public projection leaked into internal replay: %s", next)
		}
		inputs := gjson.GetBytes(next, "input").Array()
		if len(inputs) != 3*opened || inputs[len(inputs)-1].Get("type").String() != "function_call_output" || inputs[len(inputs)-1].Get("call_id").String() != fmt.Sprintf("call_%d", opened-1) {
			t.Errorf("continuation lost calls or paired results: %s", next)
		}
		return rounds[opened].response(), nil
	}, func(raw []byte) (*CodexAlphaSearchResponse, error) {
		searched++
		return nativeSearchSuccess(raw)
	}, &stream)
	if err != nil {
		t.Fatal(err)
	}
	if opened != 2 || searched != 2 || strings.Contains(stream.String(), bridge.name) || strings.Contains(stream.String(), `"function_call"`) {
		t.Fatalf("hidden calls escaped or rounds changed: opened=%d searched=%d", opened, searched)
	}
	events, terminal := assertNativeSearchContract(t, stream.String(), rounds[0].id, rounds[0].createdAt)
	wantIDs := []string{"rs_0", "fc_0", "rs_1", "fc_1", "msg_answer"}
	for i, item := range terminal.Get("response.output").Array() {
		if i >= len(wantIDs) || item.Get("id").String() != wantIDs[i] {
			t.Errorf("output order changed: %s", terminal.Raw)
		}
	}
	if terminal.Get("response.output.#").Int() != 5 || terminal.Get("response.usage.input_tokens").Int() != 30 || terminal.Get("response.usage.output_tokens").Int() != 15 || terminal.Get("response.usage.total_tokens").Int() != 45 || terminal.Get("response.usage.input_tokens_details.cached_tokens").Int() != 6 || terminal.Get("response.usage.output_tokens_details.reasoning_tokens").Int() != 3 {
		t.Fatalf("multi-round output or usage lost: %s", terminal.Raw)
	}
	if events[0].Get("response.tools.0.type").String() != "web_search_preview" || events[0].Get("response.tool_choice.type").String() != "web_search_preview" {
		t.Fatalf("response metadata exposed the replacement: %s", events[0].Raw)
	}
	translator := newAnthropicStreamTranslator("fixture-model")
	accumulator := newAnthropicResponseAccumulator("fixture-model")
	for _, event := range events {
		accumulator.apply(translator.translateEvent([]byte(event.Raw)))
	}
	response := accumulator.build([]byte(terminal.Raw))
	var text, thinking string
	searchBlocks := 0
	for _, block := range response.Content {
		text += block.Text
		thinking += block.Thinking
		if block.Type == "web_search_tool_result" {
			searchBlocks++
		}
	}
	if translator.toolInputError != nil || text != "First answer. Second answer." || thinking != "Thought 0Thought 1" || searchBlocks != 2 {
		t.Fatalf("Messages projection regressed: text=%q thinking=%q searches=%d err=%v", text, thinking, searchBlocks, translator.toolInputError)
	}
}

func TestNativeSearchResponsesParallelExternalTool(t *testing.T) {
	for _, kind := range []string{"function_call", "custom_tool_call"} {
		for _, terminalOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/terminalOnly=%t", kind, terminalOnly), func(t *testing.T) {
				bridge := &anthropicCodexWebSearch{name: "private_search", model: "fixture-model", remaining: 1}
				round := &nativeSearchFixture{id: "resp_parallel", createdAt: 1700000000}
				round.lifecycle("response.created")
				searchCall := codexSearchTestCall(bridge.name, "Synthetic query")
				client := map[string]any{"type": kind, "id": "fc_client", "call_id": "call_client", "name": "Read", "status": "completed"}
				field, prefix, input := "arguments", "response.function_call_arguments.", `{"file_path":"fixture.txt"}`
				if kind == "custom_tool_call" {
					field, prefix, input = "input", "response.custom_tool_call_input.", "read fixture.txt"
				}
				client[field] = input
				if !terminalOnly {
					round.item("response.output_item.added", 0, map[string]any{"type": "function_call", "id": "fc_search", "call_id": "call_search", "name": bridge.name, "arguments": "", "status": "in_progress"})
					round.item("response.output_item.added", 1, map[string]any{"type": kind, "id": "fc_client", "call_id": "call_client", "name": "Read", field: "", "status": "in_progress"})
				}
				// Interleave index-only private and id-only public argument events.
				// In the terminal-only case they must wait for classification.
				round.event("response.function_call_arguments.delta", map[string]any{"output_index": 0, "delta": "private-fragment"})
				round.event(prefix+"delta", map[string]any{"item_id": "fc_client", "delta": input})
				round.event("response.function_call_arguments.done", map[string]any{"output_index": 0, "arguments": searchCall["arguments"]})
				round.event(prefix+"done", map[string]any{"item_id": "fc_client", field: input})
				if !terminalOnly {
					round.item("response.output_item.done", 1, client)
				} else {
					delete(client, field) // The input done event is authoritative.
				}
				round.lifecycle("response.completed", searchCall, client)
				var stream strings.Builder
				searches := 0
				err := bridge.run(context.Background(), round.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) {
					t.Fatal("client tool must be returned before continuing")
					return nil, nil
				}, func(raw []byte) (*CodexAlphaSearchResponse, error) { searches++; return nativeSearchSuccess(raw) }, &stream)
				if err != nil {
					t.Fatal(err)
				}
				events, terminal := assertNativeSearchContract(t, stream.String(), round.id, round.createdAt)
				if searches != 1 || strings.Contains(stream.String(), bridge.name) || strings.Contains(stream.String(), "private-fragment") || terminal.Get("response.output.0.id").String() != "fc_client" || terminal.Get("response.output.0."+field).String() != input || terminal.Get("response.output.1.type").String() != "web_search_call" {
					t.Fatalf("parallel call was hidden, reordered, or leaked: %s", terminal.Raw)
				}
				translator := newAnthropicStreamTranslator("fixture-model")
				accumulator := newAnthropicResponseAccumulator("fixture-model")
				for _, event := range events {
					accumulator.apply(translator.translateEvent([]byte(event.Raw)))
				}
				response := accumulator.build([]byte(terminal.Raw))
				if translator.toolInputError != nil || response.StopReason == nil || *response.StopReason != "tool_use" || len(response.Content) != 3 || response.Content[0].Name != "Read" {
					t.Fatalf("parallel Messages tool regressed: response=%+v err=%v", response, translator.toolInputError)
				}
			})
		}
	}
}

func TestNativeSearchResponsesItemsWithoutAdded(t *testing.T) {
	for _, doneOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("doneOnly=%t", doneOnly), func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search", model: "fixture-model", remaining: 1}
			first := &nativeSearchFixture{id: "resp_first", createdAt: 1700000000}
			first.lifecycle("response.created")
			call := codexSearchTestCall(bridge.name, "Synthetic query")
			if !doneOnly {
				delete(call, "id")
			}
			first.lifecycle("response.completed", call)
			last := &nativeSearchFixture{id: "resp_last", createdAt: 1700000001}
			last.lifecycle("response.created")
			reason := nativeSearchReasoning("rs_final", "One thought. ", "Another thought.")
			message := nativeSearchMessage("msg_final", "One answer. ", "Another answer.")
			if doneOnly {
				last.item("response.output_item.done", 0, reason)
				last.item("response.output_item.done", 1, message)
			} else {
				// Missing IDs must still result in complete public associations.
				delete(message, "id")
			}
			last.lifecycle("response.completed", reason, message)
			var stream strings.Builder
			err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) { return last.response(), nil }, nativeSearchSuccess, &stream)
			if err != nil {
				t.Fatal(err)
			}
			events, terminal := assertNativeSearchContract(t, stream.String(), first.id, first.createdAt)
			counts := map[string]int{}
			texts := map[string]string{}
			for _, event := range events {
				kind := event.Get("type").String()
				counts[kind]++
				if kind == "response.output_text.delta" || kind == "response.reasoning_summary_text.delta" {
					key := "content_index"
					if strings.Contains(kind, "reasoning") {
						key = "summary_index"
					}
					texts[fmt.Sprintf("%s/%d", kind, event.Get(key).Int())] += event.Get("delta").String()
				}
			}
			if terminal.Get("response.output.#").Int() != 3 || counts["response.content_part.added"] != 2 || counts["response.content_part.done"] != 2 || counts["response.reasoning_summary_part.added"] != 2 || counts["response.reasoning_summary_part.done"] != 2 || texts["response.output_text.delta/0"] != "One answer. " || texts["response.output_text.delta/1"] != "Another answer." || texts["response.reasoning_summary_text.delta/0"] != "One thought. " || texts["response.reasoning_summary_text.delta/1"] != "Another thought." {
				t.Fatalf("missing/duplicate synthetic content: counts=%v texts=%v", counts, texts)
			}
		})
	}
}

func TestNativeSearchResponsesSparseTerminalAndReusedIDs(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search", model: "fixture-model", remaining: 1}
	first := &nativeSearchFixture{id: "resp_first", createdAt: 1700000000}
	first.lifecycle("response.created")
	reason := nativeSearchReasoning("rs_reused", "First thought")
	first.item("response.output_item.done", 0, reason)
	// No item event for the hidden call, and terminal omits the streamed reason.
	// Its compacted array index must not alias that already emitted reason.
	first.lifecycle("response.completed", codexSearchTestCall(bridge.name, "Synthetic query"))
	last := &nativeSearchFixture{id: "resp_last", createdAt: 1700000001}
	last.lifecycle("response.created")
	reason = nativeSearchReasoning("rs_reused", "Second thought")
	last.item("response.output_item.done", 0, reason)
	// Also reuse the previous round's synthetic search ID for a message.
	message := nativeSearchMessage("fc_search", "Final answer")
	last.item("response.output_item.added", 1, map[string]any{"id": "fc_search", "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}})
	last.event("response.output_text.delta", map[string]any{"output_index": 1, "item_id": "fc_search", "content_index": 0, "delta": "Final answer"})
	last.item("response.output_item.done", 1, message)
	last.lifecycle("response.completed", reason, message)
	var stream strings.Builder
	err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) { return last.response(), nil }, nativeSearchSuccess, &stream)
	if err != nil {
		t.Fatal(err)
	}
	_, terminal := assertNativeSearchContract(t, stream.String(), first.id, first.createdAt)
	items := terminal.Get("response.output").Array()
	if len(items) != 4 || items[0].Get("id").String() != "rs_reused" || items[1].Get("id").String() != "fc_search" || items[2].Get("summary.0.text").String() != "Second thought" || items[3].Get("content.0.text").String() != "Final answer" {
		t.Fatalf("sparse snapshots or reused identities corrupted output: %s", terminal.Raw)
	}
}

func TestNativeSearchResponsesNonSuccessTerminal(t *testing.T) {
	for _, tc := range []struct{ kind, wantType, errorCode string }{
		{"response.failed", "response.failed", "fixture_failure"},
		{"response.incomplete", "response.incomplete", ""},
		{"response.cancelled", "response.cancelled", "fixture_failure"},
		// Real truncation has no error. A contradictory incomplete terminal with
		// an explicit failure must not become successful continuation history.
		{"response.incomplete", "response.failed", "fixture_failure"},
	} {
		t.Run(tc.kind+"/"+tc.wantType, func(t *testing.T) {
			kind := tc.kind
			var errorInfo any
			if tc.errorCode != "" {
				errorInfo = map[string]any{"code": tc.errorCode}
			}
			bridge := &anthropicCodexWebSearch{name: "private_search", model: "fixture-model", remaining: 2}
			first := &nativeSearchFixture{id: "resp_first", createdAt: 1700000000}
			first.lifecycle("response.created")
			first.lifecycle("response.completed", codexSearchTestCall(bridge.name, "Synthetic query"))
			last := &nativeSearchFixture{id: "resp_last", createdAt: 1700000001}
			last.lifecycle("response.created")
			reason := nativeSearchReasoning("rs_last", "Partial thought")
			last.item("response.output_item.done", 0, reason)
			call := codexSearchTestCall(bridge.name, "Must not execute")
			last.item("response.output_item.added", 1, call)
			last.event("response.function_call_arguments.delta", map[string]any{"item_id": "undeclared", "output_index": 2, "delta": "never-declared-private-fragment"})
			last.event(kind, map[string]any{"response": map[string]any{
				"id": last.id, "created_at": last.createdAt, "status": strings.TrimPrefix(kind, "response."), "output": []any{reason, call},
				"error": errorInfo, "incomplete_details": map[string]any{"reason": "max_output_tokens"},
				"tools":       []any{map[string]any{"type": "function", "name": bridge.name}},
				"tool_choice": map[string]any{"type": "allowed_tools", "mode": "required", "tools": []any{map[string]any{"type": "function", "name": bridge.name}}},
				"usage":       map[string]any{"input_tokens": 7, "output_tokens": 3, "input_tokens_details": map[string]any{"cached_tokens": 1}, "output_tokens_details": map[string]any{"reasoning_tokens": 2}},
			}})
			searches, opened := 0, 0
			var stream strings.Builder
			err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) { opened++; return last.response(), nil }, func(raw []byte) (*CodexAlphaSearchResponse, error) { searches++; return nativeSearchSuccess(raw) }, &stream)
			if err != nil {
				t.Fatal(err)
			}
			_, terminal := assertNativeSearchContract(t, stream.String(), first.id, first.createdAt)
			if searches != 1 || opened != 1 || terminal.Get("type").String() != tc.wantType || terminal.Get("response.status").String() != strings.TrimPrefix(kind, "response.") || terminal.Get("response.error.code").String() != tc.errorCode || terminal.Get("response.incomplete_details.reason").String() != "max_output_tokens" || terminal.Get("response.usage.input_tokens").Int() != 17 || terminal.Get("response.usage.output_tokens").Int() != 8 || terminal.Get("response.usage.input_tokens_details.cached_tokens").Int() != 3 || terminal.Get("response.usage.output_tokens_details.reasoning_tokens").Int() != 3 || terminal.Get("response.output.#").Int() != 2 {
				t.Fatalf("terminal semantics or usage changed: %s", terminal.Raw)
			}
			if strings.Contains(stream.String(), bridge.name) || strings.Contains(stream.String(), "never-declared-private-fragment") || terminal.Get("response.tool_choice.tools.0.type").String() != "web_search" {
				t.Fatal("private function escaped through a non-success terminal")
			}
		})
	}
}

type nativeSearchClosedWriter struct{}

func (nativeSearchClosedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestNativeSearchResponsesDownstreamClose(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search", model: "fixture-model", remaining: 1}
	first := &nativeSearchFixture{id: "resp_first", createdAt: 1700000000}
	first.lifecycle("response.created")
	first.lifecycle("response.completed", codexSearchTestCall(bridge.name, "Must not execute"))
	err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) {
		t.Fatal("continued after downstream closed")
		return nil, nil
	}, func([]byte) (*CodexAlphaSearchResponse, error) {
		t.Fatal("searched after downstream closed")
		return nil, nil
	}, nativeSearchClosedWriter{})
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("downstream error was swallowed: %v", err)
	}
}

func runNativeSearchWithoutCalls(t *testing.T, bridge *anthropicCodexWebSearch, fixture *nativeSearchFixture) string {
	t.Helper()
	var stream strings.Builder
	err := bridge.run(context.Background(), fixture.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) {
		t.Fatal("unexpected continuation")
		return nil, nil
	}, func([]byte) (*CodexAlphaSearchResponse, error) {
		t.Fatal("unexpected search")
		return nil, nil
	}, &stream)
	if err != nil {
		t.Fatal(err)
	}
	return stream.String()
}

func TestNativeSearchResponsesSplitAliases(t *testing.T) {
	for _, identityFirst := range []bool{false, true} {
		for _, identityKey := range []string{"item_id", "call_id"} {
			t.Run(fmt.Sprintf("%s/identityFirst=%t", identityKey, identityFirst), func(t *testing.T) {
				bridge := &anthropicCodexWebSearch{name: "private_search"}
				fixture := &nativeSearchFixture{id: "resp_alias", createdAt: 1700000000}
				fixture.lifecycle("response.created")
				prefix := "response.output_text."
				first, second := "First ", "answer"
				identity := "A"
				added, final := nativeSearchMessage("A"), nativeSearchMessage("A", first+second)
				if identityKey == "call_id" {
					prefix, first, second, identity = "response.function_call_arguments.", `{"fixture":`, `true}`, "call_A"
					added = map[string]any{"id": "A", "type": "function_call", "call_id": identity, "name": "Read", "arguments": ""}
					final = map[string]any{"id": "A", "type": "function_call", "call_id": identity, "name": "Read", "arguments": first + second}
				}
				for i, text := range []string{first, second} {
					fields := map[string]any{"delta": text}
					if (i == 0) == identityFirst {
						fields[identityKey] = identity
					} else {
						fields["output_index"] = 0
					}
					fixture.event(prefix+"delta", fields)
				}
				fixture.item("response.output_item.added", 0, added)
				fixture.item("response.output_item.done", 0, final)
				fixture.lifecycle("response.completed", final)
				events, terminal := assertNativeSearchContract(t, runNativeSearchWithoutCalls(t, bridge, fixture), fixture.id, fixture.createdAt)
				var text string
				for _, event := range events {
					if event.Get("type").String() == prefix+"delta" {
						text += event.Get("delta").String()
					}
				}
				if text != first+second || terminal.Get("response.output.#").Int() != 1 {
					t.Fatalf("split aliases lost, reordered, or duplicated pending text: %q", text)
				}
			})
		}
	}
}

func TestNativeSearchResponsesSparseTerminalCallWithoutID(t *testing.T) {
	for _, declared := range []bool{false, true} {
		t.Run(fmt.Sprintf("declared=%t", declared), func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search", remaining: 1}
			first := &nativeSearchFixture{id: "resp_sparse", createdAt: 1700000000}
			first.lifecycle("response.created")
			reason := nativeSearchReasoning("A", "Retained thought")
			first.item("response.output_item.done", 0, reason)
			call := codexSearchTestCall(bridge.name, "Synthetic query")
			delete(call, "id")
			if declared {
				first.item("response.output_item.added", 7, call)
			}
			// Position zero is not upstream output_index zero. call_id/type must
			// distinguish this id-less function from the already published reason.
			first.lifecycle("response.completed", call)
			last := &nativeSearchFixture{id: "resp_last", createdAt: 1700000001}
			last.lifecycle("response.created")
			last.lifecycle("response.completed", nativeSearchMessage("M", "Answer"))
			var replay []json.RawMessage
			bridge.onCompleted = func(items []json.RawMessage) { replay = items }
			var stream strings.Builder
			opened, searched := 0, 0
			err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) {
				opened++
				return last.response(), nil
			}, func(raw []byte) (*CodexAlphaSearchResponse, error) { searched++; return nativeSearchSuccess(raw) }, &stream)
			if err != nil {
				t.Fatal(err)
			}
			_, terminal := assertNativeSearchContract(t, stream.String(), first.id, first.createdAt)
			if opened != 1 || searched != 1 || len(replay) != 4 || gjson.GetBytes(replay[0], "id").String() != "A" || gjson.GetBytes(replay[1], "type").String() != "function_call" || gjson.GetBytes(replay[1], "id").Exists() || terminal.Get("response.output.0.type").String() != "reasoning" || terminal.Get("response.output.0.summary.0.text").String() != "Retained thought" || terminal.Get("response.output.#").Int() != 3 {
				t.Fatal("sparse terminal positional hint overwrote or duplicated a real output")
			}
		})
	}
}

func TestNativeSearchResponsesTextCompletionByPart(t *testing.T) {
	for _, reasoning := range []bool{false, true} {
		for _, added := range []bool{false, true} {
			for _, deltaMode := range []string{"none", "partial", "full"} {
				for _, done := range []bool{false, true} {
					t.Run(fmt.Sprintf("reasoning=%t/added=%t/delta=%s/done=%t", reasoning, added, deltaMode, done), func(t *testing.T) {
						bridge := &anthropicCodexWebSearch{name: "private_search"}
						fixture := &nativeSearchFixture{id: "resp_text", createdAt: 1700000000}
						fixture.lifecycle("response.created")
						texts := []string{"First answer. ", "Second answer."}
						prefix, indexKey, path := "response.output_text.", "content_index", "content"
						empty, final := nativeSearchMessage("M"), nativeSearchMessage("M", texts...)
						if reasoning {
							prefix, indexKey, path = "response.reasoning_summary_text.", "summary_index", "summary"
							empty, final = nativeSearchReasoning("M"), nativeSearchReasoning("M", texts...)
						}
						if added {
							fixture.item("response.output_item.added", 0, empty)
						}
						for i, text := range texts {
							if deltaMode != "none" {
								delta := text
								if deltaMode == "partial" {
									delta = text[:3]
								}
								fixture.event(prefix+"delta", map[string]any{"item_id": "M", indexKey: i, "delta": delta})
							}
							if done {
								// Repeated full done events must not repeat their text.
								for n := 0; n < 2; n++ {
									fixture.event(prefix+"done", map[string]any{"item_id": "M", indexKey: i, "text": text})
								}
							}
						}
						if done {
							// The final item may carry no text at all; done is authoritative.
							final = empty
						}
						if added {
							fixture.item("response.output_item.done", 0, final)
						}
						fixture.lifecycle("response.completed", final)
						events, terminal := assertNativeSearchContract(t, runNativeSearchWithoutCalls(t, bridge, fixture), fixture.id, fixture.createdAt)
						got, doneCount := map[int64]string{}, map[int64]int{}
						translator := newAnthropicStreamTranslator("fixture-model")
						accumulator := newAnthropicResponseAccumulator("fixture-model")
						for _, event := range events {
							if event.Get("type").String() == prefix+"delta" {
								got[event.Get(indexKey).Int()] += event.Get("delta").String()
							}
							if event.Get("type").String() == prefix+"done" {
								doneCount[event.Get(indexKey).Int()]++
							}
							accumulator.apply(translator.translateEvent([]byte(event.Raw)))
						}
						for i, text := range texts {
							if got[int64(i)] != text || doneCount[int64(i)] != 1 || terminal.Get(fmt.Sprintf("response.output.0.%s.%d.text", path, i)).String() != text {
								t.Fatalf("part %d missing/duplicated: delta=%q done=%d", i, got[int64(i)], doneCount[int64(i)])
							}
						}
						var projected string
						for _, block := range accumulator.build([]byte(terminal.Raw)).Content {
							projected += block.Text + block.Thinking
						}
						if projected != strings.Join(texts, "") || translator.toolInputError != nil {
							t.Fatalf("Messages lost or repeated final text: %q", projected)
						}
					})
				}
			}
		}
	}
}

func TestNativeSearchResponsesPartPublicationOrder(t *testing.T) {
	for _, reasoning := range []bool{false, true} {
		for _, layout := range []string{"parallel", "reverse", "sparse"} {
			for _, completion := range []string{"snapshot", "text_done", "part_done"} {
				t.Run(fmt.Sprintf("reasoning=%t/%s/%s", reasoning, layout, completion), func(t *testing.T) {
					bridge := &anthropicCodexWebSearch{name: "private_search"}
					fixture := &nativeSearchFixture{id: "resp_order", createdAt: 1700000000}
					fixture.lifecycle("response.created")
					prefix, partPrefix, indexKey, path, partType := "response.output_text.", "response.content_part.", "content_index", "content", "output_text"
					empty := nativeSearchMessage("M")
					if reasoning {
						prefix, partPrefix, indexKey, path, partType = "response.reasoning_summary_text.", "response.reasoning_summary_part.", "summary_index", "summary", "summary_text"
						empty = nativeSearchReasoning("M")
					}
					fixture.item("response.output_item.added", 0, empty)
					indices, order := []int{0, 1, 2}, []int{0, 2, 1}
					if layout == "reverse" {
						order = []int{2, 1, 0}
					} else if layout == "sparse" {
						indices, order = []int{2, 5, 7}, []int{2, 0, 1}
					}
					texts := []string{"First answer. ", "Second answer. ", "Third answer."}
					parts := make([]any, indices[len(indices)-1]+1)
					for _, n := range order {
						index, text := indices[n], texts[n]
						parts[index] = map[string]any{"type": partType, "text": text}
						fixture.event(partPrefix+"added", map[string]any{"item_id": "M", indexKey: index, "part": map[string]any{"type": partType, "text": ""}})
						fixture.event(prefix+"delta", map[string]any{"item_id": "M", indexKey: index, "delta": text[:3]})
						if completion == "text_done" {
							fixture.event(prefix+"done", map[string]any{"item_id": "M", indexKey: index, "text": text})
						} else if completion == "part_done" {
							fixture.event(partPrefix+"done", map[string]any{"item_id": "M", indexKey: index, "part": parts[index]})
						}
					}
					if completion == "snapshot" {
						empty[path] = parts
					}
					fixture.lifecycle("response.completed", empty)
					events, terminal := assertNativeSearchContract(t, runNativeSearchWithoutCalls(t, bridge, fixture), fixture.id, fixture.createdAt)
					byPart, done, added, partDone := map[int]string{}, map[int]int{}, map[int]int{}, map[int]int{}
					var published, projected string
					translator := newAnthropicStreamTranslator("fixture-model")
					accumulator := newAnthropicResponseAccumulator("fixture-model")
					for _, event := range events {
						index := int(event.Get(indexKey).Int())
						switch event.Get("type").String() {
						case prefix + "delta":
							byPart[index] += event.Get("delta").String()
							published += event.Get("delta").String()
						case prefix + "done":
							done[index]++
						case partPrefix + "added":
							added[index]++
						case partPrefix + "done":
							partDone[index]++
						}
						accumulator.apply(translator.translateEvent([]byte(event.Raw)))
					}
					for n, index := range indices {
						if byPart[index] != texts[n] || done[index] != 1 || added[index] != 1 || partDone[index] != 1 || terminal.Get(fmt.Sprintf("response.output.0.%s.%d.text", path, index)).String() != texts[n] {
							t.Fatalf("part %d changed or repeated: text=%q done=%d added=%d partDone=%d", index, byPart[index], done[index], added[index], partDone[index])
						}
					}
					for _, block := range accumulator.build([]byte(terminal.Raw)).Content {
						projected += block.Text + block.Thinking
					}
					if len(added) != len(indices) || len(partDone) != len(indices) || published != strings.Join(texts, "") || projected != published || translator.toolInputError != nil {
						t.Fatalf("part indices or publication order changed: published=%q Messages=%q", published, projected)
					}
				})
			}
		}
	}
}

func TestNativeSearchResponsesPartStreaming(t *testing.T) {
	for _, reasoning := range []bool{false, true} {
		t.Run(fmt.Sprintf("reasoning=%t", reasoning), func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search"}
			var stream strings.Builder
			state := &anthropicSearchRound{output: &anthropicSearchOutput{bridge: bridge, writer: &stream}, byID: map[string]*anthropicSearchItem{}, byCall: map[string]*anthropicSearchItem{}, byIndex: map[int64]*anthropicSearchItem{}}
			prefix, key, empty := "response.output_text.", "content_index", nativeSearchMessage("M")
			if reasoning {
				prefix, key, empty = "response.reasoning_summary_text.", "summary_index", nativeSearchReasoning("M")
			}
			send := func(kind string, index int, fields map[string]any) {
				t.Helper()
				fields["type"], fields["item_id"], fields[key] = kind, "M", index
				raw, _ := json.Marshal(fields)
				if !state.event(raw) {
					t.Fatal(state.output.err)
				}
			}
			send("response.output_item.added", 0, map[string]any{"item": empty})
			for generation := 0; generation < 2; generation++ {
				index := generation * 2
				first, later, suffix := fmt.Sprintf("first-%d", generation), fmt.Sprintf("later-%d", generation), fmt.Sprintf("-suffix-%d", generation)
				send(prefix+"delta", index, map[string]any{"delta": first})
				if !strings.Contains(stream.String(), `"delta":"`+first+`"`) {
					t.Fatal("continuous part first text was delayed")
				}
				send(prefix+"done", index+1, map[string]any{"text": later})
				if strings.Contains(stream.String(), later) || state.pendingBytes == 0 || state.pendingEvents == 0 {
					t.Fatal("later part was published before the current part completed")
				}
				send(prefix+"done", index, map[string]any{"text": first + suffix})
				firstEnd, laterStart := strings.Index(stream.String(), `"delta":"`+suffix+`"`), strings.Index(stream.String(), `"delta":"`+later+`"`)
				if firstEnd < 0 || laterStart <= firstEnd || state.pendingBytes != 0 || state.pendingEvents != 0 || len(state.byID["M"].deferred) != 0 {
					t.Fatal("completion did not release later text in order and recover the pending budget")
				}
			}
		})
	}
}

func TestNativeSearchResponsesDeferredPartAliasMerge(t *testing.T) {
	for _, publishedByIndex := range []bool{false, true} {
		t.Run(fmt.Sprintf("publishedByIndex=%t", publishedByIndex), func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search"}
			fixture := &nativeSearchFixture{id: "resp_deferred_alias", createdAt: 1700000000}
			fixture.lifecycle("response.created")
			// A published item later joins a placeholder; exercise both merge
			// directions so the published cursor/identity must survive a swap.
			item := nativeSearchMessage("M")
			added := map[string]any{"item": item}
			first := map[string]any{"item_id": "M", "content_index": 0, "delta": "Fir"}
			second := map[string]any{"output_index": 3, "content_index": 1, "delta": "Sec"}
			if publishedByIndex {
				delete(item, "id")
				added["output_index"] = 3
				delete(first, "item_id")
				first["output_index"] = 3
				delete(second, "output_index")
				second["item_id"] = "M"
			}
			fixture.event("response.output_item.added", added)
			fixture.event("response.output_text.delta", second)
			fixture.event("response.output_text.delta", first)
			fixture.item("response.output_item.added", 3, nativeSearchMessage("M"))
			fixture.lifecycle("response.completed", nativeSearchMessage("M", "First answer. ", "Second answer."))
			events, terminal := assertNativeSearchContract(t, runNativeSearchWithoutCalls(t, bridge, fixture), fixture.id, fixture.createdAt)
			var text string
			for _, event := range events {
				if event.Get("type").String() == "response.output_text.delta" {
					text += event.Get("delta").String()
				}
			}
			if text != "First answer. Second answer." || terminal.Get("response.output.#").Int() != 1 {
				t.Fatalf("joining a published item lost its part cursor or pending events: %q", text)
			}
		})
	}
}

func TestNativeSearchResponsesDeferredPartLimits(t *testing.T) {
	for _, limit := range []string{"bytes", "events", "shared"} {
		t.Run(limit, func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search"}
			var stream strings.Builder
			state := &anthropicSearchRound{output: &anthropicSearchOutput{bridge: bridge, writer: &stream}, byID: map[string]*anthropicSearchItem{}, byCall: map[string]*anthropicSearchItem{}, byIndex: map[int64]*anthropicSearchItem{}}
			if !state.event(mustMarshalSearchEvent("response.output_item.added", nativeSearchMessage("M"))) {
				t.Fatal(state.output.err)
			}
			stopped := false
			for i := 0; i <= anthropicSearchMaxPendingEvents; i++ {
				fragment := "synthetic-deferred-fragment"
				if limit == "bytes" {
					fragment = strings.Repeat("x", 64<<10)
				}
				event := map[string]any{"type": "response.output_text.delta", "item_id": "M", "content_index": 1, "delta": fragment}
				if limit == "shared" && i%2 == 1 {
					event = map[string]any{"type": "response.function_call_arguments.delta", "item_id": "unknown", "delta": fragment}
				}
				raw, _ := json.Marshal(event)
				if !state.event(raw) {
					stopped = true
					break
				}
			}
			if !stopped || state.output.err == nil || !strings.Contains(state.output.err.Error(), "pending output item") || strings.Contains(stream.String(), "fragment") || strings.Contains(stream.String(), "response.output_text.delta") || state.pendingBytes > anthropicSearchMaxPendingBytes || state.pendingEvents > anthropicSearchMaxPendingEvents {
				t.Fatalf("deferred %s not bounded safely: bytes=%d events=%d err=%v", limit, state.pendingBytes, state.pendingEvents, state.output.err)
			}
		})
	}
}

func TestNativeSearchResponsesInvalidPartIndices(t *testing.T) {
	for _, prefix := range []string{"response.output_text.", "response.reasoning_summary_text."} {
		for _, index := range []any{-1, 0.5, "1", nil, int64(1 << 40), 1e100} {
			t.Run(fmt.Sprintf("%s/%v", prefix, index), func(t *testing.T) {
				bridge := &anthropicCodexWebSearch{name: "private_search"}
				var stream strings.Builder
				state := &anthropicSearchRound{output: &anthropicSearchOutput{bridge: bridge, writer: &stream}, byID: map[string]*anthropicSearchItem{}, byCall: map[string]*anthropicSearchItem{}, byIndex: map[int64]*anthropicSearchItem{}}
				key, item := "content_index", nativeSearchMessage("M")
				if strings.Contains(prefix, "reasoning") {
					key, item = "summary_index", nativeSearchReasoning("M")
				}
				if !state.event(mustMarshalSearchEvent("response.output_item.added", item)) {
					t.Fatal(state.output.err)
				}
				raw, _ := json.Marshal(map[string]any{"type": prefix + "delta", "item_id": "M", key: index, "delta": "synthetic-invalid-part"})
				if state.event(raw) || state.output.err == nil || !strings.Contains(state.output.err.Error(), "output part index") || strings.Contains(stream.String(), "synthetic-invalid-part") || len(state.byID["M"].parts) != 0 {
					t.Fatalf("invalid part index allocated or published content: %v", state.output.err)
				}
			})
		}
	}
}

func TestNativeSearchResponsesTextAfterPartDone(t *testing.T) {
	for _, late := range []string{"delta", "done", "snapshot"} {
		t.Run(late, func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search", onCompleted: func([]json.RawMessage) { t.Fatal("cached inconsistent text") }}
			fixture := &nativeSearchFixture{id: "resp_late", createdAt: 1700000000}
			fixture.lifecycle("response.created")
			fixture.item("response.output_item.added", 0, nativeSearchMessage("M"))
			fixture.event("response.output_text.done", map[string]any{"item_id": "M", "content_index": 0, "text": "First"})
			fixture.event("response.output_text.delta", map[string]any{"item_id": "M", "content_index": 1, "delta": "Second"})
			if late != "snapshot" {
				fields := map[string]any{"item_id": "M", "content_index": 0, "text": "First late-suffix"}
				if late == "delta" {
					fields["delta"] = " late-suffix"
				}
				fixture.event("response.output_text."+late, fields)
			}
			fixture.lifecycle("response.completed", nativeSearchMessage("M", "First late-suffix", "Second"))
			var stream strings.Builder
			err := bridge.run(context.Background(), fixture.response(), []byte(`{"input":[]}`), nil, nil, &stream)
			if err == nil || !strings.Contains(err.Error(), "completed output text") || strings.Contains(stream.String(), "late-suffix") {
				t.Fatalf("late suffix escaped after a later part was published: %v", err)
			}
		})
	}
}

func TestNativeSearchResponsesPendingLimits(t *testing.T) {
	for _, limit := range []string{"bytes", "events", "items", "snapshots"} {
		t.Run(limit, func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search"}
			var stream strings.Builder
			state := &anthropicSearchRound{output: &anthropicSearchOutput{bridge: bridge, writer: &stream}, byID: map[string]*anthropicSearchItem{}, byCall: map[string]*anthropicSearchItem{}, byIndex: map[int64]*anthropicSearchItem{}}
			stopped := false
			for i := 0; i <= anthropicSearchMaxPendingEvents; i++ {
				id, fragment := "unknown", "synthetic-private-fragment"
				if limit == "items" {
					id = fmt.Sprintf("unknown_%d", i)
				}
				if limit == "bytes" || limit == "snapshots" {
					id = fmt.Sprintf("unknown_%d", i%32)
					fragment = strings.Repeat("x", 64<<10)
				}
				event := map[string]any{"type": "response.function_call_arguments.delta", "item_id": id, "delta": fragment}
				if limit == "snapshots" {
					event = map[string]any{"type": "response.output_item.added", "item": map[string]any{"id": id, "type": "function_call", "arguments": fragment}}
				}
				raw, _ := json.Marshal(event)
				if !state.event(raw) {
					stopped = true
					break
				}
			}
			if !stopped || state.output.err == nil || !strings.Contains(state.output.err.Error(), "pending output item") || stream.Len() != 0 || state.pendingBytes > anthropicSearchMaxPendingBytes || state.pendingEvents > anthropicSearchMaxPendingEvents {
				t.Fatalf("unclassified %s not bounded safely: stopped=%t bytes=%d events=%d err=%v", limit, stopped, state.pendingBytes, state.pendingEvents, state.output.err)
			}
		})
	}
}

func TestNativeSearchResponsesHiddenAliasMergeReleasesPending(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search"}
	var stream strings.Builder
	state := &anthropicSearchRound{output: &anthropicSearchOutput{bridge: bridge, writer: &stream}, byID: map[string]*anthropicSearchItem{}, byCall: map[string]*anthropicSearchItem{}, byIndex: map[int64]*anthropicSearchItem{}}
	for generation := 0; generation < 2; generation++ {
		id := fmt.Sprintf("hidden_%d", generation)
		for i := 0; i < anthropicSearchMaxPendingEvents; i++ {
			event := map[string]any{"type": "response.function_call_arguments.delta", "delta": "synthetic-private-fragment"}
			if i%2 == 0 {
				event["item_id"] = id
			} else {
				event["output_index"] = generation
			}
			raw, _ := json.Marshal(event)
			if !state.event(raw) {
				t.Fatalf("pending budget did not recover: %v", state.output.err)
			}
		}
		raw, _ := json.Marshal(map[string]any{"type": "response.output_item.added", "output_index": generation, "item": map[string]any{"id": id, "type": "function_call", "call_id": "call_" + id, "name": bridge.name, "arguments": ""}})
		if !state.event(raw) || state.byID[id] != state.byIndex[int64(generation)] || len(state.items) != generation+1 || state.pendingBytes != 0 || state.pendingEvents != 0 || len(state.byID[id].pending) != 0 || stream.Len() != 0 {
			t.Fatal("hidden alias merge leaked or retained a second placeholder/buffer")
		}
	}
}

func TestNativeSearchResponsesConflictingAliases(t *testing.T) {
	for _, conflict := range []string{"identity", "type"} {
		t.Run(conflict, func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search"}
			fixture := &nativeSearchFixture{id: "resp_conflict", createdAt: 1700000000}
			fixture.lifecycle("response.created")
			fixture.event("response.output_text.delta", map[string]any{"item_id": "A", "output_index": 0, "delta": "synthetic-buffered-text"})
			item := nativeSearchMessage("B")
			if conflict == "type" {
				item = map[string]any{"id": "A", "type": "function_call", "call_id": "call_A", "name": bridge.name, "arguments": "synthetic-private-input"}
			}
			fixture.item("response.output_item.added", 0, item)
			fixture.lifecycle("response.completed", item)
			var stream strings.Builder
			err := bridge.run(context.Background(), fixture.response(), []byte(`{"input":[]}`), nil, nil, &stream)
			if err == nil || !strings.Contains(err.Error(), "conflicting output item") || strings.Contains(stream.String(), "synthetic-buffered-text") || strings.Contains(stream.String(), "synthetic-private-input") {
				t.Fatalf("conflicting aliases were accepted or exposed: %v", err)
			}
		})
	}
}

func TestNativeSearchResponsesDoneOnlyRefusal(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search"}
	fixture := &nativeSearchFixture{id: "resp_refusal", createdAt: 1700000000}
	fixture.lifecycle("response.created")
	fixture.event("response.refusal.done", map[string]any{"item_id": "M", "content_index": 0, "refusal": "Synthetic refusal"})
	fixture.lifecycle("response.completed", map[string]any{"id": "M", "role": "assistant", "status": "completed"})
	events, terminal := assertNativeSearchContract(t, runNativeSearchWithoutCalls(t, bridge, fixture), fixture.id, fixture.createdAt)
	var refusal string
	for _, event := range events {
		if event.Get("type").String() == "response.refusal.delta" {
			refusal += event.Get("delta").String()
		}
	}
	if refusal != "Synthetic refusal" || terminal.Get("response.output.0.type").String() != "message" || terminal.Get("response.output.0.content.0.refusal").String() != refusal {
		t.Fatal("done-only refusal was lost or repeated")
	}
}

func TestNativeSearchResponsesPendingLimitRunError(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search"}
	fixture := &nativeSearchFixture{id: "resp_pending", createdAt: 1700000000}
	fixture.lifecycle("response.created")
	for i := 0; i <= anthropicSearchMaxPendingEvents; i++ {
		fixture.event("response.function_call_arguments.delta", map[string]any{"item_id": "unknown", "delta": "synthetic-private-fragment"})
	}
	fixture.lifecycle("response.completed")
	bridge.onCompleted = func([]json.RawMessage) { t.Fatal("cached an overflowing stream") }
	var stream strings.Builder
	err := bridge.run(context.Background(), fixture.response(), []byte(`{"input":[]}`), nil, nil, &stream)
	if err == nil || !strings.Contains(err.Error(), "pending output item") || strings.Contains(stream.String(), "synthetic-private-fragment") {
		t.Fatalf("pending overflow was lost or exposed buffered input: %v", err)
	}
}

func TestNativeSearchResponsesUnknownFunctionOnFailure(t *testing.T) {
	for _, terminalKind := range []string{"response.failed", "response.cancelled", "response.incomplete"} {
		for _, terminalItem := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/item=%t", terminalKind, terminalItem), func(t *testing.T) {
				bridge := &anthropicCodexWebSearch{name: "private_search", onCompleted: func([]json.RawMessage) { t.Fatal("cached a non-success response") }}
				fixture := &nativeSearchFixture{id: "resp_unknown", createdAt: 1700000000}
				fixture.lifecycle("response.created")
				unknown := map[string]any{"id": "unknown", "call_id": "unknown_call", "type": "function_call", "arguments": "synthetic-private-arguments"}
				fixture.item("response.output_item.added", 0, unknown)
				fixture.event("response.function_call_arguments.delta", map[string]any{"item_id": "unknown", "delta": "synthetic-private-arguments"})
				fixture.event("response.function_call_arguments.done", map[string]any{"output_index": 0, "arguments": "synthetic-private-arguments"})
				fixture.item("response.output_item.done", 0, unknown)
				if terminalItem {
					fixture.lifecycle(terminalKind, unknown)
				} else {
					fixture.lifecycle(terminalKind)
				}
				stream := runNativeSearchWithoutCalls(t, bridge, fixture)
				_, terminal := assertNativeSearchContract(t, stream, fixture.id, fixture.createdAt)
				if terminal.Get("type").String() != terminalKind || terminal.Get("response.output.#").Int() != 0 || strings.Contains(stream, "synthetic-private-arguments") || strings.Contains(stream, "unknown_call") {
					t.Fatal("unclassified function leaked on non-success terminal")
				}
			})
		}
	}
}

func TestNativeSearchResponsesUsageDetails(t *testing.T) {
	for _, invoke := range []bool{false, true} {
		t.Run(fmt.Sprintf("invoke=%t", invoke), func(t *testing.T) {
			base, bridge, err := prepareAnthropicCodexWebSearch([]byte(`{"input":[],"tools":[{"type":"web_search"}]}`), nil)
			if err != nil {
				t.Fatal(err)
			}
			rounds := []*nativeSearchFixture{{id: "resp_usage", createdAt: 1700000000}}
			if invoke {
				rounds = append(rounds, &nativeSearchFixture{id: "resp_usage_last", createdAt: 1700000001})
			}
			for i, fixture := range rounds {
				fixture.lifecycle("response.created")
				item := nativeSearchMessage("M", "Answer")
				if i < len(rounds)-1 {
					item = codexSearchTestCall(bridge.name, "Synthetic query")
				}
				n := i + 1
				inputDetails := map[string]any{"cached_tokens": n, "image_tokens": 2 * n, "audio_tokens": n,
					"cached_tokens_details": map[string]any{"image_tokens": n, "audio_tokens": n, "fixture_metadata": "retained"},
					"future":                map[string]any{"new_tokens": n, "label": "synthetic"}}
				usage := map[string]any{"input_tokens": 10 * n, "output_tokens": 5 * n, "total_tokens": 15 * n, "input_tokens_details": inputDetails,
					"output_tokens_details": map[string]any{"reasoning_tokens": n, "image_tokens": n, "audio_tokens": n, "prediction_tokens_details": map[string]any{"accepted_tokens": n}},
					"fixture_unknown":       []any{"retained", 42}, "fixture_version": 7}
				if i == 0 {
					usage["first_round_only"] = map[string]any{"mode": "fixture"}
				}
				fixture.event("response.completed", map[string]any{"response": map[string]any{"id": fixture.id, "created_at": fixture.createdAt, "status": "completed", "output": []any{item}, "usage": usage}})
			}
			opened, searched := 0, 0
			var stream strings.Builder
			err = bridge.run(context.Background(), rounds[0].response(), base, func([]byte) (*http.Response, error) {
				opened++
				if opened >= len(rounds) {
					t.Fatal("unexpected usage continuation")
				}
				return rounds[opened].response(), nil
			}, func(raw []byte) (*CodexAlphaSearchResponse, error) { searched++; return nativeSearchSuccess(raw) }, &stream)
			if err != nil {
				t.Fatal(err)
			}
			_, terminal := assertNativeSearchContract(t, stream.String(), rounds[0].id, rounds[0].createdAt)
			n := int64(1)
			if invoke {
				n = 3
			}
			usage := terminal.Get("response.usage")
			for path, multiplier := range map[string]int64{"input_tokens": 10, "output_tokens": 5, "total_tokens": 15, "input_tokens_details.cached_tokens": 1, "input_tokens_details.image_tokens": 2, "input_tokens_details.audio_tokens": 1, "input_tokens_details.cached_tokens_details.image_tokens": 1, "input_tokens_details.cached_tokens_details.audio_tokens": 1, "input_tokens_details.future.new_tokens": 1, "output_tokens_details.reasoning_tokens": 1, "output_tokens_details.image_tokens": 1, "output_tokens_details.audio_tokens": 1, "output_tokens_details.prediction_tokens_details.accepted_tokens": 1} {
				if usage.Get(path).Int() != n*multiplier {
					t.Errorf("usage %s = %s, want %d", path, usage.Get(path).Raw, n*multiplier)
				}
			}
			if opened != len(rounds)-1 || searched != len(rounds)-1 || usage.Get("fixture_unknown.0").String() != "retained" || usage.Get("fixture_unknown.1").Int() != 42 || usage.Get("fixture_version").Int() != 7 || usage.Get("first_round_only.mode").String() != "fixture" || usage.Get("input_tokens_details.cached_tokens_details.fixture_metadata").String() != "retained" || usage.Get("input_tokens_details.future.label").String() != "synthetic" {
				t.Fatal("usage metadata was dropped or numeric metadata was incorrectly accumulated")
			}
		})
	}
}

func TestNativeSearchResponsesPrivateReplayCompletion(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search", remaining: 2, responseID: "resp_logical"}
	first := &nativeSearchFixture{id: "resp_upstream_first", createdAt: 1700000000}
	first.lifecycle("response.created")
	reason := nativeSearchReasoning("A", "Synthetic thought")
	first.item("response.output_item.done", 0, reason)
	first.lifecycle("response.completed", codexSearchTestCall(bridge.name, "First synthetic query"))
	last := &nativeSearchFixture{id: "resp_upstream_last", createdAt: 1700000001}
	last.lifecycle("response.created")
	call := codexSearchTestCall(bridge.name, "Second synthetic query")
	call["call_id"] = "call_second"
	client := map[string]any{"type": "function_call", "id": "fc_client", "call_id": "call_client", "name": "Read", "arguments": `{"fixture":true}`}
	last.lifecycle("response.completed", call, client, nativeSearchMessage("M", "Answer"))
	var stream strings.Builder
	var replay []json.RawMessage
	completed, opened, searched := 0, 0, 0
	bridge.onCompleted = func(items []json.RawMessage) {
		completed++
		if strings.Contains(stream.String(), `"type":"response.completed"`) {
			t.Fatal("private replay was attached after publishing terminal")
		}
		replay = items
	}
	err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) {
		if completed != 0 {
			t.Fatal("completion callback ran before the last round")
		}
		opened++
		return last.response(), nil
	}, func(raw []byte) (*CodexAlphaSearchResponse, error) { searched++; return nativeSearchSuccess(raw) }, &stream)
	if err != nil {
		t.Fatal(err)
	}
	_, terminal := assertNativeSearchContract(t, stream.String(), bridge.responseID, first.createdAt)
	if completed != 1 || opened != 1 || searched != 2 || len(replay) != 7 {
		t.Fatalf("incomplete private replay: completed=%d opened=%d searched=%d items=%d", completed, opened, searched, len(replay))
	}
	calls, results := map[string]bool{}, map[string]bool{}
	for _, raw := range replay {
		item := gjson.ParseBytes(raw)
		switch item.Get("type").String() {
		case "function_call":
			calls[item.Get("call_id").String()] = true
		case "function_call_output":
			results[item.Get("call_id").String()] = true
		case "web_search_call":
			t.Fatal("public projection entered private replay")
		}
	}
	if len(calls) != 3 || len(results) != 2 || !calls["call_client"] || results["call_client"] || !results["call_search"] || !results["call_second"] || gjson.GetBytes(replay[0], "id").String() != "A" || terminal.Get("response.output.#").Int() != 5 || strings.Contains(stream.String(), bridge.name) {
		t.Fatal("private replay lost a real output, hidden result, or unresolved external call")
	}
}
