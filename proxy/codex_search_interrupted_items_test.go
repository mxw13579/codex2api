package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// All fixtures here are synthetic, in-memory bridge inputs/callbacks. No
// transport, account selection, standalone search endpoint or WS server is used.
func interruptedSearchClientCall(id string) map[string]any {
	return map[string]any{"id": id, "type": "function_call", "call_id": "call_" + id, "name": "FixtureRead", "arguments": ""}
}

func assertInterruptedSearchItems(t *testing.T, events []gjson.Result, dropped map[string]int64, retained ...string) gjson.Result {
	t.Helper()
	var terminal gjson.Result
	interrupts := map[string]int{}
	for _, event := range events {
		kind := event.Get("type").String()
		if isResponsesTerminalEvent(kind) {
			terminal = event
		}
		id := event.Get("item_id").String()
		if nested := event.Get("item.id").String(); nested != "" {
			id = nested
		}
		if index, ok := dropped[id]; ok {
			if event.Get("output_index").Int() != index {
				t.Fatal("interrupted item changed its published slot")
			}
			if strings.HasSuffix(kind, ".done") {
				t.Fatal("interrupted partial item was completed")
			}
			if kind == "response.output_item.interrupted" {
				interrupts[id]++
			} else if interrupts[id] != 0 {
				t.Fatal("an event revived an interrupted item")
			}
		}
	}
	for id := range dropped {
		if interrupts[id] != 1 {
			t.Fatal("published item did not receive exactly one projected interruption")
		}
	}
	items := terminal.Get("response.output").Array()
	if !terminal.Exists() || !terminal.Get("response.output").IsArray() || len(items) != len(retained) {
		t.Fatal("terminal retained discarded output or lost surviving items")
	}
	for i, id := range retained {
		if !items[i].IsObject() || items[i].Get("id").String() != id {
			t.Fatal("terminal output contains a null slot or reordered a surviving item")
		}
	}
	return terminal
}

func TestCodexSearchInterruptedItemsTerminalAndAck(t *testing.T) {
	for _, terminalKind := range []string{"response.incomplete", "response.done"} {
		for _, ackFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ackFirst=%t", terminalKind, ackFirst), func(t *testing.T) {
				ctx, parent, bridge, base := newSearchInterruptTestBridge(t)
				cleanup := RegisterResponsesWSInterrupt(ctx, func([]byte) error { return nil })
				defer cleanup()
				bridge.onCompleted = func([]json.RawMessage) { t.Fatal("cached an incomplete turn") }
				f := &nativeSearchFixture{id: "resp_native", createdAt: 1}
				f.lifecycle("response.created")
				kept := nativeSearchReasoning("kept", "Completed synthetic thought")
				f.item("response.output_item.done", 0, kept)
				f.item("response.output_item.added", 3, nativeSearchMessage("partial_message"))
				f.event("response.output_text.delta", map[string]any{"item_id": "partial_message", "content_index": 0, "delta": "Delivered synthetic text"})
				f.event("response.output_text.delta", map[string]any{"item_id": "partial_message", "content_index": 2, "delta": "deferred-synthetic-text"})
				client := interruptedSearchClientCall("partial_function")
				f.item("response.output_item.added", 7, client)
				f.event("response.function_call_arguments.delta", map[string]any{"output_index": 7, "delta": `{"fixture":`})
				hidden := codexSearchTestCall(bridge.name, "private-synthetic-query")
				f.item("response.output_item.added", 8, hidden)
				f.event("response.function_call_arguments.delta", map[string]any{"item_id": "unknown", "output_index": 9, "delta": "unclassified-synthetic-input"})
				f.event("response.output_item.interrupted", map[string]any{"item_id": "partial_message", "reason": "synthetic-interruption", "provider_metadata": json.RawMessage(`{"counter":9007199254740993}`)})
				f.event("response.output_item.interrupted", map[string]any{"output_index": 7})
				f.event("response.output_item.interrupted", map[string]any{"item": hidden})
				f.event("response.output_item.interrupted", map[string]any{"item": map[string]any{"id": "unknown", "arguments": "unclassified-synthetic-input"}})
				// Neither explicit late done nor richer terminal snapshots may revive
				// the partial items or flush the deferred message part.
				late := nativeSearchMessage("partial_message", "Delivered synthetic text late-synthetic-suffix")
				f.item("response.output_item.added", 3, late)
				f.item("response.output_item.done", 3, late)
				client["arguments"] = `{"fixture":true}`
				f.event("response.function_call_arguments.done", map[string]any{"output_index": 7, "arguments": client["arguments"]})
				f.item("response.output_item.done", 7, client)
				after := nativeSearchMessage("after", "Completed after the discarded slots")
				f.item("response.output_item.done", 10, after)
				ack := searchInterruptAck(f.id, true)
				if ackFirst {
					f.events = append(f.events, ack)
				}
				f.event(terminalKind, map[string]any{"response": map[string]any{
					"id": f.id, "status": "incomplete", "incomplete_details": map[string]any{"reason": "interrupted"},
					"output": []any{after, hidden, client, late, kept}, "usage": map[string]any{"input_tokens": 11, "output_tokens": 2},
				}})
				if !ackFirst {
					f.events = append(f.events, ack)
				}
				writer := &searchInterruptWriter{onEvent: func(data []byte) {
					if gjson.GetBytes(data, "type").String() == "response.created" {
						if err := sendSearchInterrupt(t, parent); err != nil {
							t.Fatal(err)
						}
					}
				}}
				err := bridge.run(ctx, f.response(), base, func([]byte) (*http.Response, error) {
					t.Fatal("opened another model round after interruption")
					return nil, nil
				}, func([]byte) (*CodexAlphaSearchResponse, error) {
					t.Fatal("executed an interrupted hidden call")
					return nil, nil
				}, writer)
				if err != nil {
					t.Fatal(err)
				}
				events := searchInterruptEvents(t, writer.String())
				for _, event := range events {
					if event.Get("type").String() == "response.output_item.interrupted" && event.Get("item_id").String() == "partial_message" {
						if event.Get("reason").String() != "synthetic-interruption" || event.Get("provider_metadata.counter").Raw != "9007199254740993" {
							t.Fatal("interruption metadata or JSON precision was lost")
						}
					}
				}
				terminal := assertInterruptedSearchItems(t, events, map[string]int64{"partial_message": 1, "partial_function": 2}, "kept", "after")
				if terminal.Get("type").String() != "response.incomplete" || terminal.Get("response.usage.input_tokens").Int() != 11 || terminal.Get("response.usage.output_tokens").Int() != 2 || terminal.Get("response.usage.total_tokens").Int() != 13 {
					t.Fatal("terminal alias or usage changed")
				}
				var controls []string
				interrupts, delivered := 0, false
				for _, event := range events {
					kind := event.Get("type").String()
					if isResponsesTerminalEvent(kind) || isResponsesWSInterruptAck(kind) {
						controls = append(controls, kind)
					}
					if kind == "response.output_item.interrupted" {
						interrupts++
					}
					if kind == "response.output_text.delta" && event.Get("delta").String() == "Delivered synthetic text" {
						delivered = true
					}
					if event.Get("item.id").String() == "after" && event.Get("output_index").Int() != 3 {
						t.Fatal("a discarded output slot was reused")
					}
				}
				want := "response.incomplete,response.interrupt.accepted"
				if ackFirst {
					want = "response.interrupt.accepted,response.incomplete"
				}
				if strings.Join(controls, ",") != want || interrupts != 2 || !delivered {
					t.Fatal("ack order, delivered text or private interruption visibility changed")
				}
				for _, private := range []string{bridge.name, "private-synthetic-query", "unclassified-synthetic-input", "deferred-synthetic-text", "late-synthetic-suffix"} {
					if strings.Contains(writer.String(), private) {
						t.Fatal("discarded/private payload escaped")
					}
				}
			})
		}
	}
}

func TestCodexSearchInterruptedItemsWhileTerminalAwaitsAck(t *testing.T) {
	ctx, parent, bridge, base := newSearchInterruptTestBridge(t)
	cleanup := RegisterResponsesWSInterrupt(ctx, func([]byte) error { return nil })
	defer cleanup()
	f := &nativeSearchFixture{id: "resp_native", createdAt: 1}
	f.lifecycle("response.created")
	f.item("response.output_item.added", 0, nativeSearchMessage("partial"))
	f.event("response.output_text.delta", map[string]any{"item_id": "partial", "delta": "Delivered"})
	f.lifecycle("response.incomplete", nativeSearchMessage("partial", "Delivered late-synthetic-suffix"))
	f.event("response.output_item.interrupted", map[string]any{"item": map[string]any{"id": "partial"}})
	f.events = append(f.events, searchInterruptAck(f.id, true))
	writer := &searchInterruptWriter{onEvent: func(data []byte) {
		if gjson.GetBytes(data, "type").String() == "response.created" {
			if err := sendSearchInterrupt(t, parent); err != nil {
				t.Fatal(err)
			}
		}
	}}
	if err := bridge.run(ctx, f.response(), base, nil, nil, writer); err != nil {
		t.Fatal(err)
	}
	assertInterruptedSearchItems(t, searchInterruptEvents(t, writer.String()), map[string]int64{"partial": 0})
	if strings.Contains(writer.String(), "late-synthetic-suffix") {
		t.Fatal("terminal snapshot overrode a discard received before the ack")
	}
}

func TestCodexSearchInterruptedItemsExcludedFromContinuation(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search", remaining: 2, responseID: searchInterruptLogicalID}
	first := &nativeSearchFixture{id: "resp_first", createdAt: 1}
	first.lifecycle("response.created")
	kept := nativeSearchReasoning("kept", "Completed synthetic thought")
	first.item("response.output_item.done", 0, kept)
	first.item("response.output_item.added", 1, nativeSearchMessage("discard_message"))
	first.event("response.output_text.delta", map[string]any{"item_id": "discard_message", "delta": "Delivered"})
	client := interruptedSearchClientCall("discard_client")
	first.item("response.output_item.added", 2, client)
	first.event("response.function_call_arguments.delta", map[string]any{"item_id": "discard_client", "delta": `{"fixture":`})
	discarded := codexSearchTestCall(bridge.name, "discard-synthetic-query")
	discarded["id"], discarded["call_id"] = "discard_search", "call_discard_search"
	first.item("response.output_item.added", 3, discarded)
	for _, id := range []string{"discard_message", "discard_client", "discard_search"} {
		first.event("response.output_item.interrupted", map[string]any{"item_id": id})
	}
	first.item("response.output_item.done", 3, discarded)
	live := codexSearchTestCall(bridge.name, "retained-synthetic-query")
	// The interrupted external call must not force the aggregate to return
	// early, nor may the interrupted hidden call execute or enter replay.
	first.lifecycle("response.completed", discarded, client, nativeSearchMessage("discard_message", "Delivered stale"), live)
	last := &nativeSearchFixture{id: "resp_last", createdAt: 2}
	last.lifecycle("response.created")
	last.item("response.output_item.done", 0, nativeSearchMessage("answer", "Synthetic final answer"))
	last.lifecycle("response.completed") // Ordinary sparse completion still applies.
	var replay []json.RawMessage
	bridge.onCompleted = func(items []json.RawMessage) { replay = items }
	searches, opened := 0, 0
	var stream strings.Builder
	err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func(next []byte) (*http.Response, error) {
		opened++
		if strings.Contains(string(next), "discard_") || strings.Contains(string(next), "discard-synthetic-query") || gjson.GetBytes(next, "input.#").Int() != 3 {
			t.Fatal("discarded item entered the continuation body")
		}
		return last.response(), nil
	}, func(raw []byte) (*CodexAlphaSearchResponse, error) {
		searches++
		if gjson.GetBytes(raw, "commands.search_query.0.q").String() != "retained-synthetic-query" {
			t.Fatal("executed the discarded hidden call")
		}
		return nativeSearchSuccess(raw)
	}, &stream)
	if err != nil {
		t.Fatal(err)
	}
	terminal := assertInterruptedSearchItems(t, searchInterruptEvents(t, stream.String()), map[string]int64{"discard_message": 1, "discard_client": 2}, "kept", "fc_search", "answer")
	cached, _ := json.Marshal(replay)
	if searches != 1 || opened != 1 || len(replay) != 4 || strings.Contains(string(cached), "discard_") || terminal.Get("response.usage.input_tokens").Int() != 20 || terminal.Get("response.usage.output_tokens").Int() != 10 || terminal.Get("response.usage.total_tokens").Int() != 30 {
		t.Fatal("discarded replay/search survived, or retained multi-round usage was lost")
	}
	for _, event := range searchInterruptEvents(t, stream.String()) {
		if event.Get("item.id").String() == "fc_search" && event.Get("output_index").Int() != 3 {
			t.Fatal("search reused an interrupted slot across rounds")
		}
	}
}

func TestCodexSearchInterruptedItemsHiddenAndUnclassified(t *testing.T) {
	for _, identity := range []string{"item_id", "output_index", "item", "call_id"} {
		for _, declared := range []bool{false, true} {
			for _, late := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/declared=%t/late=%t", identity, declared, late), func(t *testing.T) {
					bridge := &anthropicCodexWebSearch{name: "private_search", remaining: 1, responseID: searchInterruptLogicalID}
					f := &nativeSearchFixture{id: "resp_private", createdAt: 1}
					f.lifecycle("response.created")
					call := codexSearchTestCall(bridge.name, "private-synthetic-input")
					if declared {
						f.item("response.output_item.added", 4, call)
					}
					f.event("response.function_call_arguments.done", map[string]any{"item_id": "fc_search", "call_id": "call_search", "output_index": 4, "arguments": call["arguments"]})
					fields := map[string]any{"arguments": "private-synthetic-input"}
					switch identity {
					case "item_id":
						fields[identity] = "fc_search"
					case "call_id":
						fields[identity] = "call_search"
					case "output_index":
						fields[identity] = 4
					case "item":
						fields[identity] = call
					}
					f.event("response.output_item.interrupted", fields)
					if late {
						f.item("response.output_item.added", 4, call)
						f.event("response.function_call_arguments.delta", map[string]any{"output_index": 4, "delta": "private-synthetic-input"})
						f.item("response.output_item.done", 4, call)
						f.lifecycle("response.completed", call)
					} else {
						f.lifecycle("response.completed")
					}
					completed := 0
					bridge.onCompleted = func(items []json.RawMessage) {
						completed++
						if len(items) != 0 {
							t.Fatal("discarded hidden/unclassified item entered private replay")
						}
					}
					stream := runNativeSearchWithoutCalls(t, bridge, f)
					events := searchInterruptEvents(t, stream)
					assertInterruptedSearchItems(t, events, nil)
					if len(events) != 2 || completed != 1 || strings.Contains(stream, "private-synthetic-input") || strings.Contains(stream, bridge.name) {
						t.Fatal("hidden/unclassified interruption leaked or was resurrected")
					}
				})
			}
		}
	}
}

func newInterruptedSearchRound() (*anthropicSearchRound, *strings.Builder) {
	stream := &strings.Builder{}
	return &anthropicSearchRound{
		output: &anthropicSearchOutput{bridge: &anthropicCodexWebSearch{name: "private_search"}, writer: stream, id: searchInterruptLogicalID},
		byID:   map[string]*anthropicSearchItem{}, byCall: map[string]*anthropicSearchItem{}, byIndex: map[int64]*anthropicSearchItem{},
	}, stream
}

func sendInterruptedSearchEvent(t *testing.T, state *anthropicSearchRound, kind string, fields map[string]any) {
	t.Helper()
	fields["type"] = kind
	raw, _ := json.Marshal(fields)
	if !state.event(raw) {
		t.Fatal(state.output.err)
	}
}

func TestCodexSearchInterruptedItemsAliasMerge(t *testing.T) {
	for _, publishedByIndex := range []bool{false, true} {
		t.Run(fmt.Sprintf("publishedByIndex=%t", publishedByIndex), func(t *testing.T) {
			state, stream := newInterruptedSearchRound()
			item := nativeSearchMessage("partial")
			added := map[string]any{"item": item}
			deferred := map[string]any{"item_id": "partial", "content_index": 1, "delta": "deferred-synthetic-input"}
			interrupted := map[string]any{"output_index": 3, "arguments": "private-synthetic-input"}
			if publishedByIndex {
				delete(item, "id")
				added["output_index"] = 3
				delete(deferred, "item_id")
				deferred["output_index"] = 3
				delete(interrupted, "output_index")
				interrupted["item"] = map[string]any{"id": "partial", "arguments": "private-synthetic-input"}
			}
			sendInterruptedSearchEvent(t, state, "response.output_item.added", added)
			publishedID := state.items[0].id
			sendInterruptedSearchEvent(t, state, "response.output_text.delta", deferred)
			sendInterruptedSearchEvent(t, state, "response.output_item.interrupted", interrupted)
			if state.pendingBytes == 0 || state.pendingEvents == 0 {
				t.Fatal("unknown alias prematurely discarded an unrelated published buffer")
			}
			// Late declaration joins the unknown tombstone and the public item.
			sendInterruptedSearchEvent(t, state, "response.output_item.added", map[string]any{"output_index": 3, "item": nativeSearchMessage("partial", "stale-synthetic-input")})
			entry := state.byID["partial"]
			if state.byIndex[3] != entry || len(state.items) != 1 || !entry.interrupted || state.pendingBytes != 0 || state.pendingEvents != 0 || len(entry.deferred) != 0 || entry.item != nil || !state.finish(entry) {
				t.Fatal("alias merge lost the interruption or retained deferred budget")
			}
			sendInterruptedSearchEvent(t, state, "response.output_item.done", map[string]any{"output_index": 4, "item": nativeSearchMessage("after", "Retained")})
			if len(state.output.items) != 2 || state.output.items[0] != nil || gjson.GetBytes(state.output.items[1], "id").String() != "after" {
				t.Fatal("alias merge reused the interrupted slot")
			}
			interrupts := 0
			for _, event := range searchInterruptEvents(t, stream.String()) {
				if event.Get("type").String() == "response.output_item.interrupted" {
					interrupts++
					if event.Get("item_id").String() != publishedID || event.Get("output_index").Int() != 0 {
						t.Fatal("alias merge changed the published interruption identity")
					}
				}
			}
			if interrupts != 1 || strings.Contains(stream.String(), "synthetic-input") {
				t.Fatal("alias merge leaked deferred/private/stale inputs")
			}
		})
	}
}

func TestCodexSearchInterruptedItemsTerminalAlias(t *testing.T) {
	for _, publishedByCall := range []bool{false, true} {
		t.Run(fmt.Sprintf("publishedByCall=%t", publishedByCall), func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search", responseID: searchInterruptLogicalID}
			f := &nativeSearchFixture{id: "resp_alias", createdAt: 1}
			f.lifecycle("response.created")
			client := interruptedSearchClientCall("partial")
			interrupted := map[string]any{"call_id": "call_partial"}
			if publishedByCall {
				delete(client, "id")
				interrupted = map[string]any{"item_id": "partial"}
			} else {
				delete(client, "call_id")
			}
			f.item("response.output_item.added", 4, client)
			f.event("response.output_item.interrupted", interrupted)
			client = interruptedSearchClientCall("partial")
			client["arguments"] = `{"stale":true}`
			// Only this terminal snapshot proves that the ID and call_id aliases
			// belong to the same item. Its array position is not an index alias.
			f.lifecycle("response.completed", client, nativeSearchMessage("kept", "Retained"))
			bridge.onCompleted = func(items []json.RawMessage) {
				if len(items) != 1 || gjson.GetBytes(items[0], "id").String() != "kept" {
					t.Fatal("terminal alias joined too late to exclude private replay")
				}
			}
			stream := runNativeSearchWithoutCalls(t, bridge, f)
			events := searchInterruptEvents(t, stream)
			publishedID := events[1].Get("item.id").String()
			assertInterruptedSearchItems(t, events, map[string]int64{publishedID: 0}, "kept")
			if strings.Contains(stream, "stale") {
				t.Fatal("terminal alias resurrected a discarded snapshot")
			}
		})
	}
}

func TestCodexSearchInterruptedItemsReleaseBudgets(t *testing.T) {
	for _, queue := range []string{"pending", "deferred", "snapshots"} {
		t.Run(queue, func(t *testing.T) {
			state, stream := newInterruptedSearchRound()
			generations := 2
			if queue == "snapshots" {
				generations = anthropicSearchMaxPendingItems
			}
			for generation := 0; generation < generations; generation++ {
				id := fmt.Sprintf("item_%d", generation)
				switch queue {
				case "pending":
					for i := 0; i < anthropicSearchMaxPendingEvents; i++ {
						fields := map[string]any{"delta": "private-synthetic-input"}
						if i%2 == 0 {
							fields["item_id"] = id
						} else {
							fields["output_index"] = generation
						}
						sendInterruptedSearchEvent(t, state, "response.function_call_arguments.delta", fields)
					}
				case "deferred":
					sendInterruptedSearchEvent(t, state, "response.output_item.added", map[string]any{"item": nativeSearchMessage(id)})
					for i := 0; i < anthropicSearchMaxPendingEvents-1; i++ {
						sendInterruptedSearchEvent(t, state, "response.output_text.delta", map[string]any{"item_id": id, "content_index": 1, "delta": "private-synthetic-input"})
					}
				case "snapshots":
					sendInterruptedSearchEvent(t, state, "response.output_item.added", map[string]any{"item": map[string]any{"id": id, "type": "function_call", "arguments": strings.Repeat("x", 16<<10)}})
				}
				if queue != "snapshots" && state.pendingEvents != anthropicSearchMaxPendingEvents {
					t.Fatal("fixture did not fill the pending/deferred event budget")
				}
				sendInterruptedSearchEvent(t, state, "response.output_item.interrupted", map[string]any{"output_index": generation, "item": map[string]any{"id": id}})
				entry := state.byID[id]
				if state.pendingBytes != 0 || state.pendingEvents != 0 || entry.item != nil || len(entry.pending) != 0 || len(entry.deferred) != 0 || len(entry.parts) != 0 || !state.checkPending(0, 0) {
					t.Fatal("interruption retained pending bytes, events, snapshots or item budget")
				}
			}
			if strings.Contains(stream.String(), "private-synthetic-input") || queue != "deferred" && stream.Len() != 0 {
				t.Fatal("buffer release published a hidden/unclassified item")
			}
		})
	}
}

func TestCodexSearchInterruptedItemsSparseTerminalControl(t *testing.T) {
	for _, kind := range []string{"response.completed", "response.incomplete"} {
		t.Run(kind, func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search"}
			f := &nativeSearchFixture{id: "resp_sparse", createdAt: 1}
			f.lifecycle("response.created")
			f.item("response.output_item.added", 0, nativeSearchMessage("message"))
			f.event("response.output_text.delta", map[string]any{"item_id": "message", "delta": "Retained partial text"})
			f.item("response.output_item.added", 1, interruptedSearchClientCall("client"))
			f.event("response.function_call_arguments.delta", map[string]any{"item_id": "client", "delta": `{"fixture":true}`})
			f.lifecycle(kind)
			_, terminal := assertNativeSearchContract(t, runNativeSearchWithoutCalls(t, bridge, f), f.id, f.createdAt)
			if terminal.Get("response.output.0.content.0.text").String() != "Retained partial text" || terminal.Get("response.output.1.arguments").String() != `{"fixture":true}` {
				t.Fatal("sparse terminal omission was incorrectly inferred to be an interruption")
			}
		})
	}
}

func TestCodexSearchInterruptedItemsBoundUnknownTombstones(t *testing.T) {
	state, stream := newInterruptedSearchRound()
	for i := 0; i < anthropicSearchMaxPendingItems; i++ {
		sendInterruptedSearchEvent(t, state, "response.output_item.interrupted", map[string]any{"item_id": fmt.Sprintf("unknown_%d", i)})
	}
	if state.event([]byte(`{"type":"response.output_item.interrupted","item_id":"one_too_many"}`)) || state.output.err == nil || stream.Len() != 0 {
		t.Fatal("discarded unknown items grew unbounded identity maps or exposed a private event")
	}
}

func TestCodexSearchInterruptedItemsRejectAmbiguousIdentity(t *testing.T) {
	for _, fields := range []map[string]any{
		{"arguments": "private-synthetic-input"},
		{"item_id": "one", "item": map[string]any{"id": "two", "arguments": "private-synthetic-input"}},
		{"call_id": "one", "item": map[string]any{"call_id": "two", "arguments": "private-synthetic-input"}},
	} {
		state, stream := newInterruptedSearchRound()
		fields["type"] = "response.output_item.interrupted"
		raw, _ := json.Marshal(fields)
		if state.event(raw) || state.output.err == nil || stream.Len() != 0 || strings.Contains(state.output.err.Error(), "private-synthetic-input") {
			t.Fatal("ambiguous interruption guessed an identity or exposed upstream payload")
		}
	}
}
