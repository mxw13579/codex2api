package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

const searchInterruptLogicalID = "resp_c2a_search_synthetic"

func searchInterruptRequest(id string) []byte {
	return []byte(fmt.Sprintf(`{"type":"response.interrupt","response_id":%q,"mode":"discard_partial_items"}`, id))
}

func searchInterruptCreated(id string) string {
	return fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, id)
}

func searchInterruptAck(id string, accepted bool) string {
	kind := "response.interrupt.failed"
	if accepted {
		kind = "response.interrupt.accepted"
	}
	return fmt.Sprintf(`{"type":%q,"response_id":%q}`, kind, id)
}

func newSearchInterruptTestBridge(t *testing.T) (context.Context, *responsesWSInterruptState, *anthropicCodexWebSearch, []byte) {
	t.Helper()
	base, bridge, err := prepareAnthropicCodexWebSearch([]byte(`{"model":"fixture-model","input":[],"tools":[{"type":"web_search"}]}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	registry := newResponsesWSInterruptRegistry()
	parentCtx, parent := registry.begin(context.Background(), "")
	t.Cleanup(func() { registry.finish(parent) })
	ctx, cleanup := withCodexSearchInterrupt(parentCtx, bridge)
	t.Cleanup(cleanup)
	bridge.responseID = searchInterruptLogicalID
	bridge.interrupt.setLogicalID(bridge.responseID)
	if err := bridge.interrupt.beginRound(); err != nil {
		t.Fatal(err)
	}
	return ctx, parent, bridge, base
}

func sendSearchInterrupt(t *testing.T, parent *responsesWSInterruptState) error {
	t.Helper()
	parent.mu.Lock()
	defer parent.mu.Unlock()
	if parent.send == nil {
		t.Fatal("logical interrupt adapter not registered")
	}
	err := parent.send(searchInterruptRequest(searchInterruptLogicalID))
	if err == nil {
		parent.requested = true
	}
	return err
}

type searchInterruptWriter struct {
	bytes.Buffer
	onEvent func([]byte)
}

func (w *searchInterruptWriter) Write(data []byte) (int, error) {
	n, err := w.Buffer.Write(data)
	if err == nil && w.onEvent != nil {
		w.onEvent(bytes.TrimSpace(bytes.TrimPrefix(data, []byte("data: "))))
	}
	return n, err
}

func searchInterruptEvents(t *testing.T, stream string) []gjson.Result {
	t.Helper()
	var events []gjson.Result
	for _, frame := range strings.Split(strings.TrimSpace(stream), "\n\n") {
		payload := strings.TrimPrefix(frame, "data: ")
		if !gjson.Valid(payload) {
			t.Fatalf("invalid search event: %s", payload)
		}
		event := gjson.Parse(payload)
		if event.Get("sequence_number").Int() != int64(len(events)) {
			t.Fatal("public sequence number changed order")
		}
		for _, path := range []string{"response.id", "response_id"} {
			if value := event.Get(path); value.Exists() && value.String() != searchInterruptLogicalID {
				t.Fatalf("native ID escaped in %s", path)
			}
		}
		events = append(events, event)
	}
	return events
}

func TestCodexSearchInterruptNativeIDAndRoundFences(t *testing.T) {
	ctx, parent, bridge, _ := newSearchInterruptTestBridge(t)
	if err := sendSearchInterrupt(t, parent); !errors.Is(err, errResponsesWSInterruptNotSupported) {
		t.Fatalf("no transport: %v", err)
	}
	var sent []string
	register := func() func() {
		return RegisterResponsesWSInterrupt(ctx, func(payload []byte) error {
			sent = append(sent, gjson.GetBytes(payload, "response_id").String())
			if gjson.GetBytes(payload, "mode").String() != "discard_partial_items" {
				t.Error("interrupt mode changed")
			}
			return nil
		})
	}
	oldCleanup := register()
	if err := sendSearchInterrupt(t, parent); !errors.Is(err, errResponsesWSInterruptNotSupported) {
		t.Fatalf("unknown native ID: %v", err)
	}
	if err := bridge.interrupt.observe([]byte(searchInterruptCreated("resp_native_one"))); err != nil {
		t.Fatal(err)
	}
	if err := sendSearchInterrupt(t, parent); err != nil {
		t.Fatal(err)
	}
	if err := sendSearchInterrupt(t, parent); err != nil || len(sent) != 1 {
		t.Fatal("duplicate control write")
	}
	if err := bridge.interrupt.beginRound(); err == nil {
		t.Fatal("unresolved interrupt allowed a new model round")
	}
	if err := bridge.interrupt.observe([]byte(searchInterruptAck("resp_foreign", true))); err == nil {
		t.Fatal("accepted a foreign acknowledgement")
	}
	if err := bridge.interrupt.observe([]byte(searchInterruptAck("resp_native_one", false))); err != nil {
		t.Fatal(err)
	}
	if bridge.interrupt.acceptedInterrupt() || bridge.interrupt.awaitingAck() {
		t.Fatal("failed acknowledgement became a successful interruption")
	}
	if err := bridge.interrupt.observe([]byte(`{"type":"response.completed","response":{"id":"resp_native_one","status":"completed"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := sendSearchInterrupt(t, parent); !errors.Is(err, errResponsesWSInterruptNotSupported) {
		t.Fatalf("completed private round was mistaken for finished public turn: %v", err)
	}
	if err := bridge.interrupt.beginRound(); err != nil {
		t.Fatal(err)
	}
	cleanup := register()
	defer cleanup()
	oldCleanup() // Must not clear the second round's callback.
	if err := bridge.interrupt.observe([]byte(searchInterruptCreated("resp_native_two"))); err != nil {
		t.Fatal(err)
	}
	if err := sendSearchInterrupt(t, parent); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent[0] != "resp_native_one" || sent[1] != "resp_native_two" {
		t.Fatalf("wrong upstream IDs: %v", sent)
	}
	if err := bridge.interrupt.observe([]byte(searchInterruptAck("resp_native_two", true))); err != nil {
		t.Fatal(err)
	}
	if err := bridge.interrupt.beginRound(); err == nil {
		t.Fatal("accepted interruption allowed another model round")
	}
}

func TestCodexSearchInterruptRejectsUnsupportedAndStaleTargets(t *testing.T) {
	for _, sendErr := range []error{ErrResponsesWSInterruptFinished, errors.New("synthetic write failure")} {
		t.Run(sendErr.Error(), func(t *testing.T) {
			ctx, parent, bridge, _ := newSearchInterruptTestBridge(t)
			unregister := RegisterResponsesWSInterrupt(ctx, func([]byte) error { return sendErr })
			defer unregister()
			if err := bridge.interrupt.observe([]byte(searchInterruptCreated("resp_native"))); err != nil {
				t.Fatal(err)
			}
			err := sendSearchInterrupt(t, parent)
			want := sendErr
			if sendErr == ErrResponsesWSInterruptFinished {
				want = errResponsesWSInterruptNotSupported
			}
			if !errors.Is(err, want) || bridge.interrupt.awaitingAck() {
				t.Fatalf("send result=%v pending=%v", err, bridge.interrupt.awaitingAck())
			}
		})
	}
	registry := newResponsesWSInterruptRegistry()
	ctx, state := registry.begin(context.Background(), "")
	defer registry.finish(state)
	calls := 0
	cleanup := RegisterResponsesWSInterrupt(ctx, func([]byte) error { calls++; return nil })
	defer cleanup()
	state.mu.Lock()
	err := state.send(searchInterruptRequest(searchInterruptLogicalID))
	state.mu.Unlock()
	if !errors.Is(err, errResponsesWSInterruptNotSupported) || calls != 0 {
		t.Fatal("raw transport accepted a gateway-owned logical ID")
	}
}

func TestCodexSearchInterruptBridgeAckOrdering(t *testing.T) {
	for _, terminal := range []struct{ name, payload, event string }{
		{"incomplete", `{"type":"response.incomplete","response":{"id":"resp_native","status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[],"usage":{"input_tokens":11,"output_tokens":0}}}`, "response.incomplete"},
		{"done_incomplete", `{"type":"response.done","response":{"id":"resp_native","status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[]}}`, "response.incomplete"},
		{"completed", `{"type":"response.completed","response":{"id":"resp_native","status":"completed","output":[]}}`, "response.completed"},
		{"failed", `{"type":"response.failed","response":{"id":"resp_native","status":"failed","error":{"code":"server_error"},"output":[]}}`, "response.failed"},
	} {
		for _, accepted := range []bool{false, true} {
			for _, ackFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/accepted=%v/ackFirst=%v", terminal.name, accepted, ackFirst), func(t *testing.T) {
					ctx, parent, bridge, base := newSearchInterruptTestBridge(t)
					sends := 0
					cleanup := RegisterResponsesWSInterrupt(ctx, func(payload []byte) error {
						sends++
						if gjson.GetBytes(payload, "response_id").String() != "resp_native" {
							t.Error("logical ID sent upstream")
						}
						return nil
					})
					defer cleanup()
					ack := searchInterruptAck("resp_native", accepted)
					frames := []string{searchInterruptCreated("resp_native"), terminal.payload, terminal.payload, ack}
					if ackFirst {
						frames = []string{searchInterruptCreated("resp_native"), ack, terminal.payload}
					}
					writer := &searchInterruptWriter{onEvent: func(payload []byte) {
						if gjson.GetBytes(payload, "type").String() == "response.created" {
							if err := sendSearchInterrupt(t, parent); err != nil {
								t.Error(err)
							}
						}
					}}
					err := bridge.run(ctx, codexSearchTestResponse(frames...), base, func([]byte) (*http.Response, error) {
						t.Error("unexpected next round")
						return nil, errors.New("unexpected round")
					}, func([]byte) (*CodexAlphaSearchResponse, error) {
						t.Error("unexpected search")
						return nil, errors.New("unexpected search")
					}, writer)
					if err != nil || sends != 1 {
						t.Fatalf("run=%v sends=%d", err, sends)
					}
					events := searchInterruptEvents(t, writer.String())
					if len(events) != 3 {
						t.Fatalf("events=%d, want created, terminal and ack", len(events))
					}
					terminalIndex, ackIndex := 1, 2
					if ackFirst {
						terminalIndex, ackIndex = 2, 1
					}
					if events[terminalIndex].Get("type").String() != terminal.event || events[ackIndex].Get("type").String() != gjson.Get(ack, "type").String() {
						t.Error("ack/terminal order or terminal classification changed")
					}
					if events[terminalIndex].Get("response.usage.input_tokens").Int() != gjson.Get(terminal.payload, "response.usage.input_tokens").Int() {
						t.Error("terminal usage lost")
					}
				})
			}
		}
	}
}

func TestCodexSearchInterruptStopsUnfinishedSearchCalls(t *testing.T) {
	for _, outcome := range []string{"accepted_before", "accepted_after", "missing", "failed_after"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, parent, bridge, base := newSearchInterruptTestBridge(t)
			cleanup := RegisterResponsesWSInterrupt(ctx, func([]byte) error { return nil })
			defer cleanup()
			round := &nativeSearchFixture{id: "resp_native", createdAt: 1}
			round.lifecycle("response.created")
			round.lifecycle("response.completed", codexSearchTestCall(bridge.name, "synthetic query"))
			frames := append([]string(nil), round.events...)
			if outcome == "accepted_before" {
				frames = []string{frames[0], searchInterruptAck("resp_native", true), frames[1]}
			} else if outcome != "missing" {
				frames = append(frames, searchInterruptAck("resp_native", outcome == "accepted_after"))
			}
			writer := &searchInterruptWriter{onEvent: func(payload []byte) {
				if gjson.GetBytes(payload, "type").String() == "response.created" {
					if err := sendSearchInterrupt(t, parent); err != nil {
						t.Error(err)
					}
				}
			}}
			searches, rounds := 0, 0
			err := bridge.run(ctx, codexSearchTestResponse(frames...), base, func([]byte) (*http.Response, error) {
				rounds++
				if err := bridge.interrupt.beginRound(); err != nil {
					return nil, err
				}
				return codexSearchTestResponse(searchInterruptCreated("resp_next"), `{"type":"response.completed","response":{"id":"resp_next","status":"completed","output":[]}}`), nil
			}, func(body []byte) (*CodexAlphaSearchResponse, error) {
				searches++
				return nativeSearchSuccess(body)
			}, writer)
			if err != nil {
				t.Fatal(err)
			}
			events := searchInterruptEvents(t, writer.String())
			var terminal gjson.Result
			acks := 0
			for _, event := range events {
				if isResponsesTerminalEvent(event.Get("type").String()) {
					terminal = event
				}
				if isResponsesWSInterruptAck(event.Get("type").String()) {
					acks++
				}
			}
			want := "response.incomplete"
			if outcome == "missing" {
				want = "response.failed"
				if acks != 0 || terminal.Get("response.error.code").String() != "interrupt_acknowledgement_missing" {
					t.Error("missing ack was fabricated or reported as successful interruption")
				}
			} else if acks != 1 {
				t.Error("ack missing or duplicated")
			}
			if outcome == "failed_after" {
				want = "response.completed"
				if searches != 1 || rounds != 1 {
					t.Error("failed interruption incorrectly stopped the aggregate")
				}
			} else if searches != 0 || rounds != 0 {
				t.Fatal("accepted/unconfirmed interruption started more search work")
			}
			if terminal.Get("type").String() != want {
				t.Fatalf("terminal=%s, want=%s", terminal.Get("type").String(), want)
			}
		})
	}
}

func TestCodexSearchInterruptBridgeWriterFailure(t *testing.T) {
	ctx, parent, bridge, base := newSearchInterruptTestBridge(t)
	cleanup := RegisterResponsesWSInterrupt(ctx, func([]byte) error { return nil })
	defer cleanup()
	if err := bridge.interrupt.observe([]byte(searchInterruptCreated("resp_native"))); err != nil {
		t.Fatal(err)
	}
	if err := sendSearchInterrupt(t, parent); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("synthetic downstream failure")
	err := bridge.run(ctx, codexSearchTestResponse(searchInterruptCreated("resp_native")), base, nil, nil, searchInterruptFailWriter{failure})
	if !errors.Is(err, failure) {
		t.Fatalf("write failure changed: %v", err)
	}
}

type searchInterruptFailWriter struct{ err error }

func (w searchInterruptFailWriter) Write([]byte) (int, error) { return 0, w.err }

var _ io.Writer = (*searchInterruptWriter)(nil)
