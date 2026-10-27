package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

// These tests run the real downstream WS handler and hosted-search adapter.
// Model traffic is an in-process executor; standalone search is loopback only.
func TestCodexSearchInterruptNativeWSHandler(t *testing.T) {
	for _, secondRound := range []bool{false, true} {
		for _, ackFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("secondRound=%v/ackFirst=%v", secondRound, ackFirst), func(t *testing.T) {
				setupSearchContextLoopback(t)
				var calls atomic.Int32
				forwarded := make(chan []byte, 1)
				done := make(chan struct{})
				fixture := newNativeWSFixture(t, func(ctx context.Context, _ []byte) (*http.Response, error) {
					n := calls.Add(1)
					id := fmt.Sprintf("resp_private_%d", n)
					if secondRound && n == 1 {
						return codexSearchTestResponse(searchInterruptCreated(id), fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"status":"completed","output":[%s]}}`, id, searchContextCall("first"))), nil
					}
					pr, pw := io.Pipe()
					controls := make(chan []byte, 1)
					unregister := RegisterResponsesWSInterrupt(ctx, func(payload []byte) error {
						copy := append([]byte(nil), payload...)
						select {
						case controls <- copy:
							forwarded <- copy
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					})
					go func() {
						defer close(done)
						defer pw.Close()
						defer unregister()
						write := func(frame string) bool {
							_, err := fmt.Fprintf(pw, "data: %s\n\n", frame)
							return err == nil
						}
						for _, frame := range searchInterruptStartFrames(id) {
							if !write(frame) {
								return
							}
						}
						select {
						case <-controls:
						case <-ctx.Done():
							return
						case <-time.After(4 * time.Second):
							_ = pw.CloseWithError(fmt.Errorf("synthetic interrupt did not arrive"))
							return
						}
						terminal := fmt.Sprintf(`{"type":"response.incomplete","response":{"id":%q,"status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[],"usage":{"input_tokens":11,"output_tokens":2}}}`, id)
						frames := []string{terminal, searchInterruptAck(id, true)}
						if ackFirst {
							frames[0], frames[1] = frames[1], frames[0]
						}
						for _, frame := range frames {
							if !write(frame) {
								return
							}
						}
					}()
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: pr}, nil
				})
				conn := fixture.connect(t)
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				request := []byte(`{"type":"response.create","stream_id":"search-lane","model":"gpt-5.5","input":[{"role":"user","content":"synthetic request"}],"tools":[{"type":"web_search"}]}`)
				if err := conn.WriteMessage(websocket.TextMessage, request); err != nil {
					t.Fatal(err)
				}
				logicalID := ""
				for {
					_, frame, err := conn.ReadMessage()
					if err != nil {
						t.Fatal(err)
					}
					if id := gjson.GetBytes(frame, "response.id").String(); id != "" {
						logicalID = id
					}
					if gjson.GetBytes(frame, "type").String() == "response.output_text.delta" {
						break
					}
				}
				if !isCodexSearchResponseID(logicalID) {
					t.Fatalf("missing logical response ID: %s", logicalID)
				}
				if err := conn.WriteJSON(map[string]string{"type": "response.interrupt", "stream_id": "search-lane", "response_id": logicalID, "mode": "discard_partial_items"}); err != nil {
					t.Fatal(err)
				}
				select {
				case frame := <-forwarded:
					wantID := "resp_private_1"
					if secondRound {
						wantID = "resp_private_2"
					}
					if gjson.GetBytes(frame, "response_id").String() != wantID || gjson.GetBytes(frame, "stream_id").Exists() {
						t.Fatalf("control frame was not mapped to current native lease: %s", frame)
					}
				case <-time.After(4 * time.Second):
					t.Fatal("interrupt was not forwarded")
				}
				var observed []string
				for len(observed) < 2 {
					_, frame, err := conn.ReadMessage()
					if err != nil {
						t.Fatal(err)
					}
					typ := gjson.GetBytes(frame, "type").String()
					if !isResponsesTerminalEvent(typ) && !isResponsesWSInterruptAck(typ) {
						continue
					}
					if gjson.GetBytes(frame, "stream_id").String() != "search-lane" || strings.Contains(string(frame), "resp_private_") {
						t.Fatal("control/terminal routing or public identity changed")
					}
					observed = append(observed, typ)
				}
				want := []string{"response.incomplete", "response.interrupt.accepted"}
				if ackFirst {
					want[0], want[1] = want[1], want[0]
				}
				if strings.Join(observed, ",") != strings.Join(want, ",") {
					t.Fatalf("event order=%v, want=%v", observed, want)
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("interrupted upstream fixture did not finish")
				}
				wantCalls := int32(1)
				if secondRound {
					wantCalls++
				}
				if calls.Load() != wantCalls {
					t.Fatal("interruption replayed or opened another model request")
				}
			})
		}
	}
}

func TestCodexSearchInterruptDuringStandaloneSearchIsExplicitlyUnsupported(t *testing.T) {
	setupSearchContextLoopback(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	searchServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-resume:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, `{"output":[{"type":"message","content":[{"type":"output_text","text":"synthetic search result"}]}]}`)
	}))
	t.Cleanup(searchServer.Close)
	t.Cleanup(func() { close(resume) })
	codexAlphaSearchURLForTest = searchServer.URL
	var calls, controls atomic.Int32
	fixture := newNativeWSFixture(t, func(ctx context.Context, _ []byte) (*http.Response, error) {
		n := calls.Add(1)
		if n == 1 {
			unregister := RegisterResponsesWSInterrupt(ctx, func([]byte) error { controls.Add(1); return ErrResponsesWSInterruptFinished })
			resp := codexSearchTestResponse(searchInterruptCreated("resp_native_one"), fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_native_one","status":"completed","output":[%s]}}`, searchContextCall("first")))
			resp.Body = &searchInterruptCloseBody{ReadCloser: resp.Body, cleanup: unregister}
			return resp, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(searchContextSSE("resp_native_two", searchContextMessage("synthetic final answer"))))}, nil
	})
	conn := fixture.connect(t)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.5","input":[{"role":"user","content":"synthetic search"}],"tools":[{"type":"web_search"}]}`)); err != nil {
		t.Fatal(err)
	}
	logicalID := ""
	var nextSequence int64
	read := func() []byte {
		t.Helper()
		_, frame, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if sequence := gjson.GetBytes(frame, "sequence_number"); !sequence.Exists() || sequence.Int() != nextSequence {
			t.Fatalf("sequence=%s, want=%d (%s)", sequence.Raw, nextSequence, frame)
		}
		nextSequence++
		return frame
	}
	for {
		frame := read()
		if id := gjson.GetBytes(frame, "response.id").String(); id != "" {
			logicalID = id
		}
		if gjson.GetBytes(frame, "item.type").String() == "web_search_call" {
			break
		}
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("loopback standalone search did not start")
	}
	if !isCodexSearchResponseID(logicalID) {
		t.Fatal("missing public ID")
	}
	for range 2 {
		if err := conn.WriteMessage(websocket.TextMessage, searchInterruptRequest(logicalID)); err != nil {
			t.Fatal(err)
		}
		ack := read()
		if gjson.GetBytes(ack, "type").String() != "response.interrupt.failed" || gjson.GetBytes(ack, "error.code").String() != "interrupt_not_supported" {
			t.Fatalf("unsupported phase fabricated successful interruption: %s", ack)
		}
	}
	if controls.Load() != 0 || calls.Load() != 1 {
		t.Fatal("interrupt reached an old model lease or started another request")
	}
	// A rejected control request does not cancel HTTP search. Resume the turn
	// and require its projected sequence to follow both locally generated acks.
	resume <- struct{}{}
	for {
		frame := read()
		if isResponsesTerminalEvent(gjson.GetBytes(frame, "type").String()) {
			if gjson.GetBytes(frame, "type").String() != "response.completed" || calls.Load() != 2 {
				t.Fatalf("search did not resume after unsupported interruption: %s", frame)
			}
			break
		}
	}
}

func searchInterruptStartFrames(id string) []string {
	return []string{
		searchInterruptCreated(id),
		`{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_synthetic","type":"message","role":"assistant","status":"in_progress","content":[]}}`,
		`{"type":"response.output_text.delta","item_id":"msg_synthetic","output_index":0,"content_index":0,"delta":"synthetic text"}`,
	}
}

func TestCodexSearchInterruptCancellationWhileAwaitingAck(t *testing.T) {
	setupSearchContextLoopback(t)
	pending, done := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	fixture := newNativeWSFixture(t, func(ctx context.Context, _ []byte) (*http.Response, error) {
		calls.Add(1)
		pr, pw := io.Pipe()
		controls := make(chan struct{}, 1)
		unregister := RegisterResponsesWSInterrupt(ctx, func([]byte) error { controls <- struct{}{}; return nil })
		go func() {
			defer close(done)
			defer pw.Close()
			defer unregister()
			for _, frame := range searchInterruptStartFrames("resp_native_cancel") {
				if _, err := fmt.Fprintf(pw, "data: %s\n\n", frame); err != nil {
					return
				}
			}
			select {
			case <-controls:
			case <-ctx.Done():
				return
			}
			_, _ = io.WriteString(pw, "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_native_cancel\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"interrupted\"},\"output\":[]}}\n\n")
			close(pending)
			<-ctx.Done() // Withhold the ack; downstream cancellation must release us.
		}()
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: pr}, nil
	})
	conn := fixture.connect(t)
	_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.5","input":[{"role":"user","content":"synthetic request"}],"tools":[{"type":"web_search"}]}`)); err != nil {
		t.Fatal(err)
	}
	logicalID := ""
	for {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if id := gjson.GetBytes(frame, "response.id").String(); id != "" {
			logicalID = id
		}
		if gjson.GetBytes(frame, "type").String() == "response.output_text.delta" {
			break
		}
	}
	if err := conn.WriteMessage(websocket.TextMessage, searchInterruptRequest(logicalID)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pending:
	case <-time.After(2 * time.Second):
		t.Fatal("fixture did not reach terminal-first ack wait")
	}
	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(upstreamDrainTimeout + 2*time.Second):
		// Disconnect retains the pre-existing bounded usage-drain grace. The
		// bridge must not keep waiting for an ack after that grace expires.
		t.Fatal("downstream cancellation left the upstream search round alive past the drain grace")
	}
	deadline := time.Now().Add(2 * time.Second)
	for fixture.account.GetActiveRequests() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if fixture.account.GetActiveRequests() != 0 || calls.Load() != 1 {
		t.Fatal("cancelled search retained account capacity or replayed a model request")
	}
}

type searchInterruptCloseBody struct {
	io.ReadCloser
	cleanup func()
}

func (b *searchInterruptCloseBody) Close() error {
	b.cleanup()
	return b.ReadCloser.Close()
}
