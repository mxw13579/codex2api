package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

func TestOpenAIResponsesWSStreamTerminalFailure(t *testing.T) {
	for _, tc := range []struct {
		name        string
		terminal    string
		wantType    string
		created     bool
		wantBinding bool
	}{
		{"done_failed", `{"type":"response.done","response":{"id":"resp_terminal","status":"failed","error":{"code":"server_error","message":"synthetic failure"},"usage":{"input_tokens":11,"output_tokens":2}},"sequence_number":9007199254740993}`, "response.failed", false, false},
		{"completed_failed", `{"type":"response.completed","response":{"id":"resp_terminal","status":"failed","error":{"code":"server_error"}},"sequence_number":3}`, "response.failed", false, false},
		{"completed_structured_error", `{"type":"response.completed","response":{"id":"resp_terminal","status":"completed","status_details":{"error":{"message":"synthetic failure"}}}}`, "response.failed", false, false},
		{"completed_http_error", `{"type":"response.completed","status":503,"response":{"id":"resp_terminal"}}`, "response.failed", false, false},
		{"explicit_failed", `{"type":"response.failed","response":{"id":"resp_terminal","status":"failed"}}`, "response.failed", false, false},
		{"created_binding_unchanged", `{"type":"response.completed","response":{"id":"resp_terminal","status":"failed"}}`, "response.failed", true, true},
		{"done_success", `{"type":"response.done","response":{"id":"resp_terminal","status":"completed","error":null}}`, "response.completed", false, true},
		{"done_incomplete", `{"type":"response.done","response":{"id":"resp_terminal","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`, "response.incomplete", false, true},
		{"incomplete", `{"type":"response.incomplete","response":{"id":"resp_terminal","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`, "response.incomplete", false, true},
		{"output_is_not_error", `{"type":"response.completed","response":{"id":"resp_terminal","status":"completed","output":[{"text":"overload error failed 503"}]}}`, "response.completed", false, true},
		{"error_event", `{"type":"error","response":{"id":"resp_terminal"},"error":{"message":"synthetic failure"},"status_code":503}`, "error", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const delta = `{"type":"response.output_text.delta","delta":"overload is ordinary text","status":503}`
			const created = `{"type":"response.created","response":{"id":"resp_terminal","status":"in_progress"}}`
			upgrader := websocket.Upgrader{}
			serverDone := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(serverDone)
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Errorf("loopback upgrade: %v", err)
					return
				}
				defer conn.Close()
				frames := []string{delta, tc.terminal}
				if tc.created {
					frames = append([]string{created}, frames...)
				}
				for _, frame := range frames {
					if err := conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
						t.Errorf("loopback write: %v", err)
						return
					}
				}
				// Keep the socket open: the terminal event, not a peer close, must
				// finish the SSE body and return the consumed connection to the pool.
				_, _, _ = conn.ReadMessage()
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			dialer := websocket.Dialer{} // Explicitly no environment proxy; loopback only.
			socket, _, err := dialer.DialContext(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatalf("loopback dial: %v", err)
			}
			defer func() {
				_ = socket.Close()
				<-serverDone
			}()
			pool := newOpenAIResponsesWSPool()
			conn := &openAIResponsesWSConn{
				spec: openAIResponsesWSSpec{accountID: 7}, conn: socket,
				created: time.Now(), busy: true, response: map[string]struct{}{},
			}
			pool.conns = append(pool.conns, conn)
			resp := openAIResponsesWSStream(ctx, pool, conn)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("reading a consumed terminal should not fail: %v", err)
			}
			if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
				t.Error("WS-to-SSE envelope changed")
			}
			events := strings.Split(strings.TrimSuffix(string(body), "\n\n"), "\n\n")
			wantCount := 2
			if tc.created {
				wantCount++
			}
			if len(events) != wantCount {
				t.Fatalf("SSE events = %d, want %d", len(events), wantCount)
			}
			if tc.created && events[0] != "data: "+created {
				t.Error("created event changed")
			}
			if events[len(events)-2] != "data: "+delta {
				t.Error("ordinary event changed")
			}
			terminal := []byte(strings.TrimPrefix(events[len(events)-1], "data: "))
			if got := gjson.GetBytes(terminal, "type").String(); got != tc.wantType {
				t.Errorf("terminal type = %q, want %q", got, tc.wantType)
			}
			for _, path := range []string{"response", "sequence_number", "error", "status", "status_code"} {
				if gjson.GetBytes(terminal, path).Raw != gjson.Get(tc.terminal, path).Raw {
					t.Errorf("metadata changed at %s", path)
				}
			}
			pool.mu.Lock()
			bound := pool.bindings[openAIResponsesWSBindingKey(7, "resp_terminal")] == conn
			busy, dead := conn.busy, conn.dead
			pool.mu.Unlock()
			if bound != tc.wantBinding {
				t.Errorf("terminal binding = %v, want %v", bound, tc.wantBinding)
			}
			if busy || dead {
				t.Errorf("consumed terminal did not release connection: busy=%v dead=%v", busy, dead)
			}
		})
	}
}
