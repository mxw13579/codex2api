package wsrelay

import (
	"bytes"
	"io"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCompletionEventTerminalFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		want    string
	}{
		{"done_failed", `{"type":"response.done","response":{"status":"failed","error":{"code":"server_error","message":"synthetic failure"}}}`, "response.failed"},
		{"completed_error", `{"type":"response.completed","response":{"status":"error"}}`, "response.failed"},
		{"done_cancelled", `{"type":"response.done","response":{"status":"cancelled"}}`, "response.failed"},
		{"completed_status_details_error", `{"type":"response.completed","response":{"status_details":{"error":{"code":"server_error"}}}}`, "response.failed"},
		{"completed_top_status", `{"type":"response.completed","status":503}`, "response.failed"},
		{"done_success", `{"type":"response.done","response":{"status":"completed","error":null}}`, "response.completed"},
		{"done_incomplete", `{"type":"response.done","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`, "response.incomplete"},
		{"output_not_evidence", `{"type":"response.done","response":{"status":"completed","output":[{"text":"overload failed error 503"}]}}`, "response.completed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(tc.payload)
			got := normalizeCompletionEvent(payload)
			if typ := gjson.GetBytes(got, "type").String(); typ != tc.want {
				t.Errorf("normalized type = %q, want %q", typ, tc.want)
			}
			if !bytes.Equal(normalizeCompletionEvent(got), got) {
				t.Error("normalization is not idempotent")
			}
			if string(payload) != tc.payload {
				t.Error("normalization mutated the input")
			}
		})
	}
}

func TestHandleMessageTerminalFailureDoesNotBindCompletedResponse(t *testing.T) {
	for _, eventType := range []string{"response.done", "response.completed", "response.incomplete"} {
		for _, accept := range []bool{true, false} {
			name := eventType + "/accepted"
			if !accept {
				name = eventType + "/callback_stopped"
			}
			t.Run(name, func(t *testing.T) {
				// No sockets or background pool workers: exercise only terminal handling
				// and a live in-memory binding, including preservation of the prior ID.
				manager := &Manager{}
				session := NewSession(7, manager)
				session.ID = "synthetic-terminal-session"
				conn := NewWsConnection(nil, session, "ws://127.0.0.1/unused")
				conn.PoolKey = "synthetic-terminal-pool"
				manager.connections.Store(conn.PoolKey, conn)
				r := &WsResponse{conn: conn, manager: manager, sessionID: session.ID, apiKey: "synthetic-key"}
				r.bindCompletedResponse("resp_prior")
				if got, _ := manager.lookupResponseConn("resp_prior", 7, r.apiKey); got != conn {
					t.Fatal("fixture did not establish the prior successful binding")
				}

				payload := []byte(`{"type":"` + eventType + `","sequence_number":9007199254740993,"response":{"id":"resp_failed","status":"failed","status_code":503,"error":{"code":"server_error","message":"synthetic failure"},"usage":{"input_tokens":11,"output_tokens":2}}}`)
				calls := 0
				err := r.handleMessage(payload, func(data []byte) bool {
					calls++
					if got := gjson.GetBytes(data, "type").String(); got != "response.failed" {
						t.Errorf("callback type = %q, want response.failed", got)
					}
					for _, path := range []string{"response", "sequence_number"} {
						if gjson.GetBytes(data, path).Raw != gjson.GetBytes(payload, path).Raw {
							t.Errorf("metadata changed at %s", path)
						}
					}
					if !r.streamCompleted {
						t.Error("terminal boundary must be recorded before callback")
					}
					if got, _ := manager.lookupResponseConn("resp_failed", 7, r.apiKey); got != nil {
						t.Error("failed response acquired a completed-response binding")
					}
					return accept
				})
				if err != io.EOF || calls != 1 {
					t.Fatalf("handleMessage = %v, callback calls = %d; want EOF and one call", err, calls)
				}
				if r.connBroken {
					t.Error("an explicit response failure is still a consumed terminal boundary")
				}
				if got, _ := manager.lookupResponseConn("resp_prior", 7, r.apiKey); got != conn {
					t.Error("failure displaced the prior successful response binding")
				}
			})
		}
	}
}
