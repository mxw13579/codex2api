package proxy

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestNormalizeResponsesTerminalEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		want    string // Empty means byte-for-byte passthrough.
	}{
		{"done_without_response", `{"type":"response.done"}`, "response.completed"},
		{"done_completed", `{"type":"response.done","response":{"status":"completed"}}`, "response.completed"},
		{"done_incomplete", `{"type":"response.done","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`, "response.incomplete"},
		{"completed_incomplete", `{"type":"response.completed","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`, "response.incomplete"},
		{"top_incomplete", `{"type":"response.done","status":"incomplete"}`, "response.incomplete"},
		{"incomplete_passthrough", `{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"error":null}}`, ""},
		{"content_filter_passthrough", `{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"content_filter"}}}`, ""},
		{"status_details_truncation", `{"type":"response.done","response":{"status":"incomplete","status_details":{"type":"incomplete","reason":"max_output_tokens","error":null}}}`, "response.incomplete"},
		{"failed_passthrough", `{"type":"response.failed","response":{"status":"completed","error":null}}`, ""},
		{"error_event_passthrough", `{"type":"error","status":503,"error":{"message":"synthetic failure"}}`, ""},
		{"inner_status_precedes_top", `{"type":"response.done","status":"incomplete","response":{"status":"completed"}}`, "response.completed"},
		{"inner_incomplete_precedes_top", `{"type":"response.done","status":"completed","response":{"status":"incomplete"}}`, "response.incomplete"},
		{"explicit_completed_precedes_reason", `{"type":"response.done","response":{"status":"completed","incomplete_details":{"reason":"max_output_tokens"}}}`, "response.completed"},
		{"explicit_failed_precedes_reason", `{"type":"response.done","response":{"status":"failed","incomplete_details":{"reason":"max_output_tokens"}}}`, "response.failed"},
		{"incomplete_cannot_hide_error", `{"type":"response.done","response":{"status":"incomplete","error":{"code":"server_error"},"incomplete_details":{"reason":"max_output_tokens"}}}`, "response.failed"},
		{"output_is_not_failure_evidence", `{"type":"response.done","response":{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"overload failed cancelled error 503"}]},{"type":"function_call","arguments":"{\"error\":{\"code\":503}}"}]}}`, "response.completed"},
		{"unknown_done_status", `{"type":"response.done","response":{"status":"future_status"}}`, ""},
		{"nonterminal_done_status", `{"type":"response.done","response":{"status":"in_progress"}}`, ""},
		{"unknown_event", `{"type":"response.future","response":{"status":"failed","error":{"code":"server_error"}}}`, ""},
		{"created", `{"type":"response.created","response":{"status":"failed","error":{"code":"server_error"}}}`, ""},
		{"delta", `{"type":"response.output_text.delta","status":503,"error":{"message":"synthetic failure"}}`, ""},
		{"output_item_done", `{"type":"response.output_item.done","status":503}`, ""},
		{"missing_type", `{"response":{"status":"failed"}}`, ""},
		{"empty", "", ""},
		{"null", "null", ""},
		{"array", `[{"type":"response.done"}]`, ""},
		{"done_sentinel", "[DONE]", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertNormalizedResponsesTerminal(t, tc.payload, tc.want)
		})
	}
}

func TestNormalizeResponsesTerminalFailureEvidence(t *testing.T) {
	for _, eventType := range []string{"response.done", "response.completed", "response.incomplete"} {
		for _, tc := range []struct {
			name   string
			fields string
		}{
			{"response_failed", `"response":{"status":"failed"}`},
			{"response_error", `"response":{"status":"error"}`},
			{"response_cancelled", `"response":{"status":"cancelled"}`},
			{"response_canceled", `"response":{"status":"canceled"}`},
			{"status_case_and_space", `"response":{"status":" FAILED "}`},
			{"response_numeric_status", `"response":{"status":503}`},
			{"response_string_status_code", `"response":{"status_code":"429"}`},
			{"response_status_code", `"response":{"status":"completed","status_code":500}`},
			{"response_error_object", `"response":{"status":"completed","error":{"code":"server_error","type":"upstream_error","message":"synthetic failure","param":"input"}}`},
			{"response_error_message_only", `"response":{"error":{"message":"synthetic failure"}}`},
			{"response_error_type_only", `"response":{"error":{"type":"upstream_error"}}`},
			{"response_error_string", `"response":{"error":"synthetic failure"}`},
			{"status_details_error", `"response":{"status_details":{"error":{"code":"server_error","message":"synthetic failure"}}}`},
			{"top_status_failed", `"status":"failed","response":{"status":"completed"}`},
			{"top_status_error", `"status":"error"`},
			{"top_status_cancelled", `"status":"cancelled"`},
			{"top_status_numeric", `"status":503`},
			{"top_status_numeric_string", `"status":"503"`},
			{"top_status_code", `"status_code":429`},
			{"conflicting_http_status", `"status":200,"status_code":503`},
			{"top_error", `"error":{"code":"server_error","message":"synthetic failure"}`},
			{"null_inner_error_top_error", `"response":{"error":null},"error":{"message":"synthetic failure"}`},
			{"http_lower_boundary", `"status_code":400`},
			{"http_upper_boundary", `"status_code":599`},
		} {
			t.Run(eventType+"/"+tc.name, func(t *testing.T) {
				assertNormalizedResponsesTerminal(t, `{"type":"`+eventType+`",`+tc.fields+`}`, "response.failed")
			})
		}
	}
}

func TestNormalizeResponsesTerminalEmptyErrorAndNonFailureStatus(t *testing.T) {
	for _, field := range []string{"error", "response.error", "response.status_details.error"} {
		for _, value := range []string{`null`, `""`, `"  "`, `{}`, `[]`, `false`, `0`, `{"code":null,"type":"","message":"  "}`} {
			t.Run(field+"/"+value, func(t *testing.T) {
				fields := `"error":` + value
				switch field {
				case "response.error":
					fields = `"response":{"error":` + value + `}`
				case "response.status_details.error":
					fields = `"response":{"status_details":{"error":` + value + `}}`
				}
				assertNormalizedResponsesTerminal(t, `{"type":"response.done",`+fields+`}`, "response.completed")
			})
		}
	}
	for _, value := range []string{`200`, `399`, `600`, `0`, `-1`, `null`, `false`, `400.5`, `"503not-a-code"`} {
		t.Run("status_code/"+value, func(t *testing.T) {
			assertNormalizedResponsesTerminal(t, `{"type":"response.done","status_code":`+value+`}`, "response.completed")
		})
	}
}

func TestNormalizeResponsesTerminalInvalidJSON(t *testing.T) {
	for _, kind := range []string{"response.done", "response.completed", "response.incomplete"} {
		for _, payload := range []string{
			`{"type":"` + kind + `","response":{"status":"failed"}`,
			`{"type":"` + kind + `"} SENSITIVE-SYNTHETIC-TRAILER`,
		} {
			got := NormalizeResponsesTerminalEvent([]byte(payload))
			if !gjson.ValidBytes(got) || gjson.GetBytes(got, "type").String() != "response.failed" || gjson.GetBytes(got, "response.status_code").Int() != 502 {
				t.Fatalf("invalid terminal must fail closed: %s", got)
			}
			if strings.Contains(string(got), "SENSITIVE-SYNTHETIC-TRAILER") {
				t.Fatal("invalid upstream bytes leaked into protocol error")
			}
		}
	}
}

func TestNormalizeResponsesTerminalPreservesMetadata(t *testing.T) {
	assertNormalizedResponsesTerminal(t, "{\n"+
		`  "type":"response.done", "sequence_number":9007199254740993,`+"\n"+
		`  "status_code":503, "error":{"code":"server_error","message":"synthetic failure"},`+"\n"+
		`  "response":{"id":"resp_synthetic","status":"cancelled","created_at":1234567890,"status_details":{"error":{"message":"detail"}},"usage":{"input_tokens":21,"output_tokens":5,"total_tokens":26},"output":[{"text":"partial"}]},`+"\n"+
		`  "extension":{"large_integer":9007199254740997,"exact_decimal":1.2300,"unknown":[null,true]}`+"\n}", "response.failed")
}

func assertNormalizedResponsesTerminal(t *testing.T, payload, wantType string) {
	t.Helper()
	input := []byte(payload)
	got := NormalizeResponsesTerminalEvent(input)
	want := payload
	if wantType != "" {
		oldType := gjson.Get(payload, "type").Raw
		want = strings.Replace(payload, `"type":`+oldType, `"type":"`+wantType+`"`, 1)
	}
	if string(got) != want {
		t.Errorf("normalized payload = %s, want %s", got, want)
	}
	if string(input) != payload {
		t.Error("normalization mutated input bytes")
	}
	if again := NormalizeResponsesTerminalEvent(got); !bytes.Equal(got, again) {
		t.Error("normalization is not idempotent")
	}
}
