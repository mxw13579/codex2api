package proxy

// Review P2: inspect ORIGINAL response.status before granting private-failure
// replay, even when legacy normalization changes done/completed into failed.
// Fixtures use only synthetic JSON. Handler cases reuse real certificate-checked
// loopback TLS + explicit CONNECT, and release attempt A only after the client
// has read the genuine rate frame. No upstream executor/transport is replaced.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

type spReviewFixStatusCase struct {
	name       string
	event      string
	statusJSON string // Empty means ABSENT, not an empty JSON string.
	private    bool
}

func spReviewFixStatusCases() []spReviewFixStatusCase {
	var cases []spReviewFixStatusCase
	for _, event := range []string{"response.failed", "response.done", "response.completed"} {
		for _, invalid := range []struct{ name, raw string }{
			{"object", `{"synthetic_control":"future_execution"}`},
			{"empty_object", `{}`},
			{"array", `["failed"]`},
			{"empty_array", `[]`},
			{"unknown_string", `"synthetic_future_status"`},
			{"boolean", `true`},
		} {
			cases = append(cases, spReviewFixStatusCase{
				name: event + "/" + invalid.name, event: event, statusJSON: invalid.raw,
			})
		}
		// Explicit compatibility policy: absent/null status is not unknown
		// control data. It remains private ONLY with this known terminal event,
		// understood structured error, no output/tools, and zero output usage.
		// A blanket requirement status == "failed" would break these controls.
		for _, valid := range []struct{ name, raw string }{
			{"failed", `"failed"`}, {"absent", ""}, {"null", `null`},
		} {
			cases = append(cases, spReviewFixStatusCase{
				name: event + "/" + valid.name, event: event, statusJSON: valid.raw, private: true,
			})
		}
	}
	// Preserve known original-status / normalized-failure combinations, not
	// just the rewritten type. The structured error below supplies failure
	// evidence even when the original lifecycle status says completed.
	for _, valid := range []struct{ name, event, raw string }{
		{"done_completed_with_error", "response.done", `"completed"`},
		{"completed_with_error", "response.completed", `"completed"`},
		{"done_incomplete_with_error", "response.done", `"incomplete"`},
		{"incomplete_with_error", "response.incomplete", `"incomplete"`},
		{"done_error_status", "response.done", `"error"`},
		{"done_cancelled_status", "response.done", `"cancelled"`},
		{"done_canceled_status", "response.done", `"canceled"`},
		{"done_http_failure_number", "response.done", `503`},
		{"done_http_failure_string", "response.done", `"503"`},
	} {
		cases = append(cases, spReviewFixStatusCase{
			name: valid.name, event: valid.event, statusJSON: valid.raw, private: true,
		})
	}
	// Phase 3: preserve the ORIGINAL numeric lexeme. Float64 rounds these
	// fractions to integral failure codes, including a value BELOW 400. The
	// replay whitelist accepts canonical integer literals only; it need not
	// inherit the legacy normalizer's decimal/exponent coercions. Every case
	// retains error.status_code=503: this tests replay eligibility, not a change
	// to the shared structured-error precedence used for the delivered status.
	for _, event := range []string{"response.failed", "response.done", "response.completed", "response.incomplete"} {
		for _, number := range []struct {
			name, raw string
			private   bool
		}{
			{"fraction_above_503_rounds_to_503", `503.000000000000000000001`, false},
			{"fraction_below_503_rounds_to_503", `502.999999999999999999999`, false},
			{"below_range_rounds_to_400", `399.99999999999999999`, false},
			{"fraction_above_599_rounds_to_599", `599.000000000000000000001`, false},
			{"fraction_below_600_rounds_to_600", `599.99999999999999999`, false},
			{"decimal_spelling", `503.0`, false},
			{"exponent_spelling", `5.03e2`, false},
			{"integer_below_range", `399`, false},
			{"integer_above_range", `600`, false},
			{"integer_lower_bound", `400`, true},
			{"integer_server_failure", `503`, true},
			{"integer_upper_bound", `599`, true},
		} {
			cases = append(cases, spReviewFixStatusCase{
				name:  "numeric_original_" + event + "/" + number.name,
				event: event, statusJSON: number.raw, private: number.private,
			})
		}
	}
	return cases
}

func spReviewFixStatusPayload(tc spReviewFixStatusCase) string {
	status := ""
	if tc.statusJSON != "" {
		status = `,"status":` + tc.statusJSON
	}
	return `{"type":"` + tc.event + `","response":{"id":"resp_synthetic_status_A"` + status +
		`,"output":[],"error":{"type":"server_error","code":"server_is_overloaded","status_code":503,"message":"synthetic-status-private-overload"},"usage":{"input_tokens":13,"output_tokens":0,"total_tokens":13,"input_tokens_details":{"cached_tokens":5},"output_tokens_details":{"reasoning_tokens":0}}}}`
}

func TestSafePreflightReviewFixPrivateStatusHelper(t *testing.T) {
	for _, tc := range spReviewFixStatusCases() {
		t.Run(tc.name, func(t *testing.T) {
			raw := spReviewFixStatusPayload(tc)
			// Use the real strict envelope parser, not a hand-certified frame.
			frames, unsafe, err := collectSafePreflightFrames(strings.NewReader(spAcceptanceFrame(raw)))
			if err != nil || unsafe != 0 || len(frames) != 1 {
				t.Fatal("synthetic status fixture did not form one valid SSE envelope")
			}
			frame := frames[0]
			original := append([]byte(nil), frame.data...)
			m, ok := safePreflightJSON(frame.data)
			if !ok {
				t.Fatal("synthetic status fixture is not duplicate-free JSON")
			}
			event, normalized := normalizeUpstreamResponsesTerminalEvent(frame.event, frame.data)
			// Expectations are explicit, not derived from the candidate classifier.
			// Shared normalization must preserve the raw status and all other
			// response fields, whether the private classifier accepts them or not.
			wantNormalized := strings.Replace(raw, `"type":"`+tc.event+`"`, `"type":"response.failed"`, 1)
			if string(normalized) != wantNormalized || !bytes.Equal(frame.data, original) {
				t.Fatal("legacy failure normalization changed raw status/evidence or input bytes")
			}
			if got := safePreflightPrivateFrame(m, "response.failed"); got != tc.private {
				t.Errorf("raw status private classification=%v, want %v", got, tc.private)
			}
			state := &safePreflightState{started: time.Now()}
			state.begin()
			if !state.canStart(context.Background()) {
				t.Fatal("fresh helper control has no retry budget")
			}
			state.observe(frame, event, normalized)
			if state.retryUnsafe() != !tc.private || state.canStart(context.Background()) != tc.private {
				t.Error("raw status observation did not enforce the expected replay boundary")
			}
			if !tc.private {
				// A later understood failure must never reopen a window already
				// closed by unknown ORIGINAL status control semantics.
				valid := spReviewFixStatusPayload(spReviewFixStatusCase{event: "response.failed", statusJSON: `"failed"`})
				known, _, parseErr := collectSafePreflightFrames(strings.NewReader(spAcceptanceFrame(valid)))
				if parseErr != nil || len(known) != 1 {
					t.Fatal("known private-failure control is not a complete envelope")
				}
				state.observe(known[0], "", known[0].data)
				if !state.retryUnsafe() || state.canStart(context.Background()) {
					t.Error("later known failure reopened replay after unknown status")
				}
			}
		})
	}
}

// Independent downstream SSE check. Unlike a string-typed status decoder, this
// accepts the ORIGINAL status as opaque JSON so malformed control semantics
// cannot turn the assertion into an unrelated JSON decoding/setup failure.
// The contract here is one failed terminal and no second attempt, not a demand
// that the error renderer erase or rewrite the original status field.
func spReviewFixStatusTerminal(t *testing.T, wire, wantType, wantID string) {
	t.Helper()
	wire = strings.ReplaceAll(wire, "\r\n", "\n")
	if !strings.HasSuffix(wire, "\n\n") {
		t.Error("status fixture ended without a complete downstream SSE envelope")
	}
	terminals := 0
	for _, envelope := range strings.Split(strings.TrimSuffix(wire, "\n\n"), "\n\n") {
		if envelope == "" {
			continue
		}
		if terminals != 0 {
			t.Error("downstream event followed the selected status-fixture terminal")
		}
		var data []string
		var event string
		for _, line := range strings.Split(envelope, "\n") {
			switch {
			case strings.HasPrefix(line, ":"):
			case strings.HasPrefix(line, "event:"):
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			default:
				t.Error("non-SSE data appeared in the downstream status fixture")
			}
		}
		if len(data) == 0 {
			continue
		}
		var payload struct {
			Type     string `json:"type"`
			Response struct {
				ID     string          `json:"id"`
				Status json.RawMessage `json:"status"`
			} `json:"response"`
		}
		if json.Unmarshal([]byte(strings.Join(data, "\n")), &payload) != nil || payload.Type == "" {
			t.Error("downstream status fixture contained invalid event JSON")
			continue
		}
		if event != "" && event != payload.Type {
			t.Error("downstream SSE event and payload type disagree")
		}
		switch payload.Type {
		case "response.failed", "response.completed", "response.incomplete", "response.done", "error":
			terminals++
			if payload.Type != wantType || wantID != "" && payload.Response.ID != wantID {
				t.Error("wrong semantic terminal or response identity in status fixture")
			}
		}
	}
	if terminals != 1 {
		t.Errorf("status fixture terminal count=%d, want exactly one", terminals)
	}
}

func TestSafePreflightReviewFixPrivateStatusHandler(t *testing.T) {
	for _, tc := range spReviewFixStatusCases() {
		t.Run(tc.name, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			db := spTerminalDeliveryDB(t)
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 1, db, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					spAcceptanceFlush(w, spAcceptanceRate)
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spReviewFixStatusPayload(tc)))
					}
					return
				}
				// If a malformed raw status is wrongly made private, this real
				// second TLS request makes the forbidden replay plainly visible.
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_status_B"))
			})
			t.Cleanup(gate.open)
			resp, reader, _ := h.request(t)
			first := spAcceptanceReadEnvelope(t, reader)
			if first != spAcceptanceRate || h.calls.Load() != 1 {
				t.Fatal("status decision did not follow the genuine downstream rate read barrier")
			}
			state := h.provePath(t, 1)
			if state.retryUnsafe() || state.committed() || !state.canStart(context.Background()) {
				t.Fatal("rate-only control already closed replay before the tested raw status")
			}
			gate.open()
			wire := first + spAcceptanceReadRest(t, reader)
			result := h.wait(t)
			wantCalls, wantStatus := 1, http.StatusServiceUnavailable
			wantType, wantID := "response.failed", ""
			wants := []spReviewFixUsageWant{{status: http.StatusServiceUnavailable, input: 13, output: 0, total: 13, cached: 5, kind: "server", retry: tc.private}}
			if tc.private {
				wantCalls, wantStatus = 2, http.StatusOK
				wantType, wantID = "response.completed", "resp_synthetic_status_B"
				wants = append(wants, spReviewFixUsageWant{status: http.StatusOK, input: 1, output: 1, total: 2})
			}
			h.provePath(t, wantCalls)
			state.mu.Lock()
			attempts := state.attempts
			state.mu.Unlock()
			if attempts != wantCalls {
				t.Errorf("status fixture began %d attempts, want %d", attempts, wantCalls)
			}
			logical := result.logicalStatus
			if logical == 0 {
				logical = result.httpStatus
			}
			if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || logical != wantStatus {
				t.Errorf("status fixture logical status=%d, want %d while preserving published HTTP 200", logical, wantStatus)
			}
			if strings.Count(wire, "event: codex.rate_limits") != 1 {
				t.Error("genuine rate envelope was lost, duplicated, or synthesized")
			}
			spReviewFixStatusTerminal(t, wire, wantType, wantID)
			spReviewFixUsage(t, db, h, wants...)
			if len(getResponseCache(responseCacheOwner(0), "resp_synthetic_status_A")) != 0 {
				t.Error("failed attempt A entered the successful response cache")
			}
			if tc.private {
				h.assertStableIdentity(t)
				if strings.Contains(wire, "resp_synthetic_status_A") || strings.Contains(wire, "synthetic-status-private-overload") {
					t.Error("understood private failure escaped the successful retry control")
				}
			} else {
				if !state.retryUnsafe() || state.canStart(context.Background()) {
					t.Error("unknown raw status did not close the Handler replay window")
				}
				if strings.Contains(wire, "resp_synthetic_status_B") || len(getResponseCache(responseCacheOwner(0), "resp_synthetic_status_B")) != 0 {
					t.Error("unknown raw status permitted a successful replay/cache entry")
				}
				h.account.Mu().RLock()
				successes, lastSuccess := h.account.SuccessStreak, h.account.LastSuccessAt
				h.account.Mu().RUnlock()
				if successes != 0 || !lastSuccess.IsZero() {
					t.Error("unknown-status failed terminal was reported as account success")
				}
			}
			// Capacity shedding is request-scoped: neither the rejected raw
			// status nor the understood retry may add a hard health penalty.
			if spAcceptanceHealthOf(h.account).streak != 0 {
				t.Error("status fixture changed the existing capacity-shed health exemption")
			}
		})
	}
}

// Unlike response.status's explicit null compatibility, a PRESENT raw type
// must be a nonempty, understood string. An explicit SSE event is a fallback
// only when the JSON key is ABSENT, never permission to erase malformed type.
type spReviewFixRawTypeCase struct {
	name, typeJSON string
	private        bool
}

func spReviewFixRawTypeCases() []spReviewFixRawTypeCase {
	return []spReviewFixRawTypeCase{
		{"object", `{"synthetic_control":"future_execution"}`, false},
		{"empty_object", `{}`, false},
		{"array", `["error"]`, false},
		{"empty_array", `[]`, false},
		{"boolean_true", `true`, false},
		{"boolean_false", `false`, false},
		{"number", `503`, false},
		{"number_zero", `0`, false},
		{"null", `null`, false},
		{"empty_string", `""`, false},
		{"absent_event_only_positive_control", "", true},
	}
}

func spReviewFixRawTypeFrame(tc spReviewFixRawTypeCase, event string) string {
	prefix := ""
	if tc.typeJSON != "" {
		prefix = `"type":` + tc.typeJSON + `,`
	}
	fields := `"headers":{"x-request-id":"synthetic-review-raw-type"}`
	if event == "error" {
		fields = strings.TrimPrefix(strings.TrimSuffix(spAcceptanceError, "}"), `{"type":"error",`)
	}
	return "event: " + event + "\ndata: {" + prefix + fields + "}\n\n"
}

func TestSafePreflightReviewFixRawTypeHelper(t *testing.T) {
	// Metadata is helper-only coverage of its early private-headers branch.
	// The real Handler matrix below deliberately exercises event:error + EOF.
	for _, event := range []string{"error", codexResponseMetadataFrameType, responseMetadataFrameType} {
		for _, tc := range spReviewFixRawTypeCases() {
			t.Run(event+"/"+tc.name, func(t *testing.T) {
				frames, unsafe, err := collectSafePreflightFrames(strings.NewReader(spReviewFixRawTypeFrame(tc, event)))
				if err != nil || unsafe != 0 || len(frames) != 1 || frames[0].event != event {
					t.Fatal("raw-type fixture did not form one complete explicit-event envelope")
				}
				frame := frames[0]
				original := append([]byte(nil), frame.data...)
				m, ok := safePreflightJSON(frame.data)
				if !ok {
					t.Fatal("raw-type fixture did not preserve valid duplicate-free JSON")
				}
				if _, present := m["type"]; present != (tc.typeJSON != "") {
					t.Fatal("fixture lost the present-versus-absent raw type distinction")
				}
				if got := safePreflightPrivateFrame(m, event); got != tc.private {
					t.Errorf("raw-type private helper=%v, want %v", got, tc.private)
				}
				if got := safePreflightPrivateFrameWithEvent(m, event, event); got != tc.private {
					t.Errorf("explicit-event private branch=%v, want %v", got, tc.private)
				}
				normalizedEvent, normalized := normalizeUpstreamResponsesTerminalEvent(frame.event, frame.data)
				if normalizedEvent != event || !bytes.Equal(normalized, original) || !bytes.Equal(frame.data, original) {
					t.Fatal("normalization stripped or rewrote original raw control evidence")
				}
				state := &safePreflightState{started: time.Now()}
				state.begin()
				if !state.canStart(context.Background()) {
					t.Fatal("fresh raw-type helper has no retry budget")
				}
				state.observe(frame, normalizedEvent, normalized)
				if state.retryUnsafe() != !tc.private || state.canStart(context.Background()) != tc.private {
					t.Error("present malformed type was treated as absent, or event-only compatibility was lost")
				}
				if !tc.private {
					known, _, parseErr := collectSafePreflightFrames(strings.NewReader(spAcceptanceFrame(spAcceptanceError)))
					if parseErr != nil || len(known) != 1 {
						t.Fatal("known error control is not a complete envelope")
					}
					state.observe(known[0], "", known[0].data)
					if !state.retryUnsafe() || state.canStart(context.Background()) {
						t.Error("later understood error reopened replay after malformed raw type")
					}
				}
			})
		}
	}
}

func TestSafePreflightReviewFixRawTypeHandler(t *testing.T) {
	for _, tc := range spReviewFixRawTypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			db := spTerminalDeliveryDB(t)
			gate := spAcceptanceNewGate()
			h := spAcceptanceNewHarness(t, 1, db, func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					spAcceptanceFlush(w, spAcceptanceRate)
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spReviewFixRawTypeFrame(tc, "error"))
					}
					return // Real EOF promotes an event-only private error candidate.
				}
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_raw_type_B"))
			})
			t.Cleanup(gate.open)
			resp, reader, _ := h.request(t)
			first := spAcceptanceReadEnvelope(t, reader)
			if first != spAcceptanceRate || h.calls.Load() != 1 {
				t.Fatal("raw-type decision preceded the actual downstream rate read")
			}
			state := h.provePath(t, 1)
			if state.retryUnsafe() || state.committed() || !state.canStart(context.Background()) {
				t.Fatal("rate-only raw-type control did not begin with replay available")
			}
			gate.open()
			wire := first + spAcceptanceReadRest(t, reader)
			result := h.wait(t)
			wantCalls, wantStatus := 1, http.StatusServiceUnavailable
			wantType, wantID := "response.failed", ""
			wants := []spReviewFixUsageWant{{status: http.StatusServiceUnavailable, kind: "server", retry: tc.private}}
			if tc.private {
				wantCalls, wantStatus = 2, http.StatusOK
				wantType, wantID = "response.completed", "resp_synthetic_raw_type_B"
				wants = append(wants, spReviewFixUsageWant{status: http.StatusOK, input: 1, output: 1, total: 2})
			}
			h.provePath(t, wantCalls) // Actual TLS requests AND final-send observations.
			state.mu.Lock()
			attempts := state.attempts
			state.mu.Unlock()
			if attempts != wantCalls {
				t.Errorf("raw-type fixture began %d attempts, want %d", attempts, wantCalls)
			}
			logical := result.logicalStatus
			if logical == 0 {
				logical = result.httpStatus
			}
			if resp.StatusCode != http.StatusOK || result.httpStatus != http.StatusOK || logical != wantStatus {
				t.Errorf("raw-type logical status=%d, want %d with published HTTP 200", logical, wantStatus)
			}
			if strings.Count(wire, "event: codex.rate_limits") != 1 {
				t.Error("raw-type fixture lost or duplicated the genuine rate envelope")
			}
			spReviewFixStatusTerminal(t, wire, wantType, wantID)
			spReviewFixUsage(t, db, h, wants...)
			if tc.private {
				h.assertStableIdentity(t)
				spAcceptanceNoPrivateA(t, wire)
			} else {
				if !state.retryUnsafe() || state.canStart(context.Background()) {
					t.Error("malformed raw type did not permanently close Handler replay")
				}
				if strings.Contains(wire, "resp_synthetic_raw_type_B") || len(getResponseCache(responseCacheOwner(0), "resp_synthetic_raw_type_B")) != 0 {
					t.Error("malformed raw type enabled a forbidden successful replay/cache entry")
				}
				h.account.Mu().RLock()
				successes, lastSuccess := h.account.SuccessStreak, h.account.LastSuccessAt
				h.account.Mu().RUnlock()
				if successes != 0 || !lastSuccess.IsZero() {
					t.Error("malformed-type failure was reported as account health success")
				}
			}
			if spAcceptanceHealthOf(h.account).streak != 0 {
				t.Error("raw-type fixture changed the capacity-shed health exemption")
			}
		})
	}
}
