package proxy

import (
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NormalizeResponsesTerminalEvent reconciles Responses terminal event names with
// explicit failure/incomplete evidence. Only type is changed: the original
// status, errors, response ID, usage and provider-specific metadata are retained.
// Ordinary and unknown events are returned unchanged. A malformed success-like
// terminal becomes a protocol failure; permissive field extraction must not turn
// invalid JSON into success. Output text is never used as failure evidence.
func NormalizeResponsesTerminalEvent(payload []byte) []byte {
	eventType := gjson.GetBytes(payload, "type").String()
	switch eventType {
	case "response.done", "response.completed", "response.incomplete":
	default:
		return payload
	}
	if !gjson.ValidBytes(payload) {
		return invalidResponsesTerminalEvent()
	}

	event := gjson.ParseBytes(payload)
	response := event.Get("response")
	normalizedType := eventType
	if responsesTerminalHasFailure(event, response) {
		// Failure evidence wins even over an inconsistent completed/incomplete
		// status or event name. Do not overwrite the original status with failed.
		normalizedType = "response.failed"
	} else if eventType != "response.incomplete" {
		// The response's explicit status takes precedence over the envelope's
		// status. Truncation details must not override an explicit final status.
		status := responsesTerminalStatus(response.Get("status"))
		if status == "" {
			status = responsesTerminalStatus(event.Get("status"))
		}
		switch status {
		case "incomplete":
			normalizedType = "response.incomplete"
		case "", "completed":
			if eventType == "response.done" {
				normalizedType = "response.completed"
			}
		default:
			// Do not invent a successful outcome for an unrecognized explicit status.
			return payload
		}
	}
	if normalizedType == eventType {
		return payload
	}
	updated, err := sjson.SetBytes(payload, "type", normalizedType)
	if err != nil {
		return payload
	}
	return updated
}

// invalidResponsesTerminalEvent omits malformed upstream bytes: they may contain
// echoed request data and cannot safely be embedded into a client error frame.
func invalidResponsesTerminalEvent() []byte {
	return []byte(`{"type":"response.failed","response":{"status":"failed","status_code":502,"error":{"code":"upstream_invalid_response","message":"Upstream returned an invalid Responses terminal event"}}}`)
}

func responsesTerminalHasFailure(event, response gjson.Result) bool {
	for _, container := range []gjson.Result{response, event} {
		status := container.Get("status")
		switch responsesTerminalStatus(status) {
		case "failed", "error", "cancelled", "canceled":
			return true
		}
		if responsesTerminalHTTPFailure(status) || responsesTerminalHTTPFailure(container.Get("status_code")) || responsesTerminalHasError(container.Get("error")) {
			return true
		}
	}
	return responsesTerminalHasError(response.Get("status_details.error"))
}

func responsesTerminalStatus(value gjson.Result) string {
	if value.Type != gjson.String {
		return ""
	}
	status := strings.ToLower(strings.TrimSpace(value.Str))
	if _, err := strconv.Atoi(status); err == nil {
		return "" // Numeric HTTP status, not a Responses lifecycle status.
	}
	return status
}

func responsesTerminalHTTPFailure(value gjson.Result) bool {
	switch value.Type {
	case gjson.Number:
		code := value.Num
		return code >= 400 && code < 600 && code == float64(int(code))
	case gjson.String:
		code, err := strconv.Atoi(strings.TrimSpace(value.Str))
		return err == nil && code >= 400 && code < 600
	default:
		return false
	}
}

func responsesTerminalHasError(value gjson.Result) bool {
	// A nonempty string in an explicit error field is evidence too; this is
	// not a keyword search through response output or other free-form text.
	if value.Type == gjson.String {
		return strings.TrimSpace(value.Str) != ""
	}
	if !value.IsObject() {
		return false
	}
	// Empty/null error placeholders (including objects with only empty error
	// fields) are common on successful and max_output_tokens terminal events.
	for _, field := range []string{"code", "type", "message"} {
		part := value.Get(field)
		if part.Type == gjson.String && strings.TrimSpace(part.Str) != "" {
			return true
		}
		if field == "code" && part.Type == gjson.Number && part.Num != 0 {
			return true
		}
	}
	return responsesTerminalHTTPFailure(value.Get("status")) || responsesTerminalHTTPFailure(value.Get("status_code"))
}
