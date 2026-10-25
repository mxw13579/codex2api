package proxy

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// normalizeUpstreamResponsesTerminalEvent applies the same terminal semantics to
// HTTP SSE and WS-derived SSE before translators, retries or caches observe it.
// An explicit SSE error remains authoritative. Ordinary event names/data are not
// rewritten, and a data-only stream does not acquire a new SSE event field.
func normalizeUpstreamResponsesTerminalEvent(event string, data []byte) (string, []byte) {
	eventType := normalizedUpstreamSSEEventType(event, data)
	switch eventType {
	case "response.done", "response.completed", "response.incomplete":
	default:
		return event, data
	}
	if gjson.GetBytes(data, "type").String() == "" {
		if !gjson.ValidBytes(data) {
			return "response.failed", invalidResponsesTerminalEvent()
		}
		updated, err := sjson.SetBytes(data, "type", eventType)
		if err != nil {
			return event, data
		}
		data = updated
	}
	data = NormalizeResponsesTerminalEvent(data)
	if normalizedType := gjson.GetBytes(data, "type").String(); event != "" && normalizedType != eventType {
		event = normalizedType
	}
	return event, data
}
