package proxy

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/codex2api/api"
	"github.com/codex2api/internal/jsondepth"
	"github.com/codex2api/proxy/internal/jsonbody"
	"github.com/tidwall/gjson"
)

// Validate before any gjson lookup or map-based rewrite: gjson selects the
// first duplicate object member while encoding/json maps select the last one.
// Scan keys only, without re-encoding values or changing native relay numbers.
func responsesHTTPContextEnvelopeError(raw []byte) *api.APIError {
	if !jsondepth.ValidBytes(raw) {
		return nil // The normal request validator owns malformed JSON errors.
	}
	root := jsonbody.View(raw)
	if !root.IsObject() {
		return nil
	}
	seen := make(map[string]bool, 2)
	duplicate := ""
	root.ForEach(func(key, _ gjson.Result) bool {
		name := key.String() // Includes JSON-escaped spellings of the same key.
		if name != "input" && name != "previous_response_id" {
			return true
		}
		if seen[name] {
			duplicate = strings.Clone(name)
			return false
		}
		seen[name] = true
		return true
	})
	if duplicate == "" {
		return nil
	}
	return api.NewAPIErrorWithDetails(api.ErrCodeInvalidRequest,
		"Duplicate top-level Responses context fields are not allowed", api.ErrorTypeInvalidRequest,
		api.ErrorDetail{Field: duplicate})
}

type responsesHTTPReplayToolKey struct {
	callID string
	output bool
}

func responsesHTTPToolReplayKey(item gjson.Result) (responsesHTTPReplayToolKey, bool) {
	_, call, output := responseContextPairType(item.Get("type").String())
	id := item.Get("call_id")
	if (!call && !output) || id.Type != gjson.String || id.String() == "" {
		return responsesHTTPReplayToolKey{}, false
	}
	return responsesHTTPReplayToolKey{callID: id.String(), output: output}, true
}

// responsesHTTPClientReplaysParent distinguishes new tool turns from a client
// replay. Call IDs can prove overlap; equal message text alone never can.
// An overlapping partial/conflicting replay is rejected rather than guessing
// which ancestors the client intended to omit or replace.
func responsesHTTPClientReplaysParent(previous []json.RawMessage, current gjson.Result) (fullReplay, conflict bool) {
	if !current.IsArray() || len(previous) == 0 {
		return false, false
	}
	seen := make(map[responsesHTTPReplayToolKey]bool)
	for _, item := range previous {
		if key, ok := responsesHTTPToolReplayKey(gjson.ParseBytes(item)); ok {
			if seen[key] {
				return false, true
			}
			seen[key] = true
		}
	}
	overlap := false
	current.ForEach(func(_, item gjson.Result) bool {
		key, ok := responsesHTTPToolReplayKey(item)
		overlap = ok && seen[key]
		return !overlap
	})
	if !overlap {
		return false, false
	}
	items := current.Array()
	if len(items) < len(previous) {
		return false, true
	}
	for i, parent := range previous {
		left, validLeft := canonicalResponsesHTTPReplayItem(parent)
		right, validRight := canonicalResponsesHTTPReplayItem([]byte(items[i].Raw))
		if !validLeft || !validRight || !bytes.Equal(left, right) {
			return false, true
		}
	}
	// Even an exact prefix does not permit redefining its call/output IDs in
	// the new tail. A not-yet-recorded output for a parent call remains valid.
	for _, item := range items[len(previous):] {
		if key, ok := responsesHTTPToolReplayKey(item); ok && seen[key] {
			return false, true
		}
	}
	return true, false
}

// Only strip the top-level bookkeeping already removed by the native input
// translator. Preserve call_id, name, namespace, arguments and numeric values;
// a lossy float64 comparison must never prove two different histories equal.
func canonicalResponsesHTTPReplayItem(raw []byte) ([]byte, bool) {
	var item map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if !json.Valid(raw) || decoder.Decode(&item) != nil || item == nil {
		return nil, false
	}
	delete(item, "id")
	delete(item, responsesInputInternalMetadataField)
	result, err := json.Marshal(item)
	return result, err == nil
}
