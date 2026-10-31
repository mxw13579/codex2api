package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	safePreflightSchemaEventType       = "codex.rate_limits"
	safePreflightSchemaMaxBytes        = 16 * 1024
	safePreflightSchemaMaxObjectDepth  = 4
	safePreflightSchemaMaxPlanBytes    = 32
	safePreflightSchemaMaxLabelBytes   = 128
	safePreflightSchemaMaxBalanceBytes = 128

	// Bound before any consumer multiplies by a time.Duration unit. Absolute
	// Unix seconds use the same conservative cap: no wall clock is consulted.
	safePreflightSchemaMaxResetSeconds  = int64((1<<63 - 1) / time.Second)
	safePreflightSchemaMaxWindowMinutes = int64((1<<63 - 1) / time.Minute)
)

// isSafePreflightRateLimitsFrame recognizes a conservative protocol acceptance
// subset, NOT the complete official upstream schema. It does not establish
// upstream provenance, freshness, successful execution, or downstream client
// compatibility. A caller must forward the original data, not a reconstructed
// object; this helper never changes data or exposes parse errors/payloads.
//
// Only callback event/data are checked. Raw SSE framing, ignored id/retry fields,
// and EOF dispatch rules are outside this helper's contract.
//
// Missing/null rate_limits or credits mean absence; at least one complete window
// or credits snapshot is required. Present plan/limit labels must be nonempty
// ASCII tokens, not null or free text. Unlabelled quotas are the default bucket;
// explicit bucket identifiers must be exactly "codex" (no case/space folding).
func isSafePreflightRateLimitsFrame(sseEvent string, data []byte) bool {
	if sseEvent != "" && sseEvent != safePreflightSchemaEventType {
		return false
	}
	if len(data) == 0 || len(data) > safePreflightSchemaMaxBytes || !utf8.Valid(data) {
		return false
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, ok := safePreflightSchemaReadValue(decoder, 0)
	if !ok {
		return false
	}
	frame, ok := value.(map[string]any)
	if !ok {
		return false
	}
	// Token, rather than a second Decode, also rejects stray closing delimiters.
	if _, err := decoder.Token(); err != io.EOF {
		return false
	}
	if frame["type"] != safePreflightSchemaEventType {
		return false
	}

	hasWindow, hasCredits := false, false
	for key, value := range frame {
		switch key {
		case "type":
			// Checked above, including its JSON string type.
		case "plan_type":
			if !safePreflightSchemaToken(value, safePreflightSchemaMaxPlanBytes) {
				return false
			}
		case "metered_limit_name", "limit_id", "limit_name":
			if !safePreflightSchemaToken(value, safePreflightSchemaMaxLabelBytes) {
				return false
			}
		case "rate_limits":
			var valid bool
			hasWindow, valid = safePreflightSchemaRateLimits(value)
			if !valid {
				return false
			}
		case "credits":
			if value != nil {
				if !safePreflightSchemaCredits(value) {
					return false
				}
				hasCredits = true
			}
		default:
			return false
		}
	}

	// Never let the precedence of one identifier hide a metered/conflicting
	// identifier. Only an explicit limit_id=codex makes limit_name a display
	// token instead of a bucket identifier.
	if id, exists := frame["limit_id"]; exists && id != "codex" {
		return false
	}
	if metered, exists := frame["metered_limit_name"]; exists && metered != "codex" {
		return false
	}
	if name, exists := frame["limit_name"]; exists && name != "codex" && frame["limit_id"] != "codex" {
		return false
	}
	return hasWindow || hasCredits
}

// safePreflightSchemaReadValue keeps duplicate detection ahead of map insertion.
// Keys are decoded first, so "type" and "\u0074ype" are the same key. Arrays are
// not part of this subset, and recursion stops before a fifth object is entered.
func safePreflightSchemaReadValue(decoder *json.Decoder, objectDepth int) (any, bool) {
	token, err := decoder.Token()
	if err != nil {
		return nil, false
	}
	switch value := token.(type) {
	case json.Delim:
		if value != '{' || objectDepth >= safePreflightSchemaMaxObjectDepth {
			return nil, false
		}
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, false
			}
			key, ok := keyToken.(string)
			if !ok || !safePreflightSchemaASCII(key) {
				return nil, false
			}
			if _, duplicate := object[key]; duplicate {
				return nil, false
			}
			child, ok := safePreflightSchemaReadValue(decoder, objectDepth+1)
			if !ok {
				return nil, false
			}
			object[key] = child
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return nil, false
		}
		return object, true
	case string:
		return value, safePreflightSchemaASCII(value)
	case json.Number, bool, nil:
		return value, true
	default:
		return nil, false
	}
}

// Every supported key/string value is ASCII. Rejecting all decoded non-ASCII
// strings also rejects encoding/json's U+FFFD replacement of lone surrogates;
// neither malformed Unicode nor valid surrogate pairs can enter this subset.
// The separate utf8.Valid check rejects invalid raw UTF-8 before decoding.
func safePreflightSchemaASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

// Token grammar: [A-Za-z0-9][A-Za-z0-9._-]*, bounded in decoded ASCII bytes.
func safePreflightSchemaToken(value any, maxBytes int) bool {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > maxBytes {
		return false
	}
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		if i == 0 || c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func safePreflightSchemaRateLimits(value any) (hasWindow, valid bool) {
	if value == nil {
		return false, true
	}
	limits, ok := value.(map[string]any)
	if !ok {
		return false, false
	}
	for key, value := range limits {
		switch key {
		case "allowed", "limit_reached":
			if _, ok := value.(bool); !ok {
				return false, false
			}
		case "primary", "secondary":
			if value != nil {
				if !safePreflightSchemaWindow(value) {
					return false, false
				}
				hasWindow = true
			}
		default:
			return false, false
		}
	}
	return hasWindow, true
}

func safePreflightSchemaWindow(value any) bool {
	window, ok := value.(map[string]any)
	if !ok || !safePreflightSchemaPercent(window["used_percent"]) {
		return false
	}
	if _, ok := safePreflightSchemaInteger(window["window_minutes"], 1, safePreflightSchemaMaxWindowMinutes); !ok {
		return false
	}
	for key := range window {
		switch key {
		case "used_percent", "window_minutes", "reset_at", "resets_at", "resets_in_seconds", "reset_after_seconds":
		default:
			return false
		}
	}
	// Absolute aliases must agree, as must relative aliases. Absolute versus
	// relative consistency needs an observation timestamp, which this stateless
	// schema helper cannot prove; do not invent one or require a reset field.
	return safePreflightSchemaResetAliases(window, "reset_at", "resets_at", 1) &&
		safePreflightSchemaResetAliases(window, "resets_in_seconds", "reset_after_seconds", 0)
}

func safePreflightSchemaResetAliases(window map[string]any, first, second string, min int64) bool {
	seen := false
	var previous int64
	for _, key := range [2]string{first, second} {
		value, exists := window[key]
		if !exists {
			continue
		}
		n, ok := safePreflightSchemaInteger(value, min, safePreflightSchemaMaxResetSeconds)
		if !ok || seen && previous != n {
			return false
		}
		previous, seen = n, true
	}
	return true
}

func safePreflightSchemaInteger(value any, min, max int64) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	// The decoder already enforced JSON number grammar. ParseInt additionally
	// rejects fractional/exponent spellings, without float64 rounding or coercion.
	n, err := strconv.ParseInt(number.String(), 10, 64)
	return n, err == nil && n >= min && n <= max
}

func safePreflightSchemaPercent(value any) bool {
	number, ok := value.(json.Number)
	if !ok {
		return false
	}
	raw := number.String()
	if raw == "" {
		return false
	}

	// The JSON decoder has already validated the number grammar. Compare the
	// bounded decimal lexeme exactly: even ParseFloat values below 100 are not
	// a sufficient proof for very long mantissas/exponents. Never allocate powers
	// of ten or round an out-of-range value into the accepted interval.
	negative := raw[0] == '-'
	if negative {
		raw = raw[1:]
	}
	mantissa, exponent := raw, "0"
	if i := strings.IndexAny(raw, "eE"); i >= 0 {
		mantissa, exponent = raw[:i], raw[i+1:]
	}
	fractionDigits := 0
	if i := strings.IndexByte(mantissa, '.'); i >= 0 {
		fractionDigits = len(mantissa) - i - 1
	}
	digits := strings.TrimLeft(strings.ReplaceAll(mantissa, ".", ""), "0")
	if digits == "" {
		return true // Signed zero is still zero.
	}
	if negative {
		return false
	}
	// This conservative subset rejects exponents outside int32. The scan and
	// comparison stay linear in the <=16KiB input even for hostile exponents.
	exp, err := strconv.ParseInt(exponent, 10, 32)
	if err != nil {
		return false
	}
	integerDigits := int64(len(digits)-fractionDigits) + exp
	if integerDigits != 3 {
		return integerDigits < 3
	}
	return digits[0] == '1' && strings.Trim(digits[1:], "0") == ""
}

func safePreflightSchemaCredits(value any) bool {
	credits, ok := value.(map[string]any)
	if !ok {
		return false
	}
	if _, ok := credits["has_credits"].(bool); !ok {
		return false
	}
	if _, ok := credits["unlimited"].(bool); !ok {
		return false
	}
	for key, value := range credits {
		switch key {
		case "has_credits", "unlimited":
		case "balance":
			if value != nil && !safePreflightSchemaBalance(value) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// Balance is a nonnegative fixed-point decimal string: [0-9]+(\.[0-9]+)?.
// Leading zeroes are harmless; signs, exponents, spaces and numeric JSON values
// are not accepted. No float conversion (and hence no monetary rounding) occurs.
func safePreflightSchemaBalance(value any) bool {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > safePreflightSchemaMaxBalanceBytes {
		return false
	}
	seenDot := false
	for i := 0; i < len(text); i++ {
		if text[i] >= '0' && text[i] <= '9' {
			continue
		}
		if text[i] != '.' || seenDot || i == 0 || i == len(text)-1 {
			return false
		}
		seenDot = true
	}
	return true
}
