package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// All fixtures are synthetic. These tests cover the acceptance subset only,
// not actual upstream login/purchases, raw SSE framing, or client compatibility.
const (
	safePreflightSchemaTestWindow  = `{"used_percent":42,"window_minutes":300}`
	safePreflightSchemaTestCredits = `{"has_credits":false,"unlimited":false,"balance":"0"}`
)

func safePreflightSchemaTestFrame(fields string) string {
	return `{"type":"codex.rate_limits",` + fields + `}`
}

func safePreflightSchemaTestWindowFrame(window string) string {
	return safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + window + `}`)
}

func checkSafePreflightSchema(t *testing.T, event string, data []byte, want bool) {
	t.Helper()
	before := append([]byte(nil), data...)
	if got := isSafePreflightRateLimitsFrame(event, data); got != want {
		t.Errorf("isSafePreflightRateLimitsFrame() = %v, want %v", got, want)
	}
	if !bytes.Equal(data, before) {
		t.Fatal("validator changed original data")
	}
}

func TestSafePreflightSchemaAcceptedShapes(t *testing.T) {
	tests := []struct {
		name, data string
	}{
		{"primary_without_reset", safePreflightSchemaTestWindowFrame(safePreflightSchemaTestWindow)},
		{"five_hour_and_weekly", `{
			"type":"codex.rate_limits","plan_type":"plus",
			"rate_limits":{
				"allowed":true,"limit_reached":false,
				"primary":{"used_percent":42.5,"window_minutes":300,"reset_at":1700000900},
				"secondary":{"used_percent":8,"window_minutes":10080,"reset_at":1700604800}
			},"credits":{"has_credits":true,"unlimited":false,"balance":"10.25"}
		}`},
		{"weekly_primary_null_secondary", `{
			"type":"codex.rate_limits","plan_type":"pro",
			"rate_limits":{"allowed":true,"limit_reached":false,
				"primary":{"used_percent":81,"window_minutes":10080,"reset_after_seconds":137160,"reset_at":1700137160},
				"secondary":null},
			"credits":{"has_credits":false,"unlimited":false,"balance":"0"}
		}`},
		{"secondary_only", safePreflightSchemaTestFrame(`"rate_limits":{"secondary":` + safePreflightSchemaTestWindow + `}`)},
		{"null_primary", safePreflightSchemaTestFrame(`"rate_limits":{"primary":null,"secondary":` + safePreflightSchemaTestWindow + `}`)},
		{"monthly_window", safePreflightSchemaTestWindowFrame(`{"used_percent":44,"window_minutes":43200,"resets_at":1702592000}`)},
		{"relative_now", safePreflightSchemaTestWindowFrame(`{"used_percent":100,"window_minutes":1,"resets_in_seconds":0}`)},
		{"equal_reset_aliases", safePreflightSchemaTestWindowFrame(`{"used_percent":0,"window_minutes":300,"reset_at":1700000900,"resets_at":1700000900,"resets_in_seconds":900,"reset_after_seconds":900}`)},
		{"credits_only", safePreflightSchemaTestFrame(`"credits":` + safePreflightSchemaTestCredits)},
		{"credits_without_balance", safePreflightSchemaTestFrame(`"credits":{"has_credits":true,"unlimited":true}`)},
		{"credits_null_balance", safePreflightSchemaTestFrame(`"credits":{"has_credits":false,"unlimited":true,"balance":null}`)},
		{"null_quota_with_credits", safePreflightSchemaTestFrame(`"rate_limits":null,"credits":` + safePreflightSchemaTestCredits)},
		{"empty_quota_with_credits", safePreflightSchemaTestFrame(`"rate_limits":{},"credits":` + safePreflightSchemaTestCredits)},
		{"null_windows_with_credits", safePreflightSchemaTestFrame(`"rate_limits":{"allowed":false,"limit_reached":true,"primary":null,"secondary":null},"credits":` + safePreflightSchemaTestCredits)},
		{"null_credits_with_window", safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `},"credits":null`)},
		{"escaped_ascii_keys_and_values", `{"\u0074ype":"codex\u002erate_limits","plan_type":"pl\u0075s","rate_\u006cimits":{"pri\u006dary":{"used_\u0070ercent":4.2e1,"window_minutes":300}},"credits":{"has_credits":true,"unlimited":false,"balance":"\u0031\u0030.0"}}`},
		{"json_whitespace_and_key_order", " \t\r\n" + `{"credits":{"unlimited":false,"has_credits":true},"type":"codex.rate_limits"}` + "\n\r\t "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, event := range []string{"", "codex.rate_limits"} {
				checkSafePreflightSchema(t, event, []byte(tt.data), true)
			}
		})
	}
}

func TestSafePreflightSchemaExactEventAndType(t *testing.T) {
	data := []byte(safePreflightSchemaTestWindowFrame(safePreflightSchemaTestWindow))
	for _, event := range []string{
		" ", "\t", "\n", " codex.rate_limits", "codex.rate_limits ",
		"codex.rate_limits\n", "CODEX.RATE_LIMITS", "Codex.rate_limits",
		"codex.rate_limits.extra", "message", "response.created", "response.metadata",
		"codex.response.metadata", "codex.rate_limits\x00", "codex.rate_limits\u200b",
	} {
		t.Run(fmt.Sprintf("event_%q", event), func(t *testing.T) {
			checkSafePreflightSchema(t, event, data, false)
		})
	}
	for _, typ := range []string{
		`null`, `true`, `0`, `[]`, `{}`, `""`, `"CODEX.RATE_LIMITS"`,
		`"codex.rate_limits "`, `" codex.rate_limits"`, `"response.created"`,
		`"codex.rate_limits\n"`, `"codex.rate_limits\u0000"`,
	} {
		t.Run("type_"+typ, func(t *testing.T) {
			frame := `{"type":` + typ + `,"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `}}`
			checkSafePreflightSchema(t, "codex.rate_limits", []byte(frame), false)
		})
	}
	checkSafePreflightSchema(t, "codex.rate_limits", []byte(`{"rate_limits":{"primary":`+safePreflightSchemaTestWindow+`}}`), false)
}

func TestSafePreflightSchemaJSONEnvelope(t *testing.T) {
	valid := safePreflightSchemaTestWindowFrame(safePreflightSchemaTestWindow)
	tests := []struct {
		name, data string
	}{
		{"empty", ""},
		{"whitespace_only", " \t\r\n "},
		{"empty_object", `{}`},
		{"array", `[` + valid + `]`},
		{"string", `"codex.rate_limits"`},
		{"number", `42`},
		{"boolean", `true`},
		{"null", `null`},
		{"second_object", valid + ` {}`},
		{"second_frame", valid + "\n" + valid},
		{"trailing_null", valid + `null`},
		{"trailing_boolean", valid + `true`},
		{"trailing_number", valid + `0`},
		{"trailing_string", valid + `""`},
		{"trailing_comma", valid + `,`},
		{"trailing_brace", valid + `}`},
		{"trailing_bracket", valid + `]`},
		{"trailing_garbage", valid + `garbage`},
		{"trailing_nul", valid + "\x00"},
		{"trailing_comment", valid + `/* comment */`},
		{"leading_comment", `// comment` + "\n" + valid},
		{"bom", "\xef\xbb\xbf" + valid},
		{"non_json_whitespace", "\v" + valid},
		{"unicode_whitespace", valid + "\u00a0"},
		{"unclosed_object", valid[:len(valid)-1]},
		{"missing_comma", `{"type":"codex.rate_limits" "credits":` + safePreflightSchemaTestCredits + `}`},
		{"object_trailing_comma", `{"type":"codex.rate_limits","credits":` + safePreflightSchemaTestCredits + `,}`},
		{"non_string_key", `{1:"codex.rate_limits","credits":` + safePreflightSchemaTestCredits + `}`},
		{"invalid_escape", safePreflightSchemaTestFrame(`"plan_type":"pl\x75s","credits":` + safePreflightSchemaTestCredits)},
		{"short_unicode_escape", safePreflightSchemaTestFrame(`"plan_type":"\u123","credits":` + safePreflightSchemaTestCredits)},
		{"invalid_unicode_escape", safePreflightSchemaTestFrame(`"plan_type":"\uZZZZ","credits":` + safePreflightSchemaTestCredits)},
		{"unescaped_newline", safePreflightSchemaTestFrame("\"plan_type\":\"pl\nus\",\"credits\":" + safePreflightSchemaTestCredits)},
		{"raw_sse_not_callback_data", "event: codex.rate_limits\ndata: " + valid + "\n\n"},
	}
	checkSafePreflightSchema(t, "", nil, false)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkSafePreflightSchema(t, "", []byte(tt.data), false)
		})
	}
}

func TestSafePreflightSchemaRequiresCompleteObservation(t *testing.T) {
	for i, data := range []string{
		`{"type":"codex.rate_limits"}`,
		safePreflightSchemaTestFrame(`"plan_type":"plus","limit_id":"codex"`),
		safePreflightSchemaTestFrame(`"rate_limits":{}`),
		safePreflightSchemaTestFrame(`"rate_limits":null`),
		safePreflightSchemaTestFrame(`"credits":null`),
		safePreflightSchemaTestFrame(`"rate_limits":{"primary":null,"secondary":null},"credits":null`),
		safePreflightSchemaTestFrame(`"rate_limits":{"allowed":true,"limit_reached":false}`),
		safePreflightSchemaTestWindowFrame(`{}`),
		safePreflightSchemaTestWindowFrame(`{"used_percent":1}`),
		safePreflightSchemaTestWindowFrame(`{"window_minutes":300}`),
		safePreflightSchemaTestWindowFrame(`{"reset_at":1700000900}`),
		safePreflightSchemaTestFrame(`"credits":{}`),
		safePreflightSchemaTestFrame(`"credits":{"has_credits":true}`),
		safePreflightSchemaTestFrame(`"credits":{"unlimited":false}`),
		safePreflightSchemaTestFrame(`"credits":{"balance":"10"}`),
		// A valid sibling must not launder an incomplete/invalid observation.
		safePreflightSchemaTestFrame(`"rate_limits":{"primary":{},"secondary":` + safePreflightSchemaTestWindow + `}`),
		safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `,"secondary":{"used_percent":0}}`),
		safePreflightSchemaTestFrame(`"rate_limits":{"primary":{}},"credits":` + safePreflightSchemaTestCredits),
		safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `},"credits":{}`),
		safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `},"credits":{"has_credits":true}`),
	} {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			checkSafePreflightSchema(t, "", []byte(data), false)
		})
	}
}

func TestSafePreflightSchemaUnknownKeys(t *testing.T) {
	for _, field := range []string{
		`"sequence":1`, `"sequence":9007199254740993`,
		`"sequence":18446744073709551616`, `"sequence":1e309`,
		`"sequence_number":9007199254740993`, `"response_id":"resp-synthetic"`,
		`"headers":{}`, `"headers":{"x-codex-turn-state":"synthetic"}`,
		`"turnstate":null`, `"turn_state":"synthetic"`, `"x-codex-turn-state":"synthetic"`,
		`"response":{}`, `"output":[]`, `"usage":{}`, `"error":null`,
		`"id":"synthetic"`, `"retry":0`, `"extra":null`, `"Type":"codex.rate_limits"`,
		`"allowed":true`, `"limit_reached":false`, `"primary":null`, `"balance":"0"`,
	} {
		t.Run("top_"+field, func(t *testing.T) {
			data := safePreflightSchemaTestFrame(`"credits":` + safePreflightSchemaTestCredits + `,` + field)
			checkSafePreflightSchema(t, "", []byte(data), false)
		})
	}
	for _, field := range []string{`"extra":null`, `"tertiary":null`, `"headers":{}`, `"response_id":"synthetic"`, `"sequence":9007199254740993`, `"PRIMARY":null`} {
		t.Run("rate_limits_"+field, func(t *testing.T) {
			data := safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `,` + field + `}`)
			checkSafePreflightSchema(t, "", []byte(data), false)
		})
	}
	for _, field := range []string{`"extra":null`, `"allowed":true`, `"limit_reached":false`, `"remaining":10`, `"headers":{}`, `"sequence":9007199254740993`, `"reset_after":10`} {
		t.Run("window_"+field, func(t *testing.T) {
			window := `{"used_percent":1,"window_minutes":300,` + field + `}`
			checkSafePreflightSchema(t, "", []byte(safePreflightSchemaTestWindowFrame(window)), false)
		})
	}
	for _, field := range []string{`"extra":null`, `"overage_reached":false`, `"currency":"USD"`, `"headers":{}`, `"sequence":9007199254740993`} {
		t.Run("credits_"+field, func(t *testing.T) {
			data := safePreflightSchemaTestFrame(`"credits":{"has_credits":true,"unlimited":false,` + field + `}`)
			checkSafePreflightSchema(t, "", []byte(data), false)
		})
	}
}

func TestSafePreflightSchemaDuplicateKeys(t *testing.T) {
	tests := []struct {
		name, data string
	}{
		{"type_equal", `{"type":"codex.rate_limits","type":"codex.rate_limits","credits":` + safePreflightSchemaTestCredits + `}`},
		{"type_conflicting", `{"type":"response.created","type":"codex.rate_limits","credits":` + safePreflightSchemaTestCredits + `}`},
		{"type_escaped", `{"type":"codex.rate_limits","\u0074ype":"codex.rate_limits","credits":` + safePreflightSchemaTestCredits + `}`},
		{"plan_escaped", safePreflightSchemaTestFrame(`"plan_type":"plus","plan_\u0074ype":"plus","credits":` + safePreflightSchemaTestCredits)},
		{"bucket_escaped", safePreflightSchemaTestFrame(`"limit_id":"codex","limit_\u0069d":"codex","credits":` + safePreflightSchemaTestCredits)},
		{"rate_limits", safePreflightSchemaTestFrame(`"rate_limits":null,"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `}`)},
		{"credits", safePreflightSchemaTestFrame(`"credits":null,"credits":` + safePreflightSchemaTestCredits)},
		{"primary", safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `,"primary":` + safePreflightSchemaTestWindow + `}`)},
		{"primary_escaped", safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `,"pri\u006dary":null}`)},
		{"secondary_escaped", safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `,"secondary":null,"secondar\u0079":null}`)},
		{"allowed", safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `,"allowed":true,"allowed":true}`)},
		{"window_used", safePreflightSchemaTestWindowFrame(`{"used_percent":42,"used_percent":42,"window_minutes":300}`)},
		{"window_used_escaped", safePreflightSchemaTestWindowFrame(`{"used_percent":42,"used_\u0070ercent":42,"window_minutes":300}`)},
		{"window_minutes", safePreflightSchemaTestWindowFrame(`{"used_percent":42,"window_minutes":300,"window_minutes":300}`)},
		{"reset", safePreflightSchemaTestWindowFrame(`{"used_percent":42,"window_minutes":300,"reset_at":1700000900,"reset_at":1700000900}`)},
		{"reset_escaped", safePreflightSchemaTestWindowFrame(`{"used_percent":42,"window_minutes":300,"reset_at":1700000900,"reset_\u0061t":1700000900}`)},
		{"credit_flag_escaped", safePreflightSchemaTestFrame(`"credits":{"has_credits":true,"has_\u0063redits":true,"unlimited":false}`)},
		{"credit_balance", safePreflightSchemaTestFrame(`"credits":{"has_credits":true,"unlimited":false,"balance":null,"balance":"0"}`)},
		{"credit_balance_escaped", safePreflightSchemaTestFrame(`"credits":{"has_credits":true,"unlimited":false,"balance":"0","bal\u0061nce":"0"}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkSafePreflightSchema(t, "", []byte(tt.data), false)
		})
	}
}

func TestSafePreflightSchemaObjectAndBooleanTypes(t *testing.T) {
	for _, raw := range []string{`true`, `false`, `0`, `1`, `"true"`, `"{}"`, `[]`, `[{}]`} {
		for _, field := range []string{"rate_limits", "credits"} {
			t.Run(field+"_"+raw, func(t *testing.T) {
				// A complete sibling must not hide a wrong-typed container.
				sibling := `"credits":` + safePreflightSchemaTestCredits
				if field == "credits" {
					sibling = `"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `}`
				}
				data := safePreflightSchemaTestFrame(`"` + field + `":` + raw + `,` + sibling)
				checkSafePreflightSchema(t, "", []byte(data), false)
			})
		}
		for _, field := range []string{"primary", "secondary"} {
			t.Run(field+"_"+raw, func(t *testing.T) {
				data := safePreflightSchemaTestFrame(`"rate_limits":{"` + field + `":` + raw + `},"credits":` + safePreflightSchemaTestCredits)
				checkSafePreflightSchema(t, "", []byte(data), false)
			})
		}
	}
	for _, raw := range []string{`null`, `0`, `1`, `"true"`, `"false"`, `[]`, `{}`} {
		for _, field := range []string{"allowed", "limit_reached"} {
			t.Run(field+"_"+raw, func(t *testing.T) {
				data := safePreflightSchemaTestFrame(`"rate_limits":{"primary":` + safePreflightSchemaTestWindow + `,"` + field + `":` + raw + `}`)
				checkSafePreflightSchema(t, "", []byte(data), false)
			})
		}
		for _, field := range []string{"has_credits", "unlimited"} {
			t.Run(field+"_"+raw, func(t *testing.T) {
				other := "has_credits"
				if field == other {
					other = "unlimited"
				}
				data := safePreflightSchemaTestFrame(`"credits":{"` + other + `":false,"` + field + `":` + raw + `}`)
				checkSafePreflightSchema(t, "", []byte(data), false)
			})
		}
	}
	// Booleans are schema-checked, not coerced or inferred from each other.
	for _, first := range []string{"true", "false"} {
		for _, second := range []string{"true", "false"} {
			t.Run(first+"_"+second, func(t *testing.T) {
				data := safePreflightSchemaTestFrame(`"rate_limits":{"allowed":` + first + `,"limit_reached":` + second + `,"primary":` + safePreflightSchemaTestWindow + `},"credits":{"has_credits":` + first + `,"unlimited":` + second + `}`)
				checkSafePreflightSchema(t, "", []byte(data), true)
			})
		}
	}
}

func TestSafePreflightSchemaPercent(t *testing.T) {
	tests := []struct {
		name, raw string
		want      bool
	}{
		{"zero", "0", true}, {"full", "100", true}, {"fraction", "0.125", true},
		{"signed_zero", "-0", true}, {"signed_fractional_zero", "-0.000e2", true},
		{"decimal", "42.5", true}, {"exponent", "4.25e1", true},
		{"positive_exponent", "1.00E+02", true}, {"negative_exponent", "1000e-1", true},
		{"leading_fraction_zeroes", "0.0001e6", true}, {"trailing_zeroes", "0.100e3", true},
		{"tiny_positive", "1e-10000", true},
		{"rounds_up_to_full", "99.999999999999999999", true},
		{"negative", "-0.1", false}, {"negative_underflow", "-1e-10000", false},
		{"over_full", "101", false}, {"rounded_over_full", "100.000000000000000001", false},
		{"exponent_rounded_over_full", "1.000000000000000001e2", false},
		{"overflow", "1e309", false}, {"negative_overflow", "-1e309", false},
		{"huge_positive_exponent", "1e99999999999999999999999999", false},
		{"large_integer", "9007199254740993", false}, {"int64_overflow", "9223372036854775808", false},
		{"quoted_number", `"42"`, false}, {"quoted_nan", `"NaN"`, false},
		{"nan", "NaN", false}, {"infinity", "Infinity", false}, {"inf", "Inf", false},
		{"null", "null", false}, {"boolean", "true", false}, {"object", "{}", false}, {"array", "[]", false},
		{"leading_plus", "+1", false}, {"leading_zero", "01", false},
		{"missing_integer", ".5", false}, {"missing_fraction", "1.", false}, {"missing_exponent", "1e", false},
		{"long_rounds_up", "99." + strings.Repeat("9", 1024), true},
		{"long_rounds_down", "100." + strings.Repeat("0", 1024) + "1", false},
		{"long_exact_full", "1" + strings.Repeat("0", 1024) + "e-1022", true},
		{"long_exponent_over_full", "1" + strings.Repeat("0", 1023) + "1e-1022", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window := `{"used_percent":` + tt.raw + `,"window_minutes":300}`
			checkSafePreflightSchema(t, "", []byte(safePreflightSchemaTestWindowFrame(window)), tt.want)
		})
	}
}

func TestSafePreflightSchemaIntegerFields(t *testing.T) {
	// Literal expectations are independent of implementation constants.
	fields := []struct {
		name, max, aboveMax string
		allowZero           bool
	}{
		{"window_minutes", "153722867", "153722868", false},
		{"reset_at", "9223372036", "9223372037", false},
		{"resets_at", "9223372036", "9223372037", false},
		{"resets_in_seconds", "9223372036", "9223372037", true},
		{"reset_after_seconds", "9223372036", "9223372037", true},
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			tests := []struct {
				raw  string
				want bool
			}{
				{"1", true}, {"0", field.allowZero}, {"-0", field.allowZero}, {field.max, true}, {field.aboveMax, false},
				{"-1", false}, {"1.0", false}, {"1.5", false}, {"1e0", false}, {"1E+0", false},
				{"0.0", false}, {"0e0", false}, {`"1"`, false}, {`"300"`, false}, {`" 1 "`, false},
				{"null", false}, {"true", false}, {"{}", false}, {"[]", false},
				{"9007199254740993", false}, {"9223372036854775807", false}, {"9223372036854775808", false},
				{"-9223372036854775808", false}, {"-9223372036854775809", false}, {"18446744073709551616", false},
				{strings.Repeat("9", 1024), false}, {"1e309", false}, {"NaN", false}, {"Inf", false},
				{"+1", false}, {"01", false}, {"0x10", false}, {"1_000", false},
			}
			for i, tt := range tests {
				t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
					window := `{"used_percent":42,`
					if field.name != "window_minutes" {
						window += `"window_minutes":300,`
					}
					window += `"` + field.name + `":` + tt.raw + `}`
					checkSafePreflightSchema(t, "", []byte(safePreflightSchemaTestWindowFrame(window)), tt.want)
				})
			}
		})
	}
}

func TestSafePreflightSchemaResetAliases(t *testing.T) {
	tests := []struct {
		name, fields string
		want         bool
	}{
		{"absolute_equal", `"reset_at":1700000900,"resets_at":1700000900`, true},
		{"absolute_conflict", `"reset_at":1700000900,"resets_at":1700000901`, false},
		{"absolute_reverse_conflict", `"resets_at":1700000901,"reset_at":1700000900`, false},
		{"relative_equal", `"resets_in_seconds":900,"reset_after_seconds":900`, true},
		{"relative_zero", `"resets_in_seconds":0,"reset_after_seconds":0`, true},
		{"relative_signed_zero", `"resets_in_seconds":-0,"reset_after_seconds":0`, true},
		{"relative_conflict", `"resets_in_seconds":0,"reset_after_seconds":900`, false},
		{"relative_reverse_conflict", `"reset_after_seconds":900,"resets_in_seconds":0`, false},
		{"absolute_type_mismatch", `"reset_at":1700000900,"resets_at":"1700000900"`, false},
		{"relative_type_mismatch", `"resets_in_seconds":900,"reset_after_seconds":900.0`, false},
		{"null_not_absence", `"reset_at":1700000900,"resets_at":null`, false},
		{"equal_absolute_overflow", `"reset_at":9223372037,"resets_at":9223372037`, false},
		{"equal_relative_overflow", `"resets_in_seconds":9223372037,"reset_after_seconds":9223372037`, false},
		// No observation time is available; different dimensions are not aliases.
		{"both_dimensions", `"reset_at":1700000900,"reset_after_seconds":900`, true},
		{"relative_zero_with_absolute", `"reset_at":1700000900,"resets_in_seconds":0`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window := `{"used_percent":42,"window_minutes":300,` + tt.fields + `}`
			checkSafePreflightSchema(t, "", []byte(safePreflightSchemaTestWindowFrame(window)), tt.want)
		})
	}
}

func TestSafePreflightSchemaDefaultBucket(t *testing.T) {
	tests := []struct {
		name, fields string
		want         bool
	}{
		{"implicit_default", "", true},
		{"limit_id", `"limit_id":"codex",`, true},
		{"limit_name", `"limit_name":"codex",`, true},
		{"metered_name", `"metered_limit_name":"codex",`, true},
		{"all_default", `"limit_id":"codex","limit_name":"codex","metered_limit_name":"codex",`, true},
		{"display_name", `"limit_id":"codex","limit_name":"gpt-5.2-codex-sonic",`, true},
		{"display_with_metered_default", `"limit_id":"codex","limit_name":"Synthetic_Display-1.0","metered_limit_name":"codex",`, true},
		{"metered_bucket", `"metered_limit_name":"codex_spark",`, false},
		{"nondefault_id", `"limit_id":"codex_spark",`, false},
		{"nondefault_name", `"limit_name":"gpt-codex",`, false},
		{"metered_conflicts_with_id", `"limit_id":"codex","metered_limit_name":"codex_spark",`, false},
		{"metered_conflicts_with_name", `"limit_name":"codex","metered_limit_name":"reserve",`, false},
		{"id_conflicts_with_metered", `"limit_id":"reserve","metered_limit_name":"codex",`, false},
		{"id_conflicts_with_name", `"limit_id":"reserve","limit_name":"codex",`, false},
		{"display_needs_id", `"metered_limit_name":"codex","limit_name":"gpt-codex",`, false},
		{"id_case_not_normalized", `"limit_id":"CODEX",`, false},
		{"name_case_not_normalized", `"limit_name":"Codex",`, false},
		{"metered_case_not_normalized", `"metered_limit_name":"CODEX",`, false},
		{"id_spaces_not_trimmed", `"limit_id":" codex ",`, false},
		{"empty_metered_not_ignored", `"limit_id":"codex","metered_limit_name":"",`, false},
		{"null_metered_not_ignored", `"limit_id":"codex","metered_limit_name":null,`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := safePreflightSchemaTestFrame(tt.fields + `"credits":` + safePreflightSchemaTestCredits)
			checkSafePreflightSchema(t, "", []byte(data), tt.want)
		})
	}
}

func TestSafePreflightSchemaTokenLabels(t *testing.T) {
	for _, field := range []string{"plan_type", "limit_id", "limit_name", "metered_limit_name"} {
		for i, raw := range []string{
			`null`, `true`, `1`, `[]`, `{}`, `""`, `" "`, `" codex"`, `"codex "`, `"codex plan"`,
			`"codex\n"`, `"codex\u0000"`, `"codex\u007f"`, `"codex\u0080"`,
			`"codex/plan"`, `"codex:plan"`, `"codex@plan"`, `"codex\\plan"`, `"codex\"plan"`,
			`".codex"`, `"_codex"`, `"-codex"`, `"套餐"`, `"cоdex"`,
		} {
			t.Run(fmt.Sprintf("%s_invalid_%d", field, i), func(t *testing.T) {
				prefix := ""
				if field == "limit_name" {
					prefix = `"limit_id":"codex",`
				}
				data := safePreflightSchemaTestFrame(prefix + `"` + field + `":` + raw + `,"credits":` + safePreflightSchemaTestCredits)
				checkSafePreflightSchema(t, "", []byte(data), false)
			})
		}
	}
	for _, field := range []struct {
		name, prefix string
		max          int
	}{
		{"plan_type", "", 32},
		{"limit_name", `"limit_id":"codex",`, 128},
	} {
		for _, length := range []int{1, field.max - 1, field.max, field.max + 1} {
			t.Run(fmt.Sprintf("%s_length_%d", field.name, length), func(t *testing.T) {
				data := safePreflightSchemaTestFrame(field.prefix + `"` + field.name + `":"` + strings.Repeat("a", length) + `","credits":` + safePreflightSchemaTestCredits)
				checkSafePreflightSchema(t, "", []byte(data), length <= field.max)
			})
		}
	}
	for _, plan := range []string{"free", "plus", "pro", "team", "enterprise", "business", "Synthetic_Plan-1.0", "0"} {
		t.Run("plan_"+plan, func(t *testing.T) {
			data := safePreflightSchemaTestFrame(`"plan_type":"` + plan + `","credits":` + safePreflightSchemaTestCredits)
			checkSafePreflightSchema(t, "", []byte(data), true)
		})
	}
}

func TestSafePreflightSchemaCreditBalance(t *testing.T) {
	tests := []struct {
		name, raw string
		want      bool
	}{
		{"null", "null", true}, {"zero", `"0"`, true}, {"integer", `"123"`, true},
		{"fraction", `"12.34"`, true}, {"leading_zeroes", `"0012.3400"`, true},
		{"decimal_zero", `"0.000"`, true},
		{"max_integer_length", `"` + strings.Repeat("9", 128) + `"`, true},
		{"max_decimal_length", `"0.` + strings.Repeat("0", 126) + `"`, true},
		{"max_decoded_length", `"` + strings.Repeat(`\u0030`, 128) + `"`, true},
		{"too_long", `"` + strings.Repeat("0", 129) + `"`, false},
		{"too_long_decoded", `"` + strings.Repeat(`\u0030`, 129) + `"`, false},
		{"empty", `""`, false}, {"numeric", "0", false}, {"fraction_numeric", "1.5", false},
		{"boolean", "false", false}, {"object", "{}", false}, {"array", "[]", false},
		{"negative", `"-1"`, false}, {"negative_zero", `"-0"`, false}, {"plus", `"+1"`, false},
		{"exponent", `"1e2"`, false}, {"nan", `"NaN"`, false}, {"inf", `"Inf"`, false},
		{"missing_integer", `".1"`, false}, {"missing_fraction", `"1."`, false},
		{"two_dots", `"1.2.3"`, false}, {"space_prefix", `" 1"`, false}, {"space_suffix", `"1 "`, false},
		{"newline", `"1\n2"`, false}, {"nul", `"1\u0000"`, false}, {"non_ascii", `"１２"`, false},
		{"comma", `"1,000"`, false}, {"underscore", `"1_000"`, false}, {"ratio", `"1/2"`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := safePreflightSchemaTestFrame(`"credits":{"has_credits":true,"unlimited":false,"balance":` + tt.raw + `}`)
			checkSafePreflightSchema(t, "", []byte(data), tt.want)
		})
	}
}

func TestSafePreflightSchemaUTF8AndSurrogates(t *testing.T) {
	for i, invalid := range [][]byte{
		{0xff}, {0x80}, {0xc0, 0xaf}, {0xe2, 0x82}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80},
	} {
		t.Run(fmt.Sprintf("raw_utf8_%d", i), func(t *testing.T) {
			data := append([]byte(`{"type":"codex.rate_limits","plan_type":"`), invalid...)
			data = append(data, []byte(`","credits":`+safePreflightSchemaTestCredits+`}`)...)
			checkSafePreflightSchema(t, "", data, false)
		})
	}
	for _, raw := range []string{
		`"\ud800"`, `"\udfff"`, `"\uD800x"`, `"\udc00\ud800"`,
		`"\ud800\u0041"`, `"\ud800\ud800"`, `"\ud800\udc00\ud800"`,
		`"\ud83d\ude00"`, `"\ufffd"`, `"�"`, `"é"`,
	} {
		t.Run(raw, func(t *testing.T) {
			data := safePreflightSchemaTestFrame(`"plan_type":` + raw + `,"credits":` + safePreflightSchemaTestCredits)
			checkSafePreflightSchema(t, "", []byte(data), false)
			data = safePreflightSchemaTestFrame(raw + `:null,"credits":` + safePreflightSchemaTestCredits)
			checkSafePreflightSchema(t, "", []byte(data), false)
		})
	}
	// The exact event is a Go string too; an invalid UTF-8 event is not blank.
	checkSafePreflightSchema(t, string([]byte{0xff}), []byte(safePreflightSchemaTestFrame(`"credits":`+safePreflightSchemaTestCredits)), false)
}

func TestSafePreflightSchemaByteLimitAndOriginalBytes(t *testing.T) {
	base := safePreflightSchemaTestWindowFrame(safePreflightSchemaTestWindow)
	for _, size := range []int{16*1024 - 1, 16 * 1024, 16*1024 + 1} {
		for _, placement := range []string{"before", "after", "both"} {
			t.Run(fmt.Sprintf("%d_%s", size, placement), func(t *testing.T) {
				padding := size - len(base)
				leading := 0
				if placement == "before" {
					leading = padding
				} else if placement == "both" {
					leading = padding / 2
				}
				data := []byte(strings.Repeat(" ", leading) + base + strings.Repeat("\n", padding-leading))
				if len(data) != size {
					t.Fatal("incorrect synthetic byte-boundary fixture")
				}
				checkSafePreflightSchema(t, "", data, size <= 16*1024)
			})
		}
	}
	for _, suffix := range []string{"", ",invalid"} {
		t.Run("backing_array_"+suffix, func(t *testing.T) {
			payload := " \n" + base + "\t " + suffix
			backing := []byte("guard-before" + payload + "guard-after")
			before := append([]byte(nil), backing...)
			data := backing[len("guard-before") : len("guard-before")+len(payload)]
			checkSafePreflightSchema(t, "codex.rate_limits", data, suffix == "")
			if !bytes.Equal(backing, before) {
				t.Fatal("validator changed backing storage outside data")
			}
		})
	}
}

func TestSafePreflightSchemaDepthAndDuplicateScanner(t *testing.T) {
	// Exercise the structural guard separately: the actual schema currently
	// needs at most three object levels, so an arbitrary fourth object is not
	// made semantically valid just because it passes the structural depth cap.
	for _, depth := range []int{1, 2, 3, 4, 5, 100, 1000} {
		t.Run(fmt.Sprintf("object_depth_%d", depth), func(t *testing.T) {
			data := strings.Repeat(`{"a":`, depth-1) + `{}` + strings.Repeat(`}`, depth-1)
			decoder := json.NewDecoder(strings.NewReader(data))
			decoder.UseNumber()
			_, ok := safePreflightSchemaReadValue(decoder, 0)
			if ok != (depth <= 4) {
				t.Fatalf("structural depth accepted = %v, want %v", ok, depth <= 4)
			}
			if ok {
				if _, err := decoder.Token(); err != io.EOF {
					t.Fatal("structural reader did not consume exactly one value")
				}
			}
			frame := safePreflightSchemaTestWindowFrame(`{"used_percent":` + data + `,"window_minutes":300}`)
			checkSafePreflightSchema(t, "", []byte(frame), false)
		})
	}
	for _, depth := range []int{1, 4, 5, 4096} {
		t.Run(fmt.Sprintf("array_depth_%d", depth), func(t *testing.T) {
			data := strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
			frame := safePreflightSchemaTestWindowFrame(`{"used_percent":` + data + `,"window_minutes":300}`)
			checkSafePreflightSchema(t, "", []byte(frame), false)
		})
	}
	tests := []struct {
		name, data string
		want       bool
	}{
		{"sibling_keys_are_independent", `{"a":{"same":1},"b":{"same":2}}`, true},
		{"root_duplicate", `{"a":1,"a":1}`, false},
		{"root_escaped_duplicate", `{"a":1,"\u0061":1}`, false},
		{"level_two_duplicate", `{"a":{"b":1,"b":1}}`, false},
		{"level_three_duplicate", `{"a":{"b":{"c":1,"\u0063":1}}}`, false},
		{"level_four_duplicate", `{"a":{"b":{"c":{"d":1,"\u0064":1}}}}`, false},
		{"lone_surrogate_key", `{"\ud800":1}`, false},
		{"lone_surrogate_value", `{"a":"\udfff"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder := json.NewDecoder(strings.NewReader(tt.data))
			decoder.UseNumber()
			if _, ok := safePreflightSchemaReadValue(decoder, 0); ok != tt.want {
				t.Errorf("structural acceptance = %v, want %v", ok, tt.want)
			}
		})
	}
}
