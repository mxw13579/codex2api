package proxy

import (
	"net/http"
	"strings"
	"testing"
)

const upstreamResponseTestUUID = "550e8400-e29b-41d4-a716-446655440000"

func TestSafeUpstreamTraceID(t *testing.T) {
	for _, value := range []string{
		"a", "0", upstreamResponseTestUUID, strings.ToUpper(upstreamResponseTestUUID),
		"req_synthetic-AZaz09.id:123", strings.Repeat("r", 128),
	} {
		if got := safeUpstreamTraceID(value); got != value {
			t.Errorf("valid fixture %q changed to %q", value, got)
		}
		if got := safeUpstreamTraceID("  " + value + "  "); got != value {
			t.Errorf("space-padded fixture %q changed to %q", value, got)
		}
	}
	for _, value := range []string{
		"", "   ", "界", "req_界", "\u00a0req_synthetic\u00a0", strings.Repeat("r", 129),
		"req one", "req\ttwo", "req\x00three", "req\x7ffour", "req\r\nInjected:yes",
		"\r\nreq_synthetic", "req_synthetic\r\n", "\treq_synthetic", "req_synthetic\t",
		"https://example.invalid/req", "https:example.invalid", "data:synthetic", "file:synthetic", `{"id":"req_synthetic"}`, "req/synthetic", "req=synthetic",
		"sk-synthetic", "SK-synthetic", "sess-synthetic", "SESS-synthetic",
		"eyJhbGciOiJub25lIn0.eyJmaXh0dXJlIjp0cnVlfQ.synthetic", "eyJshort",
		"Bearer synthetic", "Bearer:synthetic", "BEARER-synthetic", "Bearer", "Basic:synthetic",
		"Authorization:synthetic", "Cookie:synthetic", "token:synthetic", "api_key:synthetic",
		"****MASKED****", "****UUID-MASKED****", "[TOKEN-MASKED]", "[REDACTED]",
		"MASKED", "redacted", "__REDACTED__", "UUID-MASKED", "token-masked", "...",
	} {
		if got := safeUpstreamTraceID(value); got != "" {
			t.Errorf("invalid fixture %q accepted as %q", value, got)
		}
	}
}

func TestSafeUpstreamTraceIDASCIIWhitelist(t *testing.T) {
	for c := byte(0); c < 128; c++ {
		value := "req_" + string(c) + "_fixture"
		allowed := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.:", rune(c))
		got := safeUpstreamTraceID(value)
		if allowed && got != value || !allowed && got != "" {
			t.Errorf("ASCII byte %d: got %q, allowed=%t", c, got, allowed)
		}
	}
}

type upstreamResponseMetadataTestCase struct {
	name       string
	headers    http.Header
	configured string
	want       string
}

// The same contract is exercised through HTTP usage tracing, cookie logging,
// and WS metadata frames. All header values here are synthetic fixtures.
func upstreamResponseMetadataTestCases() []upstreamResponseMetadataTestCase {
	tests := []upstreamResponseMetadataTestCase{
		{name: "missing"},
		{name: "uuid", headers: http.Header{"X-Request-Id": {upstreamResponseTestUUID}}, want: upstreamResponseTestUUID},
		{name: "trim", headers: http.Header{"X-Request-Id": {"  req_synthetic  "}}, want: "req_synthetic"},
		{name: "priority", headers: http.Header{"X-Request-Id": {"req_first"}, "Request-Id": {"req_second"}, "X-Openai-Request-Id": {"req_third"}, "X-Oai-Request-Id": {"req_fourth"}, "X-Goog-Request-Id": {"req_fifth"}}, want: "req_first"},
		{name: "request_id_fallback", headers: http.Header{"X-Request-Id": {" "}, "Request-Id": {"req_second"}, "X-Openai-Request-Id": {"req_third"}}, want: "req_second"},
		{name: "openai_fallback", headers: http.Header{"X-Request-Id": {"sk-synthetic"}, "Request-Id": {""}, "X-Openai-Request-Id": {upstreamResponseTestUUID}, "X-Oai-Request-Id": {"req_fourth"}}, want: upstreamResponseTestUUID},
		{name: "oai_fallback", headers: http.Header{"X-Openai-Request-Id": {"界"}, "X-Oai-Request-Id": {"req_fourth"}, "X-Goog-Request-Id": {"req_fifth"}}, want: "req_fourth"},
		{name: "goog_fallback", headers: http.Header{"X-Oai-Request-Id": {"****MASKED****"}, "X-Goog-Request-Id": {"req_fifth"}}, want: "req_fifth"},
		{name: "configured_priority", configured: "Vendor-Trace", headers: http.Header{"Vendor-Trace": {upstreamResponseTestUUID}, "X-Request-Id": {"req_not_selected"}}, want: upstreamResponseTestUUID},
		{name: "configured_normalized", configured: "  vendor-trace  ", headers: http.Header{"Vendor-Trace": {"req_vendor"}, "X-Request-Id": {"req_not_selected"}}, want: "req_vendor"},
		{name: "configured_missing", configured: "Vendor-Trace", headers: http.Header{"X-Request-Id": {"req_not_selected"}}},
		{name: "configured_empty", configured: "Vendor-Trace", headers: http.Header{"Vendor-Trace": {" "}, "X-Request-Id": {"req_not_selected"}}},
		{name: "configured_invalid_name", configured: "Bad Header", headers: http.Header{"Bad Header": {"req_invalid"}, "X-Request-Id": {"req_not_selected"}}},
		{name: "configured_blank", configured: "  ", headers: http.Header{"X-Request-Id": {"req_not_selected"}}},
		{name: "configured_long_name", configured: strings.Repeat("X", 65), headers: http.Header{"X-Request-Id": {"req_not_selected"}}},
	}
	for _, invalid := range []struct{ name, value string }{
		{"unicode", "界"}, {"crlf", "req_synthetic\r\nInjected:yes"},
		{"leading_crlf", "\r\nreq_synthetic"}, {"trailing_crlf", "req_synthetic\r\n"},
		{"overlong", strings.Repeat("r", 129)}, {"sk_token", "sk-synthetic"},
		{"session_token", "sess-synthetic"}, {"jwt", "eyJfixture.fixture.signature"},
		{"bearer", "Bearer:synthetic"}, {"masked", "****UUID-MASKED****"},
		{"url", "https://example.invalid/req"}, {"json", `{"id":"req_synthetic"}`},
	} {
		tests = append(tests,
			upstreamResponseMetadataTestCase{name: invalid.name, headers: http.Header{"X-Request-Id": {invalid.value}}},
			upstreamResponseMetadataTestCase{name: invalid.name + "_fallback", headers: http.Header{"X-Request-Id": {invalid.value}, "X-Openai-Request-Id": {"req_fallback"}}, want: "req_fallback"},
			upstreamResponseMetadataTestCase{name: invalid.name + "_configured", configured: "Vendor-Trace", headers: http.Header{"Vendor-Trace": {invalid.value}, "X-Request-Id": {"req_not_selected"}}},
		)
	}
	for _, name := range []string{"Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-Api-Key", "Anthropic-Auth-Token", "X-Goog-Api-Key"} {
		tests = append(tests, upstreamResponseMetadataTestCase{
			name: "forbidden_" + name, configured: " " + strings.ToLower(name) + " ",
			headers: http.Header{name: {"synthetic-credential-value"}, "X-Request-Id": {"req_not_selected"}},
		})
	}
	return tests
}

func TestUpstreamResponseRequestID(t *testing.T) {
	for _, tt := range upstreamResponseMetadataTestCases() {
		t.Run(tt.name, func(t *testing.T) {
			if got := upstreamResponseRequestID(tt.headers, tt.configured); got != tt.want {
				t.Fatalf("request ID = %q, want %q", got, tt.want)
			}
		})
	}
}
