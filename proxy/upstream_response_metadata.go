package proxy

import (
	"net/http"
	"strings"

	"github.com/codex2api/auth"
)

// safeUpstreamTraceID is deliberately narrower than general log sanitization.
// Request IDs (including UUIDs) must survive unchanged, but must not become a
// channel for credentials or arbitrary upstream content. Never truncate an ID.
func safeUpstreamTraceID(value string) string {
	// Reject controls before trimming so even leading/trailing CRLF is invalid.
	for i := 0; i < len(value); i++ {
		if value[i] < ' ' || value[i] > '~' {
			return ""
		}
	}
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 128 {
		return ""
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-' || c == '_' || c == '.' || c == ':') {
			return ""
		}
	}
	lower := strings.ToLower(value)
	for _, prefix := range []string{
		"sk-", "sess-", "eyj", "bearer", "basic",
		"authorization:", "proxy-authorization:", "cookie:", "set-cookie:",
		"token:", "access_token:", "refresh_token:", "session_token:",
		"api_key:", "api-key:", "password:", "client_secret:",
		// Also reject URL schemes whose opaque form need not contain a slash.
		"http:", "https:", "ws:", "wss:", "ftp:", "file:", "data:", "mailto:", "javascript:",
	} {
		if strings.HasPrefix(lower, prefix) {
			return ""
		}
	}
	switch strings.Trim(lower, "-_.:") {
	case "", "masked", "redacted", "uuid-masked", "token-masked":
		return ""
	}
	return value
}

// A configured header is an exclusive override, even when invalid or absent.
// Keep candidate ordering shared with metadata frames, not dependent on maps.
func upstreamResponseRequestIDHeaders(configuredHeader string) []string {
	if configuredHeader != "" {
		if auth.ValidateUpstreamRequestIDHeader(configuredHeader) != nil {
			return nil
		}
		name := strings.TrimSpace(configuredHeader)
		if name == "" {
			return nil
		}
		return []string{name}
	}
	return []string{"X-Request-Id", "Request-Id", "X-Openai-Request-Id", "X-Oai-Request-Id", "X-Goog-Request-Id"}
}

func upstreamResponseRequestID(headers http.Header, configuredHeader string) string {
	for _, name := range upstreamResponseRequestIDHeaders(configuredHeader) {
		if id := safeUpstreamTraceID(headers.Get(name)); id != "" {
			return id
		}
	}
	return ""
}
