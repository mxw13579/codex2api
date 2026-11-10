package proxy

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestUpstreamErrorLogsRedactBeforeByteTruncation(t *testing.T) {
	for _, secret := range []string{
		"-----BEGIN PRIVATE KEY----- PRIVATEKEYBODYSECRET" + strings.Repeat("A", 12000) + " -----END PRIVATE KEY-----",
		"eyJ" + strings.Repeat("A", 20) + "." + strings.Repeat("B", 12000) + "." + strings.Repeat("C", 32),
	} {
		for _, test := range []struct {
			prefix int
			format func([]byte) string
		}{
			{3500, upstreamErrorConsoleBody}, {4500, upstreamErrorLogBody},
		} {
			body, err := json.Marshal(map[string]string{"detail": strings.Repeat("x", test.prefix) + " " + secret})
			if err != nil {
				t.Fatal(err)
			}
			got := test.format(body)
			if strings.Contains(got, "PRIVATEKEYBODYSECRET") || strings.Contains(got, "eyJ") || strings.Contains(got, "PRIVATE KEY") {
				t.Fatal("secret fragment survived raw-body log truncation")
			}
		}
	}
}

func TestErrorLogDirUsesEnv(t *testing.T) {
	t.Setenv("LOG_DIR", "/tmp/codex2api-logs")
	if got, want := errorLogDir(), "/tmp/codex2api-logs"; got != want {
		t.Fatalf("errorLogDir() = %q, want %q", got, want)
	}
}
