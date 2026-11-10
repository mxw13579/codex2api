package proxy

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestUpstreamErrorDetails(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"detail string", `{"detail":"Bad Request"}`, "Bad Request"},
		{"detail object", `{"detail":{"code":"bad_input","type":"invalid_request_error","message":"Invalid field"}}`, "bad_input · invalid_request_error · Invalid field"},
		{"nested detail", `{"detail":{"error":{"code":"invalid","message":"No input"}}}`, "invalid · No input"},
		{"validation array", `{"detail":[{"msg":"Field required","input":"PRIVATE-PROMPT","ctx":{"token":"SECRET"},"loc":["body","PRIVATE-FIELD"]},{"msg":"Invalid value"}]}`, "Field required; Invalid value"},
		{"validation limit", `{"detail":[{"msg":"one"},{"msg":"two"},{"msg":"three"},{"msg":"four"}]}`, "one; two; three"},
		{"precedence", `{"error":{"message":"Primary"},"detail":"Secondary"}`, "Primary"},
		{"grok compatibility", `{"code":"invalid-argument","error":"Reserved function"}`, "invalid-argument · Reserved function"},
		{"terminal event", `{"response":{"status_details":{"error":{"code":"invalid","message":"Wrong effort"}}}}`, "invalid · Wrong effort"},
		{"no object stringify", `{"detail":{"input":"PRIVATE-PROMPT"},"error":{"message":{"content":"PRIVATE-PROMPT"}}}`, "HTTP 400"},
		{"no array stringify", `{"detail":[{"input":"PRIVATE-PROMPT"}]}`, "HTTP 400"},
		{"plain text", `SECRET plain text`, "HTTP 400"},
		{"html", `<html>SECRET</html>`, "HTTP 400"},
		{"invalid json", `{"detail":"partial`, "HTTP 400"},
		{"no message", `{"code":"only_code"}`, "HTTP 400"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := usageLogErrorMessage(400, []byte(tc.body)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	if got := usageLogErrorMessage(200, []byte(`{"detail":"ignored"}`)); got != "" {
		t.Fatal(got)
	}
	if got := usageLogFailureMessage(502, "local gateway failure"); got != "local gateway failure" {
		t.Fatal(got)
	}
	if got := usageLogErrorMessage(400, []byte(`{"detail":"Authorization: Bearer sk-fixture-secret-token"}`)); strings.Contains(got, "sk-fixture-secret-token") {
		t.Fatal("secret not masked")
	}
	if got := usageLogErrorMessage(400, []byte(`{"detail":"`+strings.Repeat("x", 2000)+`"}`)); len(got) > 600 {
		t.Fatal("unbounded message")
	}
}

func TestUpstreamErrorDetailsRedactsBeforeTruncation(t *testing.T) {
	secrets := []struct{ text, marker string }{
		{"-----BEGIN PRIVATE KEY-----\nPRIVATEKEYBODYSECRET" + strings.Repeat("A", 800) + "\n-----END PRIVATE KEY-----", "PRIVATEKEYBODYSECRET"},
		{"eyJ" + strings.Repeat("A", 20) + "." + strings.Repeat("B", 800) + "." + strings.Repeat("C", 32), "eyJ"},
	}
	for _, secret := range secrets {
		for _, array := range []bool{false, true} {
			prefix := 500
			if array {
				prefix = 100
			}
			message := strings.Repeat("x", prefix) + " " + secret.text
			var detail any = message
			if array {
				detail = []map[string]string{{"msg": message}}
			}
			body, err := json.Marshal(map[string]any{"detail": detail})
			if err != nil {
				t.Fatal(err)
			}
			got := usageLogErrorMessage(400, body)
			if strings.Contains(got, secret.marker) || strings.Contains(got, "PRIVATE KEY") || len(got) > 600 {
				t.Fatalf("array=%t: secret fragment survived truncation: %q", array, got)
			}
		}
	}
}

func TestUpstreamErrorDetailsDownstream(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	(&Handler{}).sendUpstreamError(c, 400, []byte(`{"detail":"Bad Request"}`))
	if w.Code != 400 || gjson.Get(w.Body.String(), "error.message").String() != "Bad Request" || gjson.Get(w.Body.String(), "error.code").String() != "upstream_400" {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
	}
}
