package proxy

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// 深度错误的格式必须与同一路径上 handler 自己对“非法 JSON”的实际返回一致：
// Messages 为 Anthropic 格式，count_tokens 与其余路径为 OpenAI 风格。
func TestJSONDepthErrorFormatMatchesHandlerInvalidJSONFormat(t *testing.T) {
	h := newJSONDepthGuardHarness(t)
	post := func(path, body string) []byte {
		t.Helper()
		resp, err := http.Post(h.server.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status=%d", path, resp.StatusCode)
		}
		data, _ := io.ReadAll(resp.Body)
		return data
	}
	isAnthropic := func(b []byte) bool {
		return gjson.GetBytes(b, "type").String() == "error" && gjson.GetBytes(b, "error.type").Exists() && !gjson.GetBytes(b, "error.code").Exists()
	}
	deep := `{"messages":` + strings.Repeat("[", 20000)
	for _, path := range []string{"/v1/messages", "/messages", "/v1/messages/count_tokens", "/messages/count_tokens"} {
		invalid := post(path, `{"messages":[`) // 浅层非法 JSON：由 handler 自己拒绝
		depth := post(path, deep)              // 超深：由入口中间件拒绝
		if isAnthropic(invalid) != isAnthropic(depth) {
			t.Fatalf("%s: handler invalid-JSON format (anthropic=%v %s) differs from depth error format (anthropic=%v %s)",
				path, isAnthropic(invalid), invalid, isAnthropic(depth), depth)
		}
		wantAnthropic := !strings.HasSuffix(path, "/count_tokens")
		if isAnthropic(depth) != wantAnthropic {
			t.Fatalf("%s: depth error anthropic=%v want %v: %s", path, isAnthropic(depth), wantAnthropic, depth)
		}
	}
	if calls := h.upstreamCalls.Load(); calls != 0 {
		t.Fatalf("upstream calls = %d", calls)
	}
}
