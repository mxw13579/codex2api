package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 非法字节位于 messages 之前、messages 为超深嵌套时，入口的全文校验在浅层即返回，
// 校验阶段不得再对 messages 做递归式合法性检查（gjson.Valid 无深度上限，会栈溢出
// 并使整个进程崩溃）。期望与改动前完全一致：400 missing_message_role，不发上游。
func TestChatCompletionsDeepMessagesAfterInvalidPrefixDoesNotOverflow(t *testing.T) {
	h, up, _ := newPinnedCodexHandler(t, nil, func(w http.ResponseWriter, _ *http.Request) { writePinnedSuccessSSE(w) })
	body := `{"model":"gpt-5.5","x":01,"messages":` + strings.Repeat("[", 16<<20)
	r := httptest.NewRecorder()
	h.ChatCompletions(pinnedTestContext(r, "/v1/chat/completions", body, 7, 2))

	const want = `{"error":{"code":"invalid_parameter","message":"Message at index 0 is missing 'role' field","type":"invalid_request_error","details":{"field":"messages.0.role","message":"Message at index 0 is missing 'role' field","code":"missing_message_role"}}}`
	if r.Code != http.StatusBadRequest || strings.TrimSpace(r.Body.String()) != want {
		t.Fatalf("status=%d body=%s, want 400 %s", r.Code, r.Body.String(), want)
	}
	if calls := up.calls.Load(); calls != 0 {
		t.Fatalf("upstream calls = %d, want 0", calls)
	}
}
