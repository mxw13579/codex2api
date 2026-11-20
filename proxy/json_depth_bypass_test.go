package proxy

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

const (
	jsonDepthTooDeepMessage      = "Request JSON exceeds the maximum nesting depth of 10000"
	jsonDepthUnverifiableMessage = "Request body is not valid JSON and its nesting depth could not be verified to be within the maximum of 10000"
)

// 审查发现的入口绕过：前导非 JSON 空白字节 + 非 JSON Content-Type 曾让中间件放行，
// handler 不看 Content-Type 照常解析（gjson.Parse/Get 跳过前导字节），随后
// stripImageGenerationToolsFromArray 对工具做无界的 Value 递归。修复后一律在入口 400。
func TestJSONDepthGuardLeadingByteAndContentTypeBypass(t *testing.T) {
	h := newJSONDepthGuardHarness(t)
	const levels = 20000
	schema := `{"type":"object","x":` + deepClosed(levels) + `}`
	bodies := map[string]string{
		"/v1/responses":        `{"model":"gpt-5.5","input":"hi","stream":true,"tools":[{"type":"function","name":"f","parameters":` + schema + `}]}`,
		"/v1/chat/completions": `{"model":"gpt-5.5","stream":true,"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"f","parameters":` + schema + `}}]}`,
		"/v1/messages":         `{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}],"tools":[{"name":"f","input_schema":` + schema + `}]}`,
	}
	leads := []string{"\x0c", "\x00", "\x0b", "\x1f", "\xef\xbb\xbf", "x", "\x0c\x0c ", ""}
	cts := []string{"application/x-www-form-urlencoded", "application/octet-stream", "text/plain", "multipart/form-data; boundary=x", "", "application/json"}
	for path, body := range bodies {
		for _, lead := range leads {
			for _, ct := range cts {
				req, _ := http.NewRequest(http.MethodPost, h.server.URL+path, strings.NewReader(lead+body))
				if ct != "" {
					req.Header.Set("Content-Type", ct)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				respBody, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("%s lead=%q ct=%q: status=%d body=%.200s", path, lead, ct, resp.StatusCode, respBody)
				}
				want := jsonDepthTooDeepMessage
				if lead != "" && lead != "\xef\xbb\xbf" {
					want = jsonDepthUnverifiableMessage
				}
				msg := gjson.GetBytes(respBody, "error.message").String()
				if msg != want {
					t.Fatalf("%s lead=%q ct=%q: message=%q want %q", path, lead, ct, msg, want)
				}
				if path == "/v1/messages" && gjson.GetBytes(respBody, "type").String() != "error" {
					t.Fatalf("anthropic format expected: %s", respBody)
				}
			}
		}
	}
	if calls := h.upstreamCalls.Load(); calls != 0 {
		t.Fatalf("upstream calls = %d, want 0", calls)
	}
}

// 合法的 multipart 上传（文件里随机出现大量 '['/'{'）不被误拒：每个豁免路由一个。
func TestJSONDepthGuardExemptMultipartUploadsPass(t *testing.T) {
	h := newJSONDepthGuardHarness(t)
	binary := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte("[{\x00"), 30000)...)
	build := func(fields map[string]string, fileField string) (string, []byte) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		for k, v := range fields {
			_ = mw.WriteField(k, v)
		}
		if fileField != "" {
			fw, _ := mw.CreateFormFile(fileField, "a.png")
			_, _ = fw.Write(binary)
		}
		_ = mw.Close()
		return mw.FormDataContentType(), buf.Bytes()
	}
	cases := []struct {
		path      string
		fields    map[string]string
		fileField string
	}{
		{"/v1/images/edits", map[string]string{"model": "gpt-image-2", "prompt": "edit"}, "image"},
		{"/images/edits", map[string]string{"model": "gpt-image-2", "prompt": "edit"}, "image"},
		// sdp 字段里塞满括号字节，模拟非 JSON 的大文本字段。
		{"/v1/live", map[string]string{"sdp": string(binary), "session": `{"type":"realtime"}`}, ""},
		{"/live", map[string]string{"sdp": string(binary), "session": `{"type":"realtime"}`}, ""},
		{"/backend-api/codex/realtime/calls", map[string]string{"sdp": string(binary), "session": `{"type":"realtime"}`}, ""},
	}
	for _, tc := range cases {
		ct, body := build(tc.fields, tc.fileField)
		req, _ := http.NewRequest(http.MethodPost, h.server.URL+tc.path, bytes.NewReader(body))
		req.Header.Set("Content-Type", ct)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if gjson.GetBytes(respBody, "error.code").String() == "json_nesting_too_deep" {
			t.Fatalf("%s: legitimate multipart upload was rejected by the depth guard: %.200s", tc.path, respBody)
		}
	}
}

// 第二道防线：入口之外，对实际做 Value 递归展开的子结果做深度预检。
// 这里的子结果是字符串内容被 Result.Get 重新解释成 JSON（入口检查不计字符串内括号）。
func TestValueCallSitesPrecheckDepth(t *testing.T) {
	deep := deepClosed(30000)
	tool := gjson.Parse(`{"type":"function","name":"f","parameters":` + deep + `}`)
	// strip 是策略执行点：过深的对象元素被丢弃，其余照常剥离，绝不原样放行 image_generation。
	imageTool := gjson.Parse(`{"type":"image_generation"}`)
	keptTool := gjson.Parse(`{"type":"function","name":"g"}`)
	kept, removed := stripImageGenerationToolsFromArray([]gjson.Result{imageTool, tool, keptTool})
	if !removed || len(kept) != 1 || gjson.GetBytes(mustJSON(t, kept), "0.name").String() != "g" {
		t.Fatalf("too deep tool must be dropped while stripping continues: removed=%v kept=%v", removed, kept)
	}
	if payloadValueEquals(gjson.Parse(deep), nil) {
		t.Fatalf("too deep value must not match")
	}
	if _, err := convertResponsesToChatRequest([]byte(`{"model":"m","input":"hi","text":{"format":{"type":"json_schema","name":"s","schema":{"a":` + deep + `}}}}`)); err != errGrokJSONTooDeep {
		t.Fatalf("too deep schema must be rejected")
	}
	if _, err := convertResponsesToMessagesRequest([]byte(`{"model":"m","input":"hi","max_output_tokens":8,"temperature":` + deep + `}`)); err != errGrokJSONTooDeep {
		t.Fatalf("too deep field must be rejected")
	}
	if _, err := kiroResponsesToMessages([]byte(`{"model":"m","input":"hi","tool_choice":`+deep+`}`), http.Header{}); err != errKiroJSONTooDeep {
		t.Fatalf("too deep tool_choice must be rejected")
	}
	// 正常深度的转换不受影响。
	out, err := convertResponsesToChatRequest([]byte(`{"model":"m","input":"hi","tools":[{"type":"function","name":"f","parameters":{"type":"object"}}]}`))
	if err != nil || gjson.GetBytes(out, "tools.0.function.parameters.type").String() != "object" {
		t.Fatalf("normal conversion changed: %s err=%v", out, err)
	}
}
