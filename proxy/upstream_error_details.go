package proxy

import (
	"strings"

	"github.com/codex2api/security"
	"github.com/tidwall/gjson"
)

type upstreamErrorFields struct {
	message string
	code    string
	kind    string
}

// 只提取约定的字符串字段；不把 detail 对象、校验器 input/ctx 或整个响应序列化为错误信息。
func extractUpstreamErrorFields(body []byte) upstreamErrorFields {
	if !gjson.ValidBytes(body) {
		return upstreamErrorFields{}
	}
	firstValue := func(allowNumber bool, paths ...string) string {
		for _, path := range paths {
			value := gjson.GetBytes(body, path)
			if value.Type == gjson.String || (allowNumber && value.Type == gjson.Number) {
				if text := strings.TrimSpace(value.String()); text != "" {
					return security.SafeTruncate(security.SanitizeLog(text), 600)
				}
			}
		}
		return ""
	}
	fields := upstreamErrorFields{
		message: firstValue(false, "error.message", "response.error.message", "response.status_details.error.message", "message", "detail.message", "detail.error.message", "detail.msg", "detail", "error"),
		code:    firstValue(true, "error.code", "response.error.code", "response.status_details.error.code", "detail.code", "detail.error.code", "code"),
		kind:    firstValue(false, "error.type", "response.error.type", "response.status_details.error.type", "detail.type", "detail.error.type", "type"),
	}
	// FastAPI/Pydantic 校验错误的 detail 是数组。仅保留前三个 msg，绝不采用 input、ctx、loc。
	if fields.message == "" {
		if details := gjson.GetBytes(body, "detail"); details.IsArray() {
			messages := make([]string, 0, 3)
			details.ForEach(func(_, item gjson.Result) bool {
				if msg := item.Get("msg"); msg.Type == gjson.String && strings.TrimSpace(msg.String()) != "" {
					messages = append(messages, security.SafeTruncate(security.SanitizeLog(strings.TrimSpace(msg.String())), 200))
				}
				return len(messages) < 3
			})
			fields.message = strings.Join(messages, "; ")
		}
	}
	if fields.kind == "error" {
		fields.kind = ""
	}
	return fields
}
