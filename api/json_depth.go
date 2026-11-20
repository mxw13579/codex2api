package api

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/codex2api/internal/jsondepth"
	"github.com/codex2api/security"
)

// ErrCodeJSONNestingTooDeep 表示请求体 JSON 嵌套深度超过上限，或请求体不是合法 JSON 且
// 无法确认其嵌套深度在上限内（两种情况文案不同，错误码相同）。
const ErrCodeJSONNestingTooDeep ErrorCode = "json_nesting_too_deep"

// NewJSONNestingTooDeepError 返回合法 JSON 前缀内真实超深时的错误（OpenAI 风格错误体；
// WebSocket 路径也用它构造错误帧）。
func NewJSONNestingTooDeepError() *APIError {
	return NewJSONDepthError(jsondepth.StatusTooDeep)
}

// NewJSONDepthError 按 jsondepth.Check 的结论构造错误。
func NewJSONDepthError(status jsondepth.Status) *APIError {
	return NewAPIError(ErrCodeJSONNestingTooDeep, JSONDepthErrorMessage(status), ErrorTypeInvalidRequest)
}

// JSONDepthErrorMessage 返回对应拒绝原因的文案。
func JSONDepthErrorMessage(status jsondepth.Status) string {
	if status == jsondepth.StatusUnverifiable {
		return fmt.Sprintf("Request body is not valid JSON and its nesting depth could not be verified to be within the maximum of %d", jsondepth.MaxDepth())
	}
	return fmt.Sprintf("Request JSON exceeds the maximum nesting depth of %d", jsondepth.MaxDepth())
}

// JSONDepthGuardMiddleware 在请求体已确定（RequestSizeLimiter 读取、Decompressor 解压、
// BodyCache 缓存之后）且任何 JSON 解析之前检查嵌套深度。gjson 的校验/解析是无深度上限的
// 递归，超深输入会让 goroutine 栈溢出并使整个进程退出，因此必须在入口统一拦截。
//
// 默认检查所有非空请求体，与 Content-Type 和首字节无关：多数 handler 不看 Content-Type，
// 而 gjson.Parse/Get 会跳过任意前导字节去找第一个 '{'/'['。只豁免导入端点（不缓存 body，
// 由 handler 流式解析 multipart）与 jsonDepthExemptUpload 列出的、确实按 multipart 解析
// 请求体的路由。
//
// 检查是线性的、不分配内存；未通过返回 400。/v1/messages 与 /messages 按 Anthropic 错误
// 格式返回（与该 handler 自己的“Invalid JSON”错误一致），其余路径用 OpenAI 风格。
func JSONDepthGuardMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request == nil || security.IsLargeImportUploadPath(c.Request.URL.Path) {
			c.Next()
			return
		}
		body := GetRawBody(c)
		if len(body) == 0 || jsonDepthExemptUpload(c.Request.Method, c.Request.URL.Path, c.GetHeader("Content-Type")) {
			c.Next()
			return
		}
		status := jsondepth.Check(body)
		if status == jsondepth.StatusOK {
			c.Next()
			return
		}
		requestID := c.Writer.Header().Get("X-Request-ID")
		if len(requestID) > 128 {
			requestID = requestID[:128]
		}
		reason := "too_deep"
		if status == jsondepth.StatusUnverifiable {
			reason = "unverifiable"
		}
		log.Printf("[json-depth] 请求体 JSON 嵌套检查未通过 (reason=%s limit=%d request_id=%s route=%s bytes=%d)",
			reason, jsondepth.MaxDepth(), requestID, c.FullPath(), len(body))
		if isAnthropicMessagesPath(c.Request.URL.Path) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"type": "error",
				"error": gin.H{
					"type":    string(ErrorTypeInvalidRequest),
					"message": JSONDepthErrorMessage(status),
				},
			})
			return
		}
		c.AbortWithStatusJSON(http.StatusBadRequest, ErrorResponse{Error: *NewJSONDepthError(status)})
	}
}

// jsonDepthExemptUploads 是确实按 multipart/form-data 解析请求体的路由。这些 handler 在
// multipart 下只用 net/http 的表单解析读取字段与文件，不把 raw body 或其子串交给 gjson：
//   - 图片编辑：字段经 sjson 写入新构造的 JSON；raw body 仅在 NewAPI 签名时作为 ingress
//     原文保存，其后的 JSON 解析点（Prompt Filter envelope 等）用的是带深度预检的
//     jsondepth.ValidBytes，multipart 原文不是合法 JSON 会被跳过。
//   - Live 创建：session 字段经 encoding/json 的 json.Valid/Unmarshal（自带 10000 层上限）。
//   - 管理端背景上传：文件直接落盘，不解析。
//
// 二进制文件里随机出现的 '['/'{' 可能超过 1 万个，不能对这些上传做深度检查。
var jsonDepthExemptUploads = map[string]bool{
	"/v1/images/edits":                      true,
	"/images/edits":                         true,
	"/v1/live":                              true,
	"/live":                                 true,
	"/backend-api/codex/realtime/calls":     true,
	"/api/admin/settings/background-upload": true,
}

func jsonDepthExemptUpload(method, path, contentType string) bool {
	if method != http.MethodPost {
		return false
	}
	if !jsonDepthExemptUploads[strings.TrimRight(path, "/")] {
		return false
	}
	// 与 handler 的判定方式一致（images/live 都用小写前缀匹配）；不 TrimSpace，
	// 与 live 的判定逐字一致（images 额外 TrimSpace，带前导空格时这里会多检查一次，偏保守）。
	return strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data")
}

func isAnthropicMessagesPath(path string) bool {
	path = strings.TrimRight(path, "/")
	return path == "/v1/messages" || path == "/messages"
}
