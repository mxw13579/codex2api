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

// ErrCodeJSONNestingTooDeep 表示请求体 JSON 嵌套深度超过上限。
const ErrCodeJSONNestingTooDeep ErrorCode = "json_nesting_too_deep"

// NewJSONNestingTooDeepError 返回统一的“嵌套过深”错误（OpenAI 风格错误体；
// WebSocket 路径也用它构造错误帧）。
func NewJSONNestingTooDeepError() *APIError {
	return NewAPIError(ErrCodeJSONNestingTooDeep, JSONNestingTooDeepMessage(), ErrorTypeInvalidRequest)
}

// JSONNestingTooDeepMessage 是嵌套过深错误的文案。
func JSONNestingTooDeepMessage() string {
	return fmt.Sprintf("Request JSON exceeds the maximum nesting depth of %d", jsondepth.MaxDepth())
}

// JSONDepthGuardMiddleware 在请求体已确定（RequestSizeLimiter 读取、Decompressor 解压、
// BodyCache 缓存之后）且任何 JSON 解析之前检查嵌套深度。gjson 的校验/解析是无深度上限的
// 递归，超深输入会让 goroutine 栈溢出并使整个进程退出，因此必须在入口统一拦截。
//
// 检查是线性的、不分配内存；超限返回 400。/v1/messages 与 /messages 按 Anthropic 错误
// 格式返回（与该 handler 自己的“Invalid JSON”错误一致），其余路径用 OpenAI 风格。
func JSONDepthGuardMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request == nil || security.IsLargeImportUploadPath(c.Request.URL.Path) {
			c.Next()
			return
		}
		body := GetRawBody(c)
		if len(body) == 0 || !shouldCheckJSONDepth(c.ContentType(), body) || !jsondepth.TooDeep(body) {
			c.Next()
			return
		}
		requestID := c.Writer.Header().Get("X-Request-ID")
		if len(requestID) > 128 {
			requestID = requestID[:128]
		}
		log.Printf("[json-depth] 请求体 JSON 嵌套超过上限 %d，已拒绝 (request_id=%s route=%s bytes=%d)",
			jsondepth.MaxDepth(), requestID, c.FullPath(), len(body))
		if isAnthropicMessagesPath(c.Request.URL.Path) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"type": "error",
				"error": gin.H{
					"type":    string(ErrorTypeInvalidRequest),
					"message": JSONNestingTooDeepMessage(),
				},
			})
			return
		}
		c.AbortWithStatusJSON(http.StatusBadRequest, ErrorResponse{Error: *NewJSONNestingTooDeepError()})
	}
}

// shouldCheckJSONDepth 决定是否检查请求体。多数 handler 不看 Content-Type 直接按 JSON
// 解析 raw body，所以只有明确的非 JSON 类型（multipart、表单、二进制）才跳过，并且即使是
// 这些类型，首个非空白字节为 '{' 或 '[' 时仍然检查，防止伪造 Content-Type 绕过。
func shouldCheckJSONDepth(contentType string, body []byte) bool {
	if startsLikeJSONContainer(body) {
		return true
	}
	ct := strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case strings.Contains(ct, "json"):
		return true
	case strings.HasPrefix(ct, "multipart/"),
		ct == "application/x-www-form-urlencoded",
		ct == "application/octet-stream",
		strings.HasPrefix(ct, "image/"),
		strings.HasPrefix(ct, "audio/"),
		strings.HasPrefix(ct, "video/"):
		return false
	}
	return true
}

func startsLikeJSONContainer(body []byte) bool {
	for _, c := range body {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}

func isAnthropicMessagesPath(path string) bool {
	path = strings.TrimRight(path, "/")
	return path == "/v1/messages" || path == "/messages"
}
