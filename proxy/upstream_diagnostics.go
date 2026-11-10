package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/security"
	"github.com/tidwall/gjson"
)

const (
	upstreamDiagnosticBodyLimit    = 8 << 10
	upstreamDiagnosticRequestLimit = 4 << 20
)

var diagnosticCodexModelPattern = regexp.MustCompile(`^gpt-[0-9]+(?:\.[0-9]+)?(?:-(?:astra|sol|terra|luna|mini|nano|codex|spark|pro|preview|turbo|thinking|chat|latest|medium|high|low|[0-9]{4,8}))*$`)

// 正文中的标签不享有 Request ID 的 UUID 豁免。已知枚举可读，未知值只存哈希。
func diagnosticBodyLabel(value, kind string) string {
	if value == "" || len(value) > 128 || security.SanitizeLog(value) != value {
		return ""
	}
	if kind == "model" {
		if diagnosticCodexModelPattern.MatchString(value) {
			return value
		}
		for _, info := range builtinModelInfos {
			if info.ID == value {
				return value
			}
		}
	} else {
		switch value {
		case "invalid_request_error", "invalid_argument", "invalid_value", "unsupported_value", "unsupported_parameter",
			"authentication_error", "permission_error", "forbidden_error", "rate_limit_error", "rate_limit_exceeded",
			"context_length_exceeded", "invalid_encrypted_content", "insufficient_quota", "invalid_api_key",
			"token_expired", "deactivated_workspace", "model_not_found", "not_found_error", "server_error",
			"server_is_overloaded", "overloaded_error", "content_policy_violation", "invalid_prompt":
			return value
		}
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// 只含白名单元数据，不保存请求正文、工具参数、会话标识、Cookie 或任意响应头。
type upstreamRequestSummary struct {
	Bytes               int    `json:"bytes"`
	Omitted             string `json:"omitted,omitempty"`
	Model               string `json:"model,omitempty"`
	ServiceTier         string `json:"service_tier,omitempty"`
	ReasoningEffort     string `json:"reasoning_effort,omitempty"`
	Stream              bool   `json:"stream"`
	InputType           string `json:"input_type,omitempty"`
	InputItems          int    `json:"input_items"`
	Messages            int    `json:"messages"`
	Tools               int    `json:"tools"`
	HasPreviousResponse bool   `json:"has_previous_response_id"`
	HasInstructions     bool   `json:"has_instructions"`
}

type upstreamHTTPDiagnostic struct {
	DroppedTotal         uint64                 `json:"dropped_total"`
	Time                 string                 `json:"time"`
	RequestID            string                 `json:"request_id,omitempty"`
	UpstreamRequestID    string                 `json:"upstream_request_id,omitempty"`
	CFRay                string                 `json:"cf_ray,omitempty"`
	AccountID            int64                  `json:"account_id"`
	Status               int                    `json:"status"`
	Transport            string                 `json:"transport"`
	ContentType          string                 `json:"content_type,omitempty"`
	DeclaredBytes        int64                  `json:"declared_bytes"`
	ReadBytes            int64                  `json:"read_bytes"`
	BodyComplete         bool                   `json:"body_complete"`
	BodyCaptureTruncated bool                   `json:"body_capture_truncated"`
	ReadError            string                 `json:"read_error,omitempty"`
	ClosedEarly          bool                   `json:"closed_early"`
	BodyFormat           string                 `json:"body_format"`
	BodyPrefixSHA256     string                 `json:"body_prefix_sha256"`
	ErrorCode            string                 `json:"error_code,omitempty"`
	ErrorType            string                 `json:"error_type,omitempty"`
	Request              upstreamRequestSummary `json:"request"`
}

func summarizeUpstreamRequest(body []byte) upstreamRequestSummary {
	s := upstreamRequestSummary{Bytes: len(body)}
	if len(body) > upstreamDiagnosticRequestLimit {
		s.Omitted = "too_large"
		return s
	}
	if !gjson.ValidBytes(body) {
		s.Omitted = "not_json"
		return s
	}
	s.Model = diagnosticBodyLabel(gjson.GetBytes(body, "model").String(), "model")
	// 不保存任意字符串，即便客户端把正文塞进本应是枚举的字段。
	switch tier := gjson.GetBytes(body, "service_tier").String(); tier {
	case "auto", "default", "flex", "priority", "fast", "ultrafast":
		s.ServiceTier = tier
	}
	switch effort := gjson.GetBytes(body, "reasoning.effort").String(); effort {
	case "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra":
		s.ReasoningEffort = effort
	}
	s.Stream = gjson.GetBytes(body, "stream").Bool()
	s.HasPreviousResponse = gjson.GetBytes(body, "previous_response_id").Exists()
	s.HasInstructions = gjson.GetBytes(body, "instructions").Exists()
	input := gjson.GetBytes(body, "input")
	switch {
	case !input.Exists():
		s.InputType = "absent"
	case input.IsArray():
		s.InputType = "array"
		s.InputItems = int(gjson.GetBytes(body, "input.#").Int())
	case input.IsObject():
		s.InputType = "object"
	default:
		s.InputType = input.Type.String()
	}
	s.Messages = int(gjson.GetBytes(body, "messages.#").Int())
	s.Tools = int(gjson.GetBytes(body, "tools.#").Int())
	return s
}

// 优先用执行器最终定稿的明文（压缩前）；其余调用只读取可独立重开的、未压缩请求体。
// 不读取正在发送的 req.Body，不重放网络请求，且任何诊断失败都不影响业务结果。
func diagnosticRequestSummary(req *http.Request, plaintext [][]byte) upstreamRequestSummary {
	if len(plaintext) > 0 && plaintext[0] != nil {
		return summarizeUpstreamRequest(plaintext[0])
	}
	if req.Header.Get("Content-Encoding") != "" {
		return upstreamRequestSummary{Omitted: "encoded_body"}
	}
	if req.GetBody == nil {
		return upstreamRequestSummary{Omitted: "not_replayable"}
	}
	body, err := req.GetBody()
	if err != nil {
		return upstreamRequestSummary{Omitted: "unavailable"}
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, upstreamDiagnosticRequestLimit+1))
	if err != nil {
		return upstreamRequestSummary{Omitted: "read_error"}
	}
	return summarizeUpstreamRequest(data)
}

func wrapUpstreamHTTPDiagnostic(req *http.Request, resp *http.Response, account *auth.Account, plaintext ...[]byte) *upstreamDiagnosticBody {
	if req == nil || resp == nil || resp.StatusCode < 400 || resp.Body == nil || security.FileLogsDisabled() {
		return nil
	}
	d := upstreamHTTPDiagnostic{
		Time:              time.Now().UTC().Format(time.RFC3339Nano),
		RequestID:         cookieLogRequestID(req.Context()),
		UpstreamRequestID: upstreamResponseRequestID(resp.Header, account.GetUpstreamRequestIDHeader()),
		CFRay:             safeUpstreamTraceID(resp.Header.Get("Cf-Ray")),
		Status:            resp.StatusCode, Transport: "http", DeclaredBytes: resp.ContentLength,
		Request: diagnosticRequestSummary(req, plaintext),
	}
	if account != nil {
		d.AccountID = account.ID()
	}
	if mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err == nil {
		// 丢弃可能含任意字符串的 boundary 等参数。
		switch mediaType {
		case "application/json", "application/problem+json", "text/plain", "text/html", "text/event-stream", "application/octet-stream":
			d.ContentType = mediaType
		default:
			d.ContentType = "other"
		}
	}
	body := &upstreamDiagnosticBody{ReadCloser: resp.Body, ctx: req.Context(), diagnostic: d, emit: emitUpstreamHTTPDiagnostic}
	resp.Body = body
	return body
}

func emitUpstreamHTTPDiagnostic(d upstreamHTTPDiagnostic) {
	upstreamDiagnosticLogger.enqueue(d)
}

type upstreamDiagnosticBody struct {
	io.ReadCloser
	ctx        context.Context
	mu         sync.Mutex
	prefix     []byte
	diagnostic upstreamHTTPDiagnostic
	emitted    bool
	emit       func(upstreamHTTPDiagnostic)
}

// upstreamErrorMetadata is response-owned, immutable correlation. Do not look
// up a.current here: retries or WS turns may advance before the body is logged.
func (b *upstreamDiagnosticBody) upstreamErrorMetadata() upstreamErrorLogMetadata {
	b.mu.Lock()
	defer b.mu.Unlock()
	return upstreamErrorLogMetadata{
		accountID:         b.diagnostic.AccountID,
		requestID:         b.diagnostic.RequestID,
		upstreamRequestID: b.diagnostic.UpstreamRequestID,
		cfRay:             b.diagnostic.CFRay,
	}
}

// 不进行额外的响应读取：仅观察业务实际读取的字节，保留原 n/err 和 Close 语义。
func (b *upstreamDiagnosticBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	if b.emitted {
		b.mu.Unlock()
		return n, err
	}
	b.diagnostic.ReadBytes += int64(n)
	keep := min(n, upstreamDiagnosticBodyLimit-len(b.prefix))
	if keep > 0 {
		b.prefix = append(b.prefix, p[:keep]...)
	}
	if b.diagnostic.ReadBytes > upstreamDiagnosticBodyLimit {
		b.diagnostic.BodyCaptureTruncated = true
	}
	if err == io.EOF {
		b.diagnostic.BodyComplete = true
	} else if err != nil {
		b.diagnostic.ReadError = diagnosticReadError(err)
	}
	b.mu.Unlock()
	// EOF/读取失败即出日志，不依赖调用者之后是否正确 Close。
	if err != nil {
		b.finish(false)
	}
	return n, err
}

func diagnosticReadError(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	default:
		return "read_error" // 错误文本可能含代理凭据或回显正文。
	}
}

func (b *upstreamDiagnosticBody) Close() error {
	err := b.ReadCloser.Close()
	b.finish(true)
	return err
}

func (b *upstreamDiagnosticBody) finish(closed bool) {
	b.mu.Lock()
	if b.emitted {
		b.mu.Unlock()
		return
	}
	b.emitted = true
	b.diagnostic.ClosedEarly = closed && !b.diagnostic.BodyComplete
	if b.diagnostic.ClosedEarly && b.diagnostic.ReadError == "" && b.ctx != nil && b.ctx.Err() != nil {
		b.diagnostic.ReadError = diagnosticReadError(b.ctx.Err())
	}
	b.diagnostic.BodyFormat = "non_json"
	if len(b.prefix) == 0 {
		b.diagnostic.BodyFormat = "empty"
	} else if gjson.ValidBytes(b.prefix) {
		b.diagnostic.BodyFormat = "json"
	}
	fields := extractUpstreamErrorFields(b.prefix)
	b.diagnostic.ErrorCode = diagnosticBodyLabel(fields.code, "error")
	b.diagnostic.ErrorType = diagnosticBodyLabel(fields.kind, "error")
	sum := sha256.Sum256(b.prefix)
	b.diagnostic.BodyPrefixSHA256 = hex.EncodeToString(sum[:])
	// 不再复制原始错误正文到新日志；原有 bad_request.log 保留正文。本文件只保存关联/读取元数据。
	d := b.diagnostic
	b.prefix = nil
	b.mu.Unlock()
	b.emit(d)
}

func (b *upstreamDiagnosticBody) observeUpstreamCookieEvent(payload []byte) {
	forwardUpstreamCookieEvent(b.ReadCloser, payload)
}
