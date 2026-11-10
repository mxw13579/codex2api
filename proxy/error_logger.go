package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/codex2api/security"
)

// fileLogger 单个日志文件实例
type fileLogger struct {
	once   sync.Once
	logger *log.Logger
	file   *os.File
	path   string
}

var (
	badRequestLogger  = &fileLogger{path: "bad_request.log"}  // 400 错误
	serverErrorLogger = &fileLogger{path: "server_error.log"} // 5xx 错误
)

const defaultLogDir = "logs"
const upstreamErrorLogBodyMaxBytes = 8 * 1024

func upstreamErrorLogBody(body []byte) string {
	return sanitizedUpstreamErrorLogBody(body, upstreamErrorLogBodyMaxBytes, 5000, security.MaskSensitiveData)
}

// 先处理完整的有界错误体，再截取日志前缀；否则跨截断点的 JWT/PEM 不能被完整模式识别。
func sanitizedUpstreamErrorLogBody(body []byte, byteLimit, runeLimit int, sanitize func(string) string) string {
	if len(body) > maxUpstreamErrorBodyBytes {
		return fmt.Sprintf("[oversized upstream body omitted; %d bytes] ... [truncated]", len(body))
	}
	if len(strings.TrimSpace(string(body))) > 0 && !json.Valid(body) {
		return fmt.Sprintf("[non-JSON upstream body omitted; %d bytes]", len(body))
	}
	bodyStr := sanitize(string(body))
	truncated := len(bodyStr) > byteLimit
	if truncated {
		bodyStr = bodyStr[:byteLimit]
	}
	limited := security.SafeTruncate(bodyStr, runeLimit)
	truncated = truncated || len(limited) < len(bodyStr)
	if truncated {
		limited += " ... [truncated]"
	}
	return limited
}

func errorLogDir() string {
	if dir := strings.TrimSpace(os.Getenv("LOG_DIR")); dir != "" {
		return dir
	}
	return defaultLogDir
}

func (fl *fileLogger) init() *log.Logger {
	if security.FileLogsDisabled() {
		return nil
	}
	fl.once.Do(func() {
		dir := errorLogDir()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("创建日志目录失败: %v", err)
			return
		}
		f, err := os.OpenFile(filepath.Join(dir, fl.path), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Printf("打开日志文件 %s 失败: %v", fl.path, err)
			return
		}
		fl.file = f
		fl.logger = log.New(f, "", 0)
	})
	return fl.logger
}

func (fl *fileLogger) close() {
	if fl.file != nil {
		if err := fl.file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "关闭日志文件 %s 失败: %v\n", fl.path, err)
		}
	}
}

type upstreamErrorLogMetadata struct {
	accountID         int64
	requestID         string
	upstreamRequestID string
	cfRay             string
}

func upstreamErrorMetadataFromBody(body io.ReadCloser) upstreamErrorLogMetadata {
	if source, ok := body.(interface {
		upstreamErrorMetadata() upstreamErrorLogMetadata
	}); ok {
		return source.upstreamErrorMetadata()
	}
	return upstreamErrorLogMetadata{}
}

// upstreamErrorLogCorrelation links the existing redacted response-body log to
// usage_logs and upstream_errors.jsonl. Only validated IDs from this response
// are emitted. An unknown body wrapper fails closed instead of consulting a
// mutable request-wide attempt or copying arbitrary response headers.
func upstreamErrorLogCorrelation(resp *http.Response, accountID int64) string {
	if resp == nil {
		return ""
	}
	metadata := upstreamErrorMetadataFromBody(resp.Body)
	requestID := safeUpstreamTraceID(metadata.requestID)
	upstreamID, cfRay := "", ""
	if metadata.accountID == accountID {
		upstreamID = safeUpstreamTraceID(metadata.upstreamRequestID)
		cfRay = safeUpstreamTraceID(metadata.cfRay)
	}
	var out strings.Builder
	for _, field := range []struct{ name, value string }{
		{"Request-ID", requestID}, {"Upstream-Request-ID", upstreamID}, {"CF-Ray", cfRay},
	} {
		if field.value != "" {
			fmt.Fprintf(&out, "%s: %s\n", field.name, field.value)
		}
	}
	return out.String()
}

// writeEntry 写一条错误日志（自动脱敏敏感信息）
func (fl *fileLogger) writeEntry(endpoint string, statusCode int, model string, accountID int64, body []byte, responses ...*http.Response) {
	l := fl.init()
	if l == nil {
		return
	}

	// 脱敏日志内容
	safeEndpoint := security.SanitizeLog(endpoint)
	safeModel := security.SanitizeLog(model)
	bodyStr := upstreamErrorLogBody(body)

	correlation := ""
	if len(responses) > 0 {
		correlation = upstreamErrorLogCorrelation(responses[0], accountID)
	}
	ts := time.Now().Format("2006/01/02 15:04:05")
	l.Printf("========== %s ==========\nEndpoint: %s\nStatus: %d\nModel: %s\nAccount: %d\n%sResponse:\n%s\n",
		ts, safeEndpoint, statusCode, safeModel, accountID, correlation, bodyStr)
}

// logUpstreamError 根据状态码分发到对应日志文件
func logUpstreamError(endpoint string, statusCode int, model string, accountID int64, body []byte, responses ...*http.Response) {
	switch {
	case statusCode == 400:
		badRequestLogger.writeEntry(endpoint, statusCode, model, accountID, body, responses...)
	case statusCode >= 500:
		serverErrorLogger.writeEntry(endpoint, statusCode, model, accountID, body, responses...)
	}
}

// CloseErrorLogger 关闭所有错误日志文件（程序退出时调用）
func CloseErrorLogger() {
	badRequestLogger.close()
	serverErrorLogger.close()
	upstreamDiagnosticLogger.close(time.Second)
}
