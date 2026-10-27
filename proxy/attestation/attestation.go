// Package attestation 统一 Codex 桌面协议的证明生成与失败状态。
package attestation

import "context"

const (
	HeaderName = "X-Oai-Attestation"
	BundleID   = "com.openai.codex"
	Version    = 1

	PlatformWindows = "win32"
	PlatformMacOS   = "darwin"

	UnsupportedPlatform     = 1
	UnsupportedArchitecture = 2
	UnsupportedDevice       = 3
	GenerationFailed        = 4
)

// Signals 是各平台共用的账号设备画像，不包含真实硬件证明。
type Signals struct {
	Languages     []string
	Locale        string
	Timezone      string
	ScreenSizeSum uint64
	ScreenScale   float64
	SessionID     string
}

type Request struct {
	Platform string
	Arch     string
	Signals  Signals
}

// NativeResult 保留原生 supported 与 token 缺失的区别，对应错误码 3 和 4。
type NativeResult struct {
	Supported   bool     `json:"supported"`
	TokenBase64 *string  `json:"tokenBase64,omitempty"`
	LatencyMS   *float64 `json:"latencyMs,omitempty"`
}

type NativeGenerator func(context.Context) (NativeResult, error)

type Service struct {
	native NativeGenerator
}

func NewService(native NativeGenerator) *Service {
	if native == nil {
		native = generateNative
	}
	return &Service{native: native}
}

type tokenResult struct {
	token     *string
	errorCode uint64
	latencyMS *float64
}

// Generate 跟随出站平台；Linux 等平台默认不启用自动证明。
func (s *Service) Generate(ctx context.Context, request Request) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var result tokenResult
	switch request.Platform {
	case PlatformWindows:
		result.errorCode = UnsupportedPlatform
	case PlatformMacOS:
		result = s.generateMacOS(ctx, request.Arch)
	default:
		return "", nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return encodeEnvelope(result, request.Signals), nil
}

func (s *Service) generateMacOS(ctx context.Context, arch string) tokenResult {
	if arch != "arm64" {
		return tokenResult{errorCode: UnsupportedArchitecture}
	}
	native, err := s.native(ctx)
	if err != nil {
		return tokenResult{errorCode: GenerationFailed}
	}
	result := tokenResult{token: native.TokenBase64, latencyMS: native.LatencyMS}
	switch {
	case !native.Supported:
		result.token, result.errorCode = nil, UnsupportedDevice
	case native.TokenBase64 == nil:
		result.errorCode = GenerationFailed
	}
	return result
}
