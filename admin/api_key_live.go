package admin

import "github.com/codex2api/database"

// 创建时区分未配置和显式关闭；编辑仍使用已有的完整 limits 语义。
type createAPIKeyLimits struct {
	database.APIKeyLimits
	AllowLive *bool `json:"allow_live"`
}

func (in *createAPIKeyLimits) withDefaults() database.APIKeyLimits {
	limits := database.APIKeyLimits{}
	if in != nil {
		limits = in.APIKeyLimits
	}
	channel := limits.ResolveUpstreamChannel()
	limits.AllowLive = channel == database.UpstreamChannelAuto || channel == database.UpstreamChannelCodex
	if in != nil && in.AllowLive != nil {
		limits.AllowLive = *in.AllowLive
	}
	return limits
}
