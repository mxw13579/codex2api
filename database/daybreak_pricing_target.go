package database

import (
	"regexp"
	"strings"
)

var cyberPricingID = regexp.MustCompile(`^(gpt-5\.[56]-cyber)(?:-\d{4}-\d{2}-\d{2})?(?:-(?:none|minimal|low|medium|high|xhigh|max|ultra))?(?:\([^()]+\))?$`)

// DaybreakPricingTarget 返回本地派生别名的原始基础模型，不把未知型号猜成其他型号。
// latest 的默认快照依据官方模型页，仅作为未解析别名时的兜底。
func DaybreakPricingTarget(model string) string {
	normalized := normalizeBillingModelName(model)
	for _, suffix := range []string{"-daybreak-blue", "-daybreak-red"} {
		if strings.HasSuffix(strings.ToLower(strings.TrimSpace(model)), suffix) {
			return PricingManagementModelKey(normalized)
		}
	}
	switch normalized {
	case "gpt-daybreak-blue-latest":
		return "gpt-5.6-sol"
	case "gpt-daybreak-red-latest":
		return "gpt-5.6-cyber"
	default:
		return ""
	}
}

// daybreakCanonicalModel 将已公布的 Cyber 型号及 latest 默认快照与 Sol 区分。
func daybreakCanonicalModel(model string) string {
	switch model {
	case "gpt-daybreak-blue-latest":
		return "gpt-5.6-sol"
	case "gpt-daybreak-red-latest":
		return "gpt-5.6-cyber"
	}
	if match := cyberPricingID.FindStringSubmatch(model); len(match) == 2 {
		return match[1]
	}
	return ""
}

// cyberDefaultPricing 使用官方独立价目；不补造未提供的长上下文档位。
// https://developers.openai.com/api/docs/pricing （2026-10-22）
func cyberDefaultPricing(model string) *ModelPricing {
	if model != "gpt-5.6-cyber" && model != "gpt-5.5-cyber" {
		return nil
	}
	return &ModelPricing{InputPricePerMToken: 12.5, CacheReadPricePerMToken: 1.25, OutputPricePerMToken: 75}
}
