package proxy

import "github.com/codex2api/database"

// projectDaybreakPricing 按底层型号匹配各来源，不要求 LiteLLM 收录本地访问计划别名。
// 精确别名价目优先；未知基础型号继续回退项目默认价，不借用其他型号的同步价。
func projectDaybreakPricing(allowed map[string]struct{}, prices map[string]database.ModelPricingOverride) {
	for model := range allowed {
		if _, exact := prices[model]; exact {
			continue
		}
		target := database.DaybreakPricingTarget(model)
		if target == "" || target == model {
			continue
		}
		if price, exists := prices[target]; exists {
			prices[model] = price
		}
	}
}
