package proxy

import (
	"strings"

	"github.com/codex2api/database"
)

// applyOfficialImagePricingRow 只读取受支持图片模型，分别合并 Image/Text 行。
func applyOfficialImagePricingRow(out map[string]database.ModelPricingOverride, row officialPricingRow) {
	model := database.GPTImage25BillingModel(normalizeOfficialPricingModel(row.cell("model")))
	if model == "" {
		return
	}
	price := out[model]
	switch strings.ToLower(row.cell("modality")) {
	case "image":
		price.ImageInput, price.CachedImageInput, price.Output = row.price("input"), row.price("cached input"), row.price("output")
	case "text":
		price.Input, price.CachedInput = row.price("input"), row.price("cached input")
	default:
		return
	}
	out[model] = price
}

// projectImageResolutionPricing 为独立可编辑的分辨率变体继承基础模型 token 价。
// 官方没有按输出分辨率区分 token 单价；精确模型价格和用户自定义价仍优先。
func projectImageResolutionPricing(allowed map[string]struct{}, parsed map[string]database.ModelPricingOverride) {
	base, exists := parsed["gpt-image-2"]
	if !exists {
		return
	}
	for _, model := range []string{"gpt-image-2-2k", "gpt-image-2-4k"} {
		if _, enabled := allowed[model]; !enabled {
			continue
		}
		if _, exact := parsed[model]; !exact {
			parsed[model] = base
		}
	}
}
