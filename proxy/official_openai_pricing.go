package proxy

import (
	"fmt"

	"github.com/codex2api/database"
)

const openAIPricingLongContextThreshold = 272000

// ParseOpenAIOfficialPricingHTML 只读取 HTML 命名列及明确的 Standard/Fast 档位。
func ParseOpenAIOfficialPricingHTML(body []byte) (map[string]database.ModelPricingOverride, error) {
	tables, err := parseOfficialPricingHTML(body)
	if err != nil {
		return nil, err
	}
	out := make(map[string]database.ModelPricingOverride)
	for _, table := range tables {
		if table.tier != "standard" && table.tier != "fast" && table.tier != "priority" {
			continue
		}
		if err := parseOpenAIPricingTable(table, out); err != nil {
			return nil, err
		}
	}
	if err := openAIPricingIslands(body, out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("未找到格式有效的 OpenAI HTML 价格表")
	}
	return out, nil
}

// parseOpenAIPricingTable 通过列名区分文本和图片价格，排除训练费及其他单位。
func parseOpenAIPricingTable(table officialHTMLPricingTable, out map[string]database.ModelPricingOverride) error {
	for _, row := range table.rows {
		model := normalizeOfficialPricingModel(row.cell("model"))
		if model == "" || row.cell("training", "use case", "estimated cost") != "" {
			continue
		}
		if row.cell("modality") != "" {
			applyOfficialImagePricingRow(out, row)
			continue
		}
		if row.cell("short context input", "input") == "" || row.cell("short context output", "output") == "" {
			continue
		}
		if row.cell("short context cached input", "cached input") == "" {
			return fmt.Errorf("OpenAI 价格表缺少缓存读取列")
		}
		price := out[model]
		if table.tier == "fast" || table.tier == "priority" {
			price = openAIFastPricing(price, row)
		} else {
			price = mergeOpenAIStandardPricing(price, row)
		}
		out[model] = price
	}
	return nil
}

// mergeOpenAIStandardPricing 更新标准档，保留已读取的 Fast 字段。
func mergeOpenAIStandardPricing(price database.ModelPricingOverride, row officialPricingRow) database.ModelPricingOverride {
	standard := openAIStandardPricing(row)
	price.Input, price.CachedInput, price.Output = standard.Input, standard.CachedInput, standard.Output
	price.InputLong, price.CachedInputLong, price.OutputLong = standard.InputLong, standard.CachedInputLong, standard.OutputLong
	price.LongContextThresholdTokens = standard.LongContextThresholdTokens
	return price
}

// openAIStandardPricing 将标准档表头映射到系统使用的价格字段。
func openAIStandardPricing(row officialPricingRow) database.ModelPricingOverride {
	override := database.ModelPricingOverride{
		Input:           row.price("short context input", "input"),
		CachedInput:     row.price("short context cached input", "cached input"),
		Output:          row.price("short context output", "output"),
		InputLong:       row.price("long context input"),
		CachedInputLong: row.price("long context cached input"),
		OutputLong:      row.price("long context output"),
	}
	if override.InputLong > 0 || override.OutputLong > 0 {
		override.LongContextThresholdTokens = openAIPricingLongContextThreshold
	}
	return override
}

// openAIFastPricing 合并独立发布的 Fast 价格，不依赖固定列号或倍率推算。
func openAIFastPricing(override database.ModelPricingOverride, row officialPricingRow) database.ModelPricingOverride {
	override.InputPriority = row.price("short context input", "input")
	override.CachedInputPriority = row.price("short context cached input", "cached input")
	override.OutputPriority = row.price("short context output", "output")
	override.InputLongPriority = row.price("long context input")
	override.CachedInputLongPriority = row.price("long context cached input")
	override.OutputLongPriority = row.price("long context output")
	return override
}
