package proxy

import (
	"context"
	"fmt"
)

type pricingReferenceSource struct{ name, url string }

func pricingReferenceSources() []pricingReferenceSource {
	return []pricingReferenceSource{{"LiteLLM", LiteLLMPricingSyncURL}, {"models.dev", ModelsDevPricingSyncURL}, {"项目默认 JSON", DefaultModelPricingSyncURL}}
}

// fetchReferences 按模型依次尝试 LiteLLM、models.dev、项目 JSON，已有高优先级价格不覆盖。
func (sync *officialPricingSync) fetchReferences(ctx context.Context, proxyURL string) {
	for _, source := range pricingReferenceSources() {
		missing := sync.unpricedModels()
		if len(missing) == 0 || ctx.Err() != nil {
			break
		}
		parsed, err := fetchModelPricingJSON(ctx, source.url, proxyURL)
		if err != nil {
			sync.result.Warnings = append(sync.result.Warnings, fmt.Sprintf("%s 参考价格读取失败: %v", source.name, err))
			continue
		}
		for model, price := range projectClaudeOfficialPricing(missing, parsed) {
			parsed[model] = price
		}
		if imported := sync.importPricing(parsed); imported > 0 {
			sync.recordFallbacks(missing, source.url)
			sync.result.Sources = append(sync.result.Sources, source.url)
		}
	}
	sync.result.Fetched = len(sync.pricing)
}

// unpricedModels 仅返回已启用供应商中仍未取得有效价格的模型。
func (sync *officialPricingSync) unpricedModels() map[string]struct{} {
	missing := make(map[string]struct{})
	for model := range sync.allowed {
		if _, exists := sync.pricing[model]; !exists && officialPricingModelEnabled(model, sync.options) {
			missing[model] = struct{}{}
		}
	}
	return missing
}

// recordFallbacks 记录每个实际补价模型的来源，便于识别官方同步的降级结果。
func (sync *officialPricingSync) recordFallbacks(missing map[string]struct{}, sourceURL string) {
	if sync.result.Fallbacks == nil {
		sync.result.Fallbacks = make(map[string]string)
	}
	for model := range missing {
		if _, exists := sync.pricing[model]; exists {
			sync.result.Fallbacks[model] = sourceURL
		}
	}
}

// referenceNotices 在自定义价格跳过后，按实际应用的参考价格生成普通提示。
func (sync *officialPricingSync) referenceNotices() {
	counts := make(map[string]int)
	for _, sourceURL := range sync.result.Fallbacks {
		counts[sourceURL]++
	}
	for _, source := range pricingReferenceSources() {
		if count := counts[source.url]; count > 0 {
			sync.result.Notices = append(sync.result.Notices, fmt.Sprintf("%d 个模型已回退到 %s 参考价格", count, source.name))
		}
	}
}
