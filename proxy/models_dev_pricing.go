package proxy

import (
	"sort"
	"strings"

	"github.com/codex2api/database"
)

type modelsDevCostTier struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cache_read"`
	CacheWrite float64 `json:"cache_write"`
	Tier       struct {
		Type string `json:"type"`
		Size int    `json:"size"`
	} `json:"tier"`
}

type modelsDevCost struct {
	modelsDevCostTier
	Tiers []modelsDevCostTier `json:"tiers"`
}

type modelsDevModel struct {
	Cost         *modelsDevCost `json:"cost"`
	Experimental struct {
		Modes map[string]struct {
			Cost *modelsDevCost `json:"cost"`
		} `json:"modes"`
	} `json:"experimental"`
}

type modelsDevProvider struct {
	Models map[string]modelsDevModel `json:"models"`
}

var modelsDevFirstPartyProviders = []string{"openai", "xai", "anthropic"}

// convertModelsDevPricing 优先使用规范模型 ID，仅合并第一方供应商数据。
func convertModelsDevPricing(providers map[string]modelsDevProvider) map[string]database.ModelPricingOverride {
	models := mergeModelsDevProviders(providers, modelsDevFirstPartyProviders)
	ids := make([]string, 0, len(models))
	for id := range models {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left := database.PricingManagementModelKey(ids[i]) == ids[i]
		right := database.PricingManagementModelKey(ids[j]) == ids[j]
		if left != right {
			return left
		}
		return ids[i] < ids[j]
	})
	out := make(map[string]database.ModelPricingOverride)
	for _, id := range ids {
		key := database.PricingManagementModelKey(id)
		if key == "" || models[id].Cost == nil {
			continue
		}
		price := modelsDevModelToOverride(models[id])
		if price.Input <= 0 || price.Output <= 0 || validateSyncedPricing(key, price, false) != nil {
			continue
		}
		if _, exists := out[key]; !exists {
			out[key] = price
		}
	}
	return out
}

// mergeModelsDevProviders 按供应商优先级合并，禁止把转售商的同名模型混入。
func mergeModelsDevProviders(providers map[string]modelsDevProvider, names []string) map[string]modelsDevModel {
	merged := make(map[string]modelsDevModel)
	for _, name := range names {
		for id, model := range providers[name].Models {
			if name == "anthropic" && !strings.HasPrefix(id, "claude-") {
				continue
			}
			if _, exists := merged[id]; !exists {
				merged[id] = model
			}
		}
	}
	return merged
}

// modelsDevModelToOverride 保留 Fast 价格，缺少的 Fast 长上下文档位不自行推算。
func modelsDevModelToOverride(model modelsDevModel) database.ModelPricingOverride {
	price := modelsDevCostToOverride(*model.Cost)
	if fast := model.Experimental.Modes["fast"].Cost; fast != nil {
		fastPrice := modelsDevCostToOverride(*fast)
		if price.InputLong > 0 && fastPrice.InputLong <= 0 {
			return price
		}
		price.InputPriority, price.CachedInputPriority, price.OutputPriority = fastPrice.Input, fastPrice.CachedInput, fastPrice.Output
		price.InputLongPriority, price.CachedInputLongPriority, price.OutputLongPriority = fastPrice.InputLong, fastPrice.CachedInputLong, fastPrice.OutputLong
	}
	return price
}

// modelsDevCostToOverride 使用数据源明确声明的上下文阈值。
func modelsDevCostToOverride(cost modelsDevCost) database.ModelPricingOverride {
	price := database.ModelPricingOverride{Input: cost.Input, CachedInput: cost.CacheRead, Output: cost.Output, CacheWrite5m: cost.CacheWrite}
	for _, tier := range cost.Tiers {
		if tier.Tier.Type != "context" {
			continue
		}
		price.InputLong, price.CachedInputLong, price.OutputLong = tier.Input, tier.CacheRead, tier.Output
		price.LongContextThresholdTokens = tier.Tier.Size
		break
	}
	return price
}
