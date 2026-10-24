package proxy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/codex2api/database"
)

const referencePricingBodyLimit = 32 << 20

// parseModelPricingPayload 按字段识别 LiteLLM、models.dev 或项目覆盖表，拒绝错误类型。
func parseModelPricingPayload(body []byte) (map[string]database.ModelPricingOverride, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("解析定价 JSON 失败: %w", err)
	}
	for _, item := range raw {
		var fields map[string]json.RawMessage
		if json.Unmarshal(item, &fields) != nil {
			continue
		}
		if _, ok := fields["models"]; ok {
			var providers map[string]modelsDevProvider
			if err := json.Unmarshal(body, &providers); err != nil {
				return nil, fmt.Errorf("models.dev 格式不符: %w", err)
			}
			return convertModelsDevPricing(providers), nil
		}
		if _, ok := fields["litellm_provider"]; ok {
			return convertLiteLLMPricing(raw), nil
		}
	}
	return parseFlatModelPricing(raw)
}

// parseFlatModelPricing 校验项目及自定义 JSON，保留合法的部分覆盖。
func parseFlatModelPricing(raw map[string]json.RawMessage) (map[string]database.ModelPricingOverride, error) {
	out := make(map[string]database.ModelPricingOverride)
	for model, item := range raw {
		var price database.ModelPricingOverride
		if err := json.Unmarshal(item, &price); err != nil {
			return nil, fmt.Errorf("%s 定价格式不符: %w", model, err)
		}
		if err := validateSyncedPricing(model, price, false); err != nil {
			return nil, fmt.Errorf("%s 定价校验失败: %w", model, err)
		}
		out[strings.ToLower(strings.TrimSpace(model))] = price
	}
	return out, nil
}
