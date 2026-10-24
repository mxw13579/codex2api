package proxy

import (
	"fmt"
	"math"

	"github.com/codex2api/database"
)

// validateSyncedPricing 校验同步数据的数值、缓存关系和必要计费档位。
func validateSyncedPricing(model string, price database.ModelPricingOverride, complete bool) error {
	groups := [][3]float64{
		{price.Input, price.CachedInput, price.Output},
		{price.InputPriority, price.CachedInputPriority, price.OutputPriority},
		{price.InputLong, price.CachedInputLong, price.OutputLong},
		{price.InputLongPriority, price.CachedInputLongPriority, price.OutputLongPriority},
		{price.ImageInput, price.CachedImageInput, 0},
	}
	for _, group := range groups {
		if err := validatePricingGroup(group); err != nil {
			return err
		}
	}
	if err := validatePricingExtras(price); err != nil {
		return err
	}
	if !complete {
		return nil
	}
	if target := database.DaybreakPricingTarget(model); target != "" {
		model = target
	}
	return validateRequiredPricing(model, price, groups[:len(groups)-1])
}

// validatePricingExtras 校验不属于 token 三元组的价格和上下文阈值。
func validatePricingExtras(price database.ModelPricingOverride) error {
	for _, value := range []float64{price.MediaUnitCost, price.ImageUnitPrice, price.CacheWrite5m, price.CacheWrite1h} {
		if !validPricingNumber(value) {
			return fmt.Errorf("价格必须为有限的非负数")
		}
	}
	if price.LongContextThresholdTokens < 0 {
		return fmt.Errorf("长上下文阈值不能为负数")
	}
	return nil
}

// validateRequiredPricing 校验自动同步所需的完整价格，手动部分覆盖不走此分支。
func validateRequiredPricing(model string, price database.ModelPricingOverride, groups [][3]float64) error {
	if database.MediaBillingUnit(model) != "" {
		if price.MediaUnitCost <= 0 {
			return fmt.Errorf("媒体单位成本缺失")
		}
		return nil
	}
	if price.Input <= 0 || price.Output <= 0 {
		return fmt.Errorf("输入或输出价格缺失")
	}
	if err := validateCompletePricingGroups(groups); err != nil {
		return err
	}
	return validateLongContextPricing(model, price)
}

// validateCompletePricingGroups 拒绝只有缓存价或只有输入价的不完整计费档位。
func validateCompletePricingGroups(groups [][3]float64) error {
	for _, group := range groups {
		if group[0] == 0 && group[1] == 0 && group[2] == 0 {
			continue
		}
		if group[0] <= 0 || group[2] <= 0 {
			return fmt.Errorf("计费档位的输入或输出价格缺失")
		}
	}
	return nil
}

// validatePricingGroup 防止把输出价或缓存写入价写入缓存读取字段。
func validatePricingGroup(group [3]float64) error {
	for _, value := range group {
		if !validPricingNumber(value) {
			return fmt.Errorf("价格必须为有限的非负数")
		}
	}
	if group[0] > 0 && group[1] > group[0] {
		return fmt.Errorf("缓存读取价格高于对应输入价格")
	}
	return nil
}

// validPricingNumber 拒绝负价、NaN 和无穷大。
func validPricingNumber(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

// validateLongContextPricing 拒绝缺少已知长上下文价格的同步条目，防止混入旧默认档位。
func validateLongContextPricing(model string, price database.ModelPricingOverride) error {
	base := database.GetDefaultModelPricing(model)
	knownModel := database.PricingManagementModelKey(model) == database.CanonicalBillingModelKey(model)
	requiresLong := (knownModel && base.LongInputPricePerMToken > 0) || price.LongContextThresholdTokens > 0
	if requiresLong && (price.InputLong <= 0 || price.OutputLong <= 0) {
		return fmt.Errorf("长上下文输入或输出价格缺失")
	}
	if price.InputLong > 0 && price.LongContextThresholdTokens <= 0 && base.LongInputPricePerMToken <= 0 {
		return fmt.Errorf("长上下文价格缺少分档阈值")
	}
	return nil
}
