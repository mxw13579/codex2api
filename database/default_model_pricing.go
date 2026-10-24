package database

// GetDefaultModelPricing 返回内置计费规则的副本，不读取管理员或自动同步覆盖。
func GetDefaultModelPricing(model string) *ModelPricing {
	pricing := *baseModelPricing(normalizeBillingModelName(model), CanonicalBillingModelKey(model))
	return &pricing
}
