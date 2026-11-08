package database

import (
	"math"
	"testing"
)

func TestGPT61UltrafastIsSixTimesStandard(t *testing.T) {
	for _, model := range []string{"gpt-6.1-sol", "gpt-6.1-terra", "gpt-6.1-terra-high"} {
		standard := CalculateCost(100_000, 10_000, 40_000, model, "")
		if standard <= 0 {
			t.Fatalf("%s standard cost = %v", model, standard)
		}
		if got := CalculateCost(100_000, 10_000, 40_000, model, "ultrafast"); math.Abs(got-standard*6) > 1e-12 {
			t.Errorf("%s ultrafast = %v, want 6x standard %v", model, got, standard*6)
		}
	}
	if got := PricingManagementModelKey("gpt-6.1-terra"); got != "gpt-6.1-terra" {
		t.Fatalf("management key = %q", got)
	}
	if got := CanonicalBillingModelKey("gpt-6.1-terra"); got != "gpt-6.1-terra" {
		t.Fatalf("canonical key = %q, must not fall back to astra", got)
	}
}

// 管理员价格覆盖（new-api 同步的 custom 价）不能把模型自有的 ultrafast 倍率清掉。
func TestGPT61UltrafastSurvivesCustomOverride(t *testing.T) {
	prev := currentModelPricingOverrides()
	t.Cleanup(func() { SetModelPricingOverrides(prev) })
	SetModelPricingOverrides(map[string]ModelPricingOverride{
		"gpt-6.1-terra": {Source: ModelPricingSourceCustom, Input: 1, CachedInput: 0.05, Output: 5,
			InputPriority: 2, CachedInputPriority: 0.1, OutputPriority: 10},
		"gpt-6.1-sol": {Source: ModelPricingSourceCustom, Input: 2, CachedInput: 0.1, Output: 10,
			InputPriority: 4, CachedInputPriority: 0.2, OutputPriority: 20},
	})
	for _, model := range []string{"gpt-6.1-sol", "gpt-6.1-terra"} {
		standard := CalculateCost(100_000, 10_000, 0, model, "")
		if got := CalculateCost(100_000, 10_000, 0, model, "ultrafast"); math.Abs(got-standard*6) > 1e-12 {
			t.Errorf("%s ultrafast with override = %v, want %v", model, got, standard*6)
		}
		if got := CalculateCost(100_000, 10_000, 0, model, "priority"); math.Abs(got-standard*2) > 1e-12 {
			t.Errorf("%s priority with override = %v, want %v", model, got, standard*2)
		}
	}
}
