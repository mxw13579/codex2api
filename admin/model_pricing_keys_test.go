package admin

import (
	"reflect"
	"testing"
)

func TestModelPricingManagementKeysKeepsIndependentAlias(t *testing.T) {
	got := modelPricingManagementKeys([]string{
		"gpt-5.4",
		"codex-auto-review",
		"gpt-5.4-openai-compact",
	})
	want := []string{"gpt-5.4", "codex-auto-review"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("modelPricingManagementKeys() = %v, want %v", got, want)
	}
}

func TestAntigravityChannelModelsIncludesLogicalModels(t *testing.T) {
	h := &Handler{}
	models := h.antigravityChannelModels()
	hasLogical := false
	for _, m := range models {
		if m == "gemini-3.8-flash" {
			hasLogical = true
			break
		}
	}
	if !hasLogical {
		t.Fatalf("antigravityChannelModels() should include bare logical model gemini-3.8-flash, got: %v", models)
	}
}
