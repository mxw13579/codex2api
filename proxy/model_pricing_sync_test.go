package proxy

import (
	"math"
	"testing"

	"github.com/codex2api/database"
)

func TestAstraPricingSourcesPreserveLongContext(t *testing.T) {
	t.Cleanup(func() { database.SetModelPricingOverrides(nil) })
	for _, tt := range []struct {
		name  string
		body  string
		parse func([]byte) (map[string]database.ModelPricingOverride, error)
	}{
		{
			name: "flat JSON",
			body: `{"gpt-6-astra":{"input":10,"cached_input":1,"output":50,
				"input_priority":20,"cached_input_priority":2,"output_priority":100,
				"input_long":20,"cached_input_long":2,"output_long":75,
				"input_long_priority":40,"cached_input_long_priority":4,"output_long_priority":150}}`,
			parse: parseModelPricingPayload,
		},
		{
			name: "models.dev",
			body: `{"openai":{"models":{"gpt-6-astra":{"cost":{
				"input":10,"cache_read":1,"output":50,
				"tiers":[{"input":20,"cache_read":2,"output":75,"tier":{"type":"context","size":272000}}]
			}}}}}`,
			parse: parseModelPricingPayload,
		},
		{
			name: "official API HTML",
			body: `
<section data-pricing-tier="standard"><table><thead><tr><th>Model</th><th>Short context input</th><th>Short context cached input</th><th>Short context cache writes</th><th>Short context output</th><th>Long context input</th><th>Long context cached input</th><th>Long context cache writes</th><th>Long context output</th></tr></thead><tbody>
<tr><td>gpt-6-astra</td><td>$10.00</td><td>$1.00</td><td>$12.50</td><td>$50.00</td><td>$20.00</td><td>$2.00</td><td>$25.00</td><td>$75.00</td></tr>
</tbody></table></section>
<section data-pricing-tier="fast"><table><thead><tr><th>Model</th><th>Short context input</th><th>Short context cached input</th><th>Short context cache writes</th><th>Short context output</th><th>Long context input</th><th>Long context cached input</th><th>Long context cache writes</th><th>Long context output</th></tr></thead><tbody>
<tr><td>gpt-6-astra</td><td>$20.00</td><td>$2.00</td><td>$25.00</td><td>$100.00</td><td>$40.00</td><td>$4.00</td><td>$50.00</td><td>$150.00</td></tr>
</tbody></table></section>
`,
			parse: ParseOpenAIOfficialPricingHTML,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := tt.parse([]byte(tt.body))
			if err != nil {
				t.Fatalf("parse source: %v", err)
			}
			if parsed["gpt-6-astra"].InputLong != 20 {
				t.Fatalf("fixture must supply API long-context rates: %+v", parsed)
			}
			database.SetModelPricingOverrides(parsed)
			for _, tier := range []string{"", "fast", "priority"} {
				got := database.CalculateCostBreakdown(300000, 1000, 100000, "gpt-6-astra", tier)
				want := 4.275
				if tier != "" {
					want = 8.55
				}
				if !got.LongContext || math.Abs(got.TotalCost-want) > 1e-9 {
					t.Fatalf("tier=%q did not preserve API long-context rates: %+v, want total %v", tier, got, want)
				}
			}
		})
	}
}

func TestParseModelPricingPayloadFlat(t *testing.T) {
	body := []byte(`{
		"gpt-5.5": {"input": 5, "cached_input": 0.5, "output": 30, "input_priority": 10},
		"empty-model": {}
	}`)
	got, err := parseModelPricingPayload(body)
	if err != nil {
		t.Fatalf("parse flat payload: %v", err)
	}
	ov, ok := got["gpt-5.5"]
	if !ok {
		t.Fatalf("missing gpt-5.5, got keys: %v", got)
	}
	if ov.Input != 5 || ov.CachedInput != 0.5 || ov.Output != 30 || ov.InputPriority != 10 {
		t.Fatalf("unexpected override: %+v", ov)
	}
}

func TestParseModelPricingPayloadModelsDev(t *testing.T) {
	body := []byte(`{
		"openai": {
			"id": "openai",
			"models": {
				"gpt-5.5": {
					"id": "gpt-5.5",
					"cost": {
						"input": 5, "output": 30, "cache_read": 0.5,
						"tiers": [{"input": 10, "output": 45, "cache_read": 1, "tier": {"type": "context", "size": 272000}}]
					}
				},
				"gpt-5.4": {"id": "gpt-5.4", "cost": {"input": 2.5, "output": 15, "cache_read": 0.25}},
				"gpt-5.6-terra": {"id": "gpt-5.6-terra", "cost": {"input": 99, "output": 99, "cache_read": 99}},
				"text-embedding-3-large": {"id": "text-embedding-3-large"}
			}
		},
		"anthropic": {
			"id": "anthropic",
			"models": {"gpt-5.5": {"cost": {"input": 1, "output": 1}}}
		}
	}`)
	got, err := parseModelPricingPayload(body)
	if err != nil {
		t.Fatalf("parse models.dev payload: %v", err)
	}

	ov, ok := got["gpt-5.5"]
	if !ok {
		t.Fatalf("missing gpt-5.5, got keys: %v", got)
	}
	// 只取 openai provider，长上下文分档映射到 *_long。
	if ov.Input != 5 || ov.CachedInput != 0.5 || ov.Output != 30 {
		t.Fatalf("unexpected standard tier: %+v", ov)
	}
	if ov.InputLong != 10 || ov.CachedInputLong != 1 || ov.OutputLong != 45 {
		t.Fatalf("unexpected long-context tier: %+v", ov)
	}

	// gpt-5.6-terra 独立规范键，与 gpt-5.4 互不影响。
	if ov44 := got["gpt-5.4"]; ov44.Input != 2.5 {
		t.Fatalf("gpt-5.4 should keep exact entry: %+v", ov44)
	}
	if ovTerra, ok := got["gpt-5.6-terra"]; !ok {
		t.Fatalf("missing gpt-5.6-terra as independent pricing key")
	} else if ovTerra.Input != 99 {
		t.Fatalf("gpt-5.6-terra = %+v, want input 99", ovTerra)
	}

	// 无 cost 的模型跳过。
	if _, ok := got["text-embedding-3-large"]; ok {
		t.Fatalf("model without cost should be skipped")
	}
}

// models.dev 同步要覆盖 xai provider：网关同时代理 Grok，grok-4.5 等必须拿到官方价，
// 转售 provider（同名模型带加价）仍然不参与。
func TestParseModelPricingPayloadModelsDevIncludesXAI(t *testing.T) {
	body := []byte(`{
		"openai": {
			"id": "openai",
			"models": {"gpt-5.5": {"id": "gpt-5.5", "cost": {"input": 5, "output": 30, "cache_read": 0.5}}}
		},
		"xai": {
			"id": "xai",
			"models": {
				"grok-4.6": {
					"id": "grok-4.6",
					"cost": {
						"input": 2, "output": 6, "cache_read": 0.5,
						"tiers": [{"input": 4, "output": 12, "cache_read": 1, "tier": {"type": "context", "size": 200000}}]
					}
				},
				"grok-4.5": {
					"id": "grok-4.5",
					"cost": {
						"input": 2, "output": 6, "cache_read": 0.3,
						"tiers": [{"input": 4, "output": 12, "cache_read": 1, "tier": {"type": "context", "size": 200000}}]
					}
				}
			}
		},
		"some-reseller": {
			"id": "some-reseller",
			"models": {"grok-4.5": {"cost": {"input": 99, "output": 99}}}
		}
	}`)

	got, err := parseModelPricingPayload(body)
	if err != nil {
		t.Fatalf("parse models.dev payload: %v", err)
	}

	ov, ok := got["grok-4.5"]
	if !ok {
		t.Fatalf("missing grok-4.5, got keys: %v", got)
	}
	if ov.Input != 2 || ov.Output != 6 || ov.CachedInput != 0.3 {
		t.Fatalf("grok-4.5 standard tier = %+v, want xai rates (not the reseller's)", ov)
	}
	if ov.InputLong != 4 || ov.OutputLong != 12 || ov.CachedInputLong != 1 {
		t.Fatalf("grok-4.5 long tier = %+v", ov)
	}
	ov46, ok := got["grok-4.6"]
	if !ok {
		t.Fatalf("missing grok-4.6, got keys: %v", got)
	}
	if ov46.Input != 2 || ov46.Output != 6 || ov46.CachedInput != 0.5 {
		t.Fatalf("grok-4.6 standard tier = %+v, want $2/$6 cache $0.50", ov46)
	}
	if ov46.InputLong != 4 || ov46.OutputLong != 12 || ov46.CachedInputLong != 1 {
		t.Fatalf("grok-4.6 long tier = %+v", ov46)
	}
	if ov5 := got["gpt-5.5"]; ov5.Input != 5 {
		t.Fatalf("openai entries must survive alongside xai: %+v", ov5)
	}
}

func TestParseModelPricingPayloadModelsDevIncludesGoogle(t *testing.T) {
	body := []byte(`{
		"openai": {
			"id": "openai",
			"models": {"gpt-5.5": {"id": "gpt-5.5", "cost": {"input": 5, "output": 30, "cache_read": 0.5}}}
		},
		"google": {
			"id": "google",
			"models": {
				"gemini-3.8-flash": {
					"id": "gemini-3.8-flash",
					"cost": {
						"input": 0.75, "output": 3.75, "cache_read": 0.075
					}
				},
				"non-gemini-model": {
					"id": "non-gemini-model",
					"cost": {"input": 1, "output": 1}
				}
			}
		},
		"google-reseller": {
			"id": "google-reseller",
			"models": {"gemini-3.8-flash": {"cost": {"input": 99, "output": 99}}}
		}
	}`)

	got, err := parseModelPricingPayload(body)
	if err != nil {
		t.Fatalf("parse models.dev payload: %v", err)
	}

	ov, ok := got["gemini-3.8-flash"]
	if !ok {
		t.Fatalf("missing gemini-3.8-flash, got keys: %v", got)
	}
	if ov.Input != 0.75 || ov.Output != 3.75 || ov.CachedInput != 0.075 {
		t.Fatalf("gemini-3.8-flash pricing = %+v, want $0.75/$3.75/cache $0.075", ov)
	}

	if _, ok := got["non-gemini-model"]; ok {
		t.Fatalf("non-gemini google models should be filtered out")
	}
}
