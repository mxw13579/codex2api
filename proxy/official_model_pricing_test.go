package proxy

import "testing"

func TestParseOpenAIOfficialPricingHTMLIncludesFastAndLongContext(t *testing.T) {
	body := []byte(`
<section data-pricing-tier="standard"><table><thead><tr><th>Model</th><th>Short context input</th><th>Short context cached input</th><th>Short context cache writes</th><th>Short context output</th><th>Long context input</th><th>Long context cached input</th><th>Long context cache writes</th><th>Long context output</th></tr></thead><tbody>
<tr><td>gpt-5.6-sol</td><td>$5.00</td><td>$0.50</td><td>$6.25</td><td>$30.00</td><td>$10.00</td><td>$1.00</td><td>$12.50</td><td>$45.00</td></tr>
<tr><td>gpt-5.5 (&lt;272K context length)</td><td>$5.00</td><td>$0.50</td><td>-</td><td>$30.00</td><td>$10.00</td><td>$1.00</td><td>-</td><td>$45.00</td></tr>
</tbody></table></section>
<section data-pricing-tier="fast"><table><thead><tr><th>Model</th><th>Short context input</th><th>Short context cached input</th><th>Short context cache writes</th><th>Short context output</th><th>Long context input</th><th>Long context cached input</th><th>Long context cache writes</th><th>Long context output</th></tr></thead><tbody>
<tr><td>gpt-5.6-sol</td><td>$10.00</td><td>$1.00</td><td>$12.50</td><td>$60.00</td><td>$20.00</td><td>$2.00</td><td>$25.00</td><td>$90.00</td></tr>
<tr><td>gpt-5.5 (&lt;272K context length)</td><td>$12.50</td><td>$1.25</td><td>-</td><td>$75.00</td><td>-</td><td>-</td><td>-</td><td>-</td></tr>
</tbody></table></section>
`)
	got, err := ParseOpenAIOfficialPricingHTML(body)
	if err != nil {
		t.Fatalf("parse official OpenAI pricing: %v", err)
	}
	sol := got["gpt-5.6-sol"]
	if sol.Input != 5 || sol.CachedInput != 0.5 || sol.Output != 30 {
		t.Fatalf("standard prices = %+v", sol)
	}
	if sol.InputPriority != 10 || sol.CachedInputPriority != 1 || sol.OutputPriority != 60 {
		t.Fatalf("fast prices = %+v", sol)
	}
	if sol.InputLong != 10 || sol.CachedInputLong != 1 || sol.OutputLong != 45 {
		t.Fatalf("long prices = %+v", sol)
	}
	if sol.InputLongPriority != 20 || sol.CachedInputLongPriority != 2 || sol.OutputLongPriority != 90 {
		t.Fatalf("long fast prices = %+v", sol)
	}
	if sol.LongContextThresholdTokens != 272000 {
		t.Fatalf("OpenAI long context threshold = %d, want 272000", sol.LongContextThresholdTokens)
	}
	if _, ok := got["gpt-5.5"]; !ok {
		t.Fatalf("model annotation should be stripped: %v", got)
	}
}

func TestParseXAIOfficialPricingHTML(t *testing.T) {
	body := []byte(`
<section data-pricing-tier="standard"><table><thead><tr><th>Model</th><th>Context</th><th>Input / 1M tokens</th><th>Cached input / 1M tokens</th><th>Output / 1M tokens</th></tr></thead><tbody>
<tr><td>grok-4.6 (&lt; 200k prompt tokens)</td><td>500k</td><td>$2.00</td><td>$0.50</td><td>$6.00</td></tr>
<tr><td>grok-4.6 (≥ 200k prompt tokens)</td><td>500k</td><td>$4.00</td><td>$1.00</td><td>$12.00</td></tr>
<tr><td>grok-4.5 (&lt; 200k prompt tokens)</td><td>500k</td><td>$2.00</td><td>$0.30</td><td>$6.00</td></tr>
<tr><td>grok-4.5 (≥ 200k prompt tokens)</td><td>500k</td><td>$4.00</td><td>$0.60</td><td>$12.00</td></tr>
</tbody></table></section>
`)
	all, err := ParseXAIOfficialPricingHTML(body)
	if err != nil {
		t.Fatalf("parse xAI official pricing: %v", err)
	}
	got := all["grok-4.6"]
	if got.Input != 2 || got.CachedInput != 0.5 || got.Output != 6 ||
		got.InputLong != 4 || got.CachedInputLong != 1 || got.OutputLong != 12 ||
		got.LongContextThresholdTokens != 200000 {
		t.Fatalf("xAI prices = %+v", got)
	}
}

func TestParseAnthropicOfficialPricingHTMLIncludesCacheWriteAndRead(t *testing.T) {
	body := []byte(`<table><tr><th>Model</th><th>Input</th><th>5m Cache Write</th><th>1h Cache Write</th><th>Cache Read</th><th>Output</th></tr>
<tr><td>Claude Fable 5.1</td><td>$10 / MTok</td><td>$12.50 / MTok</td><td>$20 / MTok</td><td>$0.25 / MTok</td><td>$50 / MTok</td></tr>
<tr><td>Claude Fable 5</td><td>$10 / MTok</td><td>$12.50 / MTok</td><td>$20 / MTok</td><td>$1 / MTok</td><td>$50 / MTok</td></tr></table>`)
	got, err := ParseAnthropicOfficialPricingHTML(body)
	if err != nil {
		t.Fatalf("ParseAnthropicOfficialPricingHTML: %v", err)
	}
	fable51 := got["claude-fable-5-1"]
	if fable51.Input != 10 || fable51.CachedInput != 0.25 || fable51.CacheWrite5m != 12.5 || fable51.CacheWrite1h != 20 || fable51.Output != 50 {
		t.Fatalf("Fable 5.1 pricing = %+v", fable51)
	}
	fable5 := got["claude-fable-5"]
	if fable5.Input != 10 || fable5.CachedInput != 1 || fable5.CacheWrite5m != 12.5 || fable5.CacheWrite1h != 20 || fable5.Output != 50 {
		t.Fatalf("Fable 5 pricing = %+v", fable5)
	}
}

// 2026-10: OpenAI moved output before cached input and added 30m / 12h write
// columns. Fixed positions read output as cached input and the 30m write as
// output (gpt-6-astra synced as output 12.5, cached 50); columns must be read
// by header name, both for single-row and grouped (colspan) headers.
func TestParseOpenAIOfficialPricingHTMLReadsReorderedColumnsByHeader(t *testing.T) {
	flat := []byte(`
<section data-pricing-tier="standard"><table><thead><tr><th>Model</th><th>Short context input</th><th>Short context output</th><th>Short context cached input</th><th>Short context 30m write</th><th>Short context 12h write</th><th>Long context input</th><th>Long context output</th><th>Long context cached input</th><th>Long context 30m write</th><th>Long context 12h write</th></tr></thead><tbody>
<tr><td>gpt-6-astra</td><td>$10.00</td><td>$50.00</td><td>$1.00</td><td>$12.50</td><td>$25.00</td><td>$20.00</td><td>$75.00</td><td>$2.00</td><td>$25.00</td><td>$50.00</td></tr>
<tr><td>gpt-6-sol</td><td>$2.00</td><td>$10.00</td><td>$0.20</td><td>$2.50</td><td>$5.00</td><td>$4.00</td><td>$15.00</td><td>$0.40</td><td>$5.00</td><td>$10.00</td></tr>
</tbody></table></section>
`)
	grouped := []byte(`
<section data-pricing-tier="standard"><table><thead>
<tr><th rowspan="2">Model</th><th colspan="5">Short context</th><th colspan="5">Long context</th></tr>
<tr><th>Input</th><th>Output</th><th>Cached input</th><th>30m write</th><th>12h write</th><th>Input</th><th>Output</th><th>Cached input</th><th>30m write</th><th>12h write</th></tr>
</thead><tbody>
<tr><td>gpt-6-astra</td><td>$10.00</td><td>$50.00</td><td>$1.00</td><td>$12.50</td><td>$25.00</td><td>$20.00</td><td>$75.00</td><td>$2.00</td><td>$25.00</td><td>$50.00</td></tr>
<tr><td>gpt-6-sol</td><td>$2.00</td><td>$10.00</td><td>$0.20</td><td>$2.50</td><td>$5.00</td><td>$4.00</td><td>$15.00</td><td>$0.40</td><td>$5.00</td><td>$10.00</td></tr>
</tbody></table></section>
`)
	for name, body := range map[string][]byte{"flat": flat, "grouped": grouped} {
		got, err := ParseOpenAIOfficialPricingHTML(body)
		if err != nil {
			t.Fatalf("%s: parse official OpenAI pricing: %v", name, err)
		}
		astra := got["gpt-6-astra"]
		if astra.Input != 10 || astra.Output != 50 || astra.CachedInput != 1 {
			t.Fatalf("%s: standard prices = %+v", name, astra)
		}
		if astra.InputLong != 20 || astra.OutputLong != 75 || astra.CachedInputLong != 2 {
			t.Fatalf("%s: long prices = %+v", name, astra)
		}
		if astra.LongContextThresholdTokens != 272000 {
			t.Fatalf("%s: threshold = %d", name, astra.LongContextThresholdTokens)
		}
		if sol := got["gpt-6-sol"]; sol.Input != 2 || sol.Output != 10 || sol.CachedInput != 0.2 || sol.OutputLong != 15 {
			t.Fatalf("%s: gpt-6-sol = %+v", name, sol)
		}
	}
}
