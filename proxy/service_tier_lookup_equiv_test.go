package proxy

import (
	"bytes"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy/internal/jsonbody"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ---- 改动前的实现，原样保留作等价性基准 ----

func legacyNormalizeUltrafastBody(body []byte) ([]byte, bool, *Error) {
	snake := gjson.GetBytes(body, "service_tier").String()
	camel := gjson.GetBytes(body, "serviceTier").String()
	ultra := isUltrafastTier(snake) || isUltrafastTier(camel)
	if !ultra {
		return body, false, nil
	}
	if (strings.TrimSpace(snake) != "" && !isUltrafastTier(snake)) ||
		(strings.TrimSpace(camel) != "" && !isUltrafastTier(camel)) {
		return body, true, serviceTierConflict("Conflicting service_tier and serviceTier values")
	}
	body, _ = sjson.SetBytes(body, "service_tier", "ultrafast")
	body, _ = sjson.DeleteBytes(body, "serviceTier")
	return body, true, nil
}

func legacyNormalizeServiceTierField(body []byte) []byte {
	tier := strings.TrimSpace(gjson.GetBytes(body, "service_tier").String())
	if tier == "" {
		tier = strings.TrimSpace(gjson.GetBytes(body, "serviceTier").String())
	}
	if tier == "" {
		return body
	}
	body = jsonbody.SetString(body, "service_tier", tier)
	body = jsonbody.DeletePath(body, "serviceTier")
	return body
}

func legacyApplyBaseModelMappingToBody(h *Handler, rawBody []byte, supportedModels []string) ([]byte, string, string, bool) {
	originalModel := strings.TrimSpace(gjson.GetBytes(rawBody, "model").String())
	effectiveModel := originalModel
	if originalModel == "" || !gjson.ValidBytes(rawBody) || h == nil || h.store == nil {
		return rawBody, originalModel, effectiveModel, false
	}

	updatedBody := rawBody
	modelForMapping := originalModel
	mappingApplied := false
	if entry, ok := resolveReasoningEffortModelAlias(originalModel, h.store.GetReasoningEffortModels(), supportedModels); ok {
		var err error
		updatedBody, err = applyReasoningEffortModelToBody(updatedBody, entry)
		if err != nil {
			return rawBody, originalModel, effectiveModel, false
		}
		modelForMapping = entry.Model
		effectiveModel = entry.Model
		mappingApplied = !strings.EqualFold(originalModel, entry.Model)
	}

	mappedModel, ok := resolveConfiguredModelMapping(modelForMapping, h.store.GetCodexModelMapping(), supportedModels)
	if ok && mappedModel != "" && !strings.EqualFold(mappedModel, modelForMapping) {
		var err error
		updatedBody, err = sjson.SetBytes(updatedBody, "model", mappedModel)
		if err != nil {
			return rawBody, originalModel, effectiveModel, mappingApplied
		}
		effectiveModel = mappedModel
		mappingApplied = true
	}
	return updatedBody, originalModel, effectiveModel, mappingApplied
}

// ---- 用例 ----

var serviceTierEquivalenceCases = []string{
	``,
	`null`,
	`[]`,
	`{}`,
	`not json`,
	`{"model":"gpt-5.5","input":"hi"}`,
	`{"service_tier":"fast","input":"hi"}`,
	`{"input":"hi","service_tier":"priority"}`,
	`{"serviceTier":"fast","input":"hi"}`,
	`{"input":"hi","serviceTier":" flex "}`,
	`{"service_tier":"  ","serviceTier":"fast"}`,
	`{"service_tier":"","serviceTier":""}`,
	`{"service_tier":"ultrafast"}`,
	`{"serviceTier":"UltraFast"}`,
	`{"service_tier":"ultrafast","serviceTier":"fast"}`,
	`{"service_tier":"ultrafast","serviceTier":" ultrafast "}`,
	`{"service_tier":"fast","serviceTier":"ultrafast"}`,
	`{"service_tier":"fast","service_tier":"priority"}`,
	`{"service_tier":"","service_tier":"priority"}`,
	`{"serviceTier":"fast","serviceTier":"ultrafast"}`,
	`{"service\u005ftier":"fast"}`,
	`{"service_tier":"f\u0061st"}`,
	`{"serviceTier":"ultr\u0061fast"}`,
	`{"service_tier":1}`,
	`{"service_tier":null,"serviceTier":"fast"}`,
	`{"service_tier":{"x":1}}`,
	`{"service_tier":["fast"]}`,
	`{"input":[{"service_tier":"fast"}]}`,
	`{"input":"service_tier fast"}`,
	`{"service_tier":"fast"`,
	`{"service_tier":"fa`,
	`{"input":"hi","serviceTier":"fast",}`,
	`{"service_tier" "fast"}`,
}

// 按 Responses / Chat 入口的顺序串起调用，lookup 在整条链上共享。
func assertServiceTierChainEquivalent(t *testing.T, raw string) {
	t.Helper()
	body := []byte(raw)

	// 单个函数。
	wantUB, wantUU, wantUE := legacyNormalizeUltrafastBody(bytes.Clone(body))
	gotUB, gotUU, gotUE := normalizeUltrafastBody(bytes.Clone(body))
	if !bytes.Equal(wantUB, gotUB) || wantUU != gotUU || !reflect.DeepEqual(wantUE, gotUE) {
		t.Fatalf("normalizeUltrafastBody mismatch for %q", raw)
	}
	if want, got := legacyNormalizeServiceTierField(bytes.Clone(body)), normalizeServiceTierField(bytes.Clone(body)); !bytes.Equal(want, got) {
		t.Fatalf("normalizeServiceTierField mismatch for %q: %q vs %q", raw, want, got)
	}

	// 共享 lookup 的链路，中间可能夹着一次与 service tier 无关的改写。
	for _, rewrite := range []bool{false, true} {
		legacy := bytes.Clone(body)
		legacy, lu, le := legacyNormalizeUltrafastBody(legacy)
		if rewrite {
			legacy, _ = sjson.SetBytes(legacy, "model", "gpt-5.5")
		}
		legacy = legacyNormalizeServiceTierField(legacy)
		legacyTier := extractServiceTier(legacy)

		var tiers serviceTierFieldLookup
		current := bytes.Clone(body)
		current, cu, ce := normalizeUltrafastBodyWithLookup(current, &tiers)
		if rewrite {
			current, _ = sjson.SetBytes(current, "model", "gpt-5.5")
		}
		current = normalizeServiceTierFieldWithLookup(current, &tiers)
		currentTier := extractServiceTierWithLookup(current, &tiers)

		if !bytes.Equal(legacy, current) || lu != cu || !reflect.DeepEqual(le, ce) || legacyTier != currentTier {
			t.Fatalf("service tier chain (rewrite=%v) mismatch for %q:\nlegacy=%q tier=%q\nnew   =%q tier=%q", rewrite, raw, legacy, legacyTier, current, currentTier)
		}
		// Chat 入口：ultrafast 归一化后直接取 tier。
		var chatTiers serviceTierFieldLookup
		chatBody, _, _ := normalizeUltrafastBodyWithLookup(bytes.Clone(body), &chatTiers)
		legacyChat, _, _ := legacyNormalizeUltrafastBody(bytes.Clone(body))
		if extractServiceTierWithLookup(chatBody, &chatTiers) != extractServiceTier(legacyChat) {
			t.Fatalf("chat service tier mismatch for %q", raw)
		}
	}
}

func TestServiceTierLookupEquivalentToLegacy(t *testing.T) {
	for _, raw := range serviceTierEquivalenceCases {
		assertServiceTierChainEquivalent(t, raw)
	}
}

func TestServiceTierLookupEquivalentRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(1022))
	values := []string{`"fast"`, `"ultrafast"`, `" Ultrafast "`, `""`, `"  "`, `"priority"`, `null`, `1`, `"f\u0061st"`}
	keys := []string{`"service_tier"`, `"serviceTier"`, `"service\u005ftier"`, `"input"`, `"model"`}
	for n := 0; n < 3000; n++ {
		var fields []string
		for i := rng.Intn(5); i > 0; i-- {
			key := keys[rng.Intn(len(keys))]
			value := values[rng.Intn(len(values))]
			if key == `"input"` {
				value = `[{"type":"message","content":"service_tier serviceTier"}]`
			}
			fields = append(fields, key+":"+value)
		}
		raw := "{" + strings.Join(fields, ",") + "}"
		assertServiceTierChainEquivalent(t, raw)
		assertServiceTierChainEquivalent(t, raw[:rng.Intn(len(raw)+1)])
	}
}

func TestServiceTierLookupLargeBody(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"model":"gpt-5.5","input":[`)
	for i := 0; i < 3000; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"type":"message","role":"user","content":"hello"}`)
	}
	b.WriteString(`]`)
	prefix := b.String()
	for _, tail := range []string{`}`, `,"service_tier":"fast"}`, `,"serviceTier":"ultrafast"}`, `,"service_tier":" priority "}`} {
		assertServiceTierChainEquivalent(t, prefix+tail)
	}
}

func FuzzServiceTierLookupEquivalentToLegacy(f *testing.F) {
	for _, raw := range serviceTierEquivalenceCases {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		assertServiceTierChainEquivalent(t, raw)
	})
}

func TestServiceTierFieldLookupReusesOnlySameSlice(t *testing.T) {
	body := []byte(`{"service_tier":"fast"}`)
	tiers := serviceTierFieldLookup{body: body, snake: "priority", snakeKnown: true}
	if got := extractServiceTierWithLookup(body, &tiers); got != "priority" {
		t.Fatalf("same slice must reuse the recorded value, got %q", got)
	}
	if got := extractServiceTierWithLookup(bytes.Clone(body), &tiers); got != "fast" {
		t.Fatalf("different slice must be looked up again, got %q", got)
	}
}

// ---- 模型映射：先判定是否改写，再决定是否需要 ValidBytes ----

func TestApplyBaseModelMappingEquivalentToLegacy(t *testing.T) {
	type config struct{ mapping, effort string }
	configs := []config{
		{"", ""},
		{`{"gpt-legacy-*":"gpt-5.5"}`, ""},
		{`{"gpt-5.5":"gpt-5.5"}`, ""},
		{`{"gpt-5.5":"GPT-5.5"}`, ""},
		{`{"gpt-5.5":"gpt-5.4"}`, ""},
		{"", `[{"model":"gpt-5.5","effort":"xhigh"}]`},
		{`{"gpt-5.5":"gpt-5.4"}`, `[{"model":"gpt-5.5","effort":"xhigh"}]`},
		{`not json`, `not json`},
	}
	bodies := []string{
		``,
		`not json`,
		`{}`,
		`{"model":""}`,
		`{"model":"  "}`,
		`{"model":"gpt-5.5","input":"hi"}`,
		`{"model":"gpt-5.5","input":"hi",}`,
		`{"model":"gpt-5.5","input":"hi"`,
		`{"model":"gpt-legacy-1","input":"hi"}`,
		`{"model":"gpt-legacy-1","input":"hi"`,
		`{"model":"gpt-legacy-1","input":["hi",]}`,
		`{"model":"gpt-5.5(xhigh)","input":"hi"}`,
		`{"model":"gpt-5.5(xhigh)","input":"hi"`,
		`{"model":"gpt-5.5(xhigh)","reasoning":{"effort":"low"},"input":"hi"}`,
		`{"model":"gpt-5.5(xhigh)","reasoning":"bad","input":"hi"}`,
		`{"model":"gpt-5.5","model":"gpt-legacy-1"}`,
		`{"model":"gpt\u002dlegacy-1"}`,
		`{"model":123}`,
	}
	supportedSets := [][]string{nil, {"gpt-5.5"}, {"gpt-5.5", "gpt-5.4", "gpt-5.5(xhigh)"}}
	for _, cfg := range configs {
		store := auth.NewStore(nil, nil, nil)
		store.SetCodexModelMapping(cfg.mapping)
		store.SetReasoningEffortModels(cfg.effort)
		handler := NewHandler(store, nil, nil, nil)
		for _, raw := range bodies {
			for _, supported := range supportedSets {
				wb, wo, we, wm := legacyApplyBaseModelMappingToBody(handler, []byte(raw), supported)
				gb, gotO, ge, gm := handler.applyBaseModelMappingToBody([]byte(raw), supported)
				if !bytes.Equal(wb, gb) || wo != gotO || we != ge || wm != gm {
					t.Fatalf("applyBaseModelMappingToBody mismatch cfg=%+v body=%q supported=%v:\nlegacy=%q %q %q %v\nnew   =%q %q %q %v",
						cfg, raw, supported, wb, wo, we, wm, gb, gotO, ge, gm)
				}
			}
		}
		store.Stop()
	}
	var nilHandler *Handler
	wb, wo, we, wm := legacyApplyBaseModelMappingToBody(nilHandler, []byte(`{"model":"x"}`), nil)
	gb, gotO, ge, gm := nilHandler.applyBaseModelMappingToBody([]byte(`{"model":"x"}`), nil)
	if !bytes.Equal(wb, gb) || wo != gotO || we != ge || wm != gm {
		t.Fatal("nil handler mismatch")
	}
}
