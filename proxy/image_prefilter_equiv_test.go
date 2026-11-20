package proxy

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ---- 改动前的实现，原样保留作等价性基准 ----

func legacyStripResponsesImageGenerationCapabilities(body []byte) []byte {
	if tools := gjson.GetBytes(body, "tools"); tools.Exists() && tools.IsArray() {
		if kept, removed := stripImageGenerationToolsFromArray(tools.Array()); removed {
			if len(kept) == 0 {
				body, _ = sjson.DeleteBytes(body, "tools")
			} else {
				body, _ = sjson.SetBytes(body, "tools", kept)
			}
		}
	}
	body = legacyStripResponsesInputImageTools(body)
	if choice := gjson.GetBytes(body, "tool_choice"); choice.Exists() {
		var target string
		if choice.Type == gjson.String {
			target = strings.TrimSpace(choice.String())
		} else {
			target = strings.TrimSpace(choice.Get("type").String())
			if target == "namespace" {
				target = strings.TrimSpace(choice.Get("name").String())
			}
		}
		if strings.EqualFold(target, "image_generation") || strings.EqualFold(target, "image_gen") {
			body, _ = sjson.DeleteBytes(body, "tool_choice")
		}
	}
	if instructions := gjson.GetBytes(body, "instructions").String(); strings.Contains(instructions, codexImageGenerationBridgeMarker) {
		cleaned := strings.ReplaceAll(instructions, "\n\n"+codexImageGenerationBridgeText, "")
		cleaned = strings.ReplaceAll(cleaned, codexImageGenerationBridgeText, "")
		cleaned = strings.TrimSpace(cleaned)
		if cleaned == "" {
			body, _ = sjson.DeleteBytes(body, "instructions")
		} else {
			body, _ = sjson.SetBytes(body, "instructions", cleaned)
		}
	}
	return body
}

func legacyStripResponsesInputImageTools(body []byte) []byte {
	if !gjson.GetBytes(body, `input.#(type%"*additional_tools*")`).Exists() {
		return body
	}
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}
	items := input.Array()
	var replacements map[int][]byte
	for index, item := range items {
		if strings.TrimSpace(item.Get("type").String()) != "additional_tools" {
			continue
		}
		nested := item.Get("tools")
		if !nested.IsArray() {
			continue
		}
		kept, removed := stripImageGenerationToolsFromArray(nested.Array())
		if !removed {
			continue
		}
		var replacement []byte
		if len(kept) > 0 {
			var err error
			replacement, err = sjson.SetBytes([]byte(item.Raw), "tools", kept)
			if err != nil {
				continue
			}
		}
		if replacements == nil {
			replacements = make(map[int][]byte)
		}
		replacements[index] = replacement
	}
	if len(replacements) == 0 {
		return body
	}
	var encoded bytes.Buffer
	encoded.Grow(len(input.Raw))
	encoded.WriteByte('[')
	written := false
	for index, item := range items {
		replacement, changed := replacements[index]
		if changed && replacement == nil {
			continue
		}
		if written {
			encoded.WriteByte(',')
		}
		if changed {
			encoded.Write(replacement)
		} else {
			encoded.WriteString(item.Raw)
		}
		written = true
	}
	encoded.WriteByte(']')
	updated, err := sjson.SetRawBytes(body, "input", encoded.Bytes())
	if err != nil {
		return body
	}
	return updated
}

// ---- 用例 ----

var imageStripEquivalenceCases = []string{
	``,
	`null`,
	`[]`,
	`{}`,
	`not json`,
	`{"model":"gpt-5.5","input":"hi"}`,
	`{"model":"gpt-5.5","input":[{"role":"user","content":"draw image_generation please"}]}`,
	`{"tools":[]}`,
	`{"tools":[{"type":"image_generation"}]}`,
	`{"tools":[{"type":" image_generation "},{"type":"function","name":"f"}]}`,
	`{"tools":[{"type":"Image_Generation"}]}`,
	`{"tools":[{"type":"image\u005fgeneration"}],"input":[]}`,
	`{"tools":[{"type":"\u0069mage_generation","model":"gpt-image-2"},{"type":"web_search"}]}`,
	`{"tools":[{"type":"namespace","name":"image_gen","tools":[]}]}`,
	`{"tools":[{"type":"namespace","name":"other"}]}`,
	`{"tools":{"type":"image_generation"}}`,
	`{"tools":[{"type":"image_generation"}],"tools":[{"type":"function"}]}`,
	`{"tools":[{"type":"function"}],"tools":[{"type":"image_generation"}]}`,
	`{"tool_choice":"image_generation"}`,
	`{"tool_choice":" IMAGE_GEN "}`,
	`{"tool_choice":"auto"}`,
	`{"tool_choice":{"type":"image_generation"}}`,
	`{"tool_choice":{"type":"namespace","name":"Image_Gen"}}`,
	`{"tool_choice":{"type":"function","name":"image_gen"}}`,
	`{"tool_choice":"\u0069mage_gen"}`,
	`{"instructions":"<codex2api-codex-image-generation>\nfoo"}`,
	`{"instructions":"user text"}`,
	`{"instructions":"\u003ccodex2api-codex-image-generation\u003e"}`,
	`{"instructions":"\u003ccodex2api-codex-image\u002dgeneration\u003e"}`,
	`{"instructions":["<codex2api-codex-image-generation>"]}`,
	`{"input":[{"type":"additional_tools","tools":[{"type":"image_generation"},{"type":"function","name":"f"}]}]}`,
	`{"input":[{"type":"additional_tools","tools":[{"type":"image_generation"}]},{"type":"message","role":"user","content":"hi"}]}`,
	`{"input":[{"type":" additional_tools ","tools":[{"type":"namespace","name":"image_gen"}]}]}`,
	`{"input":[{"type":"additional\u005ftools","tools":[{"type":"image_generation"}]}]}`,
	`{"input":[{"type":"Additional_tools","tools":[{"type":"image_generation"}]}]}`,
	`{"input":[{"type":"message","role":"user","content":"additional_tools image_gen"}]}`,
	`{"input":[{"type":"additional_tools","tools":[{"type":"function"}]}],"tools":[{"type":"image_generation"}]}`,
	`{"input":{"type":"additional_tools","tools":[{"type":"image_generation"}]}}`,
	`{"tools":[{"type":"image_generation"}`,
	`{"tools":[{"type":"image_generation"},`,
	`{"input":[{"type":"additional_tools","tools":[{"type":"image_generation"}`,
}

func assertImageStripEquivalent(t *testing.T, raw string) {
	t.Helper()
	body := []byte(raw)
	if want, got := legacyStripResponsesImageGenerationCapabilities(bytes.Clone(body)), stripResponsesImageGenerationCapabilities(bytes.Clone(body)); !bytes.Equal(want, got) {
		t.Fatalf("stripResponsesImageGenerationCapabilities mismatch for %q:\nlegacy=%q\nnew   =%q", raw, want, got)
	}
	if want, got := legacyStripResponsesImageGenerationCapabilities(bytes.Clone(body)), stripResponsesImageGenerationTool(bytes.Clone(body)); !bytes.Equal(want, got) {
		t.Fatalf("stripResponsesImageGenerationTool mismatch for %q:\nlegacy=%q\nnew   =%q", raw, want, got)
	}
	if want, got := legacyStripResponsesInputImageTools(bytes.Clone(body)), stripResponsesInputImageTools(bytes.Clone(body)); !bytes.Equal(want, got) {
		t.Fatalf("stripResponsesInputImageTools mismatch for %q:\nlegacy=%q\nnew   =%q", raw, want, got)
	}
}

func TestImageStripPrefilterEquivalentToLegacy(t *testing.T) {
	for _, raw := range imageStripEquivalenceCases {
		assertImageStripEquivalent(t, raw)
		if !gjson.Valid(raw) || !gjson.Parse(raw).IsObject() {
			// 预处理对顶层 null 会 panic（既有行为，与本改动无关），只喂对象。
			continue
		}
		// 预处理后的形态（含自动注入的图片工具与桥接 instructions）。
		prepared := prepareResponsesBodyForOwnerDetailed([]byte(raw), "").Body
		assertImageStripEquivalent(t, string(prepared))
	}
}

func TestImageStripPrefilterEquivalentPreparedBodies(t *testing.T) {
	raws := []string{
		`{"model":"gpt-5.5","input":"hello"}`,
		`{"model":"gpt-5.5","input":[{"role":"user","content":"hello <b>&</b>"}],"tools":[]}`,
		`{"model":"gpt-5.5","input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"f","parameters":{"type":"object"}}]}`,
		`{"model":"gpt-5.5","instructions":"be nice","input":[{"role":"user","content":"hello"}]}`,
		`{"model":"gpt-5.5","input":[{"role":"user","content":"帮我画一只猫"}]}`,
		`{"model":"gpt-5.5","input":[{"type":"additional_tools","tools":[{"type":"function","name":"f"}]},{"role":"user","content":"hello"}]}`,
		`{"model":"gpt-image-2","prompt":"a cat"}`,
	}
	for _, raw := range raws {
		for _, prepared := range [][]byte{
			prepareResponsesBodyForOwnerDetailed([]byte(raw), "").Body,
			PrepareOpenAIResponsesBody([]byte(raw)),
		} {
			assertImageStripEquivalent(t, string(prepared))
		}
	}
}

func TestImageStripPrefilterEquivalentRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(220))
	toolTypes := []string{`"image_generation"`, `"function"`, `"web_search"`, `"namespace"`, `" image_generation "`, `"Image_Generation"`, `"image\u005fgeneration"`}
	names := []string{`"image_gen"`, `"f"`, `"Image_Gen"`}
	choices := []string{`"auto"`, `"image_generation"`, `"image_gen"`, `{"type":"image_generation"}`, `{"type":"namespace","name":"image_gen"}`, `{"type":"function","name":"f"}`}
	tool := func() string {
		return fmt.Sprintf(`{"type":%s,"name":%s}`, toolTypes[rng.Intn(len(toolTypes))], names[rng.Intn(len(names))])
	}
	tools := func() string {
		parts := make([]string, rng.Intn(4))
		for i := range parts {
			parts[i] = tool()
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	for n := 0; n < 3000; n++ {
		var fields []string
		if rng.Intn(2) == 0 {
			fields = append(fields, `"tools":`+tools())
		}
		if rng.Intn(3) == 0 {
			fields = append(fields, `"tool_choice":`+choices[rng.Intn(len(choices))])
		}
		if rng.Intn(3) == 0 {
			fields = append(fields, `"instructions":`+fmt.Sprintf("%q", []string{"x", codexImageGenerationBridgeText, "a\n\n" + codexImageGenerationBridgeText}[rng.Intn(3)]))
		}
		var items []string
		for i := rng.Intn(4); i > 0; i-- {
			if rng.Intn(2) == 0 {
				items = append(items, `{"type":"additional_tools","tools":`+tools()+`}`)
			} else {
				items = append(items, `{"type":"message","role":"user","content":"hi"}`)
			}
		}
		fields = append(fields, `"input":[`+strings.Join(items, ",")+`]`)
		rng.Shuffle(len(fields), func(a, b int) { fields[a], fields[b] = fields[b], fields[a] })
		raw := "{" + strings.Join(fields, ",") + "}"
		assertImageStripEquivalent(t, raw)
		assertImageStripEquivalent(t, raw[:rng.Intn(len(raw))])
	}
}

func FuzzImageStripPrefilterEquivalentToLegacy(f *testing.F) {
	for _, raw := range imageStripEquivalenceCases {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		assertImageStripEquivalent(t, raw)
	})
}

// ---- 自然语言生图意图复用 ----

var naturalImageIntentCases = []string{
	``,
	`null`,
	`[]`,
	`"x"`,
	`{}`,
	`not json`,
	`{"input":"帮我画一只猫"}`,
	`{"input":"写一个生成图片的脚本"}`,
	`{"prompt":"generate an image of a cat","input":"hello"}`,
	`{"prompt":"  ","input":"draw a cat"}`,
	`{"input":[{"role":"user","content":"draw a cat"}]}`,
	`{"input":[{"role":"user","content":[{"type":"input_text","text":"画一张海报"}]}]}`,
	`{"input":[{"type":"message","text":"make an image"}]}`,
	`{"input":["draw a", "cat"]}`,
	`{"input":[{"role":"user","content":"draw"},{"role":"user","content":"a cat"}]}`,
	`{"input":"draw a cat","input":"hello"}`,
	`{"input":"hello","input":"draw a cat"}`,
	`{"input":"draw a cat","n":1e400}`,
	`{"input":"draw a cat",}`,
	`{"input":"dr\u0061w a cat"}`,
	`{"input":"draw a cat","background":false}`,
	`{"input":"draw a cat","background":null}`,
	`{"model":"gpt-image-2","prompt":"a cat"}`,
	`{"input":"draw a cat","previous_response_id":"resp_missing"}`,
}

func TestPrepareNaturalImageIntentMatchesStandalone(t *testing.T) {
	for _, raw := range naturalImageIntentCases {
		body := []byte(raw)
		want := responsesBodyHasNaturalImageGenerationIntent(body)
		if raw == "null" {
			// 预处理对顶层 null 会 panic（既有行为，与本改动无关）。
			continue
		}

		plain := prepareResponsesBodyForOwnerDetailed(bytes.Clone(body), "")
		var got bool
		withIntent := prepareResponsesBodyForOwnerDetailedWithIntent(bytes.Clone(body), "", &got)
		if got != want {
			t.Fatalf("prepare intent for %q = %v, standalone = %v", raw, got, want)
		}
		if !bytes.Equal(plain.Body, withIntent.Body) || plain.ExpandedInputRaw != withIntent.ExpandedInputRaw {
			t.Fatalf("prepare output changed by intent option for %q", raw)
		}

		// searchReplay 分支用的选项组合。
		var replayIntent bool
		b1, e1 := prepareResponsesBodyWithOptions(bytes.Clone(body), responsesBodyPrepareOptions{forceStoreFalse: true})
		b2, e2 := prepareResponsesBodyWithOptions(bytes.Clone(body), responsesBodyPrepareOptions{forceStoreFalse: true, naturalImageIntent: &replayIntent})
		if replayIntent != want || !bytes.Equal(b1, b2) || e1 != e2 {
			t.Fatalf("replay prepare mismatch for %q: intent=%v want=%v", raw, replayIntent, want)
		}
	}
}

func TestResponsesNaturalImageIntentMemo(t *testing.T) {
	drawing := []byte(`{"input":"draw a cat"}`)
	plain := []byte(`{"input":"hello there"}`)
	memo := responsesNaturalImageIntentMemo{}
	if !memo.get(drawing) || memo.get(plain) || !memo.get(drawing) {
		t.Fatal("unknown memo must compute from the current body")
	}
	// 记录值只对同一切片生效：内容相同但另起的切片也重新计算（结果自然一致）。
	memo = responsesNaturalImageIntentMemo{body: plain, value: true, known: true}
	if !memo.get(plain) {
		t.Fatal("memo must reuse the recorded value for the recorded slice")
	}
	if memo.get(bytes.Clone(plain)) {
		t.Fatal("memo must recompute for a different slice")
	}
	memo = responsesNaturalImageIntentMemo{body: plain, value: true, known: true}
	if memo.get(plain[:len(plain)-1]) {
		t.Fatal("memo must recompute for a resliced body")
	}
}
