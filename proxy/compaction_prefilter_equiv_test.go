package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/codex2api/proxy/internal/jsonbody"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ---- 改动前的实现，原样保留作等价性基准 ----

func legacyNormalizeCompactionTriggerFinal(body []byte, addIfMissing bool) []byte {
	trigger := json.RawMessage(`{"type":"compaction_trigger"}`)
	input := jsonbody.GetView(body, "input")
	switch {
	case input.IsArray():
		items := input.Array()
		triggerCount := 0
		for _, item := range items {
			if isDirectCompactionTrigger(item) {
				triggerCount++
			}
		}
		if triggerCount == 0 && !addIfMissing {
			return body
		}
		if triggerCount == 1 && len(items) > 0 && isCanonicalCompactionTrigger(items[len(items)-1]) {
			return body
		}

		normalized := make([]json.RawMessage, 0, len(items)+1)
		for _, item := range items {
			if isDirectCompactionTrigger(item) {
				continue
			}
			raw := strings.TrimSpace(item.Raw)
			if raw == "" {
				encoded, err := json.Marshal(item.Value())
				if err != nil {
					return body
				}
				raw = string(encoded)
			}
			normalized = append(normalized, json.RawMessage(raw))
		}
		normalized = append(normalized, trigger)
		encoded, err := json.Marshal(normalized)
		if err != nil {
			return body
		}
		out, err := sjson.SetRawBytes(body, "input", encoded)
		if err != nil {
			return body
		}
		return out
	case input.IsObject() && isDirectCompactionTrigger(input):
		encoded, err := json.Marshal([]json.RawMessage{trigger})
		if err != nil {
			return body
		}
		out, err := sjson.SetRawBytes(body, "input", encoded)
		if err != nil {
			return body
		}
		return out
	case addIfMissing:
		normalized := make([]json.RawMessage, 0, 2)
		switch {
		case input.Type == gjson.String:
			msg, err := json.Marshal(map[string]any{
				"type": "message",
				"role": "user",
				"content": []any{
					map[string]any{"type": "input_text", "text": input.String()},
				},
			})
			if err != nil {
				return body
			}
			normalized = append(normalized, json.RawMessage(msg))
		case input.IsObject() && !isDirectCompactionTrigger(input):
			normalized = append(normalized, json.RawMessage(input.Raw))
		}
		normalized = append(normalized, trigger)
		encoded, err := json.Marshal(normalized)
		if err != nil {
			return body
		}
		out, err := sjson.SetRawBytes(body, "input", encoded)
		if err != nil {
			return body
		}
		return out
	default:
		return body
	}
}

func legacyRequestBodyCompactionMeta(body []byte) requestCompactionMeta {
	meta := requestCompactionMeta{}
	input := jsonbody.GetView(body, "input")
	inspect := func(item gjson.Result) {
		switch {
		case gjsonResultIsCompactionTrigger(item):
			meta.ProtocolTriggered = true
		case gjsonResultIsCompactionHistory(item):
			meta.HasHistory = true
		}
	}
	if input.Exists() {
		if input.IsArray() {
			input.ForEach(func(_, item gjson.Result) bool {
				inspect(item)
				return true
			})
		} else {
			inspect(input)
		}
	}

	meta.UsageTriggered = meta.ProtocolTriggered
	clientMetadata := gjson.GetBytes(body, "client_metadata")
	if clientMetadata.IsObject() {
		turnMetadata := clientMetadata.Get("x-codex-turn-metadata")
		if turnMetadata.Type == gjson.String && turnMetadataIndicatesCompaction(turnMetadata.String()) {
			meta.UsageTriggered = true
		}
	}
	return meta
}

// ---- 预判本身的性质 ----

// 预判用 ASCII 大小写折叠替代 strings.EqualFold，要求没有非 ASCII 码点与这些词里的
// 任一字符互为折叠（例如 K 的 KELVIN SIGN、s 的 LONG S 就会破坏这一前提）。
func TestJSONTextPrefilterWordsHaveNoNonASCIIFolds(t *testing.T) {
	for _, word := range []string{"compaction_trigger", "compaction", "context_compaction", "image_gen"} {
		for i := 0; i < len(word); i++ {
			target := string(word[i])
			for r := rune(0x80); r <= utf8.MaxRune; r++ {
				if r >= 0xD800 && r <= 0xDFFF {
					continue
				}
				if strings.EqualFold(string(r), target) {
					t.Fatalf("non-ASCII rune %U folds to %q in %q", r, target, word)
				}
			}
		}
	}
}

func TestJSONBytesMayContainText(t *testing.T) {
	cases := []struct {
		body string
		word string
		fold bool
		want bool
	}{
		{`{"type":"compaction_trigger"}`, "compaction_trigger", true, true},
		{`{"type":"COMPACTION_Trigger"}`, "compaction_trigger", true, true},
		{`{"type":"COMPACTION_Trigger"}`, "compaction_trigger", false, false},
		{`{"type":"\u0063ompaction_trigger"}`, "compaction_trigger", true, true},
		{`{"type":"\u0043OMPACTION\u005Ftrigge\u0072"}`, "compaction_trigger", true, true},
		{`{"type":"\u0063ompaction_trigge"}`, "compaction_trigger", true, false},
		{`{"type":"compaction-trigger"}`, "compaction_trigger", true, false},
		{`{"type":"compaction_trigge`, "compaction_trigger", true, false},
		{`compaction_trigger`, "compaction_trigger", true, true},
		{``, "compaction_trigger", true, false},
		{`\`, "compaction_trigger", true, false},
		{`\u00`, "compaction_trigger", true, false},
		{`\u006`, "compaction_trigger", true, false},
		{`\u0063`, "c", true, true},
		{`\uzzzz`, "c", true, false},
		{`ccompaction`, "compaction", true, true},
		{`\\compaction`, "compaction", true, true},
		{`additional_tools`, "additional_tools", false, true},
		{`additional\u005ftools`, "additional_tools", false, true},
		{`Additional_tools`, "additional_tools", false, false},
	}
	for _, tc := range cases {
		if got := jsonBytesMayContainText([]byte(tc.body), tc.word, tc.fold); got != tc.want {
			t.Fatalf("jsonBytesMayContainText(%q, %q, %v) = %v, want %v", tc.body, tc.word, tc.fold, got, tc.want)
		}
	}
}

// 对任意（含畸形）JSON，凡 gjson 能读出的字符串里含 word，预判必须为 true。
func assertPrefilterNoFalseNegative(t *testing.T, body []byte, word string, fold bool) {
	t.Helper()
	if jsonBytesMayContainText(body, word, fold) {
		return
	}
	contains := func(s string) bool {
		if fold {
			return strings.Contains(strings.ToLower(s), word) || prefilterContainsFold(s, word)
		}
		return strings.Contains(s, word)
	}
	var walk func(r gjson.Result, depth int)
	walk = func(r gjson.Result, depth int) {
		if depth > 64 {
			return
		}
		if r.Type == gjson.String && contains(r.String()) {
			t.Fatalf("false negative for %q in %q (string %q)", word, body, r.String())
		}
		if r.IsObject() || r.IsArray() {
			r.ForEach(func(k, v gjson.Result) bool {
				if contains(k.String()) {
					t.Fatalf("false negative for %q in %q (key %q)", word, body, k.String())
				}
				walk(v, depth+1)
				return true
			})
		}
	}
	walk(gjson.ParseBytes(body), 0)
}

// prefilterContainsFold 按 rune 滑动窗口做 strings.EqualFold，覆盖非 ASCII 折叠。
func prefilterContainsFold(s, word string) bool {
	runes := []rune(s)
	n := utf8.RuneCountInString(word)
	for i := 0; i+n <= len(runes); i++ {
		if strings.EqualFold(string(runes[i:i+n]), word) {
			return true
		}
	}
	return false
}

// ---- 等价性 ----

var compactionEquivalenceCases = []string{
	``,
	`null`,
	`[]`,
	`{}`,
	`not json`,
	`{"input":[]}`,
	`{"input":"hello"}`,
	`{"input":"compaction_trigger"}`,
	`{"input":{"type":"compaction_trigger"}}`,
	`{"input":{"type":"message","role":"user","content":"hi"}}`,
	`{"input":{"type":"compaction"}}`,
	`{"input":[{"type":"message","role":"user","content":"hi"}]}`,
	`{"input":[{"type":"message","role":"user","content":"compaction_trigger"}]}`,
	`{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"type compaction_trigger please"}]}]}`,
	`{"input":[{"type":"compaction_trigger"}]}`,
	`{"input":[{"type":"compaction_trigger"},{"type":"message","role":"user","content":"hi"}]}`,
	`{"input":[{"type":"message","role":"user","content":"hi"},{"type":"compaction_trigger"}]}`,
	`{"input":[{"type":"message","role":"user","content":"hi"},{"type":" Compaction_Trigger "}]}`,
	`{"input":[{"type":"COMPACTION_TRIGGER"},{"type":"message","role":"user","content":"hi"}]}`,
	`{"input":[{"type":"\u0063ompaction_trigger"},{"type":"message","role":"user","content":"hi"}]}`,
	`{"input":[{"type":"message","role":"user","content":"hi"},{"t\u0079pe":"compaction_trigger"}]}`,
	`{"input":[{"type":"compaction_trigger"},{"type":"compaction_trigger"}]}`,
	`{"input":[{"type":"compaction","encrypted_content":"x"},{"type":"message","role":"user","content":"hi"}]}`,
	`{"input":[{"type":"Context_Compaction","encrypted_content":"x"}]}`,
	`{"input":[{"type":"context\u005fcompaction"}]}`,
	`{"input":[{"type":"compaction_summary"}]}`,
	`{"input":[{"type":"message","content":[{"type":"compaction_trigger"}]}]}`,
	`{"input":[{"type":["compaction_trigger"]}]}`,
	`{"input":[{"type":"compaction_trigger","type":"message"}]}`,
	`{"input":[{"type":"message","type":"compaction_trigger"}]}`,
	`{"input":[{"type":"message"}],"input":[{"type":"compaction_trigger"}]}`,
	`{"input":[{"type":"compaction_trigger"}],"input":[{"type":"message"}]}`,
	`{"input":["compaction_trigger",1,null]}`,
	`{"input":[{"type":"compaction_trigger"},`,
	`{"input":[{"type":"message"},{"type":"compaction_trig`,
	`{"input":[{"type":"compaction_trigger"} {"type":"message"}]}`,
	`{"input":[{"type":compaction_trigger},{"type":"message"}]}`,
	`{"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"compaction\"}"},"input":[{"type":"message"}]}`,
	`{"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"Compaction\"}"}}`,
	`{"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"other\"}"}}`,
	`{"input":[{"type":"message","role":"user","content":"hi"}],"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"\\u0063ompaction\"}"}}`,
	`{"model":"gpt","instructions":"compaction notes","input":[{"type":"message","role":"user","content":"hi"}]}`,
}

func assertCompactionEquivalent(t *testing.T, raw string) {
	t.Helper()
	body := []byte(raw)
	for _, add := range []bool{false, true} {
		want := legacyNormalizeCompactionTriggerFinal(bytes.Clone(body), add)
		got := normalizeCompactionTriggerFinal(bytes.Clone(body), add)
		if !bytes.Equal(want, got) {
			t.Fatalf("normalizeCompactionTriggerFinal(add=%v) mismatch for %q:\nlegacy=%q\nnew   =%q", add, raw, want, got)
		}
	}
	if want, got := legacyRequestBodyCompactionMeta(body), requestBodyCompactionMeta(body); want != got {
		t.Fatalf("requestBodyCompactionMeta mismatch for %q: legacy=%+v new=%+v", raw, want, got)
	}
	for _, word := range []string{"compaction_trigger", "compaction"} {
		assertPrefilterNoFalseNegative(t, body, word, true)
	}
}

func TestCompactionPrefilterEquivalentToLegacy(t *testing.T) {
	for _, raw := range compactionEquivalenceCases {
		assertCompactionEquivalent(t, raw)
	}
}

func TestCompactionPrefilterEquivalentToLegacyRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(45))
	types := []string{"message", "function_call", "compaction", "compaction_trigger", "Compaction_Trigger",
		`\u0063ompaction_trigger`, " compaction_trigger ", "context_compaction", "reasoning", "compaction_summary"}
	for n := 0; n < 2000; n++ {
		var b strings.Builder
		b.WriteString(`{"model":"gpt-5.5","input":[`)
		count := rng.Intn(8)
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			switch rng.Intn(6) {
			case 0:
				fmt.Fprintf(&b, `{"type":"message","role":"user","content":%q}`, types[rng.Intn(len(types))])
			case 1:
				b.WriteString(`"compaction_trigger"`)
			default:
				fmt.Fprintf(&b, `{"type":"%s"}`, types[rng.Intn(len(types))])
			}
		}
		b.WriteString(`],"stream":true}`)
		raw := b.String()
		assertCompactionEquivalent(t, raw)
		assertCompactionEquivalent(t, raw[:rng.Intn(len(raw))])
	}
}

func TestCompactionPrefilterEquivalentLargeInput(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"input":[`)
	for i := 0; i < 4000; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"type":"message","role":"user","content":[{"type":"input_text","text":"lorem ipsum \\u4e2d"}]}`)
	}
	b.WriteString(`]}`)
	plain := b.String()
	assertCompactionEquivalent(t, plain)
	withTrigger := strings.Replace(plain, `{"input":[`, `{"input":[{"type":"compaction_trigger"},`, 1)
	assertCompactionEquivalent(t, withTrigger)
	escaped := strings.TrimSuffix(plain, `]}`) + `,{"type":"\u0063ompaction"}]}`
	assertCompactionEquivalent(t, escaped)
	if !legacyRequestBodyCompactionMeta([]byte(escaped)).HasHistory {
		t.Fatal("escaped direct compaction item must be detected")
	}
}

func FuzzCompactionPrefilterEquivalentToLegacy(f *testing.F) {
	for _, raw := range compactionEquivalenceCases {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		assertCompactionEquivalent(t, raw)
	})
}

// 快路径（无相关转义时的字面查找）必须与逐位置完整匹配逐输入一致。
var prefilterWords = []struct {
	word string
	fold bool
}{{"compaction_trigger", true}, {"compaction", true}, {"image_gen", true}, {"additional_tools", false}, {"<codex2api-codex-image-generation>", false}}

func assertPrefilterFastMatchesSlow(t *testing.T, body []byte) {
	t.Helper()
	for _, w := range prefilterWords {
		if got, want := jsonBytesMayContainText(body, w.word, w.fold), jsonTextMayContainSlow(body, w.word, w.fold); got != want {
			t.Fatalf("prefilter(%q, fold=%v) on %q: fast=%v slow=%v", w.word, w.fold, body, got, want)
		}
	}
}

func TestJSONBytesMayContainTextMatchesSlowPath(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	alphabet := []string{"c", "C", "o", "m", "p", "P", "a", "t", "i", "n", "_", "r", "g", "e", "x", "d", "l", "s",
		`\u0063`, `\u005f`, `\u0050`, `\u003c`, `\u4e2d`, `\\`, `\n`, `"`, " ", "compaction", "COMPACTION_TRIGGER",
		"image_gen", "Image_Gen", "additional_tools", "<codex2api-codex-image-generation>", "<codex2api"}
	for n := 0; n < 20000; n++ {
		var b strings.Builder
		for k := rng.Intn(24); k > 0; k-- {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		body := []byte(b.String())
		assertPrefilterFastMatchesSlow(t, body)
		for _, w := range prefilterWords {
			assertPrefilterNoFalseNegative(t, body, w.word, w.fold)
		}
	}
}

func FuzzJSONBytesMayContainTextMatchesSlowPath(f *testing.F) {
	f.Add([]byte(`{"type":"\u0063ompaction_trigger"}`))
	f.Add([]byte(`COMPACTION_trigger \u003c image_GEN`))
	f.Fuzz(func(t *testing.T, body []byte) {
		assertPrefilterFastMatchesSlow(t, body)
		for _, w := range prefilterWords {
			assertPrefilterNoFalseNegative(t, body, w.word, w.fold)
		}
	})
}
