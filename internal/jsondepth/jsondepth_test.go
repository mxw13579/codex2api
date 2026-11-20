package jsondepth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func nestedArrays(depth int) []byte {
	return []byte(strings.Repeat("[", depth) + strings.Repeat("]", depth))
}

func nestedObjects(depth int) []byte {
	return []byte(strings.Repeat(`{"a":`, depth) + "1" + strings.Repeat("}", depth))
}

func nestedMixed(depth int) []byte {
	var open, closeB strings.Builder
	for i := 0; i < depth; i++ {
		if i%2 == 0 {
			open.WriteString(`{"k":`)
			closeB.WriteString("}")
		} else {
			open.WriteString(`[1,`)
			closeB.WriteString("]")
		}
	}
	c := []byte(closeB.String())
	for i, j := 0, len(c)-1; i < j; i, j = i+1, j-1 {
		c[i], c[j] = c[j], c[i]
	}
	return []byte(open.String() + "0" + string(c))
}

// refDepth 是基于 encoding/json Decoder 的独立参考实现（仅用于合法 JSON）。
func refDepth(t *testing.T, data []byte) int {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(data))
	depth, maxDepth := 0, 0
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if d, ok := tok.(json.Delim); ok {
			if d == '[' || d == '{' {
				depth++
				if depth > maxDepth {
					maxDepth = depth
				}
			} else {
				depth--
			}
		}
	}
	return maxDepth
}

func TestWithinBoundaries(t *testing.T) {
	for _, gen := range []struct {
		name string
		fn   func(int) []byte
	}{{"arrays", nestedArrays}, {"objects", nestedObjects}, {"mixed", nestedMixed}} {
		for _, depth := range []int{9999, 10000, 10001} {
			data := gen.fn(depth)
			want := depth <= DefaultMaxDepth
			if got := Within(data, DefaultMaxDepth); got != want {
				t.Fatalf("%s depth=%d: Within=%v want %v", gen.name, depth, got, want)
			}
			if !gjson.ValidBytes(data) {
				t.Fatalf("%s depth=%d: fixture must be valid JSON", gen.name, depth)
			}
			if got := ValidBytes(data); got != want {
				t.Fatalf("%s depth=%d: ValidBytes=%v want %v", gen.name, depth, got, want)
			}
			// 未闭合的同深度前缀
			unclosed := data[:bytes.LastIndexAny(data, "[{")+1]
			if got := Within(unclosed, DefaultMaxDepth); got != want {
				t.Fatalf("%s unclosed depth=%d: Within=%v want %v", gen.name, depth, got, want)
			}
		}
	}
}

func TestWithinMatchesReferenceOnValidJSON(t *testing.T) {
	cases := []string{
		`{}`, `[]`, `1`, `"x"`, `null`, ` [ ] `,
		`{"a":[1,2,{"b":[[],[{}]]}],"c":"[[[{{{"}`,
		`["\"[[[", "\\", "\\\"[", "[{"]`,
		`{"a":{"b":{"c":{"d":[[[["x"]]]]}}}}`,
		`[-0.5e+10, 1E3, 0, true, false, null, "\/"]`,
	}
	for _, c := range cases {
		data := []byte(c)
		if !json.Valid(data) {
			t.Fatalf("fixture %q must be valid", c)
		}
		d := refDepth(t, data)
		for limit := 1; limit <= d+2; limit++ {
			// 小输入走字节数快速路径，这里把 limit 与真实深度比较时需排除它。
			if len(data) <= limit {
				continue
			}
			if got, want := Within(data, limit), d <= limit; got != want {
				t.Fatalf("%q limit=%d depth=%d: Within=%v want %v", c, limit, d, got, want)
			}
		}
	}
}

func TestWithinStringContentIsNotCounted(t *testing.T) {
	limit := 8
	inner := strings.Repeat("[{", 1000)
	cases := []string{
		`{"text":"` + inner + `"}`,
		`["` + strings.Repeat(`\"[`, 1000) + `"]`,
		`["` + strings.Repeat(`\\`, 500) + `", "` + inner + `"]`,
		`["[` + inner + `"]`,
	}
	for _, c := range cases {
		if !gjson.Valid(c) {
			t.Fatalf("fixture must be valid: %.40q", c)
		}
		if !Within([]byte(c), limit) {
			t.Fatalf("brackets inside a string must not count: %.60q", c)
		}
	}
	// 反斜杠转义的引号不能结束字符串；偶数个反斜杠后的引号才结束字符串。
	escapedEnd := `["\\"` + strings.Repeat("[", 100) + strings.Repeat("]", 100) + `]`
	if Within([]byte(escapedEnd), limit) {
		t.Fatalf("even backslashes close the string; following brackets must count")
	}
}

func TestWithinMalformedIsConservative(t *testing.T) {
	limit := 16
	deep := strings.Repeat("[", 1000)
	cases := map[string]string{
		"unclosed":                   deep,
		"stray closers then deep":    strings.Repeat("]", 1000) + deep,
		"mismatched closers":         strings.Repeat("[}", 1000),
		"invalid number prefix":      `{"x":01,"m":` + deep,
		"bad escape":                 `["\q` + deep,
		"raw newline in string":      "[\"a\n" + deep,
		"control char in string":     "[\"a\x01" + deep,
		"non-JSON leading bytes":     "abc" + deep,
		"utf8 bom":                   "\xef\xbb\xbf" + deep,
		"trailing garbage":           `{}` + deep,
		"gjson tonum quote desync":   `[[1" ,` + strings.Repeat(`["]"`, 1000),
		"gjson tonum swallows quote": `[1" ,` + deep, // 朴素的“跟踪字符串”扫描会被它绕过：gjson Value 递归 20000 层
		"bare word then quote":       `[a"` + strings.Repeat(`["]"`, 1000),
		"truncated unicode escape":   `["\u12` + deep,
		"missing colon":              `{"a" ` + deep,
		"missing comma":              `[1 ` + deep,
		"object key not string":      `{1:` + deep,
		"single quotes":              `{'a':` + deep,
		"parens":                     `[(` + deep,
		"nested within limit then":   strings.Repeat("[", 10) + "x" + deep,
	}
	for name, c := range cases {
		if Within([]byte(c), limit) {
			t.Fatalf("%s: malformed deep input must be rejected", name)
		}
	}
	// 未闭合字符串里的括号：所有解析器都把它们视为字符串内容，不应误拒。
	if !Within([]byte(`{"a":"`+deep), limit) {
		t.Fatalf("brackets inside an unterminated but otherwise valid string must not count")
	}
	// 畸形但浅的输入保持放行。
	for _, c := range []string{`{"x":01}`, `[1,]`, `{"a":1,}`, `]]]]`, `}{`, `[1 2]`, `"\q"`} {
		if !Within([]byte(c), limit) {
			t.Fatalf("shallow malformed input %q must pass", c)
		}
	}
}

func TestParseMaxDepth(t *testing.T) {
	cases := map[string]int{
		"":        DefaultMaxDepth,
		"  ":      DefaultMaxDepth,
		"abc":     DefaultMaxDepth,
		"0":       DefaultMaxDepth,
		"-5":      DefaultMaxDepth,
		"2000000": DefaultMaxDepth,
		"64":      64,
		" 20000 ": 20000,
		"1000000": 1000000,
	}
	for raw, want := range cases {
		if got := parseMaxDepth(raw); got != want {
			t.Fatalf("parseMaxDepth(%q)=%d want %d", raw, got, want)
		}
	}
}

func TestLargeLimitAllocatesStack(t *testing.T) {
	data := nestedArrays(20001)
	if !Within(data, 20001) || Within(data, 20000) {
		t.Fatalf("custom limit above default must work")
	}
}

func TestValidMatchesGJSONWithinLimit(t *testing.T) {
	cases := []string{"", " ", "{}", "[1,2]", `{"a":01}`, `[1,]`, `"x"`, "nul", "[\"a\nb\"]", `{"a":"\u00zz"}`}
	for _, c := range cases {
		if Valid(c) != gjson.Valid(c) || ValidBytes([]byte(c)) != gjson.ValidBytes([]byte(c)) {
			t.Fatalf("Valid(%q) diverges from gjson", c)
		}
	}
	deep := string(nestedArrays(DefaultMaxDepth + 1))
	if Valid(deep) || ValidBytes([]byte(deep)) {
		t.Fatalf("too deep input must be reported invalid")
	}
}

// 极大输入的耗时必须线性（不能二次方）。
func TestWithinHugeInputIsFast(t *testing.T) {
	inputs := map[string][]byte{
		"unclosed 48MB":    bytes.Repeat([]byte("["), 48<<20),
		"closed 11MB deep": nestedArrays(5_700_000),
		"strings 48MB":     []byte(`["` + strings.Repeat("a[{", 16<<20) + `"]`),
		"tail 48MB":        append([]byte(`{"x":01,"m":`), bytes.Repeat([]byte("[{"), 24<<20)...),
	}
	for name, data := range inputs {
		start := time.Now()
		_ = Within(data, DefaultMaxDepth)
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("%s took %v", name, elapsed)
		}
	}
}

// exerciseRecursiveParsers 运行所有已知的递归式解析路径。调用方保证在受限栈下运行，
// 若深度检查被绕过，会以 stack overflow 让测试进程失败。
func exerciseRecursiveParsers(data []byte) {
	_ = gjson.ValidBytes(data)
	_ = gjson.Valid(string(data))
	_ = gjson.ParseBytes(data).Value()
	root := gjson.ParseBytes(data)
	var walk func(r gjson.Result, budget int)
	walk = func(r gjson.Result, budget int) {
		if budget == 0 {
			return
		}
		if r.IsArray() {
			for _, item := range r.Array() {
				walk(item, budget-1)
			}
		} else if r.IsObject() {
			r.ForEach(func(_, v gjson.Result) bool { walk(v, budget-1); return true })
		}
	}
	walk(root, 1<<30)
	_ = json.Valid(data)
}

const (
	fuzzLimit    = 64
	fuzzRepeat   = 20000
	fuzzMaxStack = 2 << 20 // 64 层远低于该值；被绕过时 20000 层会超过它
)

func amplify(unit []byte, prefix []byte) []byte {
	out := make([]byte, 0, len(prefix)+len(unit)*fuzzRepeat)
	out = append(out, prefix...)
	for i := 0; i < fuzzRepeat; i++ {
		out = append(out, unit...)
	}
	return out
}

func checkBounded(t *testing.T, data []byte) {
	if !Within(data, fuzzLimit) {
		return
	}
	prev := debug.SetMaxStack(fuzzMaxStack)
	defer debug.SetMaxStack(prev)
	done := make(chan struct{})
	go func() { // 新 goroutine 从小栈开始，栈增长受 SetMaxStack 约束
		defer close(done)
		exerciseRecursiveParsers(data)
	}()
	<-done
}

var fuzzSeeds = []string{
	`[`, `{"a":`, `["]"`, `[1" ,`, `"`, `\`, `\"`, `["\"`, `[}`, `]`, `[a"`, `["\q`, "[\"\n", `[0`, `[-`, `{"`, `[t`, `[(`, `,[`, `:[`, `[",`,
}

func TestBoundedPropertyOnAmplifiedSeeds(t *testing.T) {
	for _, unit := range fuzzSeeds {
		for _, prefix := range fuzzSeeds {
			checkBounded(t, amplify([]byte(unit), []byte(prefix)))
		}
	}
	rng := rand.New(rand.NewSource(1))
	alphabet := []byte(`[]{}",:\ 01-eatnu` + "\n\x01")
	for n := 0; n < 3000; n++ {
		unit := make([]byte, 1+rng.Intn(6))
		for i := range unit {
			unit[i] = alphabet[rng.Intn(len(alphabet))]
		}
		prefix := make([]byte, rng.Intn(4))
		for i := range prefix {
			prefix[i] = alphabet[rng.Intn(len(alphabet))]
		}
		checkBounded(t, amplify(unit, prefix))
	}
}

// FuzzWithinBoundsRecursiveParsers：通过检查的输入在 2MB 栈上限内跑完 gjson 校验/解析。
// go test -run '^$' -fuzz FuzzWithinBoundsRecursiveParsers ./internal/jsondepth
func FuzzWithinBoundsRecursiveParsers(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s), []byte(""))
	}
	f.Fuzz(func(t *testing.T, unit, prefix []byte) {
		if len(unit) == 0 || len(unit) > 16 || len(prefix) > 32 {
			return
		}
		checkBounded(t, amplify(unit, prefix))
		checkBounded(t, append(append([]byte{}, prefix...), unit...))
	})
}

// FuzzWithinValidJSONAgreesWithReference：合法 JSON 上与 encoding/json 的深度一致
// （严格校验器不能把合法 JSON 误判为畸形，否则其后的括号会被保守计数而误拒）。
func FuzzWithinValidJSONAgreesWithReference(f *testing.F) {
	for _, s := range []string{`{}`, `[1,"a",{"b":[null,true,false,-1.5e3]}]`, `"é\n"`, `{"a":"[[{"}`, ` [ [ ] ] `} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if !json.Valid(data) {
			return
		}
		d := refDepth(t, data)
		if !Within(data, d) && len(data) > d {
			t.Fatalf("valid JSON %q with depth %d rejected at limit %d", data, d, d)
		}
		if d > 1 && len(data) > d-1 && Within(data, d-1) {
			t.Fatalf("valid JSON %q with depth %d accepted at limit %d", data, d, d-1)
		}
	})
}

func benchmarkBody(size int) []byte {
	var b strings.Builder
	b.WriteString(`{"model":"gpt-5.5","stream":true,"input":[`)
	for i := 0; b.Len() < size; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"type":"message","role":"user","content":[{"type":"input_text","text":"line %d: func f(x []int) map[string]any { return nil } \"quoted\" \\n"}]}`, i)
	}
	b.WriteString(`],"tools":[{"type":"function","name":"f","parameters":{"type":"object","properties":{"a":{"type":"array","items":{"type":"string"}}}}}]}`)
	return []byte(b.String())
}

func BenchmarkWithin600KB(b *testing.B) { benchWithin(b, 600<<10) }
func BenchmarkWithin2MB(b *testing.B)   { benchWithin(b, 2<<20) }

func BenchmarkGJSONValid2MB(b *testing.B) {
	data := benchmarkBody(2 << 20)
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		_ = gjson.ValidBytes(data)
	}
}

func benchWithin(b *testing.B, size int) {
	data := benchmarkBody(size)
	if !gjson.ValidBytes(data) || !Within(data, DefaultMaxDepth) {
		b.Fatal("benchmark body must be valid and shallow")
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Within(data, DefaultMaxDepth)
	}
}

// 长文本（较少转义）请求体：字符串内走 8 字节快速路径。
func BenchmarkWithin2MBLongStrings(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`{"model":"gpt-5.5","input":[`)
	para := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 90)
	for i := 0; sb.Len() < 2<<20; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"role":"user","content":[{"type":"input_text","text":"%s\n"}]}`, para)
	}
	sb.WriteString(`]}`)
	data := []byte(sb.String())
	if !gjson.ValidBytes(data) {
		b.Fatal("benchmark body must be valid")
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Within(data, DefaultMaxDepth)
	}
}

func TestHasStringSpecialMatchesByteLoop(t *testing.T) {
	naive := func(x uint64) bool {
		for k := 0; k < 8; k++ {
			if strStop[byte(x>>(8*k))] {
				return true
			}
		}
		return false
	}
	rng := rand.New(rand.NewSource(2))
	for n := 0; n < 200000; n++ {
		x := rng.Uint64()
		if got, want := hasStringSpecial(x), naive(x); got != want {
			t.Fatalf("hasStringSpecial(%#x)=%v want %v", x, got, want)
		}
	}
	for c := 0; c < 256; c++ {
		for pos := 0; pos < 8; pos++ {
			for _, fill := range []uint64{0x6161616161616161, 0xffffffffffffffff, 0x2020202020202020, 0x8080808080808080} {
				x := fill &^ (0xff << (8 * pos))
				x |= uint64(c) << (8 * pos)
				if got, want := hasStringSpecial(x), naive(x); got != want {
					t.Fatalf("byte %#x at %d fill %#x: got %v want %v", c, pos, fill, got, want)
				}
			}
		}
	}
}
