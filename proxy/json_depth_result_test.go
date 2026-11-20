package proxy

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// resultTooDeep 只检查 gjson.JSON 类型的结果；这类结果的 Raw 必须以 '{'/'[' 开头，
// 这样 Check 走的是“顶层是容器”的严格扫描，而不是“顶层非容器”的保守规则。
func TestResultTooDeepOnlyChecksContainers(t *testing.T) {
	doc := " \n{ \"a\" : [ {\"b\":1} , [ 2 ] ] , \"s\" : \"[[[\" , \"n\" : 1 }"
	var results []gjson.Result
	results = append(results, gjson.Parse(doc), gjson.Get(doc, "a"), gjson.Get(doc, "a.0"), gjson.Get(doc, "a.1"))
	results = append(results, gjson.Get(doc, "a").Array()...)
	gjson.Parse(doc).ForEach(func(_, v gjson.Result) bool { results = append(results, v); return true })
	gjson.Get(doc, "a").ForEach(func(_, v gjson.Result) bool { results = append(results, v); return true })
	for _, r := range results {
		if r.Type == gjson.JSON && r.Raw[0] != '{' && r.Raw[0] != '[' {
			t.Fatalf("gjson.JSON result Raw must start with a bracket: %q", r.Raw)
		}
	}
	deepString := gjson.Parse(`"` + strings.Repeat("[", 20000) + `"`)
	if deepString.Type != gjson.String || resultTooDeep(deepString) {
		t.Fatalf("string results must never be reported as too deep")
	}
	if !resultTooDeep(gjson.Parse(deepClosed(20000))) {
		t.Fatalf("deep arrays must be reported as too deep")
	}
	for _, s := range []string{"123", "true", "null", `"x"`, ""} {
		if resultTooDeep(gjson.Parse(s)) {
			t.Fatalf("scalar %q must not be too deep", s)
		}
	}
}
