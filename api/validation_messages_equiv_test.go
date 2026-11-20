package api

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// legacyValidateMessages 是改为单次 ForEach 之前的按下标实现，原样保留作等价性基准。
func legacyValidateMessages() ValidationRule {
	return func(value gjson.Result, path string) *ValidationError {
		if !value.Exists() || !value.IsArray() {
			return nil
		}

		validRoles := map[string]bool{
			"system":    true,
			"developer": true,
			"user":      true,
			"assistant": true,
			"tool":      true,
		}

		for i := 0; i < int(value.Get("#").Int()); i++ {
			msgPath := fmt.Sprintf("%s.%d", path, i)
			role := value.Get(fmt.Sprintf("%d.role", i)).String()
			if role == "" {
				return &ValidationError{
					Field:   msgPath + ".role",
					Message: fmt.Sprintf("Message at index %d is missing 'role' field", i),
					Code:    "missing_message_role",
				}
			}
			if !validRoles[role] {
				return &ValidationError{
					Field:   msgPath + ".role",
					Message: fmt.Sprintf("Invalid role '%s' at message index %d", role, i),
					Code:    "invalid_message_role",
				}
			}
			content := value.Get(fmt.Sprintf("%d.content", i))
			toolCalls := value.Get(fmt.Sprintf("%d.tool_calls", i))
			if !content.Exists() && role != "tool" {
				if !(role == "assistant" && toolCalls.Exists()) {
					return &ValidationError{
						Field:   msgPath + ".content",
						Message: fmt.Sprintf("Message at index %d is missing 'content' field", i),
						Code:    "missing_message_content",
					}
				}
			}
		}
		return nil
	}
}

func assertValidateMessagesEquivalent(t *testing.T, raw string) {
	t.Helper()
	check := func(value gjson.Result, path string) {
		want := legacyValidateMessages()(value, path)
		got := ValidateMessages()(value, path)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("ValidateMessages mismatch for %q (path %q):\nlegacy=%#v\nnew   =%#v", raw, path, want, got)
		}
	}
	root := gjson.Parse(raw)
	check(root, "messages")
	check(root.Get("messages"), "messages")
	// 经 ValidateRequest 的顶层索引取值，与线上调用路径一致。
	topLevel := map[string]gjson.Result{}
	root.ForEach(func(key, value gjson.Result) bool {
		topLevel[key.String()] = value
		return true
	})
	check(topLevel["messages"], "messages")
}

var validateMessagesEquivalenceCases = []string{
	``,
	`null`,
	`{}`,
	`[]`,
	`{"messages":[]}`,
	`{"messages":"hi"}`,
	`{"messages":{}}`,
	`{"messages":null}`,
	`{"messages":[{"role":"user","content":"hi"}]}`,
	`{"messages":[{"role":"user"}]}`,
	`{"messages":[{"content":"hi"}]}`,
	`{"messages":[{"role":"","content":"hi"}]}`,
	`{"messages":[{"role":"bogus","content":"hi"}]}`,
	`{"messages":[{"role":1,"content":"hi"}]}`,
	`{"messages":[{"role":{"x":1},"content":"hi"}]}`,
	`{"messages":[{"role":["user"],"content":"hi"}]}`,
	`{"messages":[{"role":"assistant","tool_calls":[]}]}`,
	`{"messages":[{"role":"assistant","tool_calls":null}]}`,
	`{"messages":[{"role":"assistant"}]}`,
	`{"messages":[{"role":"tool"}]}`,
	`{"messages":[{"role":"user","content":null}]}`,
	`{"messages":[{"role":"user","content":"a"},{"role":"user"}]}`,
	`{"messages":[{"role":"user","content":"a"},{"role":"x","content":"b"},{"role":"user"}]}`,
	`{"messages":[{"role":"user","content":"a"},"str",{"role":"user"}]}`,
	`{"messages":[1,2,3]}`,
	`{"messages":[null]}`,
	`{"messages":[[{"role":"user","content":"a"}]]}`,
	`{"messages":[{"role":"user","role":"bogus","content":"a"}]}`,
	`{"messages":[{"role":"bogus","role":"user","content":"a"}]}`,
	`{"messages":[{"\u0072ole":"user","content":"a"}]}`,
	`{"messages":[{"role":"\u0075ser","content":"a"}]}`,
	`{"messages":[{"role":"user","c\u006fntent":"a"}]}`,
	`{"messages":[{"role":"assistant","tool\u005fcalls":[]}]}`,
	`{"messages":[{"role":"user","content":"a"}],"messages":[{"role":"x"}]}`,
	`{"messages":[{"role":"User","content":"a"}]}`,
	`{"messages":[ {"role" : "user" , "content" : "a"} , {"role":"system","content":""} ]}`,
	// 畸形 JSON：截断、缺分隔符、多余逗号。
	`{"messages":[{"role":"user","content":"a"},`,
	`{"messages":[{"role":"user","content":"a"},{"role":"us`,
	`{"messages":[{"role":"user","content":"a"} {"role":"user"}]}`,
	`{"messages":[{"role":"user","content":"a"},,{"role":"user"}]}`,
	`{"messages":[,{"role":"user"}]}`,
	`{"messages":[{"role":"user","content":"a"}]`,
	`{"messages":[{"role":"user" "content":"a"}]}`,
	`{"messages":[{"role":"user","content":"a"},]}`,
	`{"messages":[{"role":"user","content":}]}`,
	`{"messages":[{]}`,
	`{"messages":[}`,
	`{"messages":[`,
	`{"messages":[{"role":"user","content":"a\"}]}`,
	`{"messages":[{"role":"user","content":"a"}]]]}`,
	`[{"role":"user","content":"a"},{"role":"tool"}]`,
}

func TestValidateMessagesEquivalentToLegacy(t *testing.T) {
	for _, raw := range validateMessagesEquivalenceCases {
		assertValidateMessagesEquivalent(t, raw)
	}
}

func TestValidateMessagesEquivalentToLegacyRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(417))
	roles := []string{`"user"`, `"assistant"`, `"tool"`, `"system"`, `"developer"`, `"bogus"`, `""`, `1`, `null`, `{}`, `"\u0075ser"`}
	for n := 0; n < 3000; n++ {
		count := rng.Intn(12)
		var b strings.Builder
		if rng.Intn(5) == 0 {
			// messages 之前的非法字节：入口全文校验会在浅层返回 false。
			b.WriteString(`{"model":"m","x":01,"messages":[`)
		} else {
			b.WriteString(`{"model":"m","messages":[`)
		}
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			switch rng.Intn(12) {
			case 0:
				b.WriteString(`"plain"`)
				continue
			case 1:
				b.WriteString(`null`)
				continue
			case 2:
				// 深嵌套元素：多数在 encoding/json 的 10000 层上限以内（走 ForEach），
				// 少数超过上限（走按下标的旧循环）。
				b.WriteString(deepJSONValue(rng, randomNestingDepth(rng)))
				continue
			}
			b.WriteString("{")
			var fields []string
			if rng.Intn(6) != 0 {
				fields = append(fields, `"role":`+roles[rng.Intn(len(roles))])
			}
			if rng.Intn(3) != 0 {
				fields = append(fields, `"content":"x"`)
			}
			if rng.Intn(4) == 0 {
				fields = append(fields, `"tool_calls":[{"id":"c"}]`)
			}
			if rng.Intn(8) == 0 {
				fields = append(fields, `"content":`+deepJSONValue(rng, randomNestingDepth(rng)))
			}
			rng.Shuffle(len(fields), func(a, c int) { fields[a], fields[c] = fields[c], fields[a] })
			b.WriteString(strings.Join(fields, ","))
			b.WriteString("}")
		}
		b.WriteString(`],"stream":true}`)
		raw := b.String()
		assertValidateMessagesEquivalent(t, raw)
		// 截断后的畸形版本。
		if len(raw) > 2 {
			assertValidateMessagesEquivalent(t, raw[:rng.Intn(len(raw))])
		}
	}
}

func TestValidateMessagesEquivalentLargeArray(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"messages":[`)
	for i := 0; i < 5000; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"role":"user","content":"hello"}`)
	}
	b.WriteString(`,{"role":"assistant"}]}`)
	assertValidateMessagesEquivalent(t, b.String())
}

func FuzzValidateMessagesEquivalentToLegacy(f *testing.F) {
	for _, raw := range validateMessagesEquivalenceCases {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		assertValidateMessagesEquivalent(t, raw)
	})
}

// deepJSONValue 生成嵌套 depth 层的合法 JSON 值（数组或对象交替随机）。
func deepJSONValue(rng *rand.Rand, depth int) string {
	var open, closeTail strings.Builder
	for d := 0; d < depth; d++ {
		if rng.Intn(2) == 0 {
			open.WriteString("[")
			closeTail.WriteString("]")
		} else {
			open.WriteString(`{"role":`)
			closeTail.WriteString("}")
		}
	}
	tail := []byte(closeTail.String())
	for i, j := 0, len(tail)-1; i < j; i, j = i+1, j-1 {
		tail[i], tail[j] = tail[j], tail[i]
	}
	return open.String() + `"user"` + string(tail)
}

func randomNestingDepth(rng *rand.Rand) int {
	if rng.Intn(20) == 0 {
		return 10001 + rng.Intn(200)
	}
	return 1 + rng.Intn(64)
}

func TestValidateMessagesEquivalentDeepNesting(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	for _, depth := range []int{1, 2, 63, 1000, 9999, 10000, 10001, 20000} {
		value := deepJSONValue(rng, depth)
		for _, prefix := range []string{`{"model":"m","messages":`, `{"model":"m","x":01,"messages":`} {
			assertValidateMessagesEquivalent(t, prefix+value+`}`)
			assertValidateMessagesEquivalent(t, prefix+`[`+value+`,{"role":"user","content":"x"}]}`)
			assertValidateMessagesEquivalent(t, prefix+`[{"role":"user","content":`+value+`},{"role":"user"}]}`)
			assertValidateMessagesEquivalent(t, prefix+strings.Repeat("[", depth))
		}
	}
}

// 非法字节在 messages 之前 + messages 为 1600 万层未闭合的 "["：改动前入口的全文校验
// 在 "01" 处即返回、从不深入，校验阶段只做迭代式取值，返回 missing_message_role。
// 不得因为对 messages 子段做递归式合法性检查而栈溢出（fatal error，recover 不住）。
func TestValidateRequestDeepMessagesAfterInvalidPrefixDoesNotOverflow(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","x":01,"messages":` + strings.Repeat("[", 16<<20))
	validator := NewValidator(body)
	result := validator.ValidateRequest(ChatCompletionValidationRules())
	want := ValidationError{
		Field:   "messages.0.role",
		Message: "Message at index 0 is missing 'role' field",
		Code:    "missing_message_role",
	}
	if result.Valid || len(result.Errors) != 1 || result.Errors[0] != want {
		t.Fatalf("result = %+v, want single %+v", result, want)
	}
	if got := ValidateMessages()(gjson.GetBytes(body, "messages"), "messages"); got == nil || *got != want {
		t.Fatalf("ValidateMessages = %+v, want %+v", got, want)
	}
}
