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
		b.WriteString(`{"model":"m","messages":[`)
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			switch rng.Intn(10) {
			case 0:
				b.WriteString(`"plain"`)
				continue
			case 1:
				b.WriteString(`null`)
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
