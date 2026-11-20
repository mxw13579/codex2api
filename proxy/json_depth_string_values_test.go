package proxy

import (
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// 回归：第二道防线的预检只针对对象/数组。字符串（哪怕含大量 '['）的 Value 不递归，
// 这些输入必须与 main 的行为一致：不报错、不被丢弃、值原样保留。
// 本文件只用 main 上也存在的函数，可在 main 上运行作对照。
func TestJSONDepthStringValuesBehaveLikeMain(t *testing.T) {
	brackets := strings.Repeat("[", 10001)
	pad := `"` + brackets + `"`

	// images strip：字符串元素原样保留，image_generation 照常剥离。
	kept, removed := stripImageGenerationToolsFromArray(gjson.Parse(`[{"type":"image_generation"},` + pad + `]`).Array())
	if !removed || len(kept) != 1 || kept[0] != brackets {
		t.Fatalf("strip must drop image_generation and keep the string tool: removed=%v len=%d", removed, len(kept))
	}

	// kiro：字符串型 tool_choice 走与 main 相同的取值校验（这里是“不支持的值”），
	// 而不是被深度预检当作过深拒绝。
	_, err := kiroResponsesToMessages([]byte(`{"model":"m","input":"hi","tool_choice":`+pad+`}`), http.Header{})
	if err == nil || !strings.HasPrefix(err.Error(), "unsupported tool_choice value") {
		msg := "<nil>"
		if err != nil {
			msg = err.Error()
			if len(msg) > 80 {
				msg = msg[:80]
			}
		}
		t.Fatalf("kiro string tool_choice must hit the main-equivalent value check: %s", msg)
	}
	// kiro：tools 里的字符串元素不导致整体报错。
	if _, err := kiroResponsesToMessages([]byte(`{"model":"m","input":"hi","tools":[`+pad+`]}`), http.Header{}); err != nil {
		t.Fatalf("kiro string tool: err=%v", err)
	}

	// grok：stop 为含大量 '[' 的字符串时按字符串处理。
	out, err := convertResponsesToMessagesRequest([]byte(`{"model":"m","input":"hi","max_output_tokens":8,"stop":` + pad + `}`))
	if err != nil || gjson.GetBytes(out, "stop_sequences.0").String() != brackets {
		t.Fatalf("grok string stop: err=%v out=%.120s", err, out)
	}
	// grok：标量字段为字符串时照常透传。
	out, err = convertResponsesToChatRequest([]byte(`{"model":"m","input":"hi","seed":` + pad + `}`))
	if err != nil || gjson.GetBytes(out, "seed").String() != brackets {
		t.Fatalf("grok string field: err=%v out=%.120s", err, out)
	}

	// payload 规则：字符串值与配置值相等时匹配。
	if !payloadValueEquals(gjson.Parse(pad), brackets) {
		t.Fatalf("payload rule must match a bracket-heavy string value")
	}
}
