package proxy

import (
	"github.com/tidwall/gjson"

	"github.com/codex2api/internal/jsondepth"
)

// resultTooDeep 是“第二道防线”的统一预检：在对 r 调用 Value（无深度上限的递归）之前，
// 判断 r 是否嵌套过深。只有对象/数组（gjson.JSON，Raw 以 '{' 或 '[' 开头）才会递归展开；
// 字符串、数字、布尔、null 的 Value 不递归，直接返回 false——否则字符串里的大量 '['
// 会被“顶层非容器”的保守规则误判为过深。
func resultTooDeep(r gjson.Result) bool {
	if r.Type != gjson.JSON {
		return false
	}
	return jsondepth.TooDeepString(r.Raw)
}
