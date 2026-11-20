// Package jsondepth 提供线性、非递归的 JSON 嵌套深度检查。
//
// 背景：gjson.Valid/ValidBytes 以及 gjson.Result.Value/Array/Map 等解析路径用无深度
// 上限的递归实现，客户端或上游送来几百万层的 `[[[[...` 会让 goroutine 栈超过 1GB，
// 触发 runtime 的 fatal error: stack overflow，整个进程退出（recover 拦不住）。
//
// 本包的 Within 保证：返回 true 时，任何“按 JSON 语义识别括号”的解析器（包括 gjson
// 的校验器与宽松解析器）在该输入上的容器嵌套深度都不超过 limit。做法：
//
//  1. 用严格的 RFC 8259 语法逐字节迭代校验，显式维护容器栈（位图，不递归）；合法前缀
//     内的嵌套深度一旦超过 limit 立即判为过深。
//  2. 输入在位置 q 处不再是合法 JSON 前缀时，[0,q) 是合法 JSON 前缀：所有解析器对它的
//     字符串边界与容器结构的理解一致，q 处仍打开的容器数就是当前深度 d。q 之后的字节
//     各解析器可能各有解释（例如 gjson 的宽松数字扫描会吞掉引号，导致字符串边界错位），
//     所以保守地把 data[q:] 里所有 '[' 与 '{' 都当成可能的新层：d + 尾部开括号数 ≤ limit
//     才放行。宽松解析器每深入一层都必须消费一个不同位置的开括号，因此该上界对任何解析器
//     都成立；代价是极端畸形输入（非法 JSON 且出错点之后开括号很多）会被误判为过深。
package jsondepth

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/tidwall/gjson"
)

// DefaultMaxDepth 与 encoding/json 的嵌套上限一致。
const DefaultMaxDepth = 10000

// MaxDepthEnv 覆盖默认上限；非法值（非整数、<1 或 > MaxAllowedDepth）回退默认。
const MaxDepthEnv = "CODEX_MAX_JSON_NESTING_DEPTH"

// MaxAllowedDepth 是环境变量允许的最大值。gjson 每层递归约 180 字节栈，100 万层约
// 180MB，仍远低于 1GB 的 goroutine 栈上限；再大就重新打开崩溃面。
const MaxAllowedDepth = 1000000

var (
	maxDepthOnce sync.Once
	maxDepthVal  atomic.Int64
)

func loadMaxDepth() {
	maxDepthOnce.Do(func() {
		maxDepthVal.Store(int64(parseMaxDepth(os.Getenv(MaxDepthEnv))))
	})
}

func parseMaxDepth(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultMaxDepth
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > MaxAllowedDepth {
		return DefaultMaxDepth
	}
	return n
}

// MaxDepth 返回生效的嵌套深度上限（首次调用时读取 CODEX_MAX_JSON_NESTING_DEPTH）。
func MaxDepth() int {
	loadMaxDepth()
	return int(maxDepthVal.Load())
}

// SetMaxDepthForTesting 临时覆盖上限，返回恢复函数。仅供测试使用。
func SetMaxDepthForTesting(n int) (restore func()) {
	loadMaxDepth()
	prev := maxDepthVal.Swap(int64(n))
	return func() { maxDepthVal.Store(prev) }
}

// TooDeep 报告 data 是否超过生效上限（见包注释中的保守判定）。
func TooDeep(data []byte) bool {
	return !Within(data, MaxDepth())
}

// TooDeepString 是 TooDeep 的 string 版本，不复制数据。
func TooDeepString(s string) bool {
	return !Within(stringBytes(s), MaxDepth())
}

// ValidBytes 等价于 gjson.ValidBytes，但先做深度检查：过深视为不合法，
// 从而保证 gjson 的递归校验器不会在超深输入上栈溢出。
func ValidBytes(data []byte) bool {
	if TooDeep(data) {
		return false
	}
	return gjson.ValidBytes(data)
}

// Valid 等价于 gjson.Valid，但先做深度检查。
func Valid(s string) bool {
	if TooDeepString(s) {
		return false
	}
	return gjson.Valid(s)
}

func stringBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// 扫描状态。
const (
	stValue         = iota // 期望一个值（顶层、':' 之后、数组中 ',' 之后）
	stValueOrArrEnd        // '[' 之后：值或 ']'
	stKeyOrObjEnd          // '{' 之后：键或 '}'
	stKey                  // 对象中 ',' 之后：键
	stColon                // 键之后：':'
	stAfterValue           // 值之后：',' 或闭合括号；顶层则只允许空白
)

// stackWords 覆盖默认上限的栈位图大小，放在调用栈上避免堆分配。
const stackWords = (DefaultMaxDepth + 63) / 64

// strStop 标记字符串内需要特殊处理的字节：'"'、'\\' 与控制字符。
var strStop = func() (t [256]bool) {
	for c := 0; c < 0x20; c++ {
		t[c] = true
	}
	t['"'] = true
	t['\\'] = true
	return t
}()

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// Within 报告 data 的嵌套深度是否不超过 limit（limit < 1 时按 1 处理）。
// 线性时间；limit ≤ DefaultMaxDepth 时不做堆分配。
func Within(data []byte, limit int) bool {
	if limit < 1 {
		limit = 1
	}
	// 每深入一层至少要消费一个字节；总字节数不超过 limit 时任何解析器的深度都不可能
	// 超限，直接放行。
	if len(data) <= limit {
		return true
	}

	var local [stackWords]uint64
	stack := local[:]
	if need := (limit + 64) / 64; need > len(stack) {
		stack = make([]uint64, need)
	}

	n := len(data)
	depth := 0
	state := stValue
	i := 0
	for i < n {
		c := data[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		switch state {
		case stValue, stValueOrArrEnd:
			if state == stValueOrArrEnd && c == ']' {
				depth--
				i++
				state = stAfterValue
				continue
			}
			switch {
			case c == '{' || c == '[':
				if depth >= limit {
					return false
				}
				w, b := depth>>6, uint(depth&63)
				if c == '{' {
					stack[w] |= 1 << b
					state = stKeyOrObjEnd
				} else {
					stack[w] &^= 1 << b
					state = stValueOrArrEnd
				}
				depth++
				i++
				continue
			case c == '"':
				j, ok := scanString(data, i+1)
				if !ok {
					return tailWithin(data, j, depth, limit)
				}
				i = j
			case c == '-' || isDigit(c):
				j, ok := scanNumber(data, i)
				if !ok {
					return tailWithin(data, j, depth, limit)
				}
				i = j
			case c == 't':
				if !hasLiteral(data, i, "true") {
					return tailWithin(data, i, depth, limit)
				}
				i += 4
			case c == 'f':
				if !hasLiteral(data, i, "false") {
					return tailWithin(data, i, depth, limit)
				}
				i += 5
			case c == 'n':
				if !hasLiteral(data, i, "null") {
					return tailWithin(data, i, depth, limit)
				}
				i += 4
			default:
				return tailWithin(data, i, depth, limit)
			}
			state = stAfterValue
		case stKeyOrObjEnd, stKey:
			if state == stKeyOrObjEnd && c == '}' {
				depth--
				i++
				state = stAfterValue
				continue
			}
			if c != '"' {
				return tailWithin(data, i, depth, limit)
			}
			j, ok := scanString(data, i+1)
			if !ok {
				return tailWithin(data, j, depth, limit)
			}
			i = j
			state = stColon
		case stColon:
			if c != ':' {
				return tailWithin(data, i, depth, limit)
			}
			i++
			state = stValue
		case stAfterValue:
			if depth == 0 {
				// 顶层值之后出现非空白：剩余部分按畸形尾部保守计数。
				return tailWithin(data, i, depth, limit)
			}
			top := depth - 1
			inObject := stack[top>>6]&(1<<uint(top&63)) != 0
			switch {
			case c == ',':
				i++
				if inObject {
					state = stKey
				} else {
					state = stValue
				}
			case c == '}' && inObject, c == ']' && !inObject:
				depth--
				i++
				state = stAfterValue
			default:
				return tailWithin(data, i, depth, limit)
			}
		}
	}
	// 输入在合法前缀处截断（未闭合）：前缀内的深度已逐步检查过。
	return true
}

// tailWithin 处理从 q 开始不再是合法 JSON 前缀的情况：保守地把尾部所有开括号计入深度。
func tailWithin(data []byte, q, depth, limit int) bool {
	if q < 0 {
		q = 0
	}
	if q > len(data) {
		q = len(data)
	}
	tail := data[q:]
	budget := limit - depth
	if len(tail) <= budget {
		return true
	}
	opens := bytes.Count(tail, []byte{'['})
	if opens > budget {
		return false
	}
	opens += bytes.Count(tail, []byte{'{'})
	return opens <= budget
}

// scanString 从开引号之后的 i 开始扫描严格的 JSON 字符串；成功返回闭引号之后的位置，
// 失败返回出错字节的位置。
func scanString(data []byte, i int) (int, bool) {
	n := len(data)
	for i < n {
		// 快速路径：一次检查 8 字节里有没有 '"'、'\\' 或控制字符（SWAR，判断结果精确）。
		for i+8 <= n && !hasStringSpecial(binary.LittleEndian.Uint64(data[i:])) {
			i += 8
		}
		for i < n && !strStop[data[i]] {
			i++
		}
		if i >= n {
			break
		}
		c := data[i]
		switch {
		case c == '"':
			return i + 1, true
		case c == '\\':
			if i+1 >= n {
				return i, false
			}
			switch data[i+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+5 >= n { // 不足 4 个十六进制字符
					return i, false
				}
				for k := 2; k < 6; k++ {
					if !isHex(data[i+k]) {
						return i, false
					}
				}
				i += 6
			default:
				return i, false
			}
		default: // 控制字符
			return i, false
		}
	}
	return n, false
}

const (
	swarLo = 0x0101010101010101
	swarHi = 0x8080808080808080
)

// hasStringSpecial 报告 x 的 8 个字节中是否有 '"'、'\\' 或 < 0x20 的字节。
// hasZero/hasLess 是经典的位技巧：布尔结果精确（只是不指出是哪个字节）。
func hasStringSpecial(x uint64) bool {
	less := (x - swarLo*0x20) &^ x & swarHi
	q := x ^ (swarLo * '"')
	quote := (q - swarLo) &^ q & swarHi
	b := x ^ (swarLo * '\\')
	backslash := (b - swarLo) &^ b & swarHi
	return less|quote|backslash != 0
}

// scanNumber 从 i（'-' 或数字）开始扫描严格的 JSON 数字。
func scanNumber(data []byte, i int) (int, bool) {
	n := len(data)
	if data[i] == '-' {
		i++
		if i >= n {
			return i, false
		}
	}
	switch {
	case data[i] == '0':
		i++
	case data[i] >= '1' && data[i] <= '9':
		i++
		for i < n && isDigit(data[i]) {
			i++
		}
	default:
		return i, false
	}
	if i < n && data[i] == '.' {
		i++
		if i >= n || !isDigit(data[i]) {
			return i, false
		}
		for i < n && isDigit(data[i]) {
			i++
		}
	}
	if i < n && (data[i] == 'e' || data[i] == 'E') {
		i++
		if i < n && (data[i] == '+' || data[i] == '-') {
			i++
		}
		if i >= n || !isDigit(data[i]) {
			return i, false
		}
		for i < n && isDigit(data[i]) {
			i++
		}
	}
	return i, true
}

func hasLiteral(data []byte, i int, lit string) bool {
	return len(data)-i >= len(lit) && string(data[i:i+len(lit)]) == lit
}
