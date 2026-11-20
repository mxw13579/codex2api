// Package jsondepth 提供线性、非递归的 JSON 嵌套深度检查与（带深度上限的）JSON 合法性校验。
//
// 背景：gjson.Valid/ValidBytes 以及 gjson.Result.Value/Array/Map 等解析路径用无深度
// 上限的递归实现，客户端或上游送来几百万层的 `[[[[...` 会让 goroutine 栈溢出，触发
// runtime 的 fatal error: stack overflow，整个进程退出（recover 拦不住）。
//
// 不变量：Check 返回 StatusOK 时，任何解析器（包括 gjson 的校验器、gjson.Parse/Get 的
// 宽松解析与 Result.Value 的递归展开）在该输入或其任意子串结果上的容器嵌套深度都不超过
// limit。做法：
//
//  1. 用严格的 RFC 8259 语法逐字节迭代校验，显式维护容器栈（位图，不递归）；合法前缀
//     内的嵌套深度一旦超过 limit 即判为 StatusTooDeep。
//  2. 输入在位置 q 处不再是合法 JSON 前缀时，[0,q) 是合法 JSON 前缀：所有解析器对它的
//     字符串边界与容器结构的理解一致，q 处仍打开的容器数就是当前深度 d。q 之后的字节
//     各解析器可能各有解释（例如 gjson 的宽松数字扫描会吞掉引号，导致字符串边界错位），
//     所以保守地把 data[q:] 里所有 '[' 与 '{' 都当成可能的新层：d + 尾部开括号数 ≤ limit
//     才放行，否则判为 StatusUnverifiable。宽松解析器每深入一层都必须消费一个不同位置的
//     开括号，因此该上界对任何解析器都成立。
//  3. 顶层第一个非空白字节不是 '{'/'[' 时（字符串、数字、字面量或其他字节），同样按第 2
//     条统计其后全部开括号：gjson.Get 在顶层会跳过任意字节直到第一个 '{'/'['（包括跳进
//     顶层字符串的内容），所以“顶层是字符串”时字符串内的括号也必须计入。
//  4. 开头的 UTF-8 BOM 按空白处理：gjson.Parse 遇到它直接返回空结果，gjson.Get 跳过它，
//     两者都不会因此看到与严格扫描不同的结构；合法性判定（ValidBytes）仍把 BOM 视为非法，
//     与 gjson.ValidBytes 一致。
//
// 代价：极端畸形输入（非法 JSON 或顶层不是容器，且其后开括号超过上限）会被判为
// StatusUnverifiable 而拒绝。
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
)

// DefaultMaxDepth 与 encoding/json 的嵌套上限一致。
const DefaultMaxDepth = 10000

// MaxDepthEnv 覆盖默认上限；非法值（非整数、<1 或 > MaxAllowedDepth）回退默认。
const MaxDepthEnv = "CODEX_MAX_JSON_NESTING_DEPTH"

// MaxAllowedDepth 是环境变量允许的最大值。gjson 的 Result.Value 每层递归约 560–670
// 字节栈，且 Go 栈按 2 的幂增长、默认上限 1e9 字节（实际可用约 512 MiB），约 80 万层即
// 溢出；Value 还是 O(n·d) 的（每层重扫子串），5 万层已需约 1.7 秒 CPU。20000 层约 13MB
// 栈，留足余量。
const MaxAllowedDepth = 20000

// Status 是 Check 的结论。
type Status int

const (
	// StatusOK：任何解析器在该输入上的嵌套深度都不超过上限。
	StatusOK Status = iota
	// StatusTooDeep：输入的合法 JSON 前缀内嵌套深度就已超过上限。
	StatusTooDeep
	// StatusUnverifiable：输入不是合法 JSON（或顶层不是容器），且无法确认深度在上限内。
	StatusUnverifiable
)

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

// Check 用生效上限判定 data（见包注释的不变量）。
func Check(data []byte) Status {
	return CheckLimit(data, MaxDepth())
}

// CheckLimit 用给定上限判定 data。线性时间、不做堆分配。
func CheckLimit(data []byte, limit int) Status {
	if limit < 1 {
		limit = 1
	}
	// 每深入一层至少要消费一个字节；总字节数不超过 limit 时任何解析器的深度都不可能
	// 超限，直接放行。
	if len(data) <= limit {
		return StatusOK
	}
	status, _ := scan(data, limit)
	return status
}

// Within 报告 CheckLimit(data, limit) == StatusOK。
func Within(data []byte, limit int) bool {
	return CheckLimit(data, limit) == StatusOK
}

// TooDeep 报告 data 未通过生效上限的检查（StatusTooDeep 或 StatusUnverifiable）。
func TooDeep(data []byte) bool {
	return Check(data) != StatusOK
}

// TooDeepString 是 TooDeep 的 string 版本，不复制数据。
func TooDeepString(s string) bool {
	return Check(stringBytes(s)) != StatusOK
}

// ValidBytes 在 Check 通过时与 gjson.ValidBytes 的结果逐输入一致，未通过时返回 false。
// 只扫描一遍：深度检查用的严格 RFC 8259 扫描本身就是校验器（与 gjson 的语法一致，
// 有差分测试与 fuzz 覆盖），不再调用递归的 gjson.ValidBytes。
func ValidBytes(data []byte) bool {
	status, valid := scan(data, MaxDepth())
	return status == StatusOK && valid
}

// Valid 是 ValidBytes 的 string 版本，不复制数据。
func Valid(s string) bool {
	return ValidBytes(stringBytes(s))
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

// stackWords 覆盖允许的最大上限的栈位图大小，放在调用栈上避免堆分配。
const stackWords = (MaxAllowedDepth + 63) / 64

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

func tailStatus(data []byte, q, depth, limit int) (Status, bool) {
	if tailWithin(data, q, depth, limit) {
		return StatusOK, false
	}
	return StatusUnverifiable, false
}

// scan 一遍完成深度判定与严格的 JSON 合法性判定。valid 仅在 status == StatusOK 时有意义。
func scan(data []byte, limit int) (status Status, valid bool) {
	if limit < 1 {
		limit = 1
	}
	if limit > MaxAllowedDepth {
		limit = MaxAllowedDepth
	}
	var stack [stackWords]uint64

	n := len(data)
	i := 0
	bom := false
	if n >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		bom = true
		i = 3
	}
	depth := 0
	state := stValue
	top := true // 尚未遇到顶层第一个值
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
			if top {
				top = false
				if c != '{' && c != '[' {
					// 顶层不是容器：保守统计其后全部开括号（见包注释第 3 条）。
					if !tailWithin(data, i, 0, limit) {
						return StatusUnverifiable, false
					}
				}
			}
			switch {
			case c == '{' || c == '[':
				if depth >= limit {
					return StatusTooDeep, false
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
					return tailStatus(data, j, depth, limit)
				}
				i = j
			case c == '-' || isDigit(c):
				j, ok := scanNumber(data, i)
				if !ok {
					return tailStatus(data, j, depth, limit)
				}
				i = j
			case c == 't':
				if !hasLiteral(data, i, "true") {
					return tailStatus(data, i, depth, limit)
				}
				i += 4
			case c == 'f':
				if !hasLiteral(data, i, "false") {
					return tailStatus(data, i, depth, limit)
				}
				i += 5
			case c == 'n':
				if !hasLiteral(data, i, "null") {
					return tailStatus(data, i, depth, limit)
				}
				i += 4
			default:
				return tailStatus(data, i, depth, limit)
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
				return tailStatus(data, i, depth, limit)
			}
			j, ok := scanString(data, i+1)
			if !ok {
				return tailStatus(data, j, depth, limit)
			}
			i = j
			state = stColon
		case stColon:
			if c != ':' {
				return tailStatus(data, i, depth, limit)
			}
			i++
			state = stValue
		case stAfterValue:
			if depth == 0 {
				// 顶层值之后出现非空白：剩余部分按畸形尾部保守计数。
				return tailStatus(data, i, depth, limit)
			}
			t := depth - 1
			inObject := stack[t>>6]&(1<<uint(t&63)) != 0
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
				return tailStatus(data, i, depth, limit)
			}
		}
	}
	// 扫到末尾：合法前缀内的深度已逐步检查过。只有一个完整的顶层值、没有 BOM 才合法。
	return StatusOK, !bom && depth == 0 && state == stAfterValue
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
