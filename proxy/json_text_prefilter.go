package proxy

import (
	"bytes"
	"strings"
)

// jsonBytesMayContainText 是请求体全文扫描前的廉价预判：返回 false 时可以断定 body
// 里任何 JSON 字符串（按 gjson 的解码结果）都不含 word；返回 true 只表示"可能含有"，
// 调用方必须继续走原来的精确判断。
//
// word 必须是非空 ASCII。gjson 解码后的每个 ASCII 字符只可能来自原文的同一字节或一个
// \u00XX 转义（surrogate 分支只产出 >= U+10000 或 U+FFFD，其余转义只产出控制字符和
// 标点），因此逐位置同时接受字面字节与 \uXXXX 两种写法就不会因转义漏判。
//
// fold 为 true 时按 ASCII 大小写不敏感比较，用来替代 strings.EqualFold / ToLower 的
// 判定；这只在没有任何非 ASCII 码点能折叠到 word 中字母时成立，由
// TestJSONTextPrefilterWordsHaveNoNonASCIIFolds 对所用的词逐一验证。
//
// 常见请求体里的 \u 转义只编码非 ASCII 文字或 Go 的 HTML 转义（< 等），都不会
// 解码成 word 里的字符；此时只需做字面查找（快路径）。否则退回逐位置的完整匹配。
func jsonBytesMayContainText(body []byte, word string, fold bool) bool {
	if len(word) == 0 {
		return true
	}
	if bodyHasEscapeDecodingToWordChar(body, word, fold) {
		return jsonTextMayContainSlow(body, word, fold)
	}
	return literalContainsASCII(body, word, fold)
}

// bodyHasEscapeDecodingToWordChar 报告是否存在形如 \uXXXX、解码后等于 word 中某字符
// 的片段。\\u0063 这类被转义的反斜杠也按转义处理，只会多走慢路径，不会漏判。
func bodyHasEscapeDecodingToWordChar(body []byte, word string, fold bool) bool {
	for i := 0; i < len(body); {
		j := bytes.Index(body[i:], prefilterEscapePrefix)
		if j < 0 {
			return false
		}
		p := i + j
		if r, ok := prefilterUnicodeEscape(body, p); ok && r < 0x80 && wordHasByte(word, byte(r), fold) {
			return true
		}
		i = p + 1
	}
	return false
}

var prefilterEscapePrefix = []byte(`\u`)

func wordHasByte(word string, c byte, fold bool) bool {
	for k := 0; k < len(word); k++ {
		if prefilterByteEqual(c, word[k], fold) {
			return true
		}
	}
	return false
}

// literalContainsASCII 是不考虑转义的子串查找；fold 时按 ASCII 大小写不敏感。
func literalContainsASCII(body []byte, word string, fold bool) bool {
	if !fold {
		return bytes.Contains(body, []byte(word))
	}
	// 以 word 中最少见的字符为锚点，用 IndexByte 跳到锚点的每种大小写写法，再核对窗口。
	k := prefilterAnchorIndex(word)
	anchor := word[k]
	variants := []byte{anchor}
	if lower := anchor | 0x20; 'a' <= lower && lower <= 'z' {
		variants = []byte{lower, lower &^ 0x20}
	}
	for _, a := range variants {
		for i := 0; i < len(body); {
			j := bytes.IndexByte(body[i:], a)
			if j < 0 {
				break
			}
			p := i + j
			if start := p - k; start >= 0 && start+len(word) <= len(body) && asciiFoldEqual(body[start:start+len(word)], word) {
				return true
			}
			i = p + 1
		}
	}
	return false
}

// prefilterAnchorRank 为英文与代码文本里由少到多的大致字符频率；非字母（如 '_'）
// 大小写唯一，优先作锚点。锚点只影响速度，不影响结果。
const prefilterAnchorRank = "zqxjkvbpygfwmucldrhsnioate"

func prefilterAnchorIndex(word string) int {
	best, bestRank := 0, len(prefilterAnchorRank)+1
	for k := 0; k < len(word); k++ {
		c := word[k] | 0x20
		rank := -1
		if 'a' <= c && c <= 'z' {
			rank = strings.IndexByte(prefilterAnchorRank, c)
		}
		if rank < bestRank {
			best, bestRank = k, rank
		}
	}
	return best
}

func asciiFoldEqual(window []byte, word string) bool {
	for m := 0; m < len(word); m++ {
		if !prefilterByteEqual(window[m], word[m], true) {
			return false
		}
	}
	return true
}

// jsonTextMayContainSlow 逐位置匹配，每个字符接受字面字节或 \uXXXX 转义。
func jsonTextMayContainSlow(body []byte, word string, fold bool) bool {
	for i := 0; i < len(body); i++ {
		if c := body[i]; c != '\\' && !prefilterByteEqual(c, word[0], fold) {
			continue
		}
		if jsonTextMatchesAt(body, i, word, fold) {
			return true
		}
	}
	return false
}

func jsonTextMatchesAt(body []byte, i int, word string, fold bool) bool {
	j := i
	for k := 0; k < len(word); k++ {
		if j >= len(body) {
			return false
		}
		c := body[j]
		if c == '\\' {
			r, ok := prefilterUnicodeEscape(body, j)
			if !ok || r >= 0x80 || !prefilterByteEqual(byte(r), word[k], fold) {
				return false
			}
			j += 6
			continue
		}
		if !prefilterByteEqual(c, word[k], fold) {
			return false
		}
		j++
	}
	return true
}

// prefilterUnicodeEscape 解析 body[j:] 处的 \uXXXX。gjson 用 strconv.ParseUint 解析
// 4 位十六进制，非法时得到 0（不可能等于 word 中的字符），这里非法直接判不匹配。
func prefilterUnicodeEscape(body []byte, j int) (rune, bool) {
	if j+6 > len(body) || body[j] != '\\' || body[j+1] != 'u' {
		return 0, false
	}
	var r rune
	for _, h := range body[j+2 : j+6] {
		var v byte
		switch {
		case '0' <= h && h <= '9':
			v = h - '0'
		case 'a' <= h && h <= 'f':
			v = h - 'a' + 10
		case 'A' <= h && h <= 'F':
			v = h - 'A' + 10
		default:
			return 0, false
		}
		r = r<<4 | rune(v)
	}
	return r, true
}

func prefilterByteEqual(c, want byte, fold bool) bool {
	if c == want {
		return true
	}
	if !fold {
		return false
	}
	lower := want | 0x20
	return 'a' <= lower && lower <= 'z' && c|0x20 == lower
}
