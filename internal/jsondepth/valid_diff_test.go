package jsondepth

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// assertValidMatchesGJSON：Check 通过时 ValidBytes/Valid 必须与 gjson 逐输入一致；
// 未通过时必须为 false（只有此时 gjson 才可能深递归，不能调用它）。
func assertValidMatchesGJSON(t *testing.T, data []byte) {
	t.Helper()
	got := ValidBytes(data)
	if gotS := Valid(string(data)); gotS != got {
		t.Fatalf("Valid and ValidBytes diverge on %q", data)
	}
	if Check(data) != StatusOK {
		if got {
			t.Fatalf("ValidBytes(%.80q)=true but Check failed", data)
		}
		return
	}
	if want := gjson.ValidBytes(data); got != want {
		t.Fatalf("ValidBytes(%.120q)=%v, gjson.ValidBytes=%v", data, got, want)
	}
}

var validDiffCorpus = []string{
	"", " ", "\t\n\r ", "\x0c", "\x00", "\xef\xbb\xbf", "\xef\xbb\xbf{}", "\xef\xbb", " \xef\xbb\xbf{}",
	"{}", "[]", " {} ", "\n[]\n", "{} ", "{}x", "{}{}", "[] []", "{},", "1 2",
	"0", "-0", "-", "--1", "+1", "01", "00", "-01", "1.", "1.0", ".5", "1e5", "1E5", "1e+5", "1e-5", "1e", "1e+", "1.e5", "1.5e", "-0.0e-0",
	"123456789012345678901234567890", "1x", "1 ", " 1", "1\t", "NaN", "Infinity", "-Infinity",
	"true", "false", "null", "tru", "nul", "fals", "truex", "true false", "TRUE", "nULL",
	`"`, `""`, `"a"`, `"\"`, `"\\"`, `"\\\"`, `"\/"`, `"\b\f\n\r\t"`, `"\q"`, `"\u"`, `"\u12"`, `"\u12g4"`, `"é"`, `"😀"`, `"\uD83D"`, `"\uDE00\uD83D"`,
	"\"a\nb\"", "\"a\tb\"", "\"\x01\"", "\"\x1f\"", "\"\x7f\"", "\"\xff\xfe\"", "\"\xc3\x28\"", "\"\xed\xa0\x80\"", "\"\xf4\x90\x80\x80\"",
	`{"a":1}`, `{"a":1,}`, `{"a" 1}`, `{"a":}`, `{a:1}`, `{'a':1}`, `{"a":1 "b":2}`, `{"a":1,"a":2}`, `{"":0}`, `{"a":[1,2,{"b":null}]}`,
	`[1,]`, `[,1]`, `[1,,2]`, `[1 2]`, `[}`, `{]`, `[[]`, `[]]`, `[1,[2,[3]]]`, `[{"a":"[{"}]`,
	`{"a":"b"}garbage`, "{\"a\":\"b\"}\n", `["a" ,"b"]`, `[ ]`, `{ }`, `{"a" : 1 , "b" : [ ] }`,
}

func TestValidBytesMatchesGJSONOnCorpus(t *testing.T) {
	for _, c := range validDiffCorpus {
		assertValidMatchesGJSON(t, []byte(c))
		// 包一层容器与前后空白，覆盖非顶层位置。
		assertValidMatchesGJSON(t, []byte("["+c+"]"))
		assertValidMatchesGJSON(t, []byte(`{"k":`+c+`}`))
		assertValidMatchesGJSON(t, []byte(" "+c+" "))
	}
	// 深度边界：等于上限合法、超过上限一律 false。
	for _, d := range []int{DefaultMaxDepth - 1, DefaultMaxDepth} {
		data := nestedArrays(d)
		if !ValidBytes(data) || !gjson.ValidBytes(data) {
			t.Fatalf("depth %d must be valid", d)
		}
	}
	if ValidBytes(nestedArrays(DefaultMaxDepth + 1)) {
		t.Fatalf("depth above limit must be invalid")
	}
}

func TestValidBytesMatchesGJSONRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	alphabet := []byte("[]{}\",:\\ \t\n\r0123456789-+.eEtrufalsn/bu\x01\x7f\xc3\xef\xbb\xbfx")
	for n := 0; n < 300000; n++ {
		var data []byte
		if n%2 == 0 {
			data = make([]byte, rng.Intn(24))
			for i := range data {
				data[i] = alphabet[rng.Intn(len(alphabet))]
			}
		} else {
			// 变异：从语料取一条，随机插入/删除/替换字节。
			base := []byte(validDiffCorpus[rng.Intn(len(validDiffCorpus))])
			data = append([]byte{}, base...)
			for k := rng.Intn(3); k >= 0; k-- {
				pos := 0
				if len(data) > 0 {
					pos = rng.Intn(len(data) + 1)
				}
				switch rng.Intn(3) {
				case 0:
					data = append(data[:pos], append([]byte{alphabet[rng.Intn(len(alphabet))]}, data[pos:]...)...)
				case 1:
					if pos < len(data) {
						data = append(data[:pos], data[pos+1:]...)
					}
				case 2:
					if pos < len(data) {
						data[pos] = alphabet[rng.Intn(len(alphabet))]
					}
				}
			}
		}
		assertValidMatchesGJSON(t, data)
	}
}

// FuzzValidBytesMatchesGJSON：go test -run '^$' -fuzz FuzzValidBytesMatchesGJSON ./internal/jsondepth
func FuzzValidBytesMatchesGJSON(f *testing.F) {
	for _, c := range validDiffCorpus {
		f.Add([]byte(c))
	}
	f.Add([]byte(strings.Repeat("[", 64) + strings.Repeat("]", 64)))
	f.Fuzz(func(t *testing.T, data []byte) {
		assertValidMatchesGJSON(t, data)
	})
}
