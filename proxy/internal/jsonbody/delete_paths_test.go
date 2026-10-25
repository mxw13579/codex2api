package jsonbody

import (
	"bytes"
	"strings"
	"testing"
)

var deletePathsEnvelopePaths = []string{"type", "stream_id"}

var deletePathsCleanupPaths = []string{
	"previous_response_id", "prompt_cache_retention", "safety_identifier",
	"disable_response_storage", "type", "stream_id",
}

// Keep the old, ordered calls as the oracle, including their permissive handling
// of invalid JSON and paths that are not simple top-level member names.
func deletePathsInOrder(body []byte, paths ...string) []byte {
	for _, path := range paths {
		body = DeletePath(body, path)
	}
	return body
}

func TestDeletePathsMatchesSequential(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		paths []string
	}{
		{"absent", `{"model":"synthetic-model","input":[{"type":"message"}]}`, deletePathsCleanupPaths},
		{"all_present", `{"previous_response_id":"synthetic-id","prompt_cache_retention":"24h","safety_identifier":"synthetic","disable_response_storage":true,"type":"response.create","stream_id":"lane"}`, deletePathsCleanupPaths},
		{"mixed_values", `{"previous_response_id":null,"prompt_cache_retention":false,"safety_identifier":0,"disable_response_storage":{},"type":[],"stream_id":""}`, deletePathsCleanupPaths},
		{"two_fields_absent", `{"model":"synthetic-model","input":[{"type":"message","stream_id":"nested"}]}`, deletePathsEnvelopePaths},
		{"two_fields_all_present", `{"type":"response.create","stream_id":"lane","input":[]}`, deletePathsEnvelopePaths},
		{"two_fields_malformed", `{"type":"response.create","stream_id":"lane"`, deletePathsEnvelopePaths},
		{"three_fields_all_present", `{"type":"response.create","stream_id":"lane","previous_response_id":"synthetic-id"}`, []string{"type", "stream_id", "previous_response_id"}},
		{"whitespace", " \n{ \"type\" : \"response.create\" , \"input\" : [1, 2] , \"stream_id\" : null }\t", deletePathsCleanupPaths},
		{"no_paths", `{"type":"response.create"}`, nil},
		{"singleton", `{"type":"response.create","type":"second"}`, []string{"type"}},
		{"empty_path", `{"":1,"type":"response.create"}`, []string{"", "type"}},
		{"repeated_empty_path", `{"":1,"":2,"type":"response.create"}`, []string{"", "type", ""}},
		{"duplicate_json_key_once", `{"type":"first","type":"second","stream_id":"lane"}`, []string{"type", "stream_id"}},
		{"duplicate_json_keys_unique_paths", `{"type":"first","type":"second","stream_id":"lane","previous_response_id":"synthetic-id"}`, []string{"type", "stream_id", "previous_response_id"}},
		{"repeated_path", `{"type":"first","stream_id":"lane"}`, []string{"type", "stream_id", "type", "type"}},
		{"repeated_paths_and_keys", `{"type":"first","stream_id":"a","type":"second","stream_id":"b","type":"third"}`, []string{"stream_id", "type", "stream_id", "type", "type", "type"}},
		{"escaped_keys", `{"ty\u0070e":"response.create","stream\u005fid":"lane","input":[{"ty\u0070e":"message"}]}`, deletePathsCleanupPaths},
		{"escaped_duplicate_keys", `{"ty\u0070e":null,"type":"second","ty\u0070e":false,"stream_id":"lane"}`, []string{"type", "stream_id", "type", "type", "type"}},
		{"escaped_duplicate_keys_unique_paths", `{"ty\u0070e":null,"type":"second","ty\u0070e":false,"stream\u005fid":"lane"}`, deletePathsCleanupPaths},
		{"plain_names", `{"0":"zero","a-b":1,"A_B":2,"keep":3}`, []string{"0", "a-b", "A_B"}},
		{"unicode_key", `{"类型":1,"type":"response.create"}`, []string{"类型", "type"}},
		{"nested_then_parent", `{"a":{"b":1,"c":2},"type":"response.create"}`, []string{"a.b", "type", "a"}},
		{"parent_then_nested", `{"a":{"b":1},"a":{"b":2},"type":"response.create"}`, []string{"a", "a.b", "type"}},
		{"duplicate_parent", `{"a":{"b":1},"a":{"b":2}}`, []string{"a.b", "a", "a.b"}},
		{"array_index_shifts", `{"input":[{"type":"first"},{"type":"second"},{"type":"third"}],"type":"response.create"}`, []string{"input.0", "input.0.type", "input.0", "type"}},
		{"escaped_path", `{"a.b":1,"a":{"b":2},"type":"response.create"}`, []string{`a\.b`, "a.b", "type"}},
		{"backslash_path", `{"a\\b":1,"type":"response.create"}`, []string{`a\\b`, "type"}},
		{"wildcard_path", `{"type":"response.create","test":1,"stream_id":"lane"}`, []string{"t*", "t?pe", "stream_id"}},
		{"query_path", `{"input":[{"type":"message"},{"type":"function_call"}],"type":"response.create"}`, []string{`input.#(type=="message").type`, "type"}},
		{"modifier_path", `{"type":"response.create","stream_id":"lane"}`, []string{"@this", "type", "stream_id"}},
		{"pipe_path", `{"type":"response.create","stream_id":"lane"}`, []string{"type|@this", "stream_id"}},
		{"forced_key_path", `{"type":"response.create",":type":1,"stream_id":"lane"}`, []string{":type", "stream_id"}},
		{"nested_envelope_names", `{"input":[{"type":"message","stream_id":"nested","previous_response_id":"nested","content":[{"type":"input_text","text":"{\"type\":\"text\"}"}]}],"type":"response.create","stream_id":"lane"}`, deletePathsCleanupPaths},
		{"empty_object", `{}`, deletePathsCleanupPaths},
		{"array_root", `[{"type":"first"},{"type":"second"}]`, deletePathsCleanupPaths},
		{"array_root_indices", `[{"type":"first"},{"type":"second"},{"type":"third"}]`, []string{"0", "0", "1"}},
		{"array_root_nested", `[{"type":"first"},{"type":"second"}]`, []string{"0.type", "0", "0.type"}},
		{"string_root", `"type"`, deletePathsCleanupPaths},
		{"null_root", `null`, deletePathsCleanupPaths},
		{"boolean_root", `true`, deletePathsCleanupPaths},
		{"number_root", `42`, deletePathsCleanupPaths},
		{"empty_input", ``, deletePathsCleanupPaths},
		{"whitespace_input", " \n\t", deletePathsCleanupPaths},
		{"truncated_object", `{"type":"response.create","stream_id":"lane"`, deletePathsCleanupPaths},
		{"truncated_value", `{"type":`, deletePathsCleanupPaths},
		{"missing_value", `{"type":,"stream_id":"lane"}`, deletePathsCleanupPaths},
		{"missing_comma", `{"a":1 "type":"response.create","stream_id":"lane"}`, deletePathsCleanupPaths},
		{"missing_colon", `{"type" "response.create","stream_id":"lane"}`, deletePathsCleanupPaths},
		{"trailing_comma", `{"type":"response.create","stream_id":"lane",}`, deletePathsCleanupPaths},
		{"invalid_nested_value", `{"input":[{"type":}],"stream_id":"lane"}`, deletePathsCleanupPaths},
		{"invalid_escape", `{"ty\qpe":"response.create","stream_id":"lane"}`, deletePathsCleanupPaths},
		{"unterminated_string", `{"input":"text`, deletePathsCleanupPaths},
		{"trailing_object", `{"input":[]} {"type":"response.create"}`, deletePathsCleanupPaths},
		{"trailing_garbage", `{"type":"response.create"} garbage`, deletePathsCleanupPaths},
		{"malformed_no_paths", `{"type":`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(tt.body)
			want := deletePathsInOrder([]byte(tt.body), tt.paths...)
			got := DeletePaths(body, tt.paths...)
			if !bytes.Equal(got, want) || (got == nil) != (want == nil) {
				t.Fatalf("DeletePaths = %q (nil=%v), sequential = %q (nil=%v)", got, got == nil, want, want == nil)
			}
			if string(body) != tt.body {
				t.Fatal("DeletePaths changed the caller's input backing array")
			}
		})
	}
}

func TestDeletePathsNoOpAliasesInput(t *testing.T) {
	tests := []struct {
		name  string
		body  []byte
		paths []string
	}{
		{"nil", nil, deletePathsCleanupPaths},
		{"nil_no_paths", nil, nil},
		{"empty_with_capacity", make([]byte, 0, 16), deletePathsCleanupPaths},
		{"no_paths", []byte(`{"type":"response.create"}`), nil},
		{"absent", []byte(`{"input":[{"type":"message","stream_id":"nested"}]}`), deletePathsCleanupPaths},
		{"two_fields_absent", []byte(`{"input":[{"type":"message","stream_id":"nested"}]}`), deletePathsEnvelopePaths},
		{"three_fields_absent", []byte(`{"input":[]}`), []string{"type", "stream_id", "previous_response_id"}},
		{"repeated_absent", []byte(`{"input":[]}`), []string{"type", "type", "stream_id", "type"}},
		{"singleton_absent", []byte(`{"input":[]}`), []string{"type"}},
		{"nested_absent", []byte(`{"input":[]}`), []string{"missing.child", "type"}},
		{"array_root", []byte(`[{"type":"message"}]`), deletePathsCleanupPaths},
		{"malformed_absent", []byte(`{"input":`), deletePathsCleanupPaths},
		{"long_input", deletePathsSizedBody(1<<20, ""), deletePathsCleanupPaths},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := append([]byte(nil), tt.body...)
			got := DeletePaths(tt.body, tt.paths...)
			if !bytes.Equal(got, before) || (got == nil) != (tt.body == nil) || len(got) != len(tt.body) || cap(got) != cap(tt.body) {
				t.Fatal("no-op must preserve input bytes and slice shape")
			}
			if cap(tt.body) > 0 && &got[:cap(got)][0] != &tt.body[:cap(tt.body)][0] {
				t.Fatal("no-op must return the original backing array, not a copy")
			}
		})
	}
}

func TestDeletePathsOnlyCleansTopLevelEnvelope(t *testing.T) {
	nested := `"input":[{"type":"message","stream_id":"nested","previous_response_id":"nested","content":[{"type":"input_text","text":"synthetic"}]}],"tools":[{"type":"function","name":"synthetic"}],"metadata":{"type":"keep","stream_id":"keep"}`
	body := []byte(`{"type":"response.create","stream_id":"lane",` + nested + `}`)
	before := append([]byte(nil), body...)
	got := DeletePaths(body, deletePathsCleanupPaths...)
	want := []byte(`{` + nested + `}`)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %s, want %s", got, want)
	}
	if !bytes.Equal(body, before) || &got[0] == &body[0] {
		t.Fatal("a real deletion must not rewrite or alias the caller's input")
	}
}

func deletePathsSizedBody(size int, fields string) []byte {
	prefix := `{"model":"synthetic-model","input":[{"type":"message","content":[{"type":"input_text","text":"`
	suffix := `"}]}]` + fields + `}`
	return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

var deletePathsBenchmarkSink []byte

func BenchmarkDeletePaths(b *testing.B) {
	for _, size := range []struct {
		name  string
		bytes int
	}{{"1MiB", 1 << 20}, {"10MiB", 10 << 20}} {
		b.Run(size.name, func(b *testing.B) {
			for _, scenario := range []struct {
				name   string
				paths  []string
				fields string
			}{
				{"two-fields/absent", deletePathsEnvelopePaths, ""},
				{"two-fields/allpresent", deletePathsEnvelopePaths, `,"type":"response.create","stream_id":"lane"`},
				{"six-fields/allpresent", deletePathsCleanupPaths, `,"previous_response_id":"synthetic-id","prompt_cache_retention":"24h","safety_identifier":"synthetic","disable_response_storage":true,"type":"response.create","stream_id":"lane"`},
				{"six-fields/partial", deletePathsCleanupPaths, `,"type":"response.create","stream_id":"lane"`},
				{"six-fields/absent", deletePathsCleanupPaths, ""},
			} {
				b.Run(scenario.name, func(b *testing.B) {
					body := deletePathsSizedBody(size.bytes, scenario.fields)
					for _, impl := range []struct {
						name string
						fn   func([]byte, ...string) []byte
					}{
						{"sequential", deletePathsInOrder},
						{"batch", DeletePaths},
					} {
						b.Run(impl.name, func(b *testing.B) {
							b.ReportAllocs()
							b.SetBytes(int64(len(body)))
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								deletePathsBenchmarkSink = impl.fn(body, scenario.paths...)
							}
						})
					}
				})
			}
		})
	}
}
