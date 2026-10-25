// Package jsonbody edits request bodies without sjson's whole-document copy
// when an edit would not change a single byte. Long Codex histories make every
// such copy as large as the request itself.
package jsonbody

import (
	"unsafe"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// DeletePath returns body itself when path is absent. sjson already returns the
// original slice in that case, but only after building and discarding a full
// copy of the document.
func DeletePath(body []byte, path string) []byte {
	if !gjson.GetBytes(body, path).Exists() {
		return body
	}
	body, _ = sjson.DeleteBytes(body, path)
	return body
}

// DeletePaths is equivalent to calling DeletePath for each path in order. For
// three or more distinct plain top-level names, one TopLevel lookup replaces
// the per-path presence checks. Missing paths keep the original slice without
// a whole-document copy. Smaller batches, repeated or complex paths, and
// invalid or non-object JSON retain the per-path behavior.
func DeletePaths(body []byte, paths ...string) []byte {
	// The two-field envelope cleanup must not pay for validation and batching.
	if len(paths) < 3 {
		return deletePathsSequential(body, paths)
	}
	for i, path := range paths {
		if !plainTopLevelPath(path) {
			return deletePathsSequential(body, paths)
		}
		for _, previous := range paths[:i] {
			if path == previous {
				// Repeated paths can delete successive duplicate JSON keys,
				// then must skip the copy once none remain.
				return deletePathsSequential(body, paths)
			}
		}
	}
	// TopLevel uses gjson's permissive parser. Do not trust negative presence
	// results on malformed input, where it may differ from per-path lookups.
	if !gjson.ValidBytes(body) {
		return deletePathsSequential(body, paths)
	}
	values, ok := TopLevel(body, paths...)
	if !ok {
		return deletePathsSequential(body, paths)
	}
	// Results borrow body. Snapshot every presence bit before any deletion;
	// never inspect their strings or raw bytes after body may have changed.
	present := make([]bool, len(paths))
	for i := range values {
		present[i] = values[i].Exists()
	}
	for i, path := range paths {
		if present[i] {
			// Deleting a distinct top-level name cannot remove another one;
			// presence is still known, so do not repeat DeletePath's lookup.
			body, _ = sjson.DeleteBytes(body, path)
		}
	}
	return body
}

func deletePathsSequential(body []byte, paths []string) []byte {
	for _, path := range paths {
		body = DeletePath(body, path)
	}
	return body
}

func plainTopLevelPath(path string) bool {
	if path == "" {
		return false
	}
	for i := 0; i < len(path); i++ {
		c := path[i]
		// Deliberately narrow: all escaping, nesting, queries, modifiers and
		// other path syntax must retain the original DeletePath semantics.
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// SetString skips the rewrite when path already holds the exact bytes sjson
// would write for value.
func SetString(body []byte, path, value string) []byte {
	if !needsJSONEscape(value) && holdsRaw(body, path, `"`+value+`"`) {
		return body
	}
	body, _ = sjson.SetBytes(body, path, value)
	return body
}

// SetBool skips the rewrite when path already holds the same literal.
func SetBool(body []byte, path string, value bool) []byte {
	literal := "false"
	if value {
		literal = "true"
	}
	if holdsRaw(body, path, literal) {
		return body
	}
	body, _ = sjson.SetBytes(body, path, value)
	return body
}

// View parses body without gjson.ParseBytes' full copy. Results alias body:
// read them only while body is unchanged and never retain them after the call
// that created the view returns. Copy (e.g. strings.Clone) anything kept.
func View(body []byte) gjson.Result {
	return gjson.Parse(viewString(body))
}

// GetView is gjson.GetBytes without copying the result. The same aliasing
// rules as View apply.
func GetView(body []byte, path string) gjson.Result {
	return gjson.Get(viewString(body), path)
}

// TopLevel finds the first top-level member named by each key in one pass, the
// member gjson.GetBytes returns for the same plain key. A long input array is
// skipped once instead of once per absent key. ok is false when body is not an
// object; callers then keep their per-path lookups. Results alias body like View.
func TopLevel(body []byte, keys ...string) (values []gjson.Result, ok bool) {
	root := View(body)
	if !root.IsObject() {
		return nil, false
	}
	values = make([]gjson.Result, len(keys))
	remaining := len(keys)
	root.ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		for i, want := range keys {
			if want == name && !values[i].Exists() {
				values[i] = value
				remaining--
			}
		}
		return remaining > 0
	})
	return values, true
}

func viewString(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(body), len(body))
}

// holdsRaw requires a positive Index because sjson only replaces values in place
// at a known offset; any other match takes sjson's rebuild path.
func holdsRaw(body []byte, path, raw string) bool {
	current := gjson.GetBytes(body, path)
	return current.Index > 0 && current.Raw == raw
}

// needsJSONEscape mirrors sjson's mustMarshalString: values that need it are
// written through encoding/json, so their bytes cannot be predicted here.
func needsJSONEscape(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] < ' ' || value[i] > 0x7f || value[i] == '"' || value[i] == '\\' {
			return true
		}
	}
	return false
}
