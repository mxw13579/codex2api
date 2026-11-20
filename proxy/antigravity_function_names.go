package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/codex2api/internal/jsondepth"
	"github.com/tidwall/gjson"
)

var antigravityFunctionNameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_.:-]`)

func antigravitySanitizeFunctionName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	sanitized := antigravityFunctionNameSanitizer.ReplaceAllString(name, "_")
	if len(sanitized) > 0 {
		first := sanitized[0]
		if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
			if len(sanitized) >= 64 {
				sanitized = sanitized[:63]
			}
			sanitized = "_" + sanitized
		}
	} else {
		sanitized = "_"
	}
	if len(sanitized) > 64 {
		sanitized = sanitized[:64]
	}
	return sanitized
}

func antigravityGeminiFunctionNameMap(rawJSON []byte) map[string]string {
	names := antigravityFunctionNamesFromGeminiRequest(rawJSON)
	if len(names) == 0 {
		return nil
	}
	uniqueNames := map[string]struct{}{}
	baseCounts := map[string]int{}
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, exists := uniqueNames[name]; exists {
			continue
		}
		uniqueNames[name] = struct{}{}
		baseCounts[antigravitySanitizeFunctionName(name)]++
	}
	sortedNames := make([]string, 0, len(uniqueNames))
	for name := range uniqueNames {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)
	out := map[string]string{}
	used := map[string]string{}
	for _, name := range sortedNames {
		base := antigravitySanitizeFunctionName(name)
		mapped := base
		if baseCounts[base] > 1 || used[base] != "" {
			mapped = antigravityDisambiguateFunctionName(base, name, used)
		}
		out[name] = mapped
		used[mapped] = name
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func antigravityGeminiReverseNameMap(forward map[string]string) map[string]string {
	if len(forward) == 0 {
		return nil
	}
	out := map[string]string{}
	for original, sanitized := range forward {
		if sanitized != original {
			out[sanitized] = original
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func antigravityMapGeminiFunctionName(nameMap map[string]string, name string) string {
	if mapped := nameMap[name]; mapped != "" {
		return mapped
	}
	return antigravitySanitizeFunctionName(name)
}

func antigravityRestoreGeminiFunctionName(reverse map[string]string, sanitized string) string {
	if sanitized == "" || reverse == nil {
		return sanitized
	}
	if original, ok := reverse[sanitized]; ok {
		return original
	}
	return sanitized
}

func antigravityFunctionNamesFromGeminiRequest(rawJSON []byte) []string {
	if len(rawJSON) == 0 || !jsondepth.ValidBytes(rawJSON) {
		return nil
	}
	tools := gjson.GetBytes(rawJSON, "tools")
	if !tools.IsArray() {
		return nil
	}
	names := make([]string, 0)
	var collectTool func(gjson.Result)
	collectDeclarations := func(declarations gjson.Result) {
		if !declarations.IsArray() {
			return
		}
		declarations.ForEach(func(_, declaration gjson.Result) bool {
			if name := declaration.Get("name").String(); name != "" {
				names = append(names, name)
			}
			return true
		})
	}
	collectTool = func(tool gjson.Result) {
		if nested := tool.Get("tools"); nested.IsArray() {
			nested.ForEach(func(_, nestedTool gjson.Result) bool {
				collectTool(nestedTool)
				return true
			})
			return
		}
		hasDeclarations := false
		if declarations := tool.Get("functionDeclarations"); declarations.IsArray() {
			collectDeclarations(declarations)
			hasDeclarations = true
		}
		if declarations := tool.Get("function_declarations"); declarations.IsArray() {
			collectDeclarations(declarations)
			hasDeclarations = true
		}
		if hasDeclarations {
			return
		}
		if name := tool.Get("function.name").String(); name != "" {
			names = append(names, name)
		}
	}
	tools.ForEach(func(_, tool gjson.Result) bool {
		collectTool(tool)
		return true
	})
	return names
}

func antigravityDisambiguateFunctionName(base, original string, used map[string]string) string {
	for attempt := 0; ; attempt++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", original, attempt)))
		suffix := "_" + hex.EncodeToString(digest[:6])
		prefix := base
		if maxPrefix := 64 - len(suffix); len(prefix) > maxPrefix {
			prefix = prefix[:maxPrefix]
		}
		candidate := prefix + suffix
		if used[candidate] == "" {
			return candidate
		}
	}
}

func antigravityRewriteNativeGeminiFunctionNames(request map[string]any, nameMap map[string]string) {
	if len(nameMap) == 0 {
		return
	}
	if contents, ok := request["contents"].([]any); ok {
		for index, rawContent := range contents {
			content, ok := rawContent.(map[string]any)
			if !ok {
				continue
			}
			parts, ok := content["parts"].([]any)
			if !ok {
				continue
			}
			for partIndex, rawPart := range parts {
				part, ok := rawPart.(map[string]any)
				if !ok {
					continue
				}
				for _, field := range []string{"functionCall", "function_call", "functionResponse", "function_response"} {
					nested, ok := part[field].(map[string]any)
					if !ok {
						continue
					}
					if name, _ := nested["name"].(string); strings.TrimSpace(name) != "" {
						nested["name"] = antigravityMapGeminiFunctionName(nameMap, name)
					}
				}
				parts[partIndex] = part
			}
			content["parts"] = parts
			contents[index] = content
		}
		request["contents"] = contents
	}
	for _, path := range []string{
		"toolConfig.functionCallingConfig.allowedFunctionNames",
		"tool_config.function_calling_config.allowed_function_names",
	} {
		antigravityRewriteAllowedFunctionNamesAtPath(request, path, nameMap)
	}
}

func antigravityRewriteAllowedFunctionNamesAtPath(request map[string]any, dottedPath string, nameMap map[string]string) {
	parts := strings.Split(dottedPath, ".")
	current := any(request)
	for _, key := range parts[:len(parts)-1] {
		m, ok := current.(map[string]any)
		if !ok {
			return
		}
		current = m[key]
	}
	parent, ok := current.(map[string]any)
	if !ok {
		return
	}
	lastKey := parts[len(parts)-1]
	rawNames, ok := parent[lastKey].([]any)
	if !ok {
		return
	}
	rewritten := make([]any, 0, len(rawNames))
	for _, rawName := range rawNames {
		name, _ := rawName.(string)
		rewritten = append(rewritten, antigravityMapGeminiFunctionName(nameMap, name))
	}
	parent[lastKey] = rewritten
}

func antigravityRestoreNativeGeminiResponseNames(body []byte, reverse map[string]string) []byte {
	if len(reverse) == 0 || len(body) == 0 {
		return body
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return body
	}
	candidates, ok := payload["candidates"].([]any)
	if !ok {
		return body
	}
	for candidateIndex, rawCandidate := range candidates {
		candidate, ok := rawCandidate.(map[string]any)
		if !ok {
			continue
		}
		content, ok := candidate["content"].(map[string]any)
		if !ok {
			continue
		}
		parts, ok := content["parts"].([]any)
		if !ok {
			continue
		}
		for partIndex, rawPart := range parts {
			part, ok := rawPart.(map[string]any)
			if !ok {
				continue
			}
			for _, field := range []string{"functionCall", "function_call", "functionResponse", "function_response"} {
				nested, ok := part[field].(map[string]any)
				if !ok {
					continue
				}
				if name, _ := nested["name"].(string); strings.TrimSpace(name) != "" {
					nested["name"] = antigravityRestoreGeminiFunctionName(reverse, name)
				}
			}
			parts[partIndex] = part
		}
		content["parts"] = parts
		candidate["content"] = content
		candidates[candidateIndex] = candidate
	}
	payload["candidates"] = candidates
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return out
}
