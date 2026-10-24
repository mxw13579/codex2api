package proxy

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Codex multi-agent v2 兼容层。
//
// 背景：Codex CLI/Desktop 的 v2 collaboration 工具把 spawn_agent /
// send_message / followup_task 的 message 参数标记为 encrypted。官方 OpenAI
// 上游在收到该标记时会生成密文，并由客户端存为 agent_message 的
// encrypted_content；第三方子模型无法读取。Codex 只在 function_call 带有
// 显式空数组 encrypted_function_args 且 namespace 为 collaboration 时走
// DirectPlaintextMessage 分支。
//
// 兼容层在父请求发往 Codex 上游前做两件事：
//  1. 删除三个协作工具 message schema 上的 encrypted 标记；
//  2. 把 collaboration 命名空间临时改名为 collaboration-optimize，避开上游
//     reserved schema 的逐字校验（issue #342）。
//
// 上游响应中的 collaboration-optimize 再还原为 collaboration，使 Codex 按
// 明文路径处理。无法安全判断时保持原生路径，不猜测密文内容。
//
// 核心变换参考 CLIProxyAPI 的同类实现（MIT）：
// https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go
const (
	codexMultiAgentV2CompatEnv = "CODEX_MULTI_AGENT_V2_COMPAT"

	codexCollaborationNamespace          = "collaboration"
	codexNamespaceDotPrefix              = codexCollaborationNamespace + "."
	codexNamespaceDoublePrefix           = codexCollaborationNamespace + "__"
	codexOptimizedCollaborationNamespace = "collaboration-optimize"
	codexOptimizedNamespaceDotPrefix     = codexOptimizedCollaborationNamespace + "."
	codexOptimizedNamespaceDoublePrefix  = codexOptimizedCollaborationNamespace + "__"

	// 官方客户端的协作工具只有一层 namespace。限制深度和改写数量，
	// 避免异常请求用深层嵌套或大量同名工具放大扫描/复制成本。
	codexMultiAgentV2MaxToolDepth = 8
	codexMultiAgentV2MaxToolPaths = 16
)

var codexCollaborationMessageTools = map[string]struct{}{
	"spawn_agent":   {},
	"send_message":  {},
	"followup_task": {},
}

var (
	codexCollaborationNamespaceBytes = []byte(codexCollaborationNamespace)
	codexOptimizedNamespaceBytes     = []byte(codexOptimizedCollaborationNamespace)
	codexFunctionCallBytes           = []byte("function_call")
)

// codexMultiAgentV2State 记录本次请求是否真的执行了命名空间改写。
// 只有改写过时才需要在响应侧还原，避免普通请求被二次处理。
// 账号、模型和诊断去重使用互斥锁，因为 attempt 协程可能被提前遗弃，
// 而响应回调仍会读取状态。
type codexMultiAgentV2State struct {
	mu        sync.Mutex
	optimized bool
	accountID int64
	model     string
	diagnosed map[string]struct{}
}

func (s *codexMultiAgentV2State) markOptimized(accountID int64, model string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.optimized = true
	s.accountID = accountID
	s.model = model
}

func (s *codexMultiAgentV2State) isOptimizedFor(accountID int64) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.optimized && s.accountID == accountID
}

// isOptimized 表示本次请求确实执行过命名空间改写。响应补全在函数内部再检查
// 一次，避免未来新增响应路径忘记外层账号门槛时误补 marker。
func (s *codexMultiAgentV2State) isOptimized() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.optimized
}

func (s *codexMultiAgentV2State) diagnosticContext() (int64, string) {
	if s == nil {
		return 0, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accountID, s.model
}

// markDiagnosed 返回 true 表示同一 call_id 已经记录过，调用方应跳过日志。
func (s *codexMultiAgentV2State) markDiagnosed(key string) bool {
	if s == nil || key == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.diagnosed == nil {
		s.diagnosed = make(map[string]struct{})
	}
	if _, logged := s.diagnosed[key]; logged {
		return true
	}
	s.diagnosed[key] = struct{}{}
	return false
}

type codexMultiAgentV2StateKey struct{}

func withCodexMultiAgentV2State(ctx context.Context, state *codexMultiAgentV2State) context.Context {
	if state == nil {
		return ctx
	}
	return context.WithValue(ctx, codexMultiAgentV2StateKey{}, state)
}

func codexMultiAgentV2StateFromContext(ctx context.Context) *codexMultiAgentV2State {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(codexMultiAgentV2StateKey{}).(*codexMultiAgentV2State)
	return state
}

// codexMultiAgentV2CompatEnabled 默认关闭，部署方通过环境变量显式启用。
func codexMultiAgentV2CompatEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(codexMultiAgentV2CompatEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// isCodexMultiAgentV2Client 只匹配官方 Codex 客户端身份，避免把普通
// Responses/Chat 请求或第三方客户端纳入改写。
func isCodexMultiAgentV2Client(userAgent string) bool {
	userAgent = strings.TrimSpace(userAgent)
	return strings.HasPrefix(userAgent, "Codex Desktop/") ||
		strings.HasPrefix(userAgent, "codex-tui/") ||
		userAgent == "codex_cli_rs" ||
		strings.HasPrefix(userAgent, "codex_cli_rs/") ||
		strings.HasPrefix(userAgent, "codex_exec/")
}

type codexFlatToolRename struct {
	suffix string
	prefix string
}

type codexMultiAgentV2ToolScan struct {
	messagePaths     []string
	flatNames        map[string]codexFlatToolRename // tool path -> 原名后缀与改名后的前缀
	namespacePaths   []string
	namespaceRecords map[string]struct{}
	conflict         bool
	limitExceeded    bool
}

// prepareCodexMultiAgentV2Request 是纯函数：返回改写后的请求体以及是否执行了
// 命名空间改名。任何不确定/冲突情况都返回原 body，让上游保持原生行为。
func prepareCodexMultiAgentV2Request(body []byte) ([]byte, bool, error) {
	if !bytes.Contains(body, codexCollaborationNamespaceBytes) {
		return body, false, nil
	}
	if !gjson.ValidBytes(body) {
		return body, false, fmt.Errorf("invalid JSON request body")
	}

	scan := codexMultiAgentV2ToolScan{}
	roots := gjson.GetManyBytes(body, "tools", "input")
	if tools := roots[0]; tools.IsArray() {
		scanCodexMultiAgentV2Tools(tools, "tools", "", "", true, 0, &scan)
	}
	if input := roots[1]; input.IsArray() {
		for index, item := range input.Array() {
			if !item.IsObject() || strings.TrimSpace(item.Get("type").String()) != "additional_tools" {
				continue
			}
			scanCodexMultiAgentV2Tools(item.Get("tools"), fmt.Sprintf("input.%d.tools", index), "", "", true, 0, &scan)
		}
	}

	if scan.conflict {
		return body, false, fmt.Errorf("request defines reserved %q namespace", codexOptimizedCollaborationNamespace)
	}
	if scan.limitExceeded {
		return body, false, fmt.Errorf("collaboration tool scan exceeds nesting or count limit")
	}
	// 只处理带 message.encrypted 的 collaboration 工具：没有可删的加密标记时
	// 保持原生路径，避免无意义地改 schema 和命名空间。
	if len(scan.messagePaths) == 0 || (len(scan.namespacePaths) == 0 && len(scan.flatNames) == 0) {
		return body, false, nil
	}

	updated := body
	for _, path := range scan.messagePaths {
		var err error
		updated, err = sjson.DeleteBytes(updated, path+".parameters.properties.message.encrypted")
		if err != nil {
			return body, false, err
		}
	}
	for _, path := range scan.namespacePaths {
		var err error
		updated, err = sjson.SetBytes(updated, path+".name", codexOptimizedCollaborationNamespace)
		if err != nil {
			return body, false, err
		}
	}
	for path, rename := range scan.flatNames {
		var err error
		updated, err = sjson.SetBytes(updated, path+".name", rename.prefix+rename.suffix)
		if err != nil {
			return body, false, err
		}
	}
	updated, err := rewriteCodexMultiAgentV2ToolChoice(updated, &scan)
	if err != nil {
		return body, false, err
	}
	return updated, true, nil
}

// rewriteCodexMultiAgentV2ToolChoice 同步改写指向协作工具的 tool_choice，
// 避免工具定义改名后 choice 仍指向旧名导致上游 400。只有本次确实改过
// namespace 或对应扁平工具时才改写。
func rewriteCodexMultiAgentV2ToolChoice(body []byte, scan *codexMultiAgentV2ToolScan) ([]byte, error) {
	choice := gjson.GetBytes(body, "tool_choice")
	if !choice.IsObject() {
		return body, nil
	}
	switch strings.TrimSpace(choice.Get("type").String()) {
	case "function", "custom":
		return rewriteCodexMultiAgentV2ToolChoiceItem(body, "tool_choice", choice, scan)
	case "allowed_tools":
		tools := choice.Get("tools")
		if !tools.IsArray() {
			return body, nil
		}
		updated := body
		for index, item := range tools.Array() {
			var err error
			updated, err = rewriteCodexMultiAgentV2ToolChoiceItem(
				updated, fmt.Sprintf("tool_choice.tools.%d", index), item, scan,
			)
			if err != nil {
				return body, err
			}
		}
		return updated, nil
	default:
		return body, nil
	}
}

func rewriteCodexMultiAgentV2ToolChoiceItem(body []byte, path string, item gjson.Result, scan *codexMultiAgentV2ToolScan) ([]byte, error) {
	if !item.IsObject() {
		return body, nil
	}
	namespace := strings.TrimSpace(item.Get("namespace").String())
	name := strings.TrimSpace(item.Get("name").String())
	updated := body
	if scan != nil && len(scan.namespacePaths) > 0 && namespace == codexCollaborationNamespace {
		var err error
		updated, err = sjson.SetBytes(updated, path+".namespace", codexOptimizedCollaborationNamespace)
		if err != nil {
			return body, err
		}
	}
	var suffix, renamePrefix string
	switch {
	case strings.HasPrefix(name, codexNamespaceDotPrefix):
		suffix = strings.TrimPrefix(name, codexNamespaceDotPrefix)
		renamePrefix = codexOptimizedNamespaceDotPrefix
	case strings.HasPrefix(name, codexNamespaceDoublePrefix):
		suffix = strings.TrimPrefix(name, codexNamespaceDoublePrefix)
		renamePrefix = codexOptimizedNamespaceDoublePrefix
	}
	if suffix != "" && scan != nil && scan.flatToolRenamed(suffix) {
		var err error
		updated, err = sjson.SetBytes(updated, path+".name", renamePrefix+suffix)
		if err != nil {
			return body, err
		}
	}
	return updated, nil
}

func (s *codexMultiAgentV2ToolScan) flatToolRenamed(name string) bool {
	if s == nil {
		return false
	}
	for _, rename := range s.flatNames {
		if rename.suffix == name {
			return true
		}
	}
	return false
}

// scanCodexMultiAgentV2Tools 只识别协作工具族的两种正式形态：
// 顶层 collaboration namespace 下的三个 function，以及顶层扁平
// collaboration.<tool>。其他 namespace 或裸工具名即使同名也不改写。
//
// 只有出现 parameters.properties.message.encrypted 时才记录，避免对已经
// 明文的 schema 做无意义改名。
func scanCodexMultiAgentV2Tools(tools gjson.Result, path, namespace, namespacePath string, root bool, depth int, scan *codexMultiAgentV2ToolScan) {
	if !tools.IsArray() {
		return
	}
	if depth > codexMultiAgentV2MaxToolDepth {
		scan.limitExceeded = true
		return
	}
	for index, tool := range tools.Array() {
		if !tool.IsObject() {
			continue
		}
		toolType := strings.TrimSpace(tool.Get("type").String())
		name := strings.TrimSpace(tool.Get("name").String())

		if isCodexOptimizedNamespaceName(name) {
			scan.conflict = true
			continue
		}

		switch toolType {
		case "function":
			if namespace == codexCollaborationNamespace {
				if _, ok := codexCollaborationMessageTools[name]; !ok {
					continue
				}
				if !tool.Get("parameters.properties.message.encrypted").Exists() {
					continue
				}
				if len(scan.messagePaths) >= codexMultiAgentV2MaxToolPaths {
					scan.limitExceeded = true
					return
				}
				scan.messagePaths = append(scan.messagePaths, fmt.Sprintf("%s.%d", path, index))
				if namespacePath != "" {
					if _, recorded := scan.namespaceRecords[namespacePath]; !recorded {
						if scan.namespaceRecords == nil {
							scan.namespaceRecords = make(map[string]struct{})
						}
						scan.namespaceRecords[namespacePath] = struct{}{}
						scan.namespacePaths = append(scan.namespacePaths, namespacePath)
					}
				}
				continue
			}
			if root {
				var suffix, renamePrefix string
				switch {
				case strings.HasPrefix(name, codexNamespaceDotPrefix):
					suffix = strings.TrimPrefix(name, codexNamespaceDotPrefix)
					renamePrefix = codexOptimizedNamespaceDotPrefix
				case strings.HasPrefix(name, codexNamespaceDoublePrefix):
					suffix = strings.TrimPrefix(name, codexNamespaceDoublePrefix)
					renamePrefix = codexOptimizedNamespaceDoublePrefix
				}
				if suffix == "" {
					continue
				}
				if _, ok := codexCollaborationMessageTools[suffix]; !ok {
					continue
				}
				if !tool.Get("parameters.properties.message.encrypted").Exists() {
					continue
				}
				if len(scan.messagePaths) >= codexMultiAgentV2MaxToolPaths {
					scan.limitExceeded = true
					return
				}
				toolPath := fmt.Sprintf("%s.%d", path, index)
				scan.messagePaths = append(scan.messagePaths, toolPath)
				if scan.flatNames == nil {
					scan.flatNames = make(map[string]codexFlatToolRename)
				}
				scan.flatNames[toolPath] = codexFlatToolRename{suffix: suffix, prefix: renamePrefix}
			}
		case "namespace":
			childNamespace := ""
			if root && name == codexCollaborationNamespace {
				childNamespace = codexCollaborationNamespace
			}
			toolPath := fmt.Sprintf("%s.%d", path, index)
			scanCodexMultiAgentV2Tools(tool.Get("tools"), toolPath+".tools", childNamespace, toolPath, false, depth+1, scan)
		}
	}
}

func isCodexOptimizedNamespaceName(name string) bool {
	return name == codexOptimizedCollaborationNamespace ||
		strings.HasPrefix(name, codexOptimizedNamespaceDotPrefix) ||
		strings.HasPrefix(name, codexOptimizedNamespaceDoublePrefix)
}

// restoreCodexMultiAgentV2Response 把上游响应中的临时命名空间还原为
// collaboration。只改写工具调用项和 namespace 声明，arguments/input/output
// 等不透明字段保持原样。
//
// 这里按已知响应容器定向改写，而不是整包 decode 成 any 再 marshal：
// response.completed 会回显完整工具数组，整包重编码会放大延迟和垃圾。
func restoreCodexMultiAgentV2Response(payload []byte, state *codexMultiAgentV2State) []byte {
	if len(payload) == 0 {
		return payload
	}
	if !bytes.Contains(payload, codexOptimizedNamespaceBytes) {
		// 上游可能把 namespace 归一化回 collaboration；本次请求确实改写过时，
		// 仍要检查 function_call 是否缺 encrypted_function_args。
		if state == nil || !state.isOptimized() || !bytes.Contains(payload, codexFunctionCallBytes) {
			return payload
		}
	}
	if !gjson.ValidBytes(payload) {
		return payload
	}

	paths := [...]string{"item", "output", "response.output", "tools", "response.tools", "tool_choice", "response.tool_choice"}
	results := gjson.GetManyBytes(payload, paths[:]...)
	updated := payload
	for index, result := range results {
		var err error
		updated, err = restoreCodexMultiAgentV2Container(updated, paths[index], result, state)
		if err != nil {
			return payload
		}
	}
	return updated
}

func restoreCodexMultiAgentV2Container(updated []byte, path string, container gjson.Result, state *codexMultiAgentV2State) ([]byte, error) {
	switch {
	case container.IsObject():
		return restoreCodexMultiAgentV2Item(updated, path, container, state)
	case container.IsArray():
		var err error
		for index, item := range container.Array() {
			updated, err = restoreCodexMultiAgentV2Item(updated, fmt.Sprintf("%s.%d", path, index), item, state)
			if err != nil {
				return updated, err
			}
		}
	}
	return updated, nil
}

func restoreCodexMultiAgentV2Item(updated []byte, path string, item gjson.Result, state *codexMultiAgentV2State) ([]byte, error) {
	if !item.IsObject() {
		return updated, nil
	}
	itemType := strings.TrimSpace(item.Get("type").String())
	isToolCall := itemType == "function_call" || itemType == "custom_tool_call"
	if isToolCall {
		namespace := strings.TrimSpace(item.Get("namespace").String())
		name := strings.TrimSpace(item.Get("name").String())
		restoredName := name
		restoreNamespace := namespace == codexOptimizedCollaborationNamespace
		switch {
		case strings.HasPrefix(name, codexOptimizedNamespaceDotPrefix):
			restoredName = strings.TrimPrefix(name, codexOptimizedNamespaceDotPrefix)
			restoreNamespace = true
		case strings.HasPrefix(name, codexOptimizedNamespaceDoublePrefix):
			restoredName = strings.TrimPrefix(name, codexOptimizedNamespaceDoublePrefix)
			restoreNamespace = true
		}
		if restoreNamespace {
			var err error
			updated, err = sjson.SetBytes(updated, path+".namespace", codexCollaborationNamespace)
			if err != nil {
				return updated, err
			}
		}
		// 上游可能直接返回 collaboration，而不是临时命名空间；只要本次请求
		// 确实改写过，且是三个协作工具之一，就检查并按需补全明文标记。
		if itemType == "function_call" && (restoreNamespace || namespace == codexCollaborationNamespace) {
			var err error
			var injected bool
			updated, injected, err = ensureCodexPlaintextFunctionArgs(updated, path, restoredName, item, state)
			if err != nil {
				return updated, err
			}
			if !injected {
				diagnoseCodexMultiAgentV2ToolCall(restoredName, item, state)
			}
		}
		if restoredName != name {
			var err error
			updated, err = sjson.SetBytes(updated, path+".name", restoredName)
			if err != nil {
				return updated, err
			}
		}
		return updated, nil
	}
	if itemType == "namespace" && strings.TrimSpace(item.Get("name").String()) == codexOptimizedCollaborationNamespace {
		var err error
		updated, err = sjson.SetBytes(updated, path+".name", codexCollaborationNamespace)
		if err != nil {
			return updated, err
		}
		return updated, nil
	}
	if itemType == "allowed_tools" {
		tools := item.Get("tools")
		if tools.IsArray() {
			for index, child := range tools.Array() {
				var err error
				updated, err = restoreCodexMultiAgentV2Item(
					updated, fmt.Sprintf("%s.tools.%d", path, index), child, state,
				)
				if err != nil {
					return updated, err
				}
			}
		}
		return updated, nil
	}
	// 部分响应会回显工具定义或 tool_choice；扁平工具在请求侧被改成
	// collaboration-optimize.<tool>，返回客户端前恢复原前缀。
	if itemType == "function" || itemType == "custom" {
		if namespace := strings.TrimSpace(item.Get("namespace").String()); namespace == codexOptimizedCollaborationNamespace {
			var err error
			updated, err = sjson.SetBytes(updated, path+".namespace", codexCollaborationNamespace)
			if err != nil {
				return updated, err
			}
		}
		name := strings.TrimSpace(item.Get("name").String())
		var restoredName string
		switch {
		case strings.HasPrefix(name, codexOptimizedNamespaceDotPrefix):
			restoredName = codexNamespaceDotPrefix + strings.TrimPrefix(name, codexOptimizedNamespaceDotPrefix)
		case strings.HasPrefix(name, codexOptimizedNamespaceDoublePrefix):
			restoredName = codexCollaborationNamespace + "__" + strings.TrimPrefix(name, codexOptimizedNamespaceDoublePrefix)
		}
		if restoredName != "" && restoredName != name {
			var err error
			updated, err = sjson.SetBytes(updated, path+".name", restoredName)
			if err != nil {
				return updated, err
			}
		}
	}
	return updated, nil
}

// ensureCodexPlaintextFunctionArgs 把上游省略的 encrypted_function_args 显式
// 补成 []。客户端只在 encrypted_function_args == Some([]) 时才把协作工具参数
// 当作明文；字段缺失或为 null 时仍按密文交付，父模型生成的明文 message 会被
// 客户端重新包进子会话的 encrypted_content。
//
// 补全前校验 arguments 是合法 JSON、message 是字符串，且不像已知密文前缀。
// 上游仍返回密文时保持原样，把判断留给诊断日志和上层错误。
func ensureCodexPlaintextFunctionArgs(updated []byte, path, name string, item gjson.Result, state *codexMultiAgentV2State) ([]byte, bool, error) {
	if state == nil || !state.isOptimized() {
		return updated, false, nil
	}
	if _, ok := codexCollaborationMessageTools[name]; !ok {
		return updated, false, nil
	}
	marker := item.Get("encrypted_function_args")
	if marker.Exists() && marker.Type != gjson.Null {
		return updated, false, nil
	}
	argumentsResult := item.Get("arguments")
	if argumentsResult.Type != gjson.String {
		return updated, false, nil
	}
	arguments := strings.TrimSpace(argumentsResult.String())
	if arguments == "" || !gjson.Valid(arguments) {
		return updated, false, nil
	}
	message := gjson.Get(arguments, "message")
	if !message.Exists() || message.Type != gjson.String {
		return updated, false, nil
	}
	messageText := strings.TrimSpace(message.String())
	if messageText == "" || looksLikeCodexCiphertext(messageText) {
		return updated, false, nil
	}
	updated, err := sjson.SetBytes(updated, path+".encrypted_function_args", []string{})
	if err != nil {
		return updated, false, err
	}
	accountID, model := state.diagnosticContext()
	if callID := strings.TrimSpace(item.Get("call_id").String()); callID == "" || !state.markDiagnosed(callID+"\x00"+name+"\x00plaintext") {
		log.Printf("[multi-agent-v2] 已为上游 %s 调用补全 encrypted_function_args: [] (account=%d model=%s)",
			name, accountID, model)
	}
	return updated, true, nil
}

// looksLikeCodexCiphertext 是响应补全前的 fail-closed 启发式，不是完整的
// 密文识别。已知的 Fernet 前缀、JWE 紧凑格式和 enc_ 前缀直接拒绝；长度较长
// 且只含 base64url 字符的单 token 也按密文处理。命中时保持原样，让客户端
// 继续按加密消息处理，避免把密文伪装成明文任务。
func looksLikeCodexCiphertext(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if strings.HasPrefix(value, codexEncryptedContentPrefix) ||
		strings.HasPrefix(value, "eyJ") ||
		strings.HasPrefix(value, "enc_") {
		return true
	}
	if len(value) < 64 || strings.ContainsAny(value, " \t\r\n") {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '=':
		default:
			return false
		}
	}
	return true
}

// diagnoseCodexMultiAgentV2ToolCall 在还原响应时检查上游是否真的返回了
// 明文标记。缺失 encrypted_function_args: [] 时客户端仍会按加密消息处理，
// 这是兼容层未生效的关键可诊断信号；只记录账号、模型、工具名和密文形态，
// 不记录任务正文。
func diagnoseCodexMultiAgentV2ToolCall(name string, item gjson.Result, state *codexMultiAgentV2State) {
	if _, ok := codexCollaborationMessageTools[name]; !ok {
		return
	}
	arguments := item.Get("arguments").String()
	if strings.TrimSpace(arguments) == "" {
		// output_item.added 可能还没有 arguments；等到 done 再判断。
		return
	}
	if marker := item.Get("encrypted_function_args"); marker.IsArray() && len(marker.Array()) == 0 {
		return
	}
	accountID, model := state.diagnosticContext()
	if callID := strings.TrimSpace(item.Get("call_id").String()); callID != "" {
		if state.markDiagnosed(callID + "\x00" + name) {
			return
		}
	}
	message := gjson.Get(arguments, "message").String()
	log.Printf("[multi-agent-v2] 上游 %s 调用未返回 encrypted_function_args: []，Codex 仍会按加密消息处理 (account=%d model=%s ciphertext_like=%t)",
		name, accountID, model, looksLikeCodexCiphertext(message))
}
