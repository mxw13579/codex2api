package proxy

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestIsCodexMultiAgentV2Client(t *testing.T) {
	tests := []struct {
		ua   string
		want bool
	}{
		{"Codex Desktop/0.162.0-alpha.17.2", true},
		{"codex-tui/0.162.0", true},
		{"codex_cli_rs", true},
		{"codex_cli_rs/0.162.0", true},
		{"codex_exec/0.162.0-alpha.17.2 (Windows 10.0)", true},
		{"curl/8.7.1", false},
		{"OpenAI/Python 1.0", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isCodexMultiAgentV2Client(tt.ua); got != tt.want {
			t.Fatalf("isCodexMultiAgentV2Client(%q) = %v, want %v", tt.ua, got, tt.want)
		}
	}
}

func TestPrepareCodexMultiAgentV2RequestRewritesAdditionalTools(t *testing.T) {
	payload := []byte(`{
		"model":"gpt-6.1-sol",
		"input":[
			{"type":"additional_tools","role":"developer","tools":[
				{"type":"namespace","name":"functions","tools":[{"type":"custom","name":"exec"}]},
				{"type":"namespace","name":"collaboration","tools":[
					{"type":"function","name":"followup_task","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}},
					{"type":"function","name":"interrupt_agent","parameters":{"type":"object","properties":{}}},
					{"type":"function","name":"send_message","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}},
					{"type":"function","name":"spawn_agent","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}}
				]},
				{"type":"function","name":"business_tool","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}}
			]}
		]
	}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !optimized {
		t.Fatal("expected namespace to be optimized")
	}
	if ns := gjson.GetBytes(got, "input.0.tools.1.name").String(); ns != codexOptimizedCollaborationNamespace {
		t.Fatalf("namespace = %q, want %q", ns, codexOptimizedCollaborationNamespace)
	}
	for _, path := range []string{
		"input.0.tools.1.tools.0.parameters.properties.message.encrypted",
		"input.0.tools.1.tools.2.parameters.properties.message.encrypted",
		"input.0.tools.1.tools.3.parameters.properties.message.encrypted",
	} {
		if gjson.GetBytes(got, path).Exists() {
			t.Fatalf("%s was not removed: %s", path, got)
		}
	}
	if !gjson.GetBytes(got, "input.0.tools.2.parameters.properties.message.encrypted").Exists() {
		t.Fatalf("unrelated tool encrypted marker was removed: %s", got)
	}
	if ns := gjson.GetBytes(got, "input.0.tools.0.name").String(); ns != "functions" {
		t.Fatalf("unrelated namespace = %q, want functions", ns)
	}
}

func TestPrepareCodexMultiAgentV2RequestTopLevelAndFlatTools(t *testing.T) {
	payload := []byte(`{
		"tools":[
			{"type":"function","name":"collaboration.spawn_agent","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}},
			{"type":"function","name":"collaboration.send_message","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}},
			{"type":"function","name":"collaboration.wait_agent","parameters":{"type":"object","properties":{}}}
		]
	}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !optimized {
		t.Fatal("expected flat collaboration tools to be optimized")
	}
	if name := gjson.GetBytes(got, "tools.0.name").String(); name != "collaboration-optimize.spawn_agent" {
		t.Fatalf("spawn name = %q", name)
	}
	if name := gjson.GetBytes(got, "tools.1.name").String(); name != "collaboration-optimize.send_message" {
		t.Fatalf("send name = %q", name)
	}
	if name := gjson.GetBytes(got, "tools.2.name").String(); name != "collaboration.wait_agent" {
		t.Fatalf("wait name = %q", name)
	}
	for _, path := range []string{
		"tools.0.parameters.properties.message.encrypted",
		"tools.1.parameters.properties.message.encrypted",
	} {
		if gjson.GetBytes(got, path).Exists() {
			t.Fatalf("%s was not removed: %s", path, got)
		}
	}
}

func TestPrepareCodexMultiAgentV2RequestWithoutSpawnStillRewritesMessageTools(t *testing.T) {
	payload := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration","tools":[
		{"type":"function","name":"send_message","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}},
		{"type":"function","name":"followup_task","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}}
	]}]}]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !optimized {
		t.Fatal("expected send_message/followup_task to be optimized without spawn_agent")
	}
	if ns := gjson.GetBytes(got, "input.0.tools.0.name").String(); ns != codexOptimizedCollaborationNamespace {
		t.Fatalf("namespace = %q, want %q", ns, codexOptimizedCollaborationNamespace)
	}
	for _, path := range []string{
		"input.0.tools.0.tools.0.parameters.properties.message.encrypted",
		"input.0.tools.0.tools.1.parameters.properties.message.encrypted",
	} {
		if gjson.GetBytes(got, path).Exists() {
			t.Fatalf("%s was not removed: %s", path, got)
		}
	}
}

func TestPrepareCodexMultiAgentV2RequestFlatMessageToolWithoutSpawn(t *testing.T) {
	payload := []byte(`{"tools":[
		{"type":"function","name":"collaboration.send_message","parameters":{"properties":{"message":{"encrypted":true}}}}
	]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !optimized {
		t.Fatal("expected flat collaboration.send_message to be optimized")
	}
	if name := gjson.GetBytes(got, "tools.0.name").String(); name != "collaboration-optimize.send_message" {
		t.Fatalf("name = %q, want collaboration-optimize.send_message", name)
	}
	if gjson.GetBytes(got, "tools.0.parameters.properties.message.encrypted").Exists() {
		t.Fatalf("flat message.encrypted was not removed: %s", got)
	}
}

func TestPrepareCodexMultiAgentV2RequestIgnoresSameNamedToolsOutsideCollaboration(t *testing.T) {
	payload := []byte(`{"tools":[
		{"type":"namespace","name":"collaboration","tools":[
			{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}}
		]},
		{"type":"namespace","name":"business","tools":[
			{"type":"function","name":"send_message","parameters":{"properties":{"message":{"encrypted":true}}}}
		]},
		{"type":"function","name":"followup_task","parameters":{"properties":{"message":{"encrypted":true}}}}
	]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !optimized {
		t.Fatal("expected collaboration namespace to be optimized")
	}
	if gjson.GetBytes(got, "tools.0.tools.0.parameters.properties.message.encrypted").Exists() {
		t.Fatalf("collaboration message.encrypted was not removed: %s", got)
	}
	for _, path := range []string{
		"tools.1.tools.0.parameters.properties.message.encrypted",
		"tools.2.parameters.properties.message.encrypted",
	} {
		if !gjson.GetBytes(got, path).Exists() {
			t.Fatalf("same-named tool outside collaboration was rewritten: %s", path)
		}
	}
	if name := gjson.GetBytes(got, "tools.1.name").String(); name != "business" {
		t.Fatalf("business namespace changed: %q", name)
	}
}

func TestCodexMultiAgentV2StateTracksAccountAndDiagnostics(t *testing.T) {
	state := &codexMultiAgentV2State{}
	if state.isOptimizedFor(1) {
		t.Fatal("empty state must not be optimized")
	}
	state.markOptimized(1, "gpt-5.5")
	if !state.isOptimizedFor(1) {
		t.Fatal("optimized state must match its account")
	}
	if state.isOptimizedFor(2) {
		t.Fatal("optimized state must not match a different account")
	}
	accountID, model := state.diagnosticContext()
	if accountID != 1 || model != "gpt-5.5" {
		t.Fatalf("diagnostic context = %d/%q", accountID, model)
	}
	if state.markDiagnosed("call-1") {
		t.Fatal("first diagnosis must not be treated as duplicate")
	}
	if !state.markDiagnosed("call-1") {
		t.Fatal("second diagnosis must be treated as duplicate")
	}
}

func TestPrepareCodexMultiAgentV2RequestHandlesEmptyAndNonArrayTools(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		{},
		[]byte(`{}`),
		[]byte(`{"tools":null}`),
		[]byte(`{"tools":{}}`),
		[]byte(`{"tools":[]}`),
	} {
		got, optimized, err := prepareCodexMultiAgentV2Request(payload)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", payload, err)
		}
		if optimized {
			t.Fatalf("unexpected optimization for %q", payload)
		}
		if string(got) != string(payload) {
			t.Fatalf("payload changed for %q: %s", payload, got)
		}
	}
}

func TestPrepareCodexMultiAgentV2RequestRewritesToolChoice(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		wantPath   string
		wantString string
	}{
		{
			name: "flat",
			payload: `{"tools":[{"type":"function","name":"collaboration.spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}}],
				"tool_choice":{"type":"function","name":"collaboration.spawn_agent"}}`,
			wantPath:   "tool_choice.name",
			wantString: "collaboration-optimize.spawn_agent",
		},
		{
			name: "namespace",
			payload: `{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}}]}],
				"tool_choice":{"type":"function","namespace":"collaboration","name":"spawn_agent"}}`,
			wantPath:   "tool_choice.namespace",
			wantString: "collaboration-optimize",
		},
		{
			name: "namespace non-message tool",
			payload: `{"tools":[{"type":"namespace","name":"collaboration","tools":[
				{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}},
				{"type":"function","name":"interrupt_agent","parameters":{"properties":{}}}
			]}],
				"tool_choice":{"type":"function","namespace":"collaboration","name":"interrupt_agent"}}`,
			wantPath:   "tool_choice.namespace",
			wantString: "collaboration-optimize",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, optimized, err := prepareCodexMultiAgentV2Request([]byte(tt.payload))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !optimized {
				t.Fatal("expected tool_choice request to be optimized")
			}
			if value := gjson.GetBytes(got, tt.wantPath).String(); value != tt.wantString {
				t.Fatalf("%s = %q, want %q", tt.wantPath, value, tt.wantString)
			}
		})
	}
}

func TestPrepareCodexMultiAgentV2RequestFlatDoubleUnderscore(t *testing.T) {
	payload := []byte(`{"tools":[
		{"type":"function","name":"collaboration__send_message","parameters":{"properties":{"message":{"encrypted":true}}}}
	]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !optimized {
		t.Fatal("expected flat collaboration__send_message to be optimized")
	}
	if name := gjson.GetBytes(got, "tools.0.name").String(); name != "collaboration-optimize__send_message" {
		t.Fatalf("name = %q, want collaboration-optimize__send_message", name)
	}
}

func TestPrepareCodexMultiAgentV2RequestRewritesAllowedToolsChoice(t *testing.T) {
	payload := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[
		{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}}
	]}],
		"tool_choice":{"type":"allowed_tools","tools":[
			{"type":"function","namespace":"collaboration","name":"spawn_agent"}
		]}}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !optimized {
		t.Fatal("expected allowed_tools request to be optimized")
	}
	if ns := gjson.GetBytes(got, "tool_choice.tools.0.namespace").String(); ns != codexOptimizedCollaborationNamespace {
		t.Fatalf("tool_choice.tools.0.namespace = %q", ns)
	}
}

func TestRestoreCodexMultiAgentV2ResponseRestoresToolChoice(t *testing.T) {
	payload := []byte(`{
		"tool_choice":{"type":"function","namespace":"collaboration-optimize","name":"spawn_agent"},
		"response":{"tool_choice":{"type":"allowed_tools","tools":[
			{"type":"function","namespace":"collaboration-optimize","name":"followup_task"}
		]}}
	}`)

	got := restoreCodexMultiAgentV2Response(payload, nil)
	if ns := gjson.GetBytes(got, "tool_choice.namespace").String(); ns != codexCollaborationNamespace {
		t.Fatalf("tool_choice.namespace = %q", ns)
	}
	if ns := gjson.GetBytes(got, "response.tool_choice.tools.0.namespace").String(); ns != codexCollaborationNamespace {
		t.Fatalf("response.tool_choice.tools.0.namespace = %q", ns)
	}
}

func TestPrepareCodexMultiAgentV2RequestAlreadyPlaintextUnchanged(t *testing.T) {
	payload := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[
		{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"type":"string"}}}}
	]}]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if optimized {
		t.Fatal("already plaintext schema must not be optimized")
	}
	if string(got) != string(payload) {
		t.Fatalf("already plaintext schema changed: %s", got)
	}
}

func TestPrepareCodexMultiAgentV2RequestRejectsDeepNesting(t *testing.T) {
	nested := `{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}}`
	for i := 0; i <= codexMultiAgentV2MaxToolDepth; i++ {
		nested = `{"type":"namespace","name":"layer","tools":[` + nested + `]}`
	}
	payload := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[` + nested + `]}]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err == nil {
		t.Fatal("expected nesting limit error")
	}
	if optimized {
		t.Fatal("deeply nested request must not be optimized")
	}
	if string(got) != string(payload) {
		t.Fatalf("deeply nested request changed: %s", got)
	}
}

func TestPrepareCodexMultiAgentV2RequestRejectsTooManyMessageTools(t *testing.T) {
	var tools strings.Builder
	for i := 0; i <= codexMultiAgentV2MaxToolPaths; i++ {
		if i > 0 {
			tools.WriteString(",")
		}
		tools.WriteString(`{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}}`)
	}
	payload := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[` + tools.String() + `]}]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err == nil {
		t.Fatal("expected tool count limit error")
	}
	if optimized {
		t.Fatal("oversized request must not be optimized")
	}
	if string(got) != string(payload) {
		t.Fatalf("oversized request changed: %s", got)
	}
}

func TestPrepareCodexMultiAgentV2RequestConflictUnchanged(t *testing.T) {
	payload := []byte(`{"tools":[
		{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"encrypted":true}}}}]},
		{"type":"namespace","name":"collaboration-optimize","tools":[]}
	]}`)

	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err == nil {
		t.Fatal("expected conflict error")
	}
	if optimized {
		t.Fatal("conflict must not optimize")
	}
	if string(got) != string(payload) {
		t.Fatalf("conflict changed payload: %s", got)
	}
}

func TestPrepareCodexMultiAgentV2RequestLeavesUnrelatedRequestsUnchanged(t *testing.T) {
	payload := []byte(`{"model":"gpt-6.1-sol","tools":[{"type":"function","name":"business_tool","parameters":{"properties":{"message":{"encrypted":true}}}}]}`)
	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if optimized {
		t.Fatal("unrelated request must not be optimized")
	}
	if string(got) != string(payload) {
		t.Fatalf("unrelated request changed: %s", got)
	}
}

func TestPrepareCodexMultiAgentV2RequestInvalidJSONUnchanged(t *testing.T) {
	payload := []byte(`{"collaboration":`)
	got, optimized, err := prepareCodexMultiAgentV2Request(payload)
	if err == nil {
		t.Fatal("expected invalid JSON error")
	}
	if optimized {
		t.Fatal("invalid JSON must not optimize")
	}
	if string(got) != string(payload) {
		t.Fatalf("invalid JSON changed: %s", got)
	}
}

func TestRestoreCodexMultiAgentV2Response(t *testing.T) {
	payload := []byte(`{
		"type":"response.completed",
		"response":{
			"output":[
				{"type":"function_call","name":"spawn_agent","namespace":"collaboration-optimize","arguments":{"namespace":"collaboration-optimize","name":"collaboration-optimize__opaque"}},
				{"type":"function_call","name":"collaboration-optimize__send_message","arguments":"{}","encrypted_function_args":[]},
				{"type":"function_call","name":"collaboration-optimize.spawn_agent","namespace":null,"arguments":"{}","encrypted_function_args":[]},
				{"type":"message","namespace":"collaboration-optimize","name":"collaboration-optimize__plain"},
				{"type":"function_call","name":"business_tool","namespace":"business","arguments":"{}"}
			],
			"tools":[{"type":"namespace","name":"collaboration-optimize"}]
		}
	}`)

	got := restoreCodexMultiAgentV2Response(payload, nil)
	if ns := gjson.GetBytes(got, "response.output.0.namespace").String(); ns != codexCollaborationNamespace {
		t.Fatalf("output 0 namespace = %q", ns)
	}
	if name := gjson.GetBytes(got, "response.output.1.name").String(); name != "send_message" {
		t.Fatalf("output 1 name = %q", name)
	}
	if ns := gjson.GetBytes(got, "response.output.1.namespace").String(); ns != codexCollaborationNamespace {
		t.Fatalf("output 1 namespace = %q", ns)
	}
	if name := gjson.GetBytes(got, "response.output.2.name").String(); name != "spawn_agent" {
		t.Fatalf("output 2 name = %q", name)
	}
	if ns := gjson.GetBytes(got, "response.output.2.namespace").String(); ns != codexCollaborationNamespace {
		t.Fatalf("output 2 namespace = %q", ns)
	}
	// arguments 是不透明字段，不能被改写。
	if ns := gjson.GetBytes(got, "response.output.0.arguments.namespace").String(); ns != codexOptimizedCollaborationNamespace {
		t.Fatalf("arguments namespace was rewritten: %q", ns)
	}
	// 非工具调用项的普通 namespace 字段保持原样。
	if ns := gjson.GetBytes(got, "response.output.3.namespace").String(); ns != codexOptimizedCollaborationNamespace {
		t.Fatalf("ordinary namespace was rewritten: %q", ns)
	}
	if name := gjson.GetBytes(got, "response.output.3.name").String(); name != "collaboration-optimize__plain" {
		t.Fatalf("ordinary name was rewritten: %q", name)
	}
	if name := gjson.GetBytes(got, "response.tools.0.name").String(); name != codexCollaborationNamespace {
		t.Fatalf("tools namespace = %q", name)
	}
	if ns := gjson.GetBytes(got, "response.output.4.namespace").String(); ns != "business" {
		t.Fatalf("business tool namespace changed: %q", ns)
	}

	// 幂等：再次还原不应继续变化。
	again := restoreCodexMultiAgentV2Response(got, nil)
	if string(again) != string(got) {
		t.Fatalf("restore is not idempotent:\nfirst=%s\nsecond=%s", got, again)
	}
}

func TestRestoreCodexMultiAgentV2ResponseInjectsPlaintextFunctionArgs(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		path          string
		wantInjected  bool
		wantMarker    string
		wantNamespace string
		stateNil      bool
	}{
		{
			name:          "missing marker",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_1","arguments":"{\"message\":\"do the task\"}"}}`,
			path:          "item",
			wantInjected:  true,
			wantMarker:    "[]",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "null marker",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"send_message","call_id":"call_2","arguments":"{\"message\":\"follow up\"}","encrypted_function_args":null}}`,
			path:          "item",
			wantInjected:  true,
			wantMarker:    "[]",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "existing empty marker",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"followup_task","call_id":"call_3","arguments":"{\"message\":\"again\"}","encrypted_function_args":[]}}`,
			path:          "item",
			wantMarker:    "[]",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "existing encrypted marker",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_4","arguments":"{\"message\":\"do the task\"}","encrypted_function_args":["message"]}}`,
			path:          "item",
			wantMarker:    `["message"]`,
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "ciphertext-like message",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_5","arguments":"{\"message\":\"gAAAAABciphertext\"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "invalid arguments",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_6","arguments":"not-json"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "arguments without message",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_7","arguments":"{\"task_name\":\"x\"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "empty message",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_7a","arguments":"{\"message\":\"\"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "whitespace message",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_7b","arguments":"{\"message\":\"   \"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "arguments object",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_7c","arguments":{"message":"do the task"}}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "long base64url message",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_7d","arguments":"{\"message\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "nil state does not inject",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_7e","arguments":"{\"message\":\"do the task\"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
			stateNil:      true,
		},
		{
			name:          "flat double underscore name",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","name":"collaboration-optimize__spawn_agent","call_id":"call_8","arguments":"{\"message\":\"do the task\"}"}}`,
			path:          "item",
			wantInjected:  true,
			wantMarker:    "[]",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "response output container",
			payload:       `{"type":"response.completed","response":{"output":[{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_9","arguments":"{\"message\":\"do the task\"}"}]}}`,
			path:          "response.output.0",
			wantInjected:  true,
			wantMarker:    "[]",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "non-collaboration tool untouched",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"business","name":"spawn_agent","call_id":"call_10","arguments":"{\"message\":\"do the task\"}"}}`,
			path:          "item",
			wantNamespace: "business",
		},
		{
			name:          "collaboration namespace missing marker",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"call_11","arguments":"{\"message\":\"do the task\"}"}}`,
			path:          "item",
			wantInjected:  true,
			wantMarker:    "[]",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "collaboration namespace ciphertext",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"call_12","arguments":"{\"message\":\"gAAAAABciphertext\"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
		},
		{
			name:          "collaboration namespace nil state",
			payload:       `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"call_13","arguments":"{\"message\":\"do the task\"}"}}`,
			path:          "item",
			wantNamespace: codexCollaborationNamespace,
			stateNil:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &codexMultiAgentV2State{}
			state.markOptimized(1, "gpt-5.5")
			if tc.stateNil {
				state = nil
			}
			got := restoreCodexMultiAgentV2Response([]byte(tc.payload), state)
			marker := gjson.GetBytes(got, tc.path+".encrypted_function_args")
			if tc.wantMarker == "" {
				if marker.Exists() {
					t.Fatalf("encrypted_function_args should be absent, got %s", marker.Raw)
				}
			} else if marker.Raw != tc.wantMarker {
				t.Fatalf("encrypted_function_args = %s, want %s", marker.Raw, tc.wantMarker)
			}
			if ns := gjson.GetBytes(got, tc.path+".namespace").String(); ns != tc.wantNamespace {
				t.Fatalf("namespace = %q, want %q", ns, tc.wantNamespace)
			}
			if tc.wantInjected {
				// arguments 是不透明字段，补标记不能改动正文。
				beforeArgs := gjson.GetBytes([]byte(tc.payload), tc.path+".arguments").Raw
				afterArgs := gjson.GetBytes(got, tc.path+".arguments").Raw
				if afterArgs != beforeArgs {
					t.Fatalf("arguments changed: before=%s after=%s", beforeArgs, afterArgs)
				}
				again := restoreCodexMultiAgentV2Response(got, state)
				if string(again) != string(got) {
					t.Fatalf("injection is not idempotent:\nfirst=%s\nsecond=%s", got, again)
				}
			}
		})
	}
}

func TestRestoreCodexMultiAgentV2ResponseRestoresFlatToolDefinitions(t *testing.T) {
	payload := []byte(`{
		"tools":[
			{"type":"function","name":"collaboration-optimize.spawn_agent"},
			{"type":"custom","name":"collaboration-optimize__send_message"},
			{"type":"function","name":"business_tool"}
		],
		"response":{"tools":[{"type":"function","name":"collaboration-optimize.followup_task"}]}
	}`)

	got := restoreCodexMultiAgentV2Response(payload, nil)
	if name := gjson.GetBytes(got, "tools.0.name").String(); name != "collaboration.spawn_agent" {
		t.Fatalf("tools.0.name = %q", name)
	}
	if name := gjson.GetBytes(got, "tools.1.name").String(); name != "collaboration__send_message" {
		t.Fatalf("tools.1.name = %q", name)
	}
	if name := gjson.GetBytes(got, "tools.2.name").String(); name != "business_tool" {
		t.Fatalf("business tool changed: %q", name)
	}
	if name := gjson.GetBytes(got, "response.tools.0.name").String(); name != "collaboration.followup_task" {
		t.Fatalf("response.tools.0.name = %q", name)
	}
}

func TestRestoreCodexMultiAgentV2ResponseRejectsTrailingGarbage(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"output":[{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent"}]}}garbage`)
	if got := restoreCodexMultiAgentV2Response(payload, nil); string(got) != string(payload) {
		t.Fatalf("trailing garbage payload changed: %s", got)
	}
}

func TestRestoreCodexMultiAgentV2ResponsePreservesNumbers(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"id":"resp_1","created_at":1792300000123,"output":[{"type":"function_call","name":"spawn_agent","namespace":"collaboration-optimize","arguments":"{}","encrypted_function_args":[]}]}}`)
	got := restoreCodexMultiAgentV2Response(payload, nil)
	if !strings.Contains(string(got), `"created_at":1792300000123`) {
		t.Fatalf("created_at was re-encoded: %s", got)
	}
}

func TestRestoreCodexMultiAgentV2ResponseNoop(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		[]byte(`{"type":"response.completed","response":{"output":[]}}`),
		[]byte(`{"type":"response.completed","response":{"output":[{"type":"function_call","namespace":"collaboration"}]}}`),
		[]byte(`not json collaboration-optimize`),
	} {
		if got := restoreCodexMultiAgentV2Response(payload, nil); string(got) != string(payload) {
			t.Fatalf("noop restore changed payload: %s", got)
		}
	}
}
