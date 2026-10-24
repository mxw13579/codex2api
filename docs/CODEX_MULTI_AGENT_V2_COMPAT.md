# Codex Multi-Agent v2 第三方子 Agent 兼容

最后更新：2026-10-18

本文说明 Codex2API 的可选 Codex Multi-Agent v2 明文兼容层。它解决的是：GPT 父 Agent 在 v2 下把任务交给第三方子模型（例如 DeepSeek）时，子 Agent 能创建、能回消息，却反复表示“没有收到具体任务”。

## 结论与边界

这是默认关闭的实验性兼容层，需要在部署环境显式开启。

开启后，匹配的 v2 协作调用会改用明文参数。网关无法区分子 Agent 是原生 OpenAI 还是第三方模型：只要父请求匹配，父到子的任务消息都会以明文生成和传输。需要保留原生 OpenAI 子 Agent 加密时，不要开启，或只在隔离实例和账号池开启。

兼容层不解密已经生成的历史密文，也不修改 Codex 二进制。它改变的是后续请求的 schema 和参数生成方式。

它不改认证、账号选择、模型路由、计费、限流或 User-Agent，也不影响 v1、普通 Responses、Chat Completions、图片、reasoning、压缩、其他工具、relay 账号和非 Codex 客户端。

User-Agent 判断是兼容性启发式，不是安全边界。持有 API Key 的调用方可以伪造官方 UA；改写只作用于该调用方自己的请求，但部署方不应把“仅官方客户端”当作强访问控制。

## 风险与适用账号

- 上游看到的是非标准 `collaboration-optimize` schema。上游可能按 reserved/unknown schema 拒绝，也可能记录或触发账号风控。兼容层没有自动回退：上游拒绝时请求按上游错误返回。
- 子任务正文会以明文进入第三方模型及其日志、缓存或转发路径。不要把敏感任务交给不受信的第三方。
- 开关是进程级环境变量，不能按账号或模型排除。多实例处理同一流量时，所有实例必须保持一致。
- 先在非主力账号或隔离实例灰度，验证 spawn、followup、send_message 和真实工具调用，观察上游 4xx 和账号状态，再扩大范围。
- 如果账号风控不可接受，稳妥替代是等上游提供官方明文配置，或让子 Agent 走 v1。网关侧改 schema 一定会改变上游看到的请求。

## 为什么需要这一层

Codex Multi-Agent v2 的 `spawn_agent`、`send_message`、`followup_task` 把 `message` 参数标记为加密。官方 OpenAI 上游在收到该标记时生成密文，Codex 客户端把它存成 `agent_message` 的 `encrypted_content`，只留下 `Payload:` 标头是明文。第三方子模型读不到正文，于是回复“没有任务”。

Codex 客户端只有在同时满足以下条件时才走 `DirectPlaintextMessage`：

1. `namespace` 精确等于 `collaboration`；
2. 工具名是 `spawn_agent`、`send_message` 或 `followup_task`；
3. `encrypted_function_args` 是显式空数组 `[]`。缺失或 `null` 不等价。

加密发生在父模型生成参数时，网关事后无法解密。兼容点必须在父请求的工具 schema，也就是父模型生成参数之前。

Codex 上游对 `collaboration.*` 保留工具要求 schema 逐字匹配；直接删改 `message.encrypted` 会被上游按 reserved schema 拒绝。兼容层因此使用临时命名空间 `collaboration-optimize`，并在响应返回客户端前还原。

源码依据：

- `codex-rs/core/src/tools/router.rs`：`ToolCall::direct_source()` 的判定条件。
- `codex-rs/core/src/tools/handlers/multi_agents_v2.rs`：非明文来源变成 `AgentMessage::Encrypted`。
- `codex-rs/core/src/tools/handlers/multi_agents_spec.rs`：三个协作工具的 `message` 带 `.with_encrypted()`。

## 与 CLIProxyAPI 同类实现的关系

CLIProxyAPI 已经实现同类能力（MIT），本方案参考了它的核心思路：

- 源码：[optimize_multi_agent_v2.go](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go)
  - [`OptimizeCodexMultiAgentV2Request`](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go#L138-L155)：删除加密标记并临时改名。
  - [`PrepareCodexMultiAgentV2Tools`](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go#L112-L136)：在 Responses 边界准备协作工具。
  - [`optimizeCodexCollaborationNamespace`](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go#L695-L716)：把 `collaboration` 改为 `collaboration-optimize`。
  - [`RestoreCodexMultiAgentV2Response`](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go#L718-L739)：在响应中还原命名空间。
  - [`rewriteCodexAgentMessageContent`](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go#L834-L874)：为子侧转换 `agent_message`。
- 配置：[`client.codex.optimize-multi-agent-v2`](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/config.example.yaml#L327)，默认 `false`，OAuth 和 API-key 凭据都适用。
- 许可：[MIT License](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/LICENSE)。

本实现沿用核心变换，差异是：只在原生 Codex executor 生效；冲突、无效 JSON、嵌套或数量超限时 fail-closed；不把 `encrypted_content` 重标为 `input_text`；只处理 collaboration 工具族并按 namespace 限定。

相关 issue：

- [CLIProxyAPI #4746](https://github.com/router-for-me/CLIProxyAPI/issues/4746)：`followup_task` 仍保持加密。
- [CLIProxyAPI #4801](https://github.com/router-for-me/CLIProxyAPI/issues/4801)：第三方 Responses provider 不支持 `agent_message`。
- [CLIProxyAPI #6233](https://github.com/router-for-me/CLIProxyAPI/issues/6233)：compat 模式的 `author` / `recipient` 处理。

## 启用与灰度

```bash
CODEX_MULTI_AGENT_V2_COMPAT=true
```

默认 `false`，接受 `1`、`true`、`yes`、`on`。变量按进程读取，修改后需要重启。

灰度步骤：

1. 选一个隔离实例或独立账号池，不要让负载均衡把同一客户端随机分到开启和未开启的实例。
2. 开启后让一个真实 Codex 会话执行 `spawn_agent`、`followup_task`、`send_message` 和一次只读工具调用。
3. 成功判据是子 Agent 返回任务结果；`spawn_agent` 返回 id 只说明子线程建好了。
4. 检查子请求的 `agent_message` 只包含 `input_text`，没有 `encrypted_content`。
5. 观察上游 4xx、账号状态和兼容层日志。出现异常时关闭开关并重启所有相关实例。

## 精确生效条件

兼容层同时检查：

- `CODEX_MULTI_AGENT_V2_COMPAT` 为真；
- 下游 `User-Agent` 是官方 Codex 客户端（`Codex Desktop/`、`codex-tui/`、`codex_cli_rs`、`codex_exec/`）；
- 实际选中原生 Codex 账号，relay 账号不会进入该执行器；
- 请求包含顶层 `collaboration` namespace 下的 `spawn_agent`、`send_message` 或 `followup_task`，或顶层的扁平 `collaboration.<tool>` / `collaboration__<tool>`；
- 这些工具中至少一个带 `parameters.properties.message.encrypted`；
- 请求没有已存在的 `collaboration-optimize` 命名空间冲突；
- 工具嵌套不超过 8 层，待改写工具不超过 16 个；超出时保持原请求并记录跳过日志。

任一条件不满足时不改写。

已验证的客户端版本：Codex CLI `0.162.0-alpha.17.2`。Codex Desktop 和其他版本没有单独验证；升级后需要重跑验收。

## 协议处理

请求侧：

- 扫描顶层 `tools[]` 和 `input[].additional_tools[].tools[]`；
- 只识别顶层 `collaboration` namespace 下的三个 function，以及顶层扁平 `collaboration.<tool>` / `collaboration__<tool>`；其他 namespace 下的同名工具不改写；
- 删除这三个工具的 `parameters.properties.message.encrypted`；
- 把 `collaboration` namespace 改名为 `collaboration-optimize`；扁平工具改名为 `collaboration-optimize.<tool>` 或 `collaboration-optimize__<tool>`；
- 同步改写指向这些工具的 `tool_choice`，包括 `function`、`custom` 和 `allowed_tools` 形态；
- 其他工具和无关字段保持原样。

响应侧：

- 在 `item`、`output`、`response.output`、`tools`、`response.tools`、`tool_choice`、`response.tool_choice` 中还原临时命名空间；
- 工具调用还原为 `namespace: collaboration` + `name: <tool>`；namespace 声明、回显的工具定义和 `tool_choice` 恢复原前缀；
- 上游省略 `encrypted_function_args` 或返回 `null` 时，如果 `arguments` 是合法 JSON、`message` 是字符串且不像已知密文，补全 `encrypted_function_args: []`；
- 兼容 `collaboration-optimize.<tool>` 和 `collaboration-optimize__<tool>`；
- 响应回显的扁平工具定义恢复为 `collaboration.<tool>`；
- `arguments`、`input`、`output` 等不透明字段不改写；
- 流式 SSE、非流式 JSON、HTTP 和原生 Responses WebSocket 共用同一还原逻辑；
- 数字保持原精度。

上游错误体不在还原范围。如果上游 4xx 文本包含 `collaboration-optimize`，客户端可能看到这个临时名；它是内部兼容细节，不包含任务正文。

## 安全失败策略

- 不解析任务正文，不解密，也不把 `encrypted_content` 重标为 `input_text`。
- 补全 `encrypted_function_args: []` 前检查 `arguments` 是否为字符串、`message` 是否非空；缺字段、非法 JSON、空文本、非字符串和已知密文形态都保持原样。密文识别是启发式（`gAAAA`、JWE `eyJ`、`enc_`、长 base64url token），不是完整保证。
- 请求已有 `collaboration-optimize` 冲突、JSON 无效、嵌套或数量超限时，保持原请求并记录跳过日志，不做半转换。
- 响应还原只在本次请求真的执行过改名时生效；未改名的请求不触发任何响应改写。
- 日志记录账号、模型、工具名、是否生效、是否疑似密文和跳过原因，不记录任务正文、认证信息或原始 SSE。
- 上游拒绝改写后的 schema 时不会自动回退，表现为 4xx。看到生效日志和 4xx 同时出现时，先关闭开关并保留脱敏请求样本。

## 与典型接入方式的关系

默认路径只处理父请求的 schema 和响应 namespace 还原，不处理 `agent_message`。子请求通常由上游网关直接转发给第三方模型。

子侧是否可用取决于该网关：本仓自带的 Responses→Chat 转换目前不识别 `agent_message`，会返回 `cannot be represented by Chat Completions`；子网关需要原生支持全明文 `agent_message`，或做等价转换。早期验证中的 NewAPI 会忽略该项；后续子侧规范化修复已合入 [NewAPI fork PR #259](https://github.com/DeliciousBuding/new-api/pull/259)，配合本兼容层完成了端到端验收。其他子网关仍需检查自己的处理能力。

子侧转换只能处理全明文 `input_text`，不能把 `encrypted_content` 当作任务文本。本 PR 只负责父侧，不包含未接线的子侧适配器。

## 验证

截至 2026-10-18：单元测试、集成测试和真实 Codex CLI + mock 上游通过；父侧也已通过真实生产链路验证。

开启兼容层后，父模型的 `spawn_agent` 调用被补全为 `encrypted_function_args: []`，子请求的 `agent_message` 只包含明文 `input_text`，正文与父模型发出的任务一致。

子侧是独立依赖：第三方子 Agent 请求通常不经过本仓。早期验证确认，未转换 `agent_message` 时模型读不到任务；后续接入全明文规范化后，GPT v2 父 → DeepSeek 子的初始派发、同一子 Agent 的 `followup_task` 及两次只读工具调用通过，原生 GPT 子 Agent 回归通过。父调用的 `encrypted_function_args=[]`，子消息只包含 `input_text`。这次验收没有单独覆盖 `send_message`，也不覆盖第三方父模型强制 v2。

单元与集成测试覆盖：

- 顶层和 `additional_tools`、嵌套 `collaboration` namespace、扁平 `collaboration.*` / `collaboration__*` 工具形态；
- 三个协作工具全部覆盖，其他 namespace 下的同名工具不受影响；
- 无 `spawn_agent`、只有 `send_message` / `followup_task` 的请求；
- 开关关闭、非 Codex 客户端、命名空间冲突、无效 JSON、嵌套或数量超限均保持原样；
- `tool_choice` 的 `function`、`custom`、`allowed_tools` 形态同步改写；
- SSE、非流式 JSON、HTTP 和 WebSocket 的还原；
- 扁平工具定义、双下划线、点号形式还原；
- 上游省略或返回 `null` 的 `encrypted_function_args`：明文字符串补全为 `[]`，密文、非法参数和缺字段保持原样；
- `arguments` 不被改写、数字精度保持、幂等；

真实 Codex CLI + mock 上游：

```bash
python scripts/repro-codex-multi-agent-v2.py --codex-exe /path/to/codex --child-model <model> --mode native
python scripts/repro-codex-multi-agent-v2.py --codex-exe /path/to/codex --child-model <model> --mode compat
```

`--child-model` 由调用者按本机 Codex catalog 选择；文档不固定具体模型名。

- native：子请求 `agent_message` = `input_text` 标头 + `encrypted_content`，复现失败形态；
- compat：子请求 `agent_message` 只包含 `input_text`。

脚本验证客户端判定规则；Go 集成测试验证网关还原；真实上游结果见下。脚本自身不读取或输出凭据，模型请求只发往本地 mock，Codex CLI 的 `CODEX_HOME` 指向本次运行的临时目录。

初次真实上游验证（2026-10-18，子侧修复前）：

- 父请求已切换到明文协作工具，上游 `spawn_agent` 返回的调用被补全 `encrypted_function_args: []`；
- 子请求 `agent_message` 只包含 `input_text`，正文与父模型发出的任务一致；
- 子侧网关（NewAPI）未把 `agent_message` 转成普通 user message，模型仍读不到任务；同一条请求改成 `message` + `role=user` 后返回正确结果；
- 因此当时父侧边界已通过验证，子侧转换是独立阻塞项。

后续端到端验收（2026-10-18，子侧修复后）：

- GPT v2 父 → DeepSeek 子的 `spawn_agent` 与同一子 Agent 的 `followup_task` 均返回正确任务结果；
- 子 Agent 完成两次只读工具调用，原生 GPT 子 Agent 回归通过；
- 父两次协作调用的 `encrypted_function_args=[]`，子两条 `agent_message` 均仅含 `input_text`；
- 上述结果依赖父侧兼容与子侧规范化共同生效，不表示任意子网关都已兼容。

额外验收清单：

- `send_message` 的独立真实调用及其他客户端/子网关组合；
- 升级上游或客户端后，复验任务正文、后续派发、工具调用与上游错误。

## 已知限制

- 依赖 Codex 上游遵守修改后的 schema 并生成明文 `arguments`。上游省略 `encrypted_function_args` 时网关会补全；若 `message` 仍像密文，则不补并保留诊断。
- 密文识别是启发式，不是完整保证；当前原生 Codex 上游使用 `gAAAA` 前缀，未知格式可能被误判或漏判。命中时选择不补，保持原生加密行为。
- 无法区分原生 OpenAI 和第三方子 Agent；开启后所有匹配的 v2 协作调用都走明文。
- 上游拒绝时不会自动回退，表现为 4xx。
- 临时 namespace 是内部兼容细节；如果上游 reserved schema 或客户端判定规则变化，需要重新评估。
- 只覆盖官方 Codex 客户端的 v2 协作工具；v1、普通工具和其他协议不受影响。
- 上游错误体不还原；客户端可能看到临时命名空间。
- 请求历史中的 `function_call` 记录保持原 namespace；真实账号已接受该历史，上游策略变化时需要复验。
- User-Agent 判断不是安全边界。
- 子侧网关需支持全明文 `agent_message` 或做等价转换。已修复的 NewAPI fork 组合验收通过，其他组合需自行验证。
- 第三方父模型强制 v2 不在本兼容层覆盖范围；已验收的选择是第三方父模型使用 v1、GPT 父模型使用 v2。

## 回滚

把 `CODEX_MULTI_AGENT_V2_COMPAT` 设为 `false` 或删除变量，重启所有处理相关流量的实例。没有数据库迁移或持久状态。重启会断开在途的流式请求和 WebSocket 连接，客户端需要重连。

## 参考

Codex 客户端 `0.162.0-alpha.17.2`：

- `codex-rs/core/src/tools/router.rs`
- `codex-rs/core/src/tools/handlers/multi_agents_v2.rs`
- `codex-rs/core/src/tools/handlers/multi_agents_spec.rs`

OpenAI Codex：

- [PR #26210](https://github.com/openai/codex/pull/26210)：Encrypt multi-agent v2 message payloads
- [PR #35845](https://github.com/openai/codex/pull/35845)：Support plaintext collaboration tool messages
- [Issue #36376](https://github.com/openai/codex/issues/36376)：OpenAI parent still sends encrypted V2 task to non-OpenAI child
- [Issue #36586](https://github.com/openai/codex/issues/36586)：Subagent task payload invisible to custom non-OpenAI providers
- [Issue #46939](https://github.com/openai/codex/issues/46939)：Add a config option to select plaintext delivery

codex2api：

- [Issue #342](https://github.com/james-6-23/codex2api/issues/342)：reserved `collaboration.spawn_agent` schema 报错
- [Issue #741](https://github.com/james-6-23/codex2api/issues/741) / [PR #742](https://github.com/james-6-23/codex2api/pull/742)：Codex 客户端工具和历史兼容

CLIProxyAPI（MIT）：

- [optimize_multi_agent_v2.go](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/internal/client/codex/optimize-multi-agent-v2/optimize_multi_agent_v2.go)
- [配置项 `client.codex.optimize-multi-agent-v2`](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/config.example.yaml#L327)，默认 `false`，OAuth 和 API-key 凭据都适用
- [MIT License](https://github.com/router-for-me/CLIProxyAPI/blob/911f49c/LICENSE)
- [Issue #4746](https://github.com/router-for-me/CLIProxyAPI/issues/4746)：`followup_task` 仍保持加密
- [Issue #4801](https://github.com/router-for-me/CLIProxyAPI/issues/4801)：第三方 Responses provider 不支持 `agent_message`
- [Issue #6233](https://github.com/router-for-me/CLIProxyAPI/issues/6233)：compat 模式的 `author` / `recipient` 处理
