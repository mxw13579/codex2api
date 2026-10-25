# Codex WebSocket 路由检测

这份指南用于复现账号是否实际执行在 Sol/Luna 引擎。脚本不会主动修改 NewAPI 或代理配置，但真实请求会使用凭据、可能消耗额度并触发上游限流，应仅对有权使用的账号运行。下方真实探测命令不是离线单元测试；模拟事件测试不能证明真实上游路由。

## 原理与证据

脚本连接 `wss://chatgpt.com/backend-api/codex/responses`，为每个账号发送一次 `response.create`。WebSocket 上 `stream` 是隐式的，不应再发送；`model` 是请求模型，最终 `response.completed.response.model` 是对外模型名。官方 Responses WebSocket 文档：[response.create](https://developers.openai.com/api/reference/resources/responses/websocket-events#response.create)。

判断实际路由时优先看 `engine_ids`：包含 `gpt56sol-codex-` 记为 Sol，包含 `gpt56lun-codex-` 记为 Luna；`response_model` 只能说明对外展示名称，不能单独证明执行引擎。`x-codex-primary-used-percent`、`limit_reached` 等字段仅作为同一请求的辅助证据。

## 准备凭据文件

在获准使用凭据后，将账号文件放在本机受限目录，例如 `accounts_probe.json`。也支持手写最小格式；每条必须包含 `access_token`，可选 `proxy_url`（或 `proxy`）、`account_id`、`id`/`name`/`email`。未指定代理时直连，指定时只使用该代理。标签会出现在 JSONL 中，建议使用不含个人信息或秘密的测试编号：

```json
[
  {"id": 161, "access_token": "<token>", "account_id": "<chatgpt-account-id>", "proxy_url": "socks5h://user:pass@host:port"}
]
```

不要把文件提交到 Git、聊天记录或工单。测试结束后立即删除或加密备份。
如果导出的 `access_token` 是 `configured` 等占位值，而不是实际 OAuth access token，脚本会无法完成握手；请改用包含真实凭据的受保护导出，不要把 refresh token 当作 access token 代替。

## 代理与传输约束

- 路由仅由账号的 `proxy_url` / `proxy` 决定，不继承环境代理。未指定代理时，显式传入 `http_no_proxy=["*"]` 选择直连；指定代理时传入非匹配列表，避免 `NO_PROXY` 或客户端默认的 localhost 规则绕过该代理。脚本不会在并发线程中修改进程环境。
- 已指定但无效或连接失败的代理不会回退直连，也不会尝试其他代理。
- 支持 `http://`（HTTP CONNECT，默认端口 80）、`socks5://` 和 `socks5h://`（默认端口 1080，保留各自 DNS 语义）。`https://` 代理被拒绝，因为当前客户端参数不能保证到代理的 TLS，不能将其悄悄降级成 HTTP。HTTP 代理认证本身并不提供到代理的传输加密，使用前需评估网络信任边界。
- 目标只接受不含 URL 用户名/密码的 `wss://`，显式启用证书和主机名校验，禁用自动重定向。不提供浏览器或关闭 TLS 校验的后备路径。
- 配置拒绝只报告 `status=error`、`error_type=ValueError`、`error=probe_failed`；不会回显出错值。排查时检查上述条件，不要开启携带请求头的调试日志。

## 安装与批量运行（需另行授权）

真实探测依赖 `websocket-client`；只有获准安装依赖和发起请求后才使用以下示例，离线测试不需要安装。建议使用临时虚拟环境。`--accounts -` 可从标准输入读取 JSON，避免把凭据放进 argv 或额外落盘；代理凭据也只放在受保护的输入中：

```bash
python -m venv .venv-ws-probe
. .venv-ws-probe/bin/activate       # PowerShell: .venv-ws-probe\\Scripts\\Activate.ps1
python -m pip install "websocket-client[socks]"
python scripts/codex_ws_probe.py \
  --accounts accounts_probe.json \
  --concurrency 4 \
  --repeat 3 \
  --effort low \
  --output ws-routing-$(date +%Y%m%d-%H%M%S).jsonl
```

PowerShell 示例：

```powershell
python -m pip install "websocket-client[socks]"
$out = "ws-routing-$(Get-Date -Format yyyyMMdd-HHmmss).jsonl"
python .\scripts\codex_ws_probe.py --accounts .\accounts_probe.json --concurrency 4 --repeat 3 --output $out
```

默认请求是 `gpt-5.6-sol`、`reasoning.effort=low`、`Reply with exactly OK.`。脚本允许 `none`、`minimal`、`low`、`medium`、`high`、`xhigh`、`max`；具体模型可能拒绝某些值（当前 `gpt-5.6-sol` 实测拒绝 `minimal`，最低可用档为 `none`）。如需对照可改 `--effort` 或 `--prompt`，但同一批次应保持请求形态一致。`--repeat 3` 表示每个账号独立建立 3 次连接，不是重试同一连接。

## 交给 AI 执行时的固定指令

以下说明仅供已明确授权真实探测的独立任务使用；不能把本节视为读取凭据、安装依赖或联网的默认授权：

```text
只在任务已授权的凭据、主机、依赖和联网范围内执行 scripts/codex_ws_probe.py；范围不明确就先停止确认。确认每个账号预期使用直连还是显式代理；脚本不继承环境代理，显式代理失败不得回退直连。不要把 token、代理认证信息放进 argv、聊天、仓库或日志。默认不指定 --html-dir。每个账号独立建连，concurrency=4、repeat=3、model=gpt-5.6-sol、effort=low，记录仅含元数据的 JSONL。汇总 completed/failed/error 次数、engine_family（Sol/Luna/unknown）、proxy_id、elapsed_ms 和错误类型，并按约定清理临时文件。不要依据 response_model 单独判断实际引擎；把 engine_ids 作为主要证据，并说明样本量和未证实之处。
```

获准真实探测后，应先用一个账号检查连通性，再按授权范围批量运行；缺少依赖时不要擅自安装。记录退出码和结构化错误类型即可，不要保留或上报含请求头、凭据、自由格式异常消息的调试记录。

## 结果解读

每行是一个账号/一次尝试的脱敏 JSON。重点字段：

| 字段 | 含义 |
| --- | --- |
| `requested_model` | 客户端发出的模型 |
| `effort` | 本次请求的 `reasoning.effort` |
| `response_model` | `response.completed` 对外模型名 |
| `engine_ids` | 上游报告的执行引擎实例；用于 Sol/Luna 判别 |
| `engine_family` | 脚本根据 `engine_ids` 归类的 `sol` / `luna` / `unknown` |
| `proxy_id` | 代理 URL 的 SHA-256 前 12 位，仅用于跨请求关联，不暴露代理地址 |
| `started_at` / `elapsed_ms` | UTC 开始时间和从建连到终态的墙钟耗时 |
| `x-codex-primary-used-percent` | 该次事件带回的额度百分比（若上游提供） |
| `status` / `error_type` | completed、failed 或本地错误；错误只保留异常类或失败事件类型，不保留上游自由文本、错误代码或异常消息 |
| `answer_candidates` | 从聚合后的真实 `output_text` 提取的 1～3 位数字字符串（去重、字典序）；不会把跨 chunk 数字拆开，不从 ID、工具参数、reasoning 或整段 JSON 抽数字 |
| `html_file` | 仅显式指定 `--html-dir` 且有完成正文时出现，值为该目录内新建文件的 basename |

JSONL 始终不含 `answer_text`，即使开启正文保存也不会附带正文片段。每个 `(output_index, content_index)` 独立聚合 delta；完整 done 或终态中的同一 `output_text` 替换已有内容，而不是再拼一次。终态文本也可补充未收到文本事件的内容；数字匹配不会跨独立内容段拼接。`answer_candidates` 本身仍是正文衍生数据，只适用于已获准的受控数字题，不应当作完全匿名的输出。

同一账号至少重复 3 次再下结论：一次 Luna 只能证明该次请求的实际引擎，不能证明账号永久“降智”。如果所有账号、所有代理都返回同一 engine family，应优先怀疑上游策略或测试客户端字段差异，而不是单一代理；若仅某个代理失败，才把代理作为主要变量。

## 可选正文保存：明确 opt-in

默认不保存响应正文；`--output` 只保存上述元数据 JSONL，不会隐式启用正文落盘。仅在确实需要且获准保存正文时，显式指定 `--html-dir <受限本地目录>`。非空的完成正文保存为 `probe-<随机 UUID>.html`，用排他创建模式写入，不插入账号 label、不覆盖既有文件或同名符号链接；重复运行使用不同文件名。独立文本段以换行分隔，失败/中途关闭的部分正文不会保存。

**风险：保存的是未经净化的上游原文，不保证为 HTML。** 它可能含敏感数据、脚本、外链或其他主动内容，不是安全的网页预览。不要直接用浏览器打开；优先在离线纯文本查看器检查。脚本不做内容沙箱、加密、自动清理或自动 Git 忽略；应选择仓库外的受限目录，限制权限，并按数据保留策略清理。文件名随机和路径安全不等于正文安全。

## 离线回归测试（无依赖、无网络）

从项目根目录执行：

```bash
python -m unittest discover -s scripts -p test_codex_ws_probe.py
```

`scripts/test_codex_ws_probe.py` 只使用 stdlib，通过 `sys.modules` 注入 fake `websocket`；代理覆盖测试使用合成环境设置，输入事件和凭据全是合成数据，保存测试仅使用系统临时目录。覆盖默认 JSONL 不含正文/不落盘、opt-in 保存、delta/done/终态去重、跨 chunk 数字、终态仅提取 `output_text`、恶意 label/唯一文件/拒绝覆盖、显式直连/代理选择与 TLS 参数、异常消息不回显以及空 close 帧。不读取 `.env`、账号文件或项目 `.tmp`，不需要安装 `websocket-client`。

这些测试只能验证本地逻辑和传给 fake 客户端的参数，不能验证实际 `websocket-client`/SOCKS 版本的传输行为、真实代理、TLS 握手或上游路由。测试通过不代表已经完成真实上游验证。

## 安全与复核清单

- 默认输出不含请求正文、响应全文、正文片段或完整原始事件；短数字候选是有意保留的衍生数据。显式 `--html-dir` 是唯一正文保存开关。
- 不输出 Authorization 或代理认证信息；异常消息和上游自由格式错误也不输出，避免部分/解码后的凭据绕过字符串替换脱敏。标签和路由元数据仍应按敏感业务数据管理。
- 代理密码只用于当前进程的握手，不会写入结果；仍应避免 shell 历史记录保存带密码的命令。
- 先用 1 个账号验证连通性，再批量；并发建议从 2～4 开始，避免测试本身制造 429。
- 将 `response_model`、`engine_ids`、非敏感账号标签、代理标识、时间和错误类型按批次保存，便于按时间窗口复核；不要因此额外收集请求头或正文。
- 测试结束删除凭据文件、虚拟环境中的缓存和结果文件中的任何人工补充敏感信息。
