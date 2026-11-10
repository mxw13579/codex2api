# 上游错误诊断与降智检查 HTTP-only

本文所述诊断变更只增强诊断，并将降智检查固定为 HTTP；本身不改变普通业务的选号、档位、重试策略，无需数据库迁移或新增配置开关。独立的可选 Bad Request 重试策略见 [BAD_REQUEST_RETRY.md](BAD_REQUEST_RETRY.md)，不要把诊断修复与重试缓解混为一谈。代码落地不等于已在线上部署。

## 错误信息

`usageLogErrorMessage` 在原有 `error.message`、Responses 终态错误等结构之外，支持：

- `{"detail":"Bad Request"}`；
- `detail.message`、`detail.msg`、`detail.error.message`，及对应 `code/type`；
- FastAPI/Pydantic 的 `detail` 校验数组，只取前三个 `msg`，不序列化 `input/ctx/loc`。

字段先脱敏再截断，最终摘要仍不超过 600 字符。HTML、非 JSON、无可信消息字段的响应保持 `HTTP <状态>` 兜底；不把任意原始响应直接返回客户端。现有 400/5xx 正文日志仍在 `bad_request.log` / `server_error.log`。正文日志和控制台日志也改为先脱敏再截断，避免 JWT/PEM 跨越日志截断点时泄漏敏感前缀；超过 1 MiB 的正文直接省略，不尝试无限量脱敏。

如果上游只给 `Bad Request`，改动只能恢复这句原话，不能恢复未返回的内部校验原因。

### 从状态码查到原始错误证据

主错误处理路径（Responses、Chat Completions、Messages、compact、图片 HTTP 错误，以及 WS 入站回退 HTTP 的错误）在既有正文日志中附加 `Request-ID`、`Upstream-Request-ID`、`CF-Ray`。关联来自**本次 HTTP 响应自己保存的不可变快照**，不在写日志时读取可能已经切换的“当前尝试”。只写入实际取得且通过校验的标识符；未知响应包装、纯 SSE/WS 终态错误或未安装诊断的路径省略关联，不冒用握手 ID 或其他尝试的 ID。标识符用于关联：

1. 在用量记录找到本地 `request_id` / `upstream_request_id`。
2. 在 `upstream_errors.jsonl`（必要时 `.1`）按 ID 找到同一次 HTTP 失败，查看最终参数摘要、实际读体长度、是否完整、错误 code/type。
3. 在 `bad_request.log` 或 `server_error.log` 按同一 ID 找到 `Response:` 下的上游原话。正文仍经过既有脱敏和限长，不是无限制的原始数据存档；没有新增完整对话、工具参数、图片或任意请求头采集。
4. 上游仍只给 `{"detail":"Bad Request"}`，且读体完整时，说明当前响应没有字段级解释。此时可核对实际参数摘要，并把上游 ID、CF-Ray、时间交给上游查询内部日志；不能从 HTTP 400 或一份完整的短响应推断根因。

一次本地请求可能有多个上游尝试，优先用**上游 ID + 本地 ID + 时间/账号**区分，不能只把最后一次尝试套到之前的错误正文。WS 握手 ID 不用作当前轮错误 ID。部分内部静默恢复路径未调用既有正文日志，不能保证每条诊断元数据都有正文记录。

关联信息只附加到原有日志条目，不重复复制错误正文到 JSONL；未改变重试、传输选择或业务请求字节。请求 ID 在发送前冻结，响应所属的上游 ID/CF-Ray 在收头时冻结；后续重试、账号变更或 WS 换轮不能改写旧响应的关联。既有 safe-preflight 包装会透传此快照，未知包装宁缺勿错。历史未写入的关联信息无法补回。

## Request ID

HTTP 用量追踪及现有诊断记录统一按以下顺序提取：

1. `X-Request-Id`
2. `Request-Id`
3. `X-Openai-Request-Id`
4. `X-Oai-Request-Id`
5. `X-Goog-Request-Id`

配置了账号专用请求 ID 头时，仍按原规则独占选择，不回退到其他头。敏感头名校验继续生效。值只允许有界 ASCII 标识符，拒绝超长值、控制字符、明显凭据、URL、JSON 和打码占位符。合法 UUID 在这类**专用 ID 字段**内原样保留，不关闭全局脱敏。

WS 握手 ID 不冒充每轮请求 ID；每轮 metadata 继续隔离。历史已经打码的 ID 无法恢复。本变更不启用或扩大 Cookie 采集。

## 新增 `upstream_errors.jsonl`

路径为 `${LOG_DIR:-logs}/upstream_errors.jsonl`。覆盖经过 `doTracedUpstreamRequest` 的 HTTP 状态 >=400 的响应；不覆盖尚未取得 HTTP 响应的网络失败，也不把 HTTP 200 内的 SSE/WS 错误伪装成 HTTP 4xx。原有流错误记录不变。

记录：

- 时间、账号内部 ID、本地请求 ID（调用路径已安装追踪时）、上游请求 ID、CF-Ray；
- HTTP 状态、白名单 Content-Type、声明长度；
- 实际已读字节、是否观察到 EOF、读取错误类别、是否提前关闭；
- 诊断前缀是否超过 8 KiB，以及**前缀**的 SHA-256（不是完整响应摘要）；
- 最终出站参数摘要：已知模型、档位、推理强度、stream、input 类型与数量、messages/tools 数量、是否含 instructions / previous_response_id；
- 已知错误 code/type；未知正文标签仅哈希或省略，不复用 Request ID 的 UUID 豁免。

不记录请求正文、工具参数、实际 previous_response_id、完整 URL/查询参数、Authorization、Cookie、任意响应头，亦不再次复制原始错误消息。原始错误正文仍查看既有受限日志。

### 边界与资源

- 只观察业务实际进行的响应读取，不额外补读、重试或重放网络请求。原始 `Read` 的字节/错误及 `Close` 返回值不变。
- `body_complete=true` 表示观察到 EOF，不代表上游响应语义成功。没有 EOF 时不能声称正文完整。
- 原有错误体读取上限仍为 1 MiB；为辨认是否超限最多额外读取 1 字节，返回给业务的前缀仍最多 1 MiB，并返回可识别的超限错误。
- 请求摘要最多解析 4 MiB。Codex/Responses 执行器提供压缩前最终明文；其他路径只读取可独立重开的未压缩请求体，不读取正在使用的 `req.Body`。不可用、过大或重定向后无法获取时注明省略，不猜测内容。
  - HTTP/2 连接复用会在 Go 内部复制 Request；单纯对象地址不同不能证明重定向。等价内部副本保留执行器明文摘要；真正重定向通过 `Request.Response` 关联及 URL/方法/Host/长度/编码变化识别，不套用首跳摘要。即使重定向到同一 URL 也不套用。
  - `request.omitted` 存在时，`bytes=0`、`stream=false`、`tools=0` 等只是结构零值，**不是实际请求参数**。旧版 `1.0.17` 的已知遗漏是把 HTTP/2 副本误判为重定向，压缩请求因此记为 `encoded_body`；修复已随 `1.0.18-f07bc679` 上线，见部署记录。历史摘要无法恢复，其他真实省略原因仍可能发生。
- 日志用单 worker、最多 256 条的非阻塞队列。队列满、写入失败会丢弃诊断；后续记录的 `dropped_total` 为该进程累计丢弃数。因此不能把它当作完整审计账本。
- 单进程写入时每卷最多 16 MiB，只保留活动文件和一个 `.1` 备份；文件权限 0600，新目录权限 0750。历史超限活动文件会清空，超限备份会移除。
- 多进程同时写同一路径没有跨进程轮转协调；蓝绿短暂重叠可能缺失诊断。需要严格保留时应分实例配置日志目录，不能依赖此文件证明所有请求行为。
- 退出时最多等待 1 秒排空，不能保证异常退出或磁盘阻塞时保留末尾记录。
- 遵循已有 `LOG_DISABLED`；独立于现有 Cookie 诊断开关，不需要为本功能开启 Cookie 记录。

## 降智检查

`executeIntelligenceCheck` 安装请求级 `WithUpstreamHTTPOnly`：

- Codex：优先于全局强制 WS、调用方 WS 选项；不尝试 WS 握手。
- Responses 中转：优先于账号自己的 WS 设置；不复用已有 WS 连接。
- Claude 检查：沿用原有 HTTP 路径。
- 仍使用 HTTP SSE，不改为一次性非流式响应。
- 保留被检账号、绑定代理、UA/指纹/时区、测试模型和推理强度、超时取消、计费与限额逻辑。
- `EffectiveContext.transport` 据实际结果记录 HTTP。普通业务请求以及未带 HTTP-only 标记的其他检测不受影响。

## 本地验证

核心测试：`TestUpstreamErrorDetails*`、`TestUpstreamDiagnostic*`、`TestUpstreamTrace*`、`TestUpstreamCookie*`、`TestUpstreamHTTPOnly*`、`TestIntelligence*`。关联补丁另覆盖 `TestSameUpstreamDiagnosticRequestBoundaries`、`TestUpstreamErrorLog*`，包含真实本地 HTTP/2 连接复用、302/307 重定向、同 URL 重定向、自定义重定向策略与改写正文。

全部网络 fixture 使用合成凭据和本机 loopback，不是实际上游、线上部署或性能证明。
