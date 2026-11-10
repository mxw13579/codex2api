# 上游错误诊断与降智检查 HTTP-only

本变更只增强诊断，并将降智检查固定为 HTTP；不改变普通业务的选号、档位、重试策略。无需数据库迁移或新增配置开关。代码落地不等于已在线上部署。

## 错误信息

`usageLogErrorMessage` 在原有 `error.message`、Responses 终态错误等结构之外，支持：

- `{"detail":"Bad Request"}`；
- `detail.message`、`detail.msg`、`detail.error.message`，及对应 `code/type`；
- FastAPI/Pydantic 的 `detail` 校验数组，只取前三个 `msg`，不序列化 `input/ctx/loc`。

字段先脱敏再截断，最终摘要仍不超过 600 字符。HTML、非 JSON、无可信消息字段的响应保持 `HTTP <状态>` 兜底；不把任意原始响应直接返回客户端。现有 400/5xx 正文日志仍在 `bad_request.log` / `server_error.log`。正文日志和控制台日志也改为先脱敏再截断，避免 JWT/PEM 跨越日志截断点时泄漏敏感前缀；超过 1 MiB 的正文直接省略，不尝试无限量脱敏。

如果上游只给 `Bad Request`，改动只能恢复这句原话，不能恢复未返回的内部校验原因。

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

核心测试：`TestUpstreamErrorDetails*`、`TestUpstreamDiagnostic*`、`TestUpstreamTrace*`、`TestUpstreamCookie*`、`TestUpstreamHTTPOnly*`、`TestIntelligence*`。

全部网络 fixture 使用合成凭据和本机 loopback，不是实际上游、线上部署或性能证明。
