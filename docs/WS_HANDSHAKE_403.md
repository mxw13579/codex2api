# 上游 WS 握手 403 分析与处理记录（2026-11-15）

## 结论

- 上游 WS 握手 403 均来自 Cloudflare（响应带 `Cf-Ray`，IAD 节点）。同账号、同出口降级到 HTTP 后可以通过，因此不是出口 IP 被整体封禁，而是针对 WS 升级请求的拦截。
- 拒绝比例随**单账号（单出口 IP）每分钟握手尝试次数**单调上升，最符合按出口握手频率限流的表现；单看同时在线连接数无法解释（例如账号 616 在连接数 167–186 时出现过零 403 的分钟）。
- 握手过多的根因是无会话请求的槽位复用失效：
  - 约 96% 的握手来自“无显式会话 ID”的请求（经 new-api 的普通 Chat/Responses 请求）。它们按设计走 `AcquireReusableConnection` 的共享槽位池（`proxy/executor.go` 决定走这条路径，`proxy/wsrelay/executor.go` 调用）。
  - 2026-10-09 的提交 `7c210fa5` 在槽位复用里加入了“跳过带有效续链绑定的连接”。每个完成的响应都会把连接绑定 30 分钟，于是每个槽位用一次就被占住，之后每个请求都走“槽位全忙”的兜底，新建一条一次性连接。本地复现：100 个连续无会话请求，无绑定时握手 1 次，有绑定时握手 100 次。
  - 这些一次性连接收尾后也带绑定，按聊天空闲上限保留，再被 LRU 回收：热门账号每分钟约 100 次 `idle_chat_lru` 关闭，被关连接仅闲置约 95 秒。这是上述问题的结果，不是独立原因。
- 槽位连接被不同请求共用是原设计允许的：stateless + 默认隔离模式下，握手头不带任何请求级/会话级身份（`resolveHandshakeSessionID` 返回空串），请求身份由帧体里每请求唯一的 `prompt_cache_key` 承担。
- HTTP 入站的 `previous_response_id` 由本地响应缓存展开成完整输入（`prepareResponsesBodyForOwnerDetailed`），一般不依赖上游连接上的绑定。依赖绑定的 WS 入站客户端分两类：
  - 带会话的客户端用专属连接，不是槽位连接，不受第三遍影响。
  - 不带会话头、不带 `prompt_cache_key`、靠 `previous_response_id` 续链的 WS 入站客户端也走无会话槽位池。开启第三遍后，其绑定可能被同一 base key 下无关的流量接管；下一轮得到 `ResponsesContinuationLostError`，降级为完整上下文重放（无法重放时返回 409）。在 `7c210fa5` 之前，这类请求会带着已失效的 ID 发到上游；现在会在本地识别出来。
  - 线上观察：截至 2026-11-15 06:40 +08 的 12 小时内，号池保留的日志中 WS 入站请求为 0（全部是 HTTP POST），`Responses WebSocket continuation degraded` 日志为 0 条，这一类客户端目前不存在。上线后仍需观察该日志和 409 次数。
- 带显式会话的连接按会话隔离：池 key 为 `账号|URL|会话|代理`（`proxy/wsrelay/manager.go` `poolKey`），握手头冻结 `session-id`/`thread-id`。“空闲保留 N”对这类连接是给 N 个不同会话各留一条，不能被其他会话复用；正在生成的连接不计入该保留额度。

以上均为线上日志统计得出的相关性，Cloudflare 的具体规则（窗口、阈值、维度）不可见。

## 关键数据（2026-11-15 04:00–06:00 +08，blue 1.0.18）

按“账号 × 分钟”聚合，握手尝试 = 成功握手（以关闭事件近似）+ 403：

| 每分钟握手尝试 | 分钟数 | 出现 403 的分钟 | 握手被拒比例 |
|---|---:|---:|---:|
| < 20 | 269 | 1% | 0.4% |
| 40–59 | 18 | 28% | 7.7% |
| 80–99 | 48 | 77% | 13.5% |
| 100–129 | 92 | 98% | 17.2% |
| ≥ 130 | 70 | 100% | 28.4% |

- 2 小时内 8,897 次 `handshake_403` 降级，集中在当时请求量最大的新账号（613–617）。降级前握手耗时中位数 1.9 秒、p90 13.7 秒、最大 52 秒。
- 走 WS 的流式请求首字中位数约 460ms，403 后降级 HTTP 的约 1.45–1.9 秒。
- 线上 `first_token_excludes_ws_acquire=true`，号池记录的首字不含取连接等待，低于客户实际感受。

## 已否决的方向

- **按账号限制连接总数，到上限时 LRU 回收后再新建**：会增加握手次数，在频率限流下只会让 403 更多。
- **调小空闲保留数**：同理，会让回来的会话更频繁地重新握手。
- **把流式请求全部转为非流式以隐藏 598**：598 仅占约 0.06%，代价远大于收益，另行记录。

## 本次改动（分支 `ws-retention-512-stats`）

1. **空闲聊天连接保留上限从 128 提高到 512**（`database.MaxCodexWSDownstreamKeepaliveSlots`）。默认值仍为 8，后台可在 0–512 调整，数据库列无需迁移。单连接内存约 140 KB，单账号 512 条约 70 MB。
2. **无会话请求恢复槽位复用**（`AcquireReusableConnection` 第三遍）：第一遍复用空闲且无绑定的槽位，第二遍在空槽位新建；都不成功时，按最近使用时间从旧到新接管空闲但带续链绑定的槽位，放弃其旧绑定后复用。仍不成功（全部槽位都在处理请求、探活失败等）才新建一次性连接。
   - 正在处理请求的槽位不会被接管。接管与续链取连在同一把池 key 锁内完成复验和占位，同一条连接不会同时被两边取走。
   - 被放弃的旧响应 ID 再来续链时得到 `ResponsesContinuationLostError`，走现有的完整上下文恢复路径，不会带着旧 ID 发到已被复用的连接上。新请求完成后照常产生新绑定。
   - 开关：环境变量 `CODEX_WS_SLOT_REUSE_BOUND`，默认开启；设为 `0`/`false`/`no`/`off` 时关闭第三遍，行为与改动前完全一致。
3. **续链绑定表上限从 4096 提高到 16384**（`responseConnBindingMaxEntries`）。表满时先清理过期项，仍满则放弃新绑定，计入 `binding_rejected`。
4. **握手统计 `[WS-Stats]`**：每分钟为有活动的账号输出一行，再输出一行全局统计；进程停止（`Manager.Stop`）时输出最后一个窗口，包含关闭全部连接产生的关闭。保活日志补充本轮耗时 `elapsed_ms`。统计只观测，不改变连接行为。

### `[WS-Stats]` 字段

账号行（窗口内没有任何握手、复用、兜底或关闭的账号不输出）：

| 字段 | 含义 |
|---|---|
| `window_s` | 本窗口实际经过的秒数（距上次输出），不是名义周期 |
| `handshake_ok` | 握手成功 |
| `handshake_403` | 上游返回 403 |
| `handshake_other_fail` | 上游返回其他状态码（即使此时调用方已取消）、握手超时、传输错误、调用方请求到期 |
| `handshake_canceled` | 上游没有返回响应，且调用方请求被取消 |
| `handshake_ok_ms_avg` / `handshake_ok_ms_max` | 成功握手的平均/最大耗时（含代理建连、TLS 与 Upgrade） |
| `handshake_403_ms_avg` / `handshake_403_ms_max` | 403 握手的平均/最大耗时 |
| `dial_kind` | 按发起路径统计的握手次数：`slot` 槽位新建、`oneshot` 槽位全忙后的一次性连接、`session` 带会话连接、`overflow` busy 溢出槽位 |
| `reuse_kind` | 按路径统计的连接复用次数：`slot` 空闲无绑定槽位、`slot_bound` 接管带绑定槽位（第三遍）、`session` 带会话连接、`continuation` 续链回到原连接、`overflow` 溢出槽位 |
| `slot_fallback` | 无会话请求落到一次性连接兜底的次数 |
| `slot_fallback_bound` / `slot_fallback_busy` | 每次兜底时槽位状态的累加：空闲但带绑定的槽位数 / 正在处理请求的槽位数 |
| `closes` | 按原因统计的连接关闭次数 |
| `connections` / `busy` | 输出时该账号的连接数 / 有在途请求的连接数。`busy` 只统计仍在池内的连接，已摘池但仍在收尾的连接不计入 |
| `chat_idle_limit` | 当前聊天空闲保留上限 |

四类握手结果互不重叠，`handshake_ok + handshake_403 + handshake_other_fail + handshake_canceled` 等于握手尝试总数，也等于 `dial_kind` 各项之和。

全局行：`[WS-Stats] global window_s=N bindings=N binding_limit=16384 binding_rejected=N connections=N`，分别为当前续链绑定数、绑定表上限、本窗口因表满被放弃的新绑定数、全部账号连接总数。有任何账号活动或 `binding_rejected>0` 时输出。

统计口径的已知偏差：

- 执行器发送失败后对槽位 key 的重拨，在 `dial_kind`/`reuse_kind` 中计为 `session`。
- 拨号器配置错误（尚未发起握手）不计入任何握手类别，也不计入 `dial_kind`。
- 握手成功但随后未通过建连后的早期读取检查的，仍计为 `handshake_ok`。

开启第三遍后，`reuse_kind` 的 `slot_bound` 应增加，`dial_kind` 的 `oneshot` 应明显下降。`slot_fallback` 不会归零：同一 base key 下同时在途的请求超过无状态槽位数时，超出部分仍新建一次性连接。线上当前 `codex_ws_stateless_slots=32`（代码默认 8，上限 32）。热门账号重点看 `slot_fallback_busy`，它表示兜底发生时正在处理请求的槽位数。

## 上线与观察

1. 发版后保持现有设置（128），先确认 `[WS-Stats]` 输出与预期一致。
2. 后台把 `codex_ws_downstream_keepalive_slots` 调到 256，观察 1 小时，再调到 512。
3. 每一步对比：
   - `handshake_403` 次数（基线约 4,450 次/小时）；
   - 热门账号 `idle_chat_lru` 关闭次数（基线约 100 次/分钟）；
   - `handshake_ok` 次数与 `handshake_ok_ms_avg`；
   - `dial_kind` 中 `oneshot` 的占比与 `slot_fallback`；
   - new-api 记录的 Chat 首字中位数（基线 7.2 秒）和 `client_gone` 次数；
   - 上游异常关闭（`peer_close`、1006/1008）是否随连接数增加。OpenAI 对单账号同时在线 WS 是否有上限目前未知（约 200 条未见问题）。
4. 出现异常时可在后台调低保留数，无需回滚版本。注意：调低保留数会在 30 秒内（下一次后台巡检）按 LRU 批量关闭超出的空闲连接，这些会话随后都要重新握手，应分步调低（例如 512 → 256 → 128），每步观察 `[WS-Stats]` 后再继续。
5. 槽位接管（第三遍）出现问题时，设置 `CODEX_WS_SLOT_REUSE_BOUND=0` 并重启即可恢复改动前行为。

## 暂缓：403 后按账号暂停新握手

记录在案，待 512 保留上线并有统计数据后再决定是否实施。

- 某账号握手 403 后，30 秒内该账号的**新**连接直接走 HTTP，不再握手；连续被拒时暂停时间翻倍，最长 5 分钟。到期后先放一次握手试探，成功即恢复。
- 已建立的连接照常复用，不主动关闭任何连接。
- 预期收益：省掉每次 403 前约 2 秒的等待；在频率限流下不再继续消耗 Cloudflare 计数，封禁可能更快结束。
- 接入点：`proxy/handler.go` `shouldFallbackWebsocketHandshakeForbiddenToHTTP` 及其在 Responses、Chat、Messages、WS 入站（`proxy/responses_ws.go`）的四处调用；暂停状态仅保存在进程内存。
- 蓝绿发布期间两个实例各自计数，暂停状态不共享。

## 暂缓：取连时“检查后销毁”的旧竞态

2026-10-09 的提交 `7c210fa5` 起就存在，与本次改动无关，本次未修改。

- 位置：`proxy/wsrelay/manager.go` `acquirePreferredConnection`，以及 `AcquireReusableConnection` 第一遍里的同类写法：先判断 `canReuseConnection(wc)` 为假，再读一次在途请求数，为 0 就销毁连接。
- 触发：第一次判断时连接还有 1 个在途请求；持有者随即收尾（`WsResponse.Close` 里的 `RemovePendingRequest`，不持 pool-key 锁）；第二次读到 0，于是一条健康的连接被当成不可用连接销毁。
- 后果：响应此时已经结束，不会截断输出；代价是多一次握手，关闭原因会被记成 `unusable_connection` 或 `lifetime_rotation`。按线上请求量估算每小时至多几次。
- 建议修法：在途数为 0 时再确认一次 `!canReuseConnection(wc)` 才销毁，第一遍同理。上线后可从 `[WS-Stats]` 的 `closes` 里观察 `unusable_connection` 的次数，再决定是否处理。
