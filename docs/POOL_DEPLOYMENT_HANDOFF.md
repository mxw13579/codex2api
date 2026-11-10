# 号池部署与代码交接（2026-11-08）

## 2026-11-10 1.0.18 发布补记（最新）

- 用户授权蓝绿上线。应用提交 `f07bc679a196769b481c3c93e2cbe809e5410d88` 已推送 `github/main`；从干净 LF Git 归档构建 `muze/codex2api:1.0.18`，活动槽 **blue**，实际 build **`1.0.18-f07bc679`**。
- 镜像 ID：`sha256:12856f73de523509a4ce98edd739b6de4f1d4b49f005fd9e524259c60e12d335`。回滚版本为 green 的 `1.0.17`，保留其镜像。
- 新增可配置的精确 Bad Request 独立重试，发布后确认 **关闭／额外 1 次／同号**；没有顺带启用策略。普通重试 5 次、429 重试 60 次、间隔 30ms、全局并发 20、强制 WS 开启、瞬时 429 冷却和持续重试关闭均沿用原值。规则见 `BAD_REQUEST_RETRY.md`。
- 同时上线 HTTP/2 Request 内部复制导致参数摘要遗漏的修复，以及既有错误正文日志的响应级 ID 关联。它们改进证据，不代表偶发 400 或图片 complexity 问题已经修复。
- 数据库仅新增 `system_settings.bad_request_retry_policy` 文本列，旧版读写兼容；已确认生产新列及默认值。发版前备份数据库（290,166,949 字节，`pg_restore -l` 验证通过）、活动程序与私有 rollout 状态。
- 发版前等待正在运行的手动检测结束；未取消、延期或重跑任何任务。一条当时属于禁用账号的手动排队记录不满足执行条件，未为发布修改它；自动检测保持关闭。停旧槽前再次确认没有活动图片、质量或降智任务。
- 08:20 UTC 切流时 green 保留 296 条连接，初始退出码 2；连接与队列自然归零后，green 于 **08:28:36 UTC** 停止，最终 drain 退出码 **0**。没有强断旧连接，PG、Redis、HAProxy 容器和启动时间未变化；旧服务器只查询镜像和站点 health。
- 最终 Go 全量、前端类型检查/构建、LF 副本 426 项前端测试、47 项蓝绿离线测试通过。Linux 镜像前后端构建通过。切换期 14 次 health 为 200，切换后 60 次均为新版且 200；两处现有 Nginx 站点也返回新版。
- 有界业务观察仍包含 400/499；切换后的混合实例诊断样本已出现有效参数摘要和新增正文关联，但不能据此断言全部请求无遗漏、400 根因已消失或所有业务零错误。未为验收主动发送真实模型请求。

## 2026-11-10 1.0.17 发布补记（历史）

- 用户授权提交、推送并蓝绿发布。应用提交 `8d708c98aea6b4d6c6bde97181a3e7f05a36ad50` 已推送 `github/main`；从干净 LF Git 归档构建 `muze/codex2api:1.0.17`，活动槽 **green**，实际 build **`1.0.17-8d708c98`**。
- 镜像 ID：`sha256:7b20d58a7de3852ba73d7ad1ae830ed95d52e7f1938de8043eb506a4ad23a633`。回滚版本为 blue 的 `1.0.16`，不要清理其镜像。
- 新增上游 detail 错误解析、Request ID 保留和有界异步 HTTP 失败诊断日志；降智检查请求级强制 HTTP，保留 SSE，普通业务 WS 策略不变。范围与限制见 `UPSTREAM_ERROR_DIAGNOSTICS.md`。
- 发布前备份数据库（276,274,522 字节）、活动程序与私有 rollout 状态，并验证 `pg_restore -l`；相对运行版没有数据库迁移、依赖或发布工具变化。
- 03:32 UTC 切流时 blue 保留 209 条连接，部署退出码 2；03:41 UTC 确认连接/队列为零且无活动后台任务后，执行 drain 成功，blue 于 03:41:41 UTC 停止，最终退出码 0。未强制断连。PG、Redis、HAProxy 容器和启动时间均未变化；旧服务器仅做镜像与站点 health 只读检查。
- Go 全量测试通过；最后日志安全改动后，proxy/admin/wsrelay 全包复跑通过；47 项蓝绿单测通过，Linux 前后端镜像构建通过。切换期 14 次 health 均 200，切换后 60 次均为新版且 200，两处现有 Nginx 站点 health 均为新版。业务样本仍有 400/499/500，不能据此宣称全部请求零错误。
- 线上新诊断日志首批 4 条 HTTP 400 均保留本地 ID、上游 ID、CF-Ray，记录完整读体，权限 0600，丢弃计数 0；用量记录已可见 `Bad Request` 和上游 ID。这是有界样本，不是完整审计或所有摘要字段的证明。
- 自动降智检查保持关闭；原有 4 条自动队列未删除、延期或执行。本次没有主动触发付费模型探测，降智 HTTP-only 的路径验证来自本地测试。
- 同期发现图片工作站的上游 `Complexity 'medium' is not supported for this image model`：运行旧 `1.0.16` 时已出现，新版仍存在；本版本未改图片请求构造。生图桥接固定 `reasoning.effort=medium` 是待验证线索，尚未证明与上游 complexity 一一映射，不能声称已修复。

## 2026-11-09 1.0.16 发布补记（历史）

- 用户明确授权后，从提交 `17968c9989187339e922022183e826f341a88eef` 的干净 Git 归档构建 `muze/codex2api:1.0.16`，通过现有蓝绿工具发布到 **blue**；实际 `/health` 为 **`1.0.16-17968c99`**。
- 镜像 ID：`sha256:a2af2c57269caba38d4cee913ed8175a0b7c02551a1bf16a59fdf4652fa8bbfc`。本地回滚版本为 green 的 `1.0.15`。
- 10:05 UTC 切换新流量，保留 green 既有连接；10:10 UTC 确认连接与排队均为零且没有活动异步任务后，执行 `release.sh drain --drain-timeout 0` 成功停止 green。未强断 SSE/WS；最终排空退出码为 0。
- 新增限制：只有 `ultrafast` 要求明确 ProMax，Team/Pro/ProLite/Plus/Free/未知套餐不合格；fast/priority/默认档沿用原调度，不排除 ProMax。无须新环境开关或数据库迁移。详见 `ULTRAFAST_PROMAX_ROUTING_PLAN.md`。
- 发布前完成数据库备份、归档可读性检查、活动程序和私有 rollout 状态备份；无 schema 差异。PostgreSQL、Redis、HAProxy 未重启；旧服务器只查询镜像标签，没有修改或升级。
- 发布前运行蓝绿离线测试通过；此前全量 Go 测试、最终 proxy/wsrelay 全包及针对性回归通过。构建包含前端与 Linux 后端构建。切换期 14 次健康采样均 200，切换后 60 次均 200 且为新版；两个站点经现有 Nginx 的 health 均为新版。Responses/Chat/Messages 的冲突档位探针均返回本地 400，没有发起模型请求。
- 最近 3 分钟的一次用量聚合采样：41 条 ultrafast 请求均对应当前识别为 ProMax 的账号，HTTP 200；上游实际档位字段均回报 `default`。这是有界样本，不是完整请求追踪，也不证明上游兑现了 ultrafast。业务采样仍有 499 等状态，不能据健康采样宣称所有业务零错误。
- 本次未延期、删除或重跑智能检测。上次 1.0.15 延后记录复查为一条 `normal`、一条 `interrupted`，均已不在运行/排队；不能声称上次两条均正常完成。此前保留的旧 blue 已在此次替换前自然排空并通过 release 工具停止。

## 2026-11-09 1.0.15 发布补记（历史）

- 新池 blue 曾通过管理后台在线更新变成上游 `v3.1.4`，但镜像标签仍是 `1.0.12`。线上版本必须同时核实 `/health` 与镜像信息。
- 已从提交 `47b1ba9e030736cc70084885ad4bff33eddff3d0` 构建 `1.0.15` 并通过现有蓝绿工具切到 green；`/health` 返回 `1.0.15-47b1ba9e`。blue 的已有长连接仍保留自然排空，不得为了结束发布强制删除。
- 真实 IP 配置、信任边界与日志字段见 `deploy/nginx/README.md`。入口只对新池相关站点启用 Cloudflare 来源验证，应用记录管理操作来源、结果及在线更新前后版本。共享管理密钥不等于个人身份。
- 发版前完成数据库与程序备份。尚未运行的两项手动智能检测为交接临时延后，私有运维记录保存原调度时间；排空后恢复，延后最多 30 分钟。自动清理会在延后到期前停止，避免未重新检查后台任务就停旧实例。
- 两个此前未上线的等待窗口实现包含在此版本中，但环境开关仍为默认 0。未实现 fast/ultrafast 按套餐选账号。
- 验证：全量 Go 测试、最终管理模块回归、前端类型检查、LF 源码快照的 420 项前端测试、47 项蓝绿离线测试通过。切换时 37 次和切换后 60 次入口健康采样全部 200；这不是所有业务请求零错误的证明。
- PostgreSQL、Redis、HAProxy 没有重启；旧号池和 new-api 没有升级。后续操作以实时 release 状态为准。


本文随代码提交，供换电脑和后续开发交接使用。仅包含实现与发布状态，不包含服务器地址、SSH 私钥路径、账号、API Key、业务日志或客户数据。机器登录方式保存在不提交的本地 `CLAUDE.md`，需另行安全迁移。

## 1. 先读：生产操作边界

- 旧服务器承载 new-api 等生产服务，默认只允许低开销只读分析。不得自行修改参数、加载镜像、重启容器或执行数据库写操作。
- 新号池也已接入生产流量。代码开发、构建完成不等于授权部署；生产发布需用户明确批准具体动作。
- 应用发布使用现有 `deploy/rolling/release.py` 蓝绿切换；不得通过重建当前活动容器代替排空。
- 不要执行整个 Compose 项目的 `down`，它会停止数据库；不要直接删除仍有连接的槽。
- 不要启用带 `flush ruleset` 的主机防火墙配置覆盖 Docker 的规则。
- 本次交接提交不包含任何生产发布或配置变更。

## 2. 版本和源码对应关系

此前基础提交：`9ccbde48c2fcceb3ae35e8d976abfc37bf96d8f0`。

此前镜像用本地 Git 暂存区生成 tree，再以 `git archive <tree>` 构建。因此未提交的代码已经有一部分在生产运行；不能只看 HEAD 推断线上代码。以下为本次会话最后核实的发布记录，不是实时查询结果：

| 镜像版本 | 内容 | 最后确认状态 | 构建源码树 |
|---|---|---|---|
| 1.0.7 | 基础 main | 旧号池最后确认使用；只读维护 | 基础提交 `9ccbde48` |
| 1.0.8 | pprof、HAProxy 显式连接参数 | 已被后续版本包含 | `d9b8a56ea6ee0e8791bbfb2604ef2d32003b2df4` |
| 1.0.9 | usage_logs 保留机制 | 已被后续版本包含 | `6b28ebed365e01c9c7a66013e142cc1266e98c0d` |
| 1.0.10 | usage_logs.id/序列 BIGINT | 新池的已知回滚版本 | `c536ff85358cdc1082596ef98468a601959ad58f` |
| 1.0.11 | GPT-6.1 ultrafast 6 倍计费 | 单独构建，合入 1.0.12 发布 | `9adc64aa6f064466147b265982f71220f980931c` |
| 1.0.12 | priority/fast/ultrafast 直接走 HTTP 上游 | **新池最后确认运行版本，blue 活动槽** | `9cec2d7b0eb60663b9544307db7bd74bc95a8c3e` |
| 1.0.13 | 借号前短暂等待 | **只构建，未部署** | `fd2551d8bff018a45abbec27488774470201c063` |
| 1.0.14 | 前置元数据透传缓冲窗口 | **只构建，未部署** | `919de44d847c794ff32aecb12fc6ebdadba06e7f` |

本交接提交包含上述全部代码和测试，不能直接视为生产 1.0.12 的纯源码快照。镜像仓库此前无法匿名拉取，日常在构建主机生成本地镜像，不要假设 `--pull` 可用。

下一次操作前先读取 `release.sh status`、运行容器 image ID、镜像 labels 和 `/health` 的 `build_version`，再决定要操作哪个槽。历史记录不能替代实时核实。

## 3. 部署布局和运行配置

项目根目录为 `/data/docker/codex-pool/codex2api/`：

```text
.env                  私有环境变量文件
docker-compose.yaml   PostgreSQL、Redis，以及仅供 prepare 的 seed profile
release.sh            包装 release.py，传入本目录 rollout 作为状态目录
deploy/rolling/       发布脚本和 README
rollout/              状态、生成的 Compose、HAProxy 配置和控制 socket
data/postgres/        PostgreSQL 数据
data/redis/           Redis 数据
data/images/          图片文件
logs/                 应用日志
backup/               数据库备份与配置备份
```

- 常态运行一个应用槽；发布期间新旧两槽短暂共存，共用 PostgreSQL 和 Redis。
- HAProxy 绑定内网地址，应用没有可绕过 HAProxy 的主机端口。
- `rollout/` 必须是 700；其中有凭据。此前全目录被改成 777，已修复，不能再次递归设 777。
- 应用资源配置记录：`GOMEMLIMIT=72GiB`、容器内存上限 88GiB、请求体逻辑内存预算 16GiB、单请求体上限 200MiB。
- PostgreSQL：18，`shared_buffers=8GB`、`effective_cache_size=48GB`、`max_connections=300`。
- Redis：7，2026-11-06 热更新到 `maxmemory=24gb`、`save ""`、AOF 关闭；Compose 已同步。2026-11-08 删除旧 `dump.rdb`。Redis 重启后的缓存冷启动需要纳入运维计划。
- Redis 存在会话绑定和协调租约，不能笼统理解为“丢失完全无影响”。`volatile-lru` 也可能淘汰带 TTL 的协调数据；未完成分类隔离。
- 每日 PG 备份仅确认在本机保留 7 天，未完成异机恢复演练。
- 后台参数会被用户调整。Redis PoolSize 只在应用启动时读取；后台修改 PG 连接池上限则即时生效。
- **尚未完成 1500–2000 活跃业务并发的代表性压测。** 连接上限、一次 CPU 采样和 `/health` 探测都不能证明容量达标。蓝绿期间两份应用内存上限之和也可能超过整机内存。

## 4. 本次保存的代码

### 4.1 pprof 和发布入口

- `pprof_server.go`：`CODEX_PPROF_ADDR` 默认空，启用时限制回环地址；使用独立 HTTP mux，不挂到业务路由。
- 生产配置过 `127.0.0.1:6060`，访问范围是容器网络命名空间的回环。profile 可能包含敏感运行信息，不应上传公开仓库。
- `deploy/rolling/release.py`：显式设置 global/frontend `maxconn=20000`、backend `fullconn=20000`。
- `fullconn` 不是后端硬性最大连接数，不能把此前统计中的 2000 当作性能瓶颈。

### 4.2 日志清理与 BIGINT

- `database/usage_log_retention.go`、`usage_log_retention_job.go`：每分钟检查，按时间或估算逻辑体积裁剪最旧明细。
- 开关：`CODEX_USAGE_LOG_RETENTION_HOURS=48`、`CODEX_USAGE_LOG_MAX_MB=1024`，默认均关闭。
- 只删除已由总汇总和小时汇总消费的行；发现 API Key 配有窗口额度时延长保留时间，以免额度少计。
- **1GiB 是估算逻辑体积，不是 PostgreSQL 文件的硬上限**；DELETE 不会立即缩小磁盘文件。窗口额度保护还可能让保留超过 48 小时和 1GiB。
- Go 中日志 ID 已为 int64。新表用 BIGSERIAL；已有表和序列在后台检查后升级 BIGINT。
- 自动升级限制不超过 10 万行，`lock_timeout=3s`；大表跳过并提示维护窗口处理。`lock_timeout` 只限制等锁时间，不限制拿锁后重写时长，不能宣称这类 DDL 完全无阻塞。

需复核的清理边界：时间与 ID 不完全有序时的裁剪、保留最新水位避免清空触发汇总重建、在线修改窗口额度时的竞态、advisory lock 释放失败处理及长期磁盘膨胀。已有测试不等于这些边界均已验证。

### 4.3 GPT 价格与加速档上游传输

- `database/billing.go`、`database/gpt61_ultrafast_test.go`：gpt-6.1-sol/terra 的 ultrafast 为标准价 6 倍；terra 有独立价格项和规范名。
- 模型 custom 价格曾通过管理接口按 new-api 同步，生产数据中的覆盖价不包含在本仓库；新环境不能只靠默认价格表复原。
- `proxy/fast_tier_transport.go`：priority/fast/ultrafast 对 Codex 走 HTTP，普通档保留原策略。共接入 Responses、Chat、Messages 及 WS 入站 Responses 四个路径；中转账号自己的上游 WS 保持原行为。
- 此实现没有此前讨论的 `CODEX_FAST_TIER_HTTP` 环境开关；不要配置一个不存在的开关。
- HTTP 入站 Responses/Chat 已有 handler 测试；Messages 与 WS 入站仍缺专门端到端覆盖。
- 请求档位、上游实际档位、计费档位须分开看。上游返回 default、hint 的存在以及不同请求的速度对比，都不足以单独证明快档是否兑现。

### 4.4 借号前等待（未上线）

涉及 `auth/antigravity_balance.go`、`auth/store.go`、`proxy/affinity_overflow_wait.go` 及测试。

- `CODEX_AFFINITY_OVERFLOW_WAIT_MS` 默认 0，上限 10000。
- `CODEX_AFFINITY_OVERFLOW_WAIT_MIN_BYTES` 默认 32768，小请求不等待。依据是 Content-Length，不是 token 数。
- 借到备用账号后先归还其槽位，等绑定账号，超时后再执行原借号逻辑。

**上线前必须复核：**当前等待使用 `context.Background()`，不直接继承客户端取消；等待路径禁用借号但不固定一个不可变的原账号 ID；可能多次等待；设置时长先做整数乘法再截断，超大输入存在溢出边界。新代码复用了历史上存在冷却自唤醒问题的等待路径。此次交接不修复或掩盖这些限制，不建议未经补充验收就打开生产开关。

### 4.5 前置元数据透传缓冲窗口（未上线）

涉及 `proxy/preflight_defer_window.go`、`proxy/handler.go` 及测试。

- `CODEX_PREFLIGHT_PASSTHROUGH_DEFER_MS` 默认 0，上限 5000；只在元数据透传已开启时生效。
- 主 Codex Responses 流：窗口内缓冲前置帧；尚未提交时，可重试失败进入现有预算，确定性失败返回真实 HTTP 状态；正文到达立即发送；定时器到期恢复旧式透传。
- **不是“元数据已经透传后仍可安全重试”完整方案。** 超时写出缓冲（可能含生命周期 ID）后仍受原有已提交保护。
- 不覆盖中转独立流路径和 WS 客户端入站路径；已写工具身份、正文等不可随意重放。
- handler 测试覆盖 HTTP 模拟上游，尚需真实 WS 上游的测试、取消/边界大小/缓冲溢出和写失败覆盖。定时器与现有 writer 的生命周期需进一步审查。

## 5. 已上线：ultrafast 仅 ProMax，fast/默认不变

`1.0.16` 已实现并通过蓝绿部署完成，见最新发布补记与 `ULTRAFAST_PROMAX_ROUTING_PLAN.md`。

- 原始套餐只认可 `promax`、`pro_max`、`pro-max` 的大小写/两端空白归一形式，不使用会折叠成 Pro 的通用归一函数。
- 同一 Key 内叠加套餐资格过滤，保持原有分组、模型、额度和状态条件；不要求拆 Key，不建立互斥账号池。
- 重试、粘性、preferred、pinned 与发送前检查覆盖 ultrafast 限制；Key 范围内无 ProMax 时明确拒绝，不降档。
- 无法安全恢复的续链明确失败，Payload 不得静默降档或绕过套餐门禁。
- fast/priority/默认档不新增套餐条件，ProMax 仍可参与。
- 上游实际档位需要独立观测，套餐选号合格不代表上游已经兑现 ultrafast。

## 6. 调度和缓存调查的边界

- indexed 历史会话记录有索引恢复、等待公平性、重复冷却自唤醒等未解决问题。当前未通过复现回归证明修复；不要直接切 indexed 或 shadow 作为优化。
- 保留 strict 是用户讨论后的当前方向；bounded 可能迁移仍有缓存的会话，不能保证提高命中率。
- 删除的账号是失效账号，用户已明确说明；“延迟删号”不是已证实的改进措施。
- new-api 渠道的 prompt_cache_key→X-Session-ID 同步已由用户配置，早期缺配置的描述已过时。
- 上游 prompt token 缓存、Redis 响应上下文缓存是不同指标。Redis 全局 keyspace_hits/misses 不能用作某一类响应缓存的命中率。
- WS 403/1006、借号日志条数不等于独立失败请求数；必须按请求和 attempt 对齐，且不能只凭代理入口地址推断真实出口 IP 的信誉。

## 7. 验证记录与交接操作

历史已执行：
- 发布脚本离线测试；main、auth、proxy、database、admin 全量 Go 测试（当时通过）。
- 临时 PostgreSQL 上的 retention/BIGINT 集成测试；大表跳过测试曾有后台迁移竞态，已调整测试等待。
- Linux 容器里部分等待、传输和缓冲窗口测试的 race 检查。
- 生产蓝绿切换的健康探测；这只验证入口可用，不代表所有业务/SSE/WS 场景无损。

本次交接会在提交前重新运行相关本地检查，最终结果见交接回复。PG 集成测试需专用可写数据库（`CODEX2API_TEST_POSTGRES_DSN`）；不可指向生产数据库。

换电脑建议：从本交接分支克隆代码，再通过安全渠道复制本地 `CLAUDE.md`、SSH 配置/密钥、必要的工具配置与运维脚本。不要上传 `.env`、rollout 状态、数据库备份、会话转录、pprof 原始文件。现有 `stash@{0}` 为历史上游合并备份，未自动应用或并入本次提交，需要时单独审阅。
