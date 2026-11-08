# 号池部署与代码交接（2026-11-08）

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

## 5. 待办：fast→Pro，ultrafast→Pro Max

用户最近询问是否能按档位分配套餐；**只分析过，没有实现、没有部署**。

- 当前 fast/ultrafast 选账号没有套餐硬门禁，仅改变传输路径。
- 原始 plan_type 可以区分 pro、prolite、promax。通用 `auth.NormalizePlanType()` 会把后两者折叠为 pro，不能直接用它实施 Pro Max 限定。
- API Key 的 `PlanAllow` 按原始套餐精确过滤，可用两个 Key 人工隔离；同一个入口按 service_tier 自动分流需新增过滤。
- 建议：ultrafast 严格只选具备权限的 Pro Max；fast 的普通 Pro 优先/是否允许 Pro Max 兜底须明确规则。
- 首次选号、重试、排队、粘性复用都要受门禁约束。带 previous_response_id/turn-state 时不能为换套餐直接换号；无法安全续聊需明确错误。
- 不擅自降级、不把请求档位当作上游实际档位，不修改旧服务器 new-api 渠道。

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
