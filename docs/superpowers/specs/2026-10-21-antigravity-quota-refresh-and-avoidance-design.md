# Antigravity 账户配额自动刷新与耗尽回避设计规格

> 状态：审查修订版（第三轮，2026-10-21）。6 项关键取舍已由用户确认（§11.2），11 项按现写法默认采纳；第三轮逐条复核确认的 6 项事实与规格自洽性问题已直接修复（记录见 §14），并新增 2 项默认采纳的约定（§11.2 #16/#17），未改变任何已确认取舍；第三轮复核后的口径冲突与传播闭环补丁见 §15；第四轮全文档源码复核补丁见 §16。全文待用户最终确认后可提交。未经允许不提交推送或实施。  
> 适用分支：`feat/antigravity-quota-refresh-and-scheduling`（基于基点 `1bdfc85c`）。  
> 范围限定：针对 Antigravity OAuth 渠道（APIKey 保持既有无配额调度逻辑）；不影响 Codex、Claude 与 Grok 渠道。  
> 核心目标：在综合协调既有模型目录刷新与 401 恢复等刷新策略的前提下，建立独立的 Store 级配额定时刷新机制（默认 1h，设置页可调 $[3, 60]\text{min}$，周期内连续均匀分槽 $I/N$）；在全调度路径对确认周配额为 0 或（存在 5h 限制且 5h 配额为 0）的账户实施可靠整账户硬回避；增强现有 Fresh 加权选号（以配额压力替换旧 usage，周与 5h 1:1 平均为建议，亲和忽略软评分）；建立覆盖计划重排与配额权重的双重防抖机制（重排 100ms/max1s 与 soft 1s/max5s 为建议参数），确保硬状态即刻生效；通过 DB 无关短事务 CAS、受控 carryforward、`quota_epoch` 失效纪元与 Outbox 达成跨实例最终一致。  
> 阅读约定：文件链接以工作区根目录为基准；新增类型与事务伪代码仅表达规格契约，不是可直接编译的实现；设计决定的确认状态统一见 §11.2。

---

## 1. 背景与现状分析

### 1.1 现有实现与事实澄清
- **现有刷新策略代码事实（以下为源码核查结果，区别于后文设计建议）**：
  1. *通用 Token 巡检完全排除 AG*：在 [auth/codex_refresh.go:345-378](<auth/codex_refresh.go#L345-L378>) 中，`shouldBackgroundRefresh` 明确通过 `acc.isAntigravityAPILocked()` 排除 AG 账号（L360）。[auth/store.go:488](<auth/store.go#L488>) 默认巡检间隔为 2 分钟，`parallelRefreshAll`（L12350-12376）的 10 并发工作池完全不包含 AG 账号，`RefreshSingle`（L12216）亦显式拒绝 AG 账号。当前系统**不存在**默认常规定时 AG token 轮换，代码中无全局 `RefreshEnabled` bool 开关；系统配置中的 `background_refresh_interval_minutes` 仅用于通用凭据巡检，绝不控制 AG 配额 summary。
  2. *模型目录刷新 (Catalog) 现状*：在 [auth/antigravity_catalog.go:14,18-124](<auth/antigravity_catalog.go#L14-L124>) 中，启动 10s 触发首批全池批刷新（[auth/store.go:6698,6735](<auth/store.go#L6698-L6735>)），后续每 30 分钟一次；仅调用 `fetchQuota`（L37）获取可用模型清单，既不轮换 token 也不读取 summary；自建 4-worker 协程池执行批 CAS 更新，超时预算为 30s/号与 5min/批；该逻辑不检查 lazy 模式；写回时旧的 `Groups` 与 `AICredits` 被原样拼入新快照并附带生成新的 `UpdatedAt`（L49-61），会直接造成配额时效洗新和覆盖。此外，quota-only 变更未触发现有 outbox 白名单触发器（[database/scheduler_outbox.go:191-221](<database/scheduler_outbox.go#L191-L221>) 的账号触发器列清单与 WHEN 条件既不含 `antigravity_quota` 也不含 `credential_generation`），现有 consumer 对内存中已有 Account 对象未投影 quota（[auth/scheduler_outbox_consumer.go:464-608](<auth/scheduler_outbox_consumer.go#L464-L608>)），10 分钟周期的调度状态重建（[auth/store.go:6704](<auth/store.go#L6704>)）同样不修复已有 AG quota 投影，不能声称已存在自动同步兜底。`antigravity_quota` 不在加密敏感字段集合内（[database/credential_crypto.go:34-42](<database/credential_crypto.go#L34-L42>)），触发器可直接以 `json_extract` 比较明文。
  3. *完整 Sync 与 401 恢复现状*：在 [auth/antigravity_client.go:559-629](<auth/antigravity_client.go#L559-L629>) 中，`Sync` 按需换 token（L566-570），校验 verified identity/project，再顺序获取 models（L583）、summary（L606）与 credits（L608）。快照 metadata 目前仅有 `QuotaGroupsObserved` 等基础字段，需扩展为逐子请求观测并记录最终 bearer 与 scope 凭据。401/lazy 恢复链路（[auth/store.go:7319-7348,12388-12395,12691-12841](<auth/store.go#L12691-L12841>)）使用 family lease + DB-first CAS 递增 generation，其内部 `criticalCtx` 脱离 caller cancel（lease 最长持有 4 分钟），并非严格受限 30s 总时长。成功换票仅清除 `unauthorized` 冷却，绝不清除模型 429 冷却。
  4. *管理端手动同步现状*：在 [admin/antigravity_accounts.go:1966-2117](<admin/antigravity_accounts.go#L1966-L2117>) 中，手工刷新直接调用 `client.Sync`（L2012）并执行 CAS（L2073,2075），目前缺少跨实例共同 family lease，不能声称现有全都安全单飞；`/quota`（L1226）仅是手工 Refresh 别名，`/refresh` 为全量 `client.Sync`，`/sync` 为控制面状态刷新（100s 超时，[admin/antigravity_state.go:244](<admin/antigravity_state.go#L244>)），`/state` GET 只读本地持久化状态（10s，[admin/antigravity_state.go:215](<admin/antigravity_state.go#L215>)）；AG 批量刷新上限为 100 且顺序执行，通用 batch 为 4 并发；新账号导入/OAuth 建立亦执行 fullSync；通用 WHAM 与 admin usage 明确拒绝 AG。
  5. *代际递增与 Carryforward 先例*：在 [database/grok_state.go:818-895](<database/grok_state.go#L818-L895>) 中，`UpdateAccountCredentialsCAS` 每次递增 generation 并保持 family，且 Grok 拥有同事务 carryforward 既有事实（L872-889）；`ReplaceAccountCredentialsCAS`（L897+）不继承。必须注意 family 相同不代表身份未变，主体改变或身份重验失败须隔离（[auth/store.go:12481-12529](<auth/store.go#L12481-L12529>)），不能松绑。
  6. *系统设置与 Outbox 同步现状*：在 [admin/antigravity_settings.go:50-98](<admin/antigravity_settings.go#L50-L98>) 中，PUT 从本地内存 merge 后全量 Save 覆盖。当前 settings outbox consumer（[auth/scheduler_outbox_consumer.go:317-336](<auth/scheduler_outbox_consumer.go#L317-L336>)）仅重载 `scheduler-engine`，未加载 `antigravity_config`；[main.go:243-248](<main.go#L243-L248>) 仅在启动时加载一次。因此跨副本新设置必须明确增补落地；配置读改写必须通过 DB 事务锁或 CAS 防止并发覆盖（如实例 A 更新 interval 被实例 B 旧缓存保存 redirect 覆盖）。
  7. *实验性调度器现状*：[auth/refresh_scheduler_integration.go](<auth/refresh_scheduler_integration.go>) 虽含 `REFRESH_SCHEDULER_ENABLED` 与 `SMART_REFRESH_ENABLED` 等环境变量与 API，但全仓检索仅有定义、测试与内部调用，`main.go` 未初始化调用，并非已上线的默认机制。若未来显式启用，AG 依然走统筹入口，本次不顺带开启，不重造跨 provider 调度框架。
- **上游配额契约现状**：在 [auth/antigravity_client.go:1076-1112](<auth/antigravity_client.go#L1076-L1112>) 中，`fetchQuotaSummary` 尝试调用 `retrieveUserQuotaSummary` 解析 `groups[].buckets[].window`。本次查阅仅见模拟桩与测试 fixture，未找到可核对的真实 Google 上游脱敏成功 payload 或权威协议证据；不据此断言所有环境都不存在这些证据。真实脱敏数据验证是后续上线的前置条件，需另行授权，不作为本轮设计的前置阻碍。按 2026-10-21 决议（D2=B）**不设影子期**，因此该验证是门禁默认开启前的**唯一**上线门槛，验收内容见 §10.1 #27。
- **缺失字段归零风险**：在 [auth/antigravity_client.go:1099-1102](<auth/antigravity_client.go#L1099-L1102>) 中，若 `RemainingFraction == nil` 会被直接赋予 `0.0` 并持久化为裸 `float64`。该问题在当前系统仅参与加权软排序，但在引入硬门禁后，会将字段缺失或协议微调误判为额度归零。旧快照因历史已将 nil 写入 0.0，其显式 0 亦不可信，绝不能作为硬门禁生效依据。
- **401 恢复语义边界**：在 [auth/antigravity_types.go:170-176](<auth/antigravity_types.go#L170-L176>) 与 [auth/antigravity_types.go:193-207](<auth/antigravity_types.go#L193-L207>) 中，`antigravityUnauthorizedRecoveryLocked` 仅是针对处于 `unauthorized` 冷却状态账号的窄恢复例外（narrow recovery），并非无条件重置 `available = true`，不可误读为全场景绕过。
- **会话粘性迁移现状**：当前源码 [auth/store.go:7678-7743](<auth/store.go#L7678-L7743>) 中，`strict` 会话在绑定账号容量满或不可用时，若未显式标记 `preserveBinding`，确实会 fallthrough 到 `nextAccountForFreshAffinityWithDispatch` 借用其他健康账号，不可表述为“原有机制绝不迁移”。
- **加权选号作用域与源码事实**：在 [auth/antigravity_balance.go:42-55](<auth/antigravity_balance.go#L42-L55>) 中，现有加权选号生效条件极其严格，仅在 `opts.Provider` 为 Antigravity 渠道、调度模式为 `round_robin`、且进程开关 `ANTIGRAVITY_BALANCED_AFFINITY` 开启时生效。通用入口 `auto` 且 provider 为空时走原路径；native Gemini/countTokens 的 auto Key 入口已强制 AG Provider，满足其他条件时仍加权。其他 scheduler mode 与普通 affinity-off/空 key 的既有旁路不在本次扩展范围，preserveBinding 分支以实际是否进入 Fresh 为准。这是软权重兼容边界；本次新硬门禁则独立覆盖全路径。
- **Fresh 评分与同额双罚事实**：在 [auth/antigravity_balance.go:147-150](<auth/antigravity_balance.go#L147-L150>) 中，现有打分公式为 `score = load + 0.50*usage + 0.35*penalty`。其中 `usage` 来自 `acc.usagePercentForScheduling()`（[auth/scheduling_usage_key.go:80-97](<auth/scheduling_usage_key.go#L80-L97>)），其实现已取了 `groups` 桶内最小剩余比例（折算最大已用）或 `models` 平均值。因此，引入周与 5h 配额加权时，**绝对不能在保留旧 usage 的基础上再叠加配额扣分，否则会造成同额双罚**，必须用统一的 quotaPressure 项替换原 usage 项。同时必须保留 `scheduler_priority` 与 `tierPriority` 分层、FNV tie-break 打散、`handled=true` 避免回退重扫以及对每个候选账号至多求值一次 filter 的安全边界。
- **旧聚合函数兼容性保护**：[auth/scheduling_usage_key.go](<auth/scheduling_usage_key.go>) 中的旧通用聚合函数服务于 `remaining_quota` 与 `fill_first` 等其他调度模式，不宜直接全局更改其语义；本设计为 Antigravity 加权选号引入独立的 Fresh quota term。

### 1.2 对既有规格的显式修订
- 对标前序规格 [docs/superpowers/specs/2026-10-17-antigravity-affinity-balance-design.md](<docs/superpowers/specs/2026-10-17-antigravity-affinity-balance-design.md>)（仅针对模型 429 进行加权避让与 Fresh 选号避险，维持绑定不变）：
  1. 明确将“周配额为 0 或（存在 5h 限制且 5h 配额为 0）”升级为硬性调度回避，对确认耗尽的账号实施整账号调度阻断；不限制必要的额度查询与认证恢复控制面请求，不追杀取得资格前尚未观测到耗尽的在途任务（边界见 §6.2）。
  2. 统筹现有 30m 目录与 1h 配额查询：建立统一 Store 级执行协调入口，共享已持久化已注册账户 4 并发许可，分别按各自周期（$C/N$ 与 $I/N$）平滑派发，单实例单账号真网络单飞（§5.1, §5.2）。
  3. 修正代际绝对失效：支持已验证同一主体身份例行换票下的受控 carryforward，复用 `Sync` 最终 bearer 真实观测，杜绝无条件重复查询（§4.1, §4.2）。
  4. 增强现有加权选号，以周与 5h 剩余额度软权重（建议 1:1 平均）替换原 usage 项，亲和账号完全忽略软评分（§7.1-§7.3）。
  5. 明确计划重排（建议 100ms/max1s）与配额权重（建议 1s/max5s）双重防抖，硬门禁即刻生效（§7.5）。
  6. 修正 strict 硬零语义：**不引入"硬零禁止借号"特例**。绑定账号被配额门禁拒绝与容量满、鉴权失败同等处理，`preserveBinding=false` 沿用既有借号与重绑语义，`preserveBinding=true` 的续链仍不借号；`strict` 不再作为例外（§6.3，决议 D4=C）。

---

## 2. 核心目标与非目标

### 2.1 核心目标
1. **统筹现有多刷新策略的独立定时连续分散调度**：保留模型目录 30 分钟独立周期，新增配额 summary 默认 60 分钟独立周期（可在管理端设置页修改，范围 $[3, 60]$ 分钟）；两流共用 Store 级协调入口与已注册账户 4 并发许可，分别以 $C/N$ 与 $I/N$ 平滑派发，消除突发风暴与请求堆叠。
2. **严密的数据契约与状态判定**：引入 `schema_version = 2` 与按来源隔离的 metadata，严格区分 `known_positive`、`known_zero`、`unknown` 和 `absent` 四态，杜绝空值归零与浮点精度误杀，防止旧快照误封。
3. **单账号真网络单飞与观测复用**：单实例每账号建立 `controlPlaneFlight`，网络前先 claim 账户 flight 再取共享许可；允许已验证相同身份的例行 token 轮换同事务 carryforward，允许复用 `Sync` 最终 bearer 取得的有效观测，杜绝无谓重复网络。
4. **全局调度硬门禁与线性化**：在全调度路径、各策略模式及获取入口，与并发占用获取在 `Account.mu` 锁下严格线性化，彻底回避确认耗尽的账号。
5. **现有加权选号的额度权重增强与亲和隔离**：在受限生效边界内，以周剩余与存在的 5h 剩余比例计算综合压力 `quotaPressure` 替换原 `usage` 项；健康绑定账号完全豁免此软权重，亲和优先不因评分迁号。
6. **覆盖计划重排与配额权重的双重防抖**：建立重排防抖与配额权重防抖机制，合并高频振荡与微变，同时确保硬门禁、代际失效与清零即刻生效。
7. **DB 无关单调短事务与端到端跨实例最终一致**：基于**逐 source** 的 `attempt_seq`、generation 与 `quota_epoch` 实施短事务原子预约与提交协议（预约紧贴各子请求；epoch 递增可作废清除前发出的在途观测），防止乱序写覆盖与清除后被迟到响应写回；补齐 Outbox 触发器（含 `quota_epoch`）与内存单调投影，补齐 settings outbox 真实重载。

### 2.2 非目标
- **不按模型推测配额组映射**：当前上游 summary 缺乏权威的 Model-to-Group 映射契约，不基于 `displayName` 做脆弱字符串推测，实行整账户回避（明确接受跨组借道成本）。
- **不新增调度器模式（Scheduler Mode）与通用权重调节 UI**：用户明确选择增强现有加权选号，遵循 YAGNI 原则，不引入新的可选调度模式，不在系统设置或前端页面提供所有权重的调节 UI，防抖窗口与评分权重采用建议系统常量。
- **不篡改通用用量聚合语义与 auto 渠道边界**：不直接修改 `scheduling_usage_key.go` 的通用用量逻辑以防波及其他渠道；auto 通用入口（provider 为空）保持原选号路径，不擅自越界扩大。
- **不扩展渠道范围**：不触及 Codex、Claude 与 Grok 渠道；APIKey 保持既有无配额调度逻辑。
- **不自造全局分布式任务账本或破坏 OAuth 安全恢复**：不引入全局 distributed slot 账本或 maintenance_jobs 表，复用既有 family lease 与安全恢复链路。
- **不承诺强一致实时计量与零 429**：配额同步为周期性快照，两次快照间的瞬时耗尽仍由上游 429 及既有冷却机制熔断。

---

## 3. 配额状态数据契约、校验与每桶时效

### 3.1 持久化数据契约与向后兼容
持久化必须保留现有 canonical 字段 `quota_groups`，兼容旧 `groups` 仅作为反序列化只读兼容与旧界面展示，严禁随意更名替换破坏前端交互。**采用单一表示（本轮修订）**：不新增并行 V2 类型，就地扩展 `AntigravityQuotaSnapshot` / `AntigravityQuotaBucket` / `AntigravityModelQuota`（[auth/antigravity_types.go:300-330](<auth/antigravity_types.go#L300-L330>)），继续写入同一个 `antigravity_quota` JSON。下列结构是**现有类型的 V2 形态字段说明**，不是可直接编译的新类型：

```go
// 现有 AntigravityQuotaSnapshot 的 V2 形态（同一 antigravity_quota JSON）
type AntigravityQuotaSnapshot struct {
    // 既有字段保持不变：catalog_model_ids、internal_model_ids、models、
    // model_forwarding_rules、forbidden、ai_credits、updated_at。
    SchemaVersion  int                     `json:"schema_version"`          // 必须为 2；缺省视为 legacy
    Groups         []AntigravityQuotaGroup `json:"quota_groups,omitempty"`  // canonical 键名
    SourceMetadata map[string]SourceMeta   `json:"source_metadata"`         // key: "models", "groups", "credits"
    UpdatedAt      time.Time               `json:"updated_at"`              // 仅兼容展示，不作新鲜度凭据
}

type SourceMeta struct {
    Producer           string    `json:"producer"` // "summary_loop", "catalog_loop", "manual", "sync_recovery"
    Trigger            string    `json:"trigger"`  // "scheduled", "admin", "http_401", "reset_due"
    CredentialFamilyID string    `json:"credential_family_id"`
    QuotaIdentityScope string    `json:"quota_identity_scope"` // verified subject+project+auth_kind 摘要，不含 secret
    Generation         int64     `json:"generation"`           // 匹配当前 credential_generation
    CurrentAttemptSeq  int64     `json:"current_attempt_seq"`
    AppliedSeq         int64     `json:"applied_seq"`
    ObservedAt         time.Time `json:"observed_at"`
    LastAttemptAt      time.Time `json:"last_attempt_at"`
    LastSuccessAt      time.Time `json:"last_success_at"`
    FailureCount       int       `json:"failure_count"`
    NotBefore          time.Time `json:"not_before"`
    PresenceVerified   bool      `json:"presence_verified"` // 标记字段完整存在且校验通过
    ContractVersion    string    `json:"contract_version,omitempty"`
    SummaryIntegrity   bool      `json:"summary_integrity"` // 标记本次 summary 响应完整权威
}

// 现有 AntigravityQuotaBucket 的 V2 形态（models[] 的 RemainingFraction 同样补 presence）
type AntigravityQuotaBucket struct {
    BucketID          string    // 既有
    Window            string    // 既有
    RemainingFraction *float64  `json:"remaining_fraction"` // 由 float64 改为可空
    FractionPresent   bool      `json:"fraction_present"`   // 解析器是否真正读到该字段
    ResetTime         string    `json:"reset_time,omitempty"` // 保留现有 wire，运行时解析
    ObservedAt        time.Time `json:"observed_at"`
    ObservationSeq    int64     `json:"observation_seq"`
    TTLSeconds        int       `json:"ttl_seconds"`
    // 既有 display_name / description 保留；remaining_percent 仅供显示，不作门禁依据。
}
// group保留现有display_name/description/buckets，不虚构上游稳定group_id；现有额外字段保留。
```

- **历史数据兼容原则**：旧 schema（无 `schema_version = 2` 或无 metadata）由于历史解析已将 nil 写入 0.0，其显式 0 亦不可信，**最多只允许影响软排序，绝不允许直接触发硬门禁**。legacy 的软排序参与方式严格按 §7.2 执行：groups 窗口因缺 presence 被省略、models 条目缺 presence 时回退中性 0.5，因此升级后 legacy 账号在首轮新观测写入 presence 前（catalog ≤30min 补 models、summary ≤$I$ 补 groups）软评分暂时中性，随后自愈；不得解读为"legacy groups 数值直接参与 quotaPressure"。legacy 总 `UpdatedAt` 仅作兼容展示，不能用于 V2 groups 新鲜度判定。
- **单一表示的代价与读点清单（修订）**：把 `remaining_fraction` 从 `float64` 改为 `*float64` 不是"仅新增字段"的零成本改动，必须同步修改全部读点：[auth/scheduling_usage_key.go:80,103](<auth/scheduling_usage_key.go#L80>)、[auth/antigravity_catalog.go:49](<auth/antigravity_catalog.go#L49>)、admin 投影与前端类型；`fetchQuotaSummary`（[auth/antigravity_client.go:1099](<auth/antigravity_client.go#L1099>)）与 models 解析（[auth/antigravity_client.go:1034](<auth/antigravity_client.go#L1034>)）中的 `nil → 0.0` 必须同时改为 presence 记录。空值或未验证 presence 不得硬封。历史 `remaining_percent` 仅供显示，不作为门禁依据。
- **混合版本与回滚语义（修订）**：旧副本读取 V2 JSON 时，`null` 被解码为 `float64` 的零值且不报错，因此旧副本只会把该账号当作"已用 100%"参与软排序，**不会**执行硬门禁；滚动升级期间门禁覆盖不完整（旧副本不执行门禁）。门禁默认 `true`（D1=B），升级完成即生效，因此发布说明必须注明"先升级全量副本，再依赖门禁生效"。回滚后同样退化为软排序。
- **混合版本的写入危害（本轮新增：读侧兼容不等于可混布）**：上述兼容只覆盖**读侧**。旧二进制的设置 PUT 以本机内存缓存为基准 merge 后**无 guard 整包写库**（[admin/antigravity_settings.go:67-91](<admin/antigravity_settings.go#L67-L91>)、[database/antigravity_settings.go:45-49](<database/antigravity_settings.go#L45-L49>)），而旧结构体不含 `quota_refresh_*` / `quota_gate_enabled` / `config_revision`（[auth/antigravity_settings.go:19-28](<auth/antigravity_settings.go#L19-L28>)）。因此混布窗口内一次与配额完全无关的 redirects 保存就会：(a) 静默清除 `quota_gate_enabled=false` 等止血开关（字段缺失即回落到默认 `true`，误封被重新启用）；(b) 把 `config_revision` 归零，使新副本随后生成的低 revision 被"按 revision 拒旧"的 consumer 连续拒绝。该危害**无法由新侧 CAS 防御**（缺陷在旧二进制自身的写路径），故列为发布约束：滚动升级期间冻结 Antigravity 设置写入（设置写入口仅路由到已完成升级的副本，或临时关闭该入口）；revision 回退的检测与基线重建规则见 §8.2 PUT 列表第 5 项。
- **元数据可信条件**：groups 来源的 `generation` 与 `quota_identity_scope` 必须匹配账号当前代际与身份，`applied_seq > 0`、`observed_at` 非零且不在未来，并经已审定解析器验证。`presence_verified` 表示解析器保留了 presence，实际门禁还必须检查每桶数值。`summary_integrity` 只在契约验证通过且解析器能证明本次返回为完整清单时置 true（例如存在显式全量标志，或已审定 schema 足以覆盖全部组）；HTTP 200、空数组、部分清单或仅"解析成功"都不足以置 true。契约验证前该字段恒为 false，因此 `absent` 实际不可达，5h "缺席"不会被证明。缺少这些证据的旧数据保持 unknown。这里 `summary_integrity=false` 仅禁止以缺席确认 absent 或删除旧桶，不否定已通过单桶 presence、数值、来源版本与时效验证的 known_zero/known_positive；部分清单中的有效零仍须硬回避。
- **观测时间**：使用发起该来源请求时的本地 UTC 时间作为 `observed_at`，**该时刻严格取该 source 在 §4.1 #1 中预约事务的提交时刻**（预约紧邻其网络子请求），不以响应落库时刻给慢响应续命；由此各来源序号与 `observed_at` 严格同序，`mySeq > applied_seq` 即等价于"接受更晚发起的观测"。`last_success_at` 是完成校验并成功应用的时刻，仅用于任务诊断与正常 due。桶 TTL 从自身 `observed_at` 起算。真实契约验证与解析器认可规则属于上线验收，不由测试 fixture 自动授予。

### 3.2 数值校验、坏桶隔离与白名单
配额解析状态严格收敛为四态：`known_positive`、`known_zero`、`absent`、`unknown`。
1. **数值范围定义与校验算法**：
   数值解析仅确认finite [0,1]与presence，真正形成known状态前还须验证OAuth/schema、family/scope/generation、source applied_seq、窗口白名单和每桶有效期；过期或证据不足即unknown。不能仅凭数值0硬封。
   - 数值在 `(0.0, 1.0]` 区间内：判定为 `known_positive`。即便数值极小（如 0.001）展示为 0%，只要严格大于 0 即绝对不可判定为耗尽。
   - 数值严格等于 `0.0` 且经 presence 验证：判定为 `known_zero`。
   - `> 1.0`、负数、`nil`、缺失、`NaN`、`Inf`：统一判定为 `unknown`。
2. **结构完整性与坏桶隔离策略**：
   - 整批观测按结构完整性验证：若响应为 empty、null、groups 缺失或解析畸形，不得推进 `last_success_at`，亦不能作为窗口不存在的证据，保留历史快照直至 TTL。
   - 若上游返回结构整体合法，但个别桶 fraction 无效：该坏桶判定为 `unknown`，**替换所有已返回桶，坏桶绝不复活历史旧零**；未在返回清单中的组/桶，仅当契约能权威证明返回为完整清单时才执行删除，否则予以保留。
   - 没有真实契约支持前，绝不把空数组 `groups: []` 当作无 5h 限制的依据；只有权威完整且合法的 summary 响应才能证明窗口为 `absent`。
3. **窗口标识白名单（Allowlist）**：
   - 5 小时窗口严格匹配：`5h`、`five_hour`、`5_hour`、`fivehour`。
   - 周窗口严格匹配：`weekly`、`week`、`7d`、`seven_day`。
   - 其余未在白名单中的窗口（如 daily/monthly 等未证实窗口）一律不参与硬门禁判定。归一化仅做去首尾空白与转小写，不从套餐名、reset 距离、bucket 名称推算窗口。
4. **非完整观测合并算法**：
   - 保留未返回的旧桶时，必须原样保留它的 `observed_at` 与 `observation_seq`，不得因来源成功时间变新而洗新。按已验证稳定 bucket ID 合并；无稳定 ID 的歧义响应不得按展示名称或数组下标强行覆盖旧桶，视为未通过该部分校验并保留旧数据至其原到期时间。
   - 重复冲突 ID 同样不得作为新的耗尽依据。完整权威清单才可整体替换并确认 absent；已返回的坏桶覆盖为 unknown，不能继续使用其旧零。

### 3.3 每桶独立时效、已知零阻断期与后台到期维护
- **时效分立原则**：来源 metadata 用于记录最近观测，各桶保留自己的 `observed_at`；未返回旧桶沿用原时间。对每个桶计算 `ttl_deadline = bucket.observed_at + bucket.ttl_seconds`；`ttl_seconds = max(1h, 2 * I)` 在该桶观测预约时按当时配置写入。当 $I$ 在既有草案约束范围 $[3, 60]$ 分钟内（默认 $I=60$ 分钟）时，`ttl_seconds` 取值区间为 $[60, 120]$ 分钟（即 1–2 小时），与系统时效契约自洽。各实例使用同一持久化 TTL，避免配置传播差异或重启改变旧桶寿命。
- **观测有效期只约束"未知"，不解除"已确认"（本轮修订）**：TTL 的职责是判定观测是否还能支撑 `known_positive` / `unknown` 与软权重，**不得**作为已确认零额度的解除条件。否则会出现"确认 weekly=0、resetAt 在三天后，2 小时后 TTL 到点转 unknown 即重新放行"的静默失败，与硬门禁目标直接冲突。
- **统一过期判定（按状态分流）**：从兼容字段 `reset_time` 解析 `resetAt`，再按桶状态分别计算：
  - `known_positive` / `unknown`：`valid_until = min(resetAt, ttl_deadline)`；`now >= valid_until` 即转 `unknown`。
  - `known_zero`：**阻断持续到可信 `resetAt`**；TTL 到期本身不解除阻断。无可信 `resetAt` 时回退为 `valid_until = ttl_deadline` 兜底，避免永久封禁。
  - `resetAt` 可信条件：解析成功、非零、晚于该桶 `observed_at`，且不超过按窗口设定的上限（weekly $\le 8$ 天、5h $\le 6$ 小时）；越界视为不可信，按 TTL 兜底。
- **陈旧已知零（stale zero）**：观测年龄超过 `ttl_seconds` 但 `resetAt` 仍未到时，保持阻断并把该桶标记为 `stale`：进入优先补查队列、计数并告警、管理端按来源显示"事实过期"，不得静默放行。
- **重新评估而非恢复承诺**：`resetAt` 到期转 `unknown` 是窗口滚动的合法结果，仅用于触发一次重新评估与优先补查，不代表账户必定恢复；interval 变更只重排后续 due，新观测采用新 TTL，不延长或复活已存在的旧桶。
- **账户级阻断与桶隔离**：账号存在**任一**未过期的有效零桶（周归零，或存在 5h 限制且 5h 归零）即阻断整账户；单个桶 reset 到期降级为 unknown 时，绝不清空其他仍有效的周零桶。
- **只读 Predicate 与事件驱动维护**：
  - 门禁判定函数为纯内存只读谓词，严禁在请求热路径中执行任何 I/O 或派生 goroutine；判定必须按上述分流规则同时比较桶状态与 `resetAt`，不能只看 TTL。
  - 后台维护最小到期有序索引（基于各桶 `resetAt`、`ttl_deadline` 与 stale 阈值），实现到期事件驱动唤醒、去重优先 due 插入，以及"转为阻断"与"解除阻断"两个方向的等待者通知，不依赖 10 分钟低频重建。
  - 优先刷新任务（如零额度 reset 或 stale/unknown 补查）与常规定时任务完全共用调度器的连续分散 admission 节奏、单实例 inflight 预算与指数退避，不以“优先”为名绕过均匀预算发起突发并发探测。
  - **即使管理员关闭自动刷新，本地到期维护与门禁判定依然常驻运行**：已知零按 `resetAt` 到期解除，无 reset 的零按 TTL 兜底；因此既不存在"不开巡检就永远封禁"，也不存在"TTL 到点静默放行"。

---

## 4. 存储短事务协议、真网络单飞与多实例传播

### 4.1 平台无关短事务 CAS 协议与受控 Carryforward
现有实现已是平台无关的 Go 侧合并 + 短事务（[database/grok_state.go:994-1050](<database/grok_state.go#L994-L1050>)、[database/grok_state.go:821-895](<database/grok_state.go#L821-L895>) 都先 `SELECT ... FOR UPDATE` 再合并写回），**不存在**上一版草案假想的混合 `json_set` / `->>` SQL，因此无需"废弃"任何现有实现。本次在其之上新增 per-source 预约序号与 evidence 列，落点在独立的新文件 `database/antigravity_quota_source.go`：

以下为数据库无关的短事务协议，不是可直接执行的 SQL 或现有 helper API：
1. **预约（本轮修订：逐 source、紧贴该子请求联网前）**：取得账户 flight/许可后，**在每个 source 自己的网络子请求发起前**单独锁定当前 DB 行，校验未删除、expected generation、family、quota identity scope 与 `quota_epoch`（见下文失效纪元）；**仅**为该一个 source 递增 `current_attempt_seq` 并记录真实尝试时间，提交后才发起该子请求。复合 operation（同刻 due 合并、`Sync` 全量刷新）**严禁在 operation 开始时一次性预占各 source 序号**：`Sync` 的 identity/models/summary/credits 是顺序执行的（[auth/antigravity_client.go:559-629](<auth/antigravity_client.go#L559-L629>)），一次性预占会让"预约序"与"实际观测序"解耦——先预约、却因 identity/onboardUser 等慢子请求延后观测的来源会持有一枚偏小的序号，其更晚取得的观测被更早已提交的观测合法拒绝，且该倒挂在多副本下重试也无法消除。逐 source 预约后，序号序、预约提交时刻与 §3.1 `observed_at` 三者同序。未取得执行资格不得空耗序号。序号用于失败诊断与租约排序，**不单独构成成功提交前置条件**。
2. **普通来源成功（Latest-Success-Wins）**：再次锁行，要求身份范围未变、`generation == expectedGeneration`、本 source 的 `mySeq > applied_seq`；只合并本 source，把 `applied_seq` 推进到 `mySeq`，保存子请求开始时刻、presence 与完整性证据。不得要求 `current_attempt_seq == mySeq`——后发**失败**不得否决先发**成功**。未返回旧桶保留原时标，其他来源不变。
3. **来源失败**：仅当前预约可更新失败诊断与来源退避，绝不推进 `applied_seq`、`observed_at` 或成功时间，不清已有有效零，不消费成功观测覆盖窗。
4. **凭据及 final-bearer 观测**：各子请求**各自在其联网前**于旧代际预约（"网络前"按子请求计，不是 operation 开始前，见 #1）；Sync 为每个子请求保存开始时刻、最终 bearer 版本与 verified scope。凭据 CAS 的同一锁行事务校验原 expected generation、各 source 当前预约与 `quota_epoch`，先判断同已验证scope（subject+project+auth_kind+family）的carryforward，再原子写入新 token 与已验证新观测，关联本事务生成的新代际。不能网络返回后才重新预约，把旧结果冒充最新结果。
5. **token 必须落库而 source fence 失败（本轮修订：按 reload 结果三分支互斥分流，替代原"必须持久化"的无条件指令）**：CAS 出错或 `applied=false` 时，调用方**先在同一 family lease 内 reload 持久化行**，再按下列互斥分支处置；**不得凭"本次操作完成得更晚"推断自己持有更新的 RT**——RT 是不透明串，冲突方之间没有任何可比的新旧信号。

   | reload 结果 | 判定 | 处置 |
   | :--- | :--- | :--- |
   | 持久化行的 access token 已不同于本次操作读到的旧值（durable winner 已完成同身份换票） | winner 的凭据即最新，本次结果更旧 | **信任 winner**：丢弃本次 token 与全部 source 观测，reload 发布运行时状态；**绝不回写**。与 [auth/store.go:12803-12811](<auth/store.go#L12803-L12811>) 现有行为一致。 |
   | 身份范围（subject+project+auth_kind+family）可验证未变，且持久化行仍等于本次操作读到的预轮换凭据（无人抢先更新） | 本方是唯一持有已被 Google 消费 RT 的一方 | **受控补写（本规格新增）**：在仍持有 family lease 且 expected generation 未变的前提下，重放**一次**仅含凭据字段的 UpdateCAS，**不携带任何 source 观测**；再失败即转下一分支的失败处置，不循环重试。 |
   | family 已更换 / subject 或 project 已改变 / 账号已删除 / 已被隔离 | 无法证明 RT 谱系连续 | **禁止把本次 RT 写入当前身份**：沿用现有策略落永久失败标记（`antigravity_permanent_refresh_error`，[auth/store.go:12462](<auth/store.go#L12462>)）并按需 `RemoveAccount` 拉出调度（[auth/store.go:12796-12801](<auth/store.go#L12796-L12801>)），需管理员重新建立身份，并写审计"已被 Google 消费的 RT 未能落库"。 |

   三分支之外不得自行发明第四种"覆盖写入"。任何分支都**绝不**顺带接纳来源不匹配的配额观测；仅身份范围可验证不变时保留原有效观测。原文"必须持久化新 refresh token"仅在第二分支成立，**不是**跨分支的无条件义务：已消费但未落库的 RT 属于可恢复故障（管理员重新授权），不构成用新旧不确定的 token 覆盖 durable 状态的理由。
6. 所有事务从锁定的最新 credentials 合并并保持凭据加密，兼容 SQLite/PG；不在事务中联网。`applied=false` 是写入 fence 失败，不是网络去重证明。

- **Latest-Success-Wins 语义（本轮修订）**：成功提交只要求 `mySeq > applied_seq` 且 generation/scope 匹配，因此晚到的慢响应被**更新的成功**拒绝，而不会被"更新的失败"误杀。反例（旧规则的实际危害）：A 在 00:02 发起（慢），B 在 00:05 发起并失败（`current_attempt_seq` 已推进但不提交），A 在 00:07 成功——旧规则拒绝 A 的有效观测，已确认的零额度要再等一个完整周期才可能更新；新规则提交 A，B 的失败只更新诊断。失败写入绝不推进 `applied_seq`，故不会覆盖任何成功快照。**多副本推论（本轮新增）**：单实例内每账号 flight 已把同账号各来源串行化，序号序恒等于观测序；跨副本"quota 不宣称分布式单飞"（§4.2）仍允许并发，但逐 source 预约使两副本的预约在同一 DB 行锁上串行，**后发起的观测必获更大序号**，故 `mySeq > applied_seq` 在多副本下依然等价于 Latest-Success-Wins 的字面语义，不需要比较跨节点时钟。
- **配额事实失效纪元 `quota_epoch`（本轮新增，供 §8.2 清除操作及未来任何"整户清事实"使用）**：每个账号持久化一个单调递增的 `quota_epoch`，与凭据 generation 独立，**存放于 credentials JSON 的 `antigravity_quota_epoch` 键**（与 `antigravity_quota` 在同一 credentials 合并事务内写入，因而共享同一组 outbox 触发器与 payload，见 §4.3）——`MergeAccountCredentialsForGeneration`（[database/grok_state.go:959-968](<database/grok_state.go#L959-L968>)，不递增 generation）与例行 generation 递增都**不**改变它。**预约事务**读取并绑定当前 epoch，**提交事务**要求 epoch 未被改变；清除事实操作在同一短事务内删除该账号全部 quota source 事实与定时器并**递增 `quota_epoch`**。因此清除前已发出的任何在途探测（无论其 `mySeq` 多大）提交时都因 epoch 不匹配被拒，也不会与清除自身触发的优先补查争抢序号空间。generation 与 scope 在清除时均不变，**不构成清除屏障**，故不得以"保留凭据故 generation 不变"为由省略本字段。内存与 Outbox 投影同样按 epoch 单调，见 §4.3。
- **代际变迁与受控 Carryforward 规则（默认采纳，§11.2 #4）**：
  > 落点注意：`UpdateAccountCredentialsCAS` 是 Codex/Grok/AG 共用的通用函数（[database/grok_state.go:821](<database/grok_state.go#L821>)），AG carryforward 必须按 `upstream_type` 条件化，或改为 AG 专用包装，不得改变 Grok 既有语义；§9 已补列该文件。
  1. *常规 UpdateCAS（例行 token 轮换）*：当凭据轮换经校验确认已验证 Google subject、project、auth_kind 及 `credential_family_id` 确实未变（同一上游主体身份）、未被隔离且旧 source 可信时，在**同一事务内**把已成功落库的旧 source metadata 认可到新 generation，**严格保留原 observedAt、reset、TTL、applied_seq 与 known_zero 状态**，不洗新也不解除有效零门禁。旧在途网络请求预约仍作废，不得随意重标。该变更是 AG 受控新增，不声称 Grok 现有行为已处理 AG。
  2. *ReplaceCAS / 身份变动*：当主体改变、project 改变、family 改变、重新导入或身份重验失败时，**彻底清除旧 scope 事实与旧定时器，不继承 known_zero**，由新身份重新建立认证硬栅栏。不能仅凭 family 相同就继承。

### 4.2 统一纳管、真网络单飞与 Sync 观测复用
- **单实例单账号真网络单飞（Control-Plane Flight，默认采纳，§11.2 #3）**：
  单实例内针对每个 `accountID` 维护唯一的 `controlPlaneFlight`（可参考现有 `lazyRefreshInFlight`（`sync.Map`，见 [auth/store.go:3954](<auth/store.go#L3954>)）的 try-acquire 单飞模式，但**不能直接复用**：该原语仅提供不可取消的 try-acquire，不支持 Context 取消与 operation owner 识别，须新建支持取消/owner 的 flight 原语），所有周期 catalog、quota summary、manual、lazy 及 401 均经过统筹入口。
  - flight 必须支持 Context 取消与 operation owner 识别，不能用不可取消的 mutex 等待实现管理端有界等待。后台对 flight/许可仅 try-acquire，失败立即释放已持资源并顺延；交互操作在原有 Context 下有界等待。
  - 最外层 operation 持有 flight/permit；summary 内部遇 401 沿用同一 owner，不递归申请自身资源。等待摘要任务成功不等于完成管理员 fullSync，需核对所有请求目的。
  - 401 合并等待后，比较失败 bearer、最新持久化 bearer 及认证恢复结果；不得仅凭最近成功 summary 就跳过真实鉴权失败。
  - *执行顺序与双通道（本轮修订）*：先 claim 账户 flight，再按操作类型取许可，再按需获取跨实例 `family lease`。**后台流**（周期 catalog/summary、reset 到期与 stale/unknown 补查）竞争 Store 级共享业务任务许可（上限 4）；**交互/恢复通道**（请求侧 401、lazy、管理员单账号手动同步）不占后台 4 许可，只受每账号 flight 与 family lease 约束，避免 4 个 30s 级后台任务把用户请求排队（否则"最高网络优先"名不副实）。管理员批量刷新维持现有顺序执行，不并发扇出。严禁反序，严禁持有 `Account.mu` 或 DB 事务等待网络；后台任务若取不到许可则释放 flight 顺延，非重复网络。双通道为默认采纳项（§11.2 #10）。
  - *交互通道故障态背压（复核补充）*：交互通道不占后台 4 许可且无 Store 级并发上限；当上游大面积返回 401 或凭证批量失效时，N 个不同 family 的账号可同时进入恢复，形成跨账号网络突发。这是"最高网络优先"的显式取舍而非缺陷；实现时建议为交互通道叠加一个大于 4 的软上限或 401 恢复专用令牌桶作为可选缓解，并在监控中暴露交互通道并发峰值。
  - *手动请求有界等待*：管理员手动同步遇到 in-flight 任务时，采用沿用既有 90/100s Context 的**有界等待**，不引入新 HTTP 409 API；等待后重读持久化结果；若仅由 summary flight 成功则不可假称已完成 full sync，按需补全缺席来源；仅当同目的操作覆盖identity/models/groups/credits全部所需目的、scope/gen匹配且有效，才可视为完成fullSync；部分成功仅复用已满足来源并按需补全，不能因summary成功假称完成手动全量刷新。若等待期间在途的正是 4min `CriticalContext` 换票临界段，manual 在 90/100s 到点后返回"操作仍在进行、请稍后重读"的可重试错误，不得谎报成功或终态失败。
  - *嵌套 401 恢复防死锁*：由同一 operation owner token 继续持有已取得的 flight 与（交互通道）许可，仅按需申请 family lease，不递归重新申请自身 gate 导致死锁。不得以最新 summary 就跳过真实 401 认证恢复，亦不得为 $I$ 周期频率限制阻止必需 auth。
  - *管理端 manual 加入 family lease 与 Store 纳管*：改造 [admin/antigravity_accounts.go](<admin/antigravity_accounts.go>)，手动同步不再直接调用 `client.Sync`（当前见 [admin/antigravity_accounts.go:2012](<admin/antigravity_accounts.go#L2012>)），改为调用 `auth.Store` 暴露的统一入口（如 `Store.SyncAntigravityAccountForAdmin`），由 Store 持有 flight/许可/lease 并复用同一套 per-source 提交协议与运行时投影；admin 只负责参数校验与结果映射。手动换票必须纳入相同 family lease 并执行 DB-first CAS，不新造 OAuth 链路；纯只读查询不占 family lease。同 family 跨账号按既有 family 锁保护，多副本只读可重复，本次不宣称分布式 quota 单飞。
- **复用 Sync Final-Bearer 观测（默认采纳，§11.2 #5）**：
  修正旧版“恢复副产品 summary 一律失效必须重发”的绝对化规定：token 交换**前**取得且无法验证 scope 的快照不可重标；但 `Sync` 在使用本次最后确认的 bearer、建立已验证 scope 后真实返回的 models、groups 与 credits 观测，允许在凭据 CAS 的**同一锁行短事务**内校验归属后，原子提交新 tokens 并将观测关联至本次事务产生的新 generation。仅当返回数据缺失、协议不可信或旧 schema 无法复用时才安排后续补查，杜绝无条件二次 summary。失败 summary 不清空 carry 的有效 groups。

### 4.3 内存原位单调发布与 Outbox 传播
- **内存原位单调发布**：数据库事务成功后，内存状态在 `Account.mu` 锁内根据 `CredentialGeneration` 与各来源的 `applied_seq` 执行单调原位合并：
  - 仅当 `quota_epoch` 与内存一致、generation 匹配且 `applied_seq` 大于或等于内存当前序列时才允许发布；晚到的旧发布直接丢弃。相等序号只允许幂等重放相同数据；不能用更旧的诊断或预约元数据覆盖最新诊断。**epoch 递增时序号空间重新初始化**：epoch 更新的**空事实**（清除操作）直接覆盖任何旧 epoch 的事实并清空门禁，不受旧 epoch 序号约束——否则远端副本内存中的 `known_zero` 永远无法被"删除"这一动作单调覆盖；epoch 更旧或相同的空事实一律丢弃，绝不允许删除以更低 epoch 生效，也不得把 epoch 递增当作 generation 变化去清 scope（清 scope 只由 §4.1 ReplaceCAS 触发）。
  - DB reload原子应用新代际：同身份经§4.1认可的carryforward保持有效门禁，不先清零再补回；真换身份/隔离才清旧scope。先废旧soft timer，再同步初始化新raw/effective状态；旧reload不得回滚generation，不匹配scope的quota不得挂到新身份。
  - 数据库写入失败绝对禁止向内存发布。
  - 原地更新已有指针中的配额字段，绝对保留 `Account` 原始指针、在途并发计数、请求计数与会话绑定关系。
  - 内存 raw 状态发布后，硬门禁立即基于 raw 状态求值；调度软权重则通过独立的 effective soft state 经防抖后发布（见 §7.4）。
- **跨实例 Outbox 传播与投影**：
  1. 完善 [database/scheduler_outbox.go:191-221,336-364](<database/scheduler_outbox.go#L191-L364>) 的账号触发器：WHEN 条件必须同时纳入 `antigravity_quota`、**`credential_generation` 与 `quota_epoch`**——只补 quota 会漏掉"generation 变而 token 未变"的代际投影，只比 quota 会漏掉"仅 epoch 变化"的清除事件；三者缺一，都会让新事实无法跨实例传播或让远端内存 epoch 永久落后。`antigravity_quota` 与 `antigravity_quota_epoch` 均为明文存储（[database/credential_crypto.go:34-42](<database/credential_crypto.go#L34-L42>)），可直接 `json_extract` 比较；SQLite 与 PG 两种方言必须同步修改，并各加一条触发器级测试（见 §10.1 #33）。
  2. 完善 [auth/scheduler_outbox_consumer.go:464-608](<auth/scheduler_outbox_consumer.go#L464-L608>)，在 `applyPersistentAccountSnapshot` 中增补对已有 `Account` 对象的配额反序列化、`quota_epoch` 投影与门禁投影，正常目标 $\le 5\text{s}$。**自愈义务**：本机因内存 epoch 落后而被发布规则丢弃的观测，必须在该 outbox 事件到达时以新 epoch 基线重新投影一次，否则库中新事实与内存长期分叉。
  3. 各实例维护独立的 resetAt/TTL 本地索引，槽位消费采用本实例局部 round 账本，**不跨副本共享 consumed_round**，通过 durable observationID 幂等去重。

### 4.4 多实例并发语义与收敛前提
多实例部署下各节点独立运行配额调度器，无分布式强一致排他锁。设N_g/N_m分别为每实例groups/models来源资格账号数，I为groups间隔（默认60min）、C=30min为models周期、M为副本数；两集合采用相同稳定基础资格，但来源特定认证/权限栅栏可使成员数不同。单实例**后台流**最大并发为 4 个业务任务许可；交互/恢复通道不占该预算（每账号由 flight 串行），因此进程内并发可短暂超过 4。在多副本架构下，每个账号在单副本上大约每 $I$ 周期触发一次，后台流全集群总请求速率约为 $M \times (N_g/I + N_m/C)$（非全局唯一去重，交互请求另计；该值未扣除 §5.4 覆盖窗复用与 manual/401 消费，是**上界**而非稳态实测值）。端点 fallback、一次安全认证恢复及 summary 重试另计，受有界候选端点和任务预算约束；4 许可仅为并发安全上限，不是突发连发的批处理配额。
- **收敛前提**：系统最终一致收敛的前提是“存在一次无更新预约抢占且成功落库的完整时间窗口”。
- **实例间错峰与活性**：各实例通过独立的周期初始相位 $\theta$ 实现实例间错峰（详见 §5.3），旋转整体 slot 但保持本机槽位均匀间距，最大化减少并发冲突；各节点独立调度，不谎称全局一次去重或分布式协调。持续冲突时通过监控告警由运维干预，本次设计不承诺分布式队列级别的绝对无饥饿。

---

## 5. 现有刷新策略协调矩阵与定时调度算法

### 5.1 现有刷新策略协调矩阵（设计分析与统筹方案）

为彻底厘清现有刷新机制并杜绝重复建设与行为冲突，特制定以下协调矩阵：

| 刷新/探测路径 | 现有代码事实 | 本次规格定位与提议（确认状态见 §11.2） | 触发源与周期 | 并发许可与排它锁 | 观测结果持久化与复用 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **通用 Token 后台巡检** | [codex_refresh.go:360](<auth/codex_refresh.go#L360>) 显式排除 AG；2min 巡检拒绝 AG | **保持排除**；绝不把 AG 塞入 2min 循环，不依赖该机制刷新 AG | 通用巡检，每 2min | 不涉及 AG | 不适用 |
| **模型目录刷新 (Catalog)** | [antigravity_catalog.go](<auth/antigravity_catalog.go>) 10s 首发后 30m 批处理，4 worker 自建池，旧 groups 拼回新时间洗新 | **纳入统筹**；保留 $C=30\text{min}$ 周期但改为 $C/N$ 连续平滑派发；废除独立 4-worker 池，纳管入后台共享 4 许可与 flight；接入 per-source CAS 不洗新 groups | 独立目录流，每 30min | 后台共享 4 许可 + 账户 flight | 仅更新 models source 分区，不推进 groups 时效 |
| **配额概要自动刷新 (Summary)** | 现有代码无独立定时 summary 循环 | **本次新增**；默认 $I=60\text{min}$，设置页可调 $[3, 60]\text{min}$，按 $I/N$ 连续平滑派发，两流各自独立调度 | 独立配额流，默认 60min | 后台共享 4 许可 + 账户 flight | 仅更新 groups source 分区，四态校验与时效 |
| **请求侧 401 恢复** | [store.go:12691+](<auth/store.go#L12691>) 遇到 401 触发完整 `Sync`，持有 family lease 最长 4min | **最高网络优先**；单账号 flight + 交互通道（不占后台 4 许可），不等待名义 slot；安全换票 CriticalContext 必须收尾 | 业务请求遇 401，按需 | 交互通道 + 账户 flight + 跨实例 family lease | UpdateCAS 受控 carryforward，原子复用 final-bearer 观测 |
| **Lazy 预热模式** | 请求到达时触发 lazy 换票；[antigravity_catalog.go](<auth/antigravity_catalog.go>) 现有逻辑不检查 lazy | **独立共存**；quota_refresh 开启时，lazy 下仍保持低频配额观测与必要认证恢复例外；quota关闭仅停止groups流及其为观测触发的换票，不改变lazy自身按需换票 | 请求触发 / 周期配额 | 账户 flight（交互通道） | 正常更新各 source 分区 |
| **管理端手动同步** | [admin/accounts:2012](<admin/antigravity_accounts.go#L2012>) 执行 fullSync，缺少 family lease；/quota 为别名 | **高优先执行**；改造为经 Store 统一入口加入 family lease 与 DB CAS；遇在途 flight 有界等待（90/100s）；可复用同 scope 有效结果 | 管理员点击，单账号/批次 $\le 100$ | 交互通道 + 账户 flight + family lease（换票时）；批量顺序执行 | 完整更新各分区；成功 summary 消费本地本轮 slot |
| **Bootstrap 账号建立** | OAuth 回调/导入执行完整 Sync 验证并建账 | **独立准入**；落库前不占用已注册账号 4 许可；落库后建立初始快照并纳入协调池 | 用户导入/回调，一次性 | 独立入口限制 | 初始化凭据与 V2 快照 |
| **状态查询与 Outbox** | `/state` GET 只读本地持久化状态；Outbox 消费端增补投影 | **纯本地/只读**；绝不触发网络请求；Outbox 消费端按 durable observationID 幂等投影 | HTTP GET / DB 事件 | 纯内存读锁 / 本地事务 | 投影内存 raw 状态与硬门禁 |
| **WHAM / Recovery 探针** | 通用探针明确拒绝 AG | **保持排除**；不因配额功能触发通用探针 | 外部或通用探针 | 不涉及 AG | 不适用 |
| **实验性 Refresh Scheduler** | [refresh_scheduler.go](<auth/refresh_scheduler.go>) 存在但在 main 中未初始化调用 | **保持未启用**；本次不顺带开启；未来若启用，AG 入口仍走共享协调器 | 环境变量未启用 | 未接入 | 不适用 |

### 5.2 统一 Store 控制面协调器架构（默认采纳，§11.2 #2/#10）
1. **已注册 AG 账户共享控制面许可**：
   - 全 Store 单实例维护**唯一**的已持久化已注册 AG 账户业务任务协调器，设置共享信号量上限为 `4`。
   - 彻底废除模型目录原有的独立 4-worker 批处理池；所有自动 catalog、自动 summary 向**后台共享 4 许可**申请准入；手动同步与 401/lazy 走**交互通道**（不占该 4 许可），但仍经同一协调器做 flight 与 family lease 统筹，避免请求路径被后台任务排队。
   - bootstrap 尚未有 DB ID 的 OAuth/import 验证保持现有独立入口与并发限制，明确不纳入“4 个已注册账户”计数；落库后纳入共享协调与观测发布。capability/connection 生成 probe 显式单独，不因 quota 自动触发。
2. **多源双流独立平滑派发（Dual-Stream Spaced Admission）**：
   - 模型目录流（Catalog Stream）：周期 $C = 30\text{min}$，基准步长 $\Delta_c = C / N_m$；
   - 配额概要流（Groups Summary Stream）：周期 $I$（默认 $60\text{min}$），基准步长 $\Delta_g = I / N_g$；
   - **两流各自独立排程**：各自维护独立的最小堆与名义槽位公式（$T_{\text{slot}}$），启动时沿用现有 10s 首批触发节奏（[auth/store.go:6698](<auth/store.go#L6698>)），随后各自铺满周期；**不宣称 30s 启动窗口**（现状无此锚点，如需更大错峰窗口属新调度器设计选择，须在实现时显式定义）；两流共用后台 4 并发上限与账户 flight，绝不谎称叠加后仍为严格等距单 HTTP。
   - **同刻 Due 合并**：若同一账号在同一时刻两流均到达 due，合并为一个复合 operation，在持有单飞锁下**顺序执行**两个网络子请求，分别记录 attempt/success，保留各自调用时刻；不得为了合并提前非 due source，也不可因 catalog 成功就消费 groups 或反之。
3. **操作类型优先级队列（Priority Ordering）**：
   - 协调器按以下优先级调度执行（**优先级类别是第一关键字**，类别内再按 §5.3.3 的等待锚与名义时间仲裁）：
     1. *安全状态与门禁判定*：本地内存即刻应用，零等待、不排队；
     2. *401 / Lazy 认证恢复*：最高网络优先级，走交互通道，不受周期槽位与后台 4 许可约束；
     3. *管理员显式手动同步*：高网络优先级，交互通道，有界等待在途任务；
     4. *已超期配额任务（含 reset 到期与 stale/unknown 补查）*：优先于常规巡检；
     5. *常规周期配额刷新（Summary）*；
     6. *常规周期模型目录刷新（Catalog）*。
   - 高优先操作不等待名义 slot；成功观测可按 §5.4 覆盖对应来源的一轮，但后台 4 许可繁忙时后台任务仍需有界等待（带超时），严禁饿死目录或其他账号；已进入换票临界段不可强杀；新低优先后台任务在高优先任务积压时暂停抢占许可。

### 5.3 连续均匀分散调度算法（Continuous Spaced Scheduling）
替换草案上一版“启动全量到期立即触发 + 批次扫描 + 5分钟整批超时”方案（非声称已有此quota实现），建立**连续均匀分散调度器**，将所有具备控制面刷新资格的 OAuth 账号在整个刷新间隔内尽可能均匀平铺触发：

1. **刷新资格集合定义（Refresh Eligibility Set）**：
   - 设全量 Antigravity 账号集合中满足**控制面刷新资格**的 OAuth 账号集合为 $\mathcal{S}$，其元素数量为 $N = |\mathcal{S}|$。
   - **纳入资格**：具备 OAuth 项目标识及有效路由配置（含允许直连的配置），未软删除、同时满足`Disabled == 0`且`DispatchPaused == 0`，且认证未被标记为永久不可恢复。可恢复的 `unauthorized` 冷却账号仍计入集合以便恢复；不按代理瞬时健康反复增删成员。
   - **核心边界**：**资格集合不同于推理调度可选池（Dispatch Schedulable Pool）**。配额确认归零（`known_zero`）以及处于模型 429 冷却中的账号**必须严格纳入**刷新资格集合 $\mathcal{S}$，以便及时探测配额重置与冷却结束；推理门禁只断业务请求，绝不断控制面巡检。
2. **连续均匀分散数学基准**：
   - 设 $I$ 为当前生效的刷新间隔（默认 60 分钟，既有草案约束范围 $[3, 60]$ 分钟）。名义相邻触发槽位基准间距：$\Delta = I / N \ (N \ge 1)$。若 $N = 0$，调度器进入空闲挂起状态，不启动 timer；若 $N = 1$，$\Delta = I$。
   - **稳定有序序列**：将 $\mathcal{S}$ 中的账号按确定性规则排序，映射为序号 $j \in [0, N-1]$。
   - **周期名义槽位公式（Nominal Slot，仅说明 groups 流，catalog 同形 C/N）**：
     $$T_{\text{slot}}(j, k) = T_{\text{epoch}} + k \cdot I + \left(\left(j + \frac{1}{2}\right) \cdot \Delta + \theta\right) \bmod I$$
     其中 $T_{\text{epoch}}$ 为调度器基准纪元时刻；$k \in \mathbb{N}$ 为周期轮次序号；$\theta \in [0, I)$ 为**单实例周期共有初始相位**（启动时确定）。每个名义时间由 $(j,k)$ 独立计算，禁止累加舍入后的 $\Delta$ 造成漂移。
   - **实例间错峰与单机均匀**：各实例副本独立随机初始化其全局相位 $\theta$ 实现实例间错峰，但 $\theta$ 在单实例内部是全局常量，**只整体旋转时间轴上的所有槽位，绝不破坏槽位之间 $\Delta$ 的均匀恒定间距**。严禁使用各自独立的 `hash(accountID) % I` 或每个账号独立随机抖动，避免泊松聚集与局部突发。
3. **调度器架构：最小堆驱动与可取消高精度 Timer**：
   - 调度器单实例全局仅启动一个调度执行循环，内部维护按生效 `next_due` 排序的最小堆，由一个关联 Context 的高精度可取消单定时器（`time.NewTimer`）驱动唤醒。
   - 定时器唤醒时，调度器**仅从已处于 due 状态且满足 `not_before` 及资格的候选**中出队：先按 §5.2.3 的优先级类别分层，同类内绝对优先选择 `latest_start` 已超期的任务，其次按最长等待锚，再按 `nominal_due` 与 `AccountID` 升序仲裁；严禁提前抢派未来账号。未知新账号的等待锚严格取首次加入资格池时刻。
4. **统一派发节奏（Spaced Admission）与退避**：
   - 每个来源流分别维护 `next_start_not_before[source]`，步长为I/N_g或C/N_m；不得用同一个I/N门禁串行阻挡两类任务。实际移交共享许可后才推进所属来源门禁：
     $$\text{next\_start\_not\_before} = \max(\text{now}, \text{next\_start\_not\_before}) + \Delta$$
   - 空唤醒或重排不提前也不后推门禁；缩短I特例按 `gate := min(gate, now + Δ_new)` 一次收紧groups流，不释放历史令牌。仅复用观测而未联网的轮次不推进网络门禁；复合operation的两来源分别取得派发资格并记账。
    - **重排期限与公平性**：每个(账号,source)保留等待锚（最近实际派发，未尝试取首次入池）与绝对 `latest_start=锚+2P`，P为本来源周期。成员变化不重置全局epoch/共有θ及等待年龄，账号名义相位可随N变化但受latest_start约束，不把last_attempt=0当无限古老；同期限按等待年龄/账号ID公平仲裁。重排不放宽既有期限，缩短周期时取旧期限与 `max(锚,设置生效时刻)+2P_new` 的较早者；已逾期记录lag并最早可行派发，不要求兑现过去时刻。容量充足无退避时既有账号间隔≤2P，新账号首排≤P，冲突以既有到期任务优先。过渡窗口可非严格等距，稳定后下一完整周期恢复均匀。
    - **优先与失败轮次**：reset/unknown优先仅替换唯一pending，提前探测消费原被替代轮次，下次越过该轮（00:02提前跑00:10，下一次01:10），仍受not_before。失败不生成成功覆盖，但已实际执行轮次不得重放，后续按退避建立下一轮，不能写成失败不消费已执行轮次。
    - 名义时间独立取整，严格Δ±ε仅用于计时精度足够、无退避/无重排/总共享容量充足的FakeClock理想稳态；生产慢请求或过载只保证有界并发、不回放与可观测延迟。
   - **指数退避与 Not-Before**：连续失败首次 $k=1$，$\text{backoff} = \min(I \times 2^k, \max(4I, 60\text{min}))$。默认 $I=60$ 时为 2h/4h 封顶，$I=3$ 时从 6m 起逐级放大到 60m 封顶；**原式 $\min(I \times 2^k, \max(I, 60\text{min}))$ 在 $I=60$ 时退化为恒定 60min，等于没有退避，必须修正**。失败账号在最小堆中对齐到不早于 $\max(T_{\text{next\_slot}}, \text{not\_before})$ 的合法名义槽位。
   - **无历史补偿（No Slot Catch-Up）**：错过历史槽位**绝不积攒补偿令牌，不回放补发**，直接对齐未来下一个名义槽位。

### 5.4 观测覆盖窗与局部槽位消费
- 任何 manual、401、lazy 或 bootstrap 成功来源结果立即更新本机 raw 与 outbox。
- **覆盖窗判定准则**：出队前若同 scope 该 source 当前最新可信观测有效，且 `startedAt`（即 §3.1 的 `observed_at`，取该 source 预约事务提交时刻，二者同值）位于待消费 slot 的覆盖窗 `(nominal_due - 该source周期, nominal_due]` 内，可消费该账号该 source 本轮而不再联网；该观测最多消费本地该 source 一轮，后续推进到下一名义轮次而非 `now + I`。
- **逾期但尚未执行的轮次**：manual/401子请求在名义due之后开始并已成功，仍可替代该尚未执行的逾期轮（如00:12替代00:10），按本地observationID仅覆盖此一轮，后续到下一名义轮，不立即再打summary。更早逾期轮记skipped；下一轮即使形式落入覆盖窗也不能再消费同一观测。重启按持久化观测重建coverage，不回放旧epoch任务、不跨副本共享round。
- **未来边界判定**：覆盖 pending 未来 due 只在 `due - observed < 周期` 且 `already-success <= now` 时成立；同一观测绝不连续推进多个周期。失败、继承旧值或仅token变化不产生新的成功覆盖；已实际执行的失败轮仍记已执行并不得重放（§5.3），同source重复outbox幂等。
- 重启用持久化observed_at/last_success_at恢复成功coverage，以last_attempt_at恢复实际派发等待锚与失败轮次；not_before和failure_count一并恢复，不能把已执行失败轮当未执行。legacy last_synced不能证明groups被观测，不制造V2可信度。
- **观测复用时效示例**：若管理员在 00:02 手动同步成功并落库，该观测落在原本定于 00:10 的槽位覆盖窗 `(00:10 - 60m, 00:10]` 内，则直接消费 00:10 这一轮，该账号下次正常触发推进至 01:10；但在 01:10 到来时，00:02 的观测已超出覆盖窗 `(00:10, 01:10]`，系统必须重新发起网络探测，绝不将临时新鲜度视作永久有效。

### 5.5 开关分层、凭据恢复与安全收尾
- quota开关/间隔仅控制groups自动流（含reset/stale补查），不控制30min models、手动、请求401/lazy预热与本地到期维护。关闭立即停止新自动groups网络及该流触发的主动换票，目录仍可用现有token只读查询。
- **lazy明确例外**：建议AG专用quota开启时，lazy下仍按同一 interval $I$（默认 60min，不存在独立的"低频"档，与 §5.1 lazy 行保持一致）执行 summary 观测及必要认证恢复；页面帮助必须明示，不声称lazy仍绝无主动认证。关闭quota不禁止业务lazy/401；不以"不开巡检会永久误封"作理由（已知零按 resetAt 解除、无 reset 按 TTL 兜底；见 §3.3，TTL 不再解除已确认零）。
- 通用background_refresh_interval_minutes、Codex keepalive、WHAM/recovery不代替AG周期；不将AG加入普通2min循环，实验刷新器本次不启用。安全状态立即本地应用，不等待周期/许可/防抖；网络恢复受许可/family lease，不承诺网络零等待。

- groups流只查summary，models流只查目录；仅显式开启的groups周期或交互操作遇401/临期确需恢复时复用安全换票。目录流本身不触发换票，quota关闭不能由目录旁路主动续期。
- **诚实预算边界**：该恢复链路调用完整 `client.Sync`（[auth/antigravity_client.go:559-609](<auth/antigravity_client.go#L559-L609>)）。普通探测执行 30s 预算，手动操作为 90/100s 既有超时；**安全换票临界区通过 `lease.CriticalContext()` 执行，允许最多持有 4min family lease 且不随 caller 取消**。规格明确承认严格普通预算不覆盖既有安全换票临界段，避免换票中途被粗暴杀死导致 DB 遗留失效 refresh token。
- 容量核算中 worker 占用时间 $D$ 包含实际恢复开销。单次探测最多进入一次恢复流程；`Store.Stop` 先停派发、取消普通 HTTP，等待 critical worker 收尾并 join 后再关闭 DB。

---

## 6. 全局调度硬门禁与会话粘性线性化

### 6.1 门禁判定准则
在 `auth.Account` 内存态中定义纯内存只读谓词：
```text
先验证OAuth和V2逐桶身份/来源/presence/数值/有效期（按 §3.3 分流：零桶看 resetAt，正值看 min(resetAt, ttl)）；
任一有效weekly零桶或存在的有效5h零桶 → 阻断；
其他情况不新增本门禁，继续受既有健康/冷却/鉴权/并发条件限制。
```
- **阻断准则**：
  1. 必须是 Antigravity OAuth 账号；
  2. Weekly 窗口状态为 `known_zero` 且未过期 $\rightarrow$ **阻断**；
  3. 存在 5h 限制，且 5h 窗口状态为 `known_zero` 且未过期 $\rightarrow$ **阻断**；
  4. 其他情况（包括 `known_positive`、`absent`、`unknown`、reset 到期）均不阻断。
- **硬状态即刻生效**：本次新增的周 0 或（存在 5h 且 5h 0）整账户硬门禁立即生效，**绝不因配额权重防抖而产生任何延迟**。raw zero 只要到达内存发布，硬门禁立即可读并阻断调度。
- **门禁与既有限制独立**：未知、absent 或正余额仅表示不因本门禁阻断，不豁免真实 429 冷却、管理禁用、鉴权或并发限制。两道准入条件为 AND，正余额刷新不得清除模型冷却或会话绑定。
- **失败不洗新**：查询失败仅记录诊断，未过期已知零继续阻断；已知零按 §3.3 的 `resetAt` 到期解除、无 reset 时按 TTL 兜底，绝不把观测过期当作"余额恢复"放行。

### 6.2 调度全路径与原子线性化防线
1. **原子线性化**：在 `Account.mu` 锁内，配额门禁校验与既有 `ActiveRequests`/`OccupiedRequests` 占用记账严格线性化：若阻断则返回 `quota_exhausted`，不计为 `capacity_full`，并原子撤销 reservation，绝不泄漏 slot；此前已取得 slot 发出的在途请求受保护不予追杀。
2. **`quota_exhausted` 的对外契约（本轮新增，必须与既有错误映射对齐）**：
   - 选号层：被阻断账号按"不可选"处理，不进入 `capacityFull` 分支（不误报容量满），**但不改变既有借号/重绑语义**——硬零与容量满、鉴权失败同属"不可用原因"，处理方式见 §6.3；**不因配额耗尽进入 `WaitForDispatchAvailable` 长时间等待**（`resetAt` 可能在数小时后）。
   - 全部候选均因配额阻断时，返回类型化错误 `ErrQuotaExhausted`，HTTP 映射为 **429**，并带 `Retry-After`：取被阻断候选中最早可信 `resetAt`（上限 3600s），无可信 reset 时取 60s。
   - 与上游真实 429 冷却并存时，二者在日志与指标中必须可区分（`reason=quota_gate` vs `reason=upstream_429`）；真实 429 冷却语义不变。
   - 门禁状态变化（转为阻断 / 解除阻断 / reset 到期）必须调用 `notifySchedulerAvailability`，避免排队等待者空等到超时。
   - 绑定账号被配额阻断但候选集合内仍有其他可用账号时：`preserveBinding=false` 的 strict/bounded/off 会话按 §6.3 借号并重绑，**不返回 429**；`preserveBinding=true` 的续链不借号，按下方续链条目返回 `reason=quota_gate_bound` 的 429。仅当候选集合整池因配额阻断时才返回 `reason=quota_gate` 的 429。与"无可用账号 503"（非配额原因）明确区分——503 仅用于容量类与选号超时，不用于配额阻断。
   - **类型化原因与传播落点（本轮修订）**：底层判定（`isAvailableLocked`、`takeByIDModeWithCapacity`、FastScheduler 候选过滤、`tryReclaimSessionSlot`）必须以**可区分的阻断原因**返回，而不是仅返回 nil。原因需随本次选号结果上浮到 [proxy/retry_exclusions.go:558-634](<proxy/retry_exclusions.go#L558-L634>)，使其能在"是否进入 `waitForRetryAccountAvailableWithGuard` 队列等待"处区分配额阻断与容量满（前者不得排队等待——`resetAt` 常在数小时后，等待只会白耗 30s 预算），并由 [proxy/scheduler_queue_error.go:20-62](<proxy/scheduler_queue_error.go#L20-L62>) 的 `schedulerSelectionAPIError`/`writeSchedulerQueueError` 统一映射为 429 + `Retry-After`（Gemini/Anthropic/Chat/Responses 四协议共用该出口）。**严禁靠对整池重新求值 filter 来推断原因**，否则破坏 §7.1"每候选至多求值一次 filter"与 `handled=true` 不回退重扫的边界。
   - **分类口径（本轮新增）**：`ErrQuotaExhausted` 的判定对象是**本次请求允许使用的候选集合**（经 API Key 授权、filter、分组与 policy 约束后的集合），不是全局健康账号池。混合原因（部分因配额阻断、部分因容量满或冷却）不满足"全部因配额阻断"，按既有无可用账号路径处理，不得升级为 429。
   - **续链遇阻断的对外状态（本轮新增，§11.2 #16 默认采纳）**：`preserveBinding=true` 的续链绑定账号被硬门禁阻断时按 §6.3 返回 nil 保留绑定、不借号，此时**既不返回"整池耗尽"的 429，也不进入队列等待**，而是返回 429 + `Retry-After` 且 `reason=quota_gate_bound`，与整池耗尽（`reason=quota_gate`）、容量类 503 三者可在日志与指标中区分；不得因绑定不可用而静默重绑。
3. **覆盖全分支**：全覆盖 `round_robin`、`remaining_quota`、`fill_first`；全覆盖 FastScheduler 与 Store 遍历；全覆盖 lazy 与 nonlazy；全覆盖新会话、绑定复用、按ID选取、队列重选；还须在isAvailableLocked的AG提前返回前、takeByIDModeWithCapacity的continuation/spark旁路、tryReclaimSessionSlot成功返回前及applyAntigravitySchedulerOverrideLocked/antigravityUnauthorizedRecoveryLocked之后复检，401恢复或续链不得清除quota_exhausted；原生Gemini生成不得绕门禁。**countTokens 的无号回退（本轮事实更正）**：仓库**已存在**本地近似估算器 [estimateInputTokens](<proxy/count_tokens.go#L31-L43>)，且 native countTokens 入口当前就在三条路径上以 `200 + 估算值` 静默回退：无可用账号（[proxy/handler_gemini.go:247](<proxy/handler_gemini.go#L247>)）、不可重试传输错误（[:278](<proxy/handler_gemini.go#L278>)）、上游 404（[:296](<proxy/handler_gemini.go#L296>)）。因此 D8=A 的实际含义是**改变既有行为**，而非"本规格不去实现"：**仅当本次请求的候选集合全部因配额阻断时**，countTokens 与生成一致返回 429 + `Retry-After`，不再落入 200 估算回退；其余无号成因（选号超时、队列准入拒绝、上游 404、不可重试传输错误）**保持现状不变**，本规格不借此移除既有兼容性回退。若后续要把该估算扩展为通用 token 计量，须另行立项并显式标注为近似。WS 与 compact 协议继续排除 Antigravity。

### 6.3 会话粘性模式交互与硬零语义（2026-10-21 决议 D4=C）
- **不设 strict 特例**：绑定账号被配额门禁拒绝时，按既有流程处理——`preserveBinding=false` 的 strict/bounded/off 会话照常 fallthrough 到 Fresh 选号并**重绑**；`preserveBinding=true` 的续链请求仍返回 nil 并保留绑定，不借号。本规格**不引入**"硬零禁止借号"的 strict 例外：硬零只是一种不可用原因，与容量满、鉴权失败并列。
- **preserveBinding与权威续链**：遇配额阻断返回nil，不剥离续链换号；即使本地binding过期也不得靠新首绑绕过权威续链。无权威续链且无有效绑定才按原规则首绑；无上述约束的bounded/off可按既有规则重选。容量/路由等非配额失败保留原借用语义。
- **绑定账号被配额阻断的对外状态（本轮新增，§11.2 #16）**：选号层按上一条返回 nil 保留绑定、不借号；边缘层必须映射为 429 + `Retry-After` 且 `reason=quota_gate_bound`，**不进入 30s 排队等待、不适用 503**（`ErrNoAvailableAccount`/选号超时仅用于容量类等非配额原因）。配额阻断在 `resetAt` 前不会因释放而恢复，等待只会把可诊断的 429 拖成超时。
- **硬零借号 ≠ 评分迁号**：硬零导致的借号/重绑不属于"因软评分迁号"；§11.1 #6 仍然成立——健康且未被硬门禁拒绝的绑定账号，绝不因 `quotaPressure` 较低被换掉。
- **健康亲和优先**：已有未过期有效健康绑定直接复用，**完全忽略软配额评分权重，绝不因为绑定的账号配额分数低而主动迁移或借号**。无可用账号沿用原等待/无号错误（ErrNoAvailableAccount为503），不将健康请求映射成错误；整池因配额耗尽无可用账号时走 §6.2.2 的 429。

---

## 7. 额度调度权重增强与双重防抖机制

### 7.1 现有 Antigravity 加权选号兼容边界与增强模型
- **兼容边界与生效范围**：仅在满足现有条件时生效：即 `opts.Provider` 为 Antigravity 渠道、调度模式为 `round_robin`、且进程开关 `ANTIGRAVITY_BALANCED_AFFINITY` 开启的 Fresh 首绑与重试选号路径。auto 渠道且 provider 为空保持原路径；native Gemini/countTokens 即使来自 auto Key 亦因强制 Provider 而参与加权。其他调度模式或无 key 路径不扩展。
- **增强软评分公式**：
  $$\text{score} = \text{load} + w_u \times \text{quotaPressure} + w_p \times \text{recentPenalty}$$
  - 分数越小越优；$\text{load}$ 与 $\text{recentPenalty}$ 保持实时计算；$w_u, w_p$ 沿用现有 `AntigravityThrottleWeights()`（默认 $w_u=0.50$、$w_p=0.35$，见 [auth/antigravity_ratelimit.go:53-54](<auth/antigravity_ratelimit.go#L53-L54>)），**不新增独立权重常量**，避免同一概念两套配置；
  - $\text{quotaPressure} \in [0.0, 1.0]$ 为新引入的配额综合压力项，**直接替换原公式中的 $w_u \times \text{usage}$ 项，绝不与旧 usage 叠加，彻底避免同额双罚**；
  - 保留 `scheduler_priority` 与 `tierPriority` 分层：高优先级与健康档位账号优先，层内按 score 升序；
  - 保留 FNV tie-break 打散与 `handled=true` 避免回退重扫，快照循环内每个候选账号至多求值一次 filter。

### 7.2 窗口聚合与 quotaPressure 计算规范
- **窗口聚合（三态，本轮修订）**：按周与 5h 分别聚合，取各窗口有效桶原始 `remainingFraction` 最小值，并返回三态：`Present`（有已验证数值）、`Exists`（权威清单证实存在但数值/时效不可用）、`Absent`（权威完整清单证明不存在；无法证明时视为存在性未知）。窗口白名单见 §3.2.3。
- **权重与压力定义（2026-10-21 已确认，D6=A）**：
  - 周维度默认权重 $W_{\text{week}} = 1.0$，5h 维度默认权重 $W_{5\text{h}} = 1.0$；压力 $p = 1.0 - r$；
  - 最终 $\text{quotaPressure}$ 为各项归一化加权平均，**已确认采用 1:1 平均**。已知代价：周剩 .01 / 5h 剩 1 时压力 .495（旧 tightest 语义为 .99），即用 5h 充裕抵消周额度紧张；该取舍被显式接受，**不再评估** `max(weekPressure, 5hPressure)` 或按窗口长度加权。任一有效 known zero 仍优先触发硬门禁阻断。
- **四态判定矩阵与压力计算算法**：
  ```go
  // windowAggregate 是三态聚合结果：Present=true 表示有已验证数值；
  // Exists=true 表示权威清单证实该窗口存在（数值暂不可用）；两者皆 false 表示缺席或存在性未知。
  type windowAggregate struct {
      Present bool
      Exists  bool
      Value   float64 // 仅在 Present 时有意义
  }

  func (snap *AntigravityQuotaSnapshot) ComputeQuotaPressure(now time.Time) float64 {
      week := snap.AggregateWindow(WindowWeekly, now)
      five := snap.AggregateWindow(WindowFiveHour, now)

      var total, weight float64
      // 周窗口：present→实际压力；证实存在但数值不可用→中性 0.5；缺席/存在性未知→省略。
      switch {
      case week.Present:
          total += 1.0 - week.Value
          weight += 1.0
      case week.Exists:
          total += 0.5
          weight += 1.0
      }
      // 5h 窗口：与周同规则，避免"无 5h 限制"被中性分惩罚。
      switch {
      case five.Present:
          total += 1.0 - five.Value
          weight += 1.0
      case five.Exists:
          total += 0.5
          weight += 1.0
      }
      if weight > 0 {
          return total / weight
      }

      // groups 完全不可用：回退 models 级压力（只统计 presence 有效的条目）。
      if models := snap.AggregateModels(now); models.Present {
          return 1.0 - models.Value
      }
      // 全无事实：全局中性先验。
      return 0.5
  }
  ```

- **规则一致性（修订）**：`absent` 与"存在性未知"在两个窗口上一律**省略**，不再给周 0.5 先验——否则"无周限制"的账号会被无端降权，且旧 API 无法区分 absent 与 unknown。中性 0.5 只在"没有任何窗口事实、也没有 models 事实"时作为全局先验，soft 值不得写回真实余额。
- **models 回退（本轮新增，防止功能静默失效）**：groups 完全不可用（无 summary、窗口全部未识别、旧 schema、quota 关闭期间）时，回退到 models 级压力 `1 - avg(present remaining)`；`Models[].RemainingFraction` 必须带 presence（[auth/antigravity_client.go:1034](<auth/antigravity_client.go#L1034>) 的 `nil → 0` 同步改为记录 presence），只对 present 条目求平均，全部缺失则视为无事实。没有此回退时，summary 不可用的账号软评分恒为常量 0.5，原 usage 避让信号被整体移除，加权选号会静默退化。
- **有效零仍先硬排**：不以 `q == 1` 判门禁、不加"近零硬封"；`.5` 仅是 soft 先验，不能用于阻断。

### 7.3 会话粘性与亲和优先隔离
- 健康绑定账号直接复用，**完全忽略软配额评分权重，绝不因分数较低主动换绑**。仅在绑定不可用、硬门禁阻断或并发满且允许重选时进入 Fresh 选号应用新权重。“忽略”仅指不以评分高低换号，绝不豁免硬零阻断、管理禁用或并发限制。

### 7.4 读锁内原子读取与派生 Soft State
- 在同一 `Account.mu` 读锁内原子读取 load 与 quotaPressure。内存 raw 状态发布后，硬门禁立即可读；软权重**默认在读时基于 raw 事实计算**（同一锁内读取分窗口值/存在性/`valid_until`/scope/gen/sourceSeq，并按 §3.3 即时判定每个窗口是否仍有效），避免出现第二份配额真相。若保留 §7.5.2 的防抖发布，则 effective soft state 只能是**派生缓存**：不得参与门禁、不得回写真实额度、不得在缓存与 raw 不一致时影响阻断判定；缓存缺失或版本不匹配时回退 raw 计算。raw、DB、Outbox、fastSchedulerUpdate/健康/limit/门禁立即发布；soft 只供 Fresh 选号，不写回 admin usage，也不为每次选号建排序 timer。

### 7.5 双重防抖机制与实现建议（默认采纳，§11.2 #6/#9）
1. **计划重排防抖（Reschedule Debounce）**：
   - **建议参数**：`quiet_window = 100ms`，`max_wait = 1s`。
   - 首个变更事件到达锁定 `max_wait`，后续事件合并，$\le 1\text{s}$ 内完成重排；账号禁用、删除或人工暂停立即生效不等防抖。
2. **额度权重更新防抖（Quota Weight Debounce）**：
   - **建议参数**：单账号 `quiet_window = 1s`，`max_wait = 5s`。
   - 仅合并正正数值，quiet满1s即发布最新值（即使累计差<0.01），持续更新无quiet则first_dirty+5s强制发布，后续事件不重置上限。差值相对已发布值，不是永久死区；load/penalty实时。
   - **实现建议（本轮修订）**：软权重只在 Fresh 选号时被读取，没有外部订阅者，1s/5s 防抖不改变任何可观察行为，却引入 per-account timer 生命周期与 Stop 竞态。建议保留"两处防抖"的需求目标，但软侧实现为**读时计算 + 可选 1s 缓存**；若坚持独立发布状态机，须在 §11.2 #9 说明其唯一收益（例如前端实时展示）与对应验收方式。
   - **硬状态即时性**：有效零立即限制；`resetAt` 到期或观测按 §3.3 转 unknown/absent 时重算（TTL 到期**不**解除已知零）；正余额恢复不等 soft 且不清 429。代际变化先废旧timer，同身份carry不清有效knownzero，真换身份清旧scope。观测时间不因soft发布变新。
3. **生命周期**：单账号最多一条pending；回调携AccountID、对象身份、gen/rawVersion/sourceSeq，在Account锁内重验对象仍注册、版本及每桶有效期后才publish。Stop取消timer但不能只靠timer.Stop防竞态；同身份carry保留事实但旧timer失效，新raw同步初始化。

### 7.6 典型场景计算示例
- **示例 1（基础对比）**：A (week 0.8, 5h 0.8) $\Rightarrow p=0.2$, score = 0.10；B (week 0.2, 5h 0.2) $\Rightarrow p=0.8$, score = 0.40。A 优于 B。
- **示例 2（无 5h 对比）**：A (week 0.8, 无 5h 即 absent) $\Rightarrow p=0.2$, score = 0.10；B (week 0.8, 5h 0.2) $\Rightarrow p=(0.2+0.8)/2=0.5$, score = 0.25。A 优于 B。
- **示例 3（存在但数值未知的中性项）**：A (week 0.8, 5h 证实存在但数值/时效不可用) $\Rightarrow p=(0.2+0.5)/2=0.35$, score = 0.175；B (无已识别窗口，models 平均剩余 0.5) $\Rightarrow p=1-0.5=0.5$, score = 0.25。A 优于 B，且 B 不被硬封。
- **示例 4（亲和优先）**：已绑定账号 A (week 0.1, score 差) 直接复用，不换选空闲账号 B (week 0.9)；若 A 硬零，按 D4=C 借号并重绑，不返回 429（仅整池无可用账号时才 429）。
- **示例 5（models-only 回退，防止静默失效）**：A 无 groups 事实、models 平均剩余 0.8 $\Rightarrow p=0.2$；B 无 groups 事实、models 平均剩余 0.4 $\Rightarrow p=0.6$。没有该回退时二者都会是常量 0.5，排序退化为仅看 load/penalty。

---

## 8. 系统配置契约、落地点与热更新规范

### 8.1 配置数据契约
在 `system_settings.antigravity_config` 对应结构体 [auth/antigravity_settings.go](<auth/antigravity_settings.go>) 中扩展：
```go
type AntigravitySettings struct {
    ModelRedirects              map[string]string `json:"model_redirects,omitempty"`
    RedirectOverridesEffort     bool              `json:"redirect_overrides_effort,omitempty"`
    ExposeThoughts              bool              `json:"expose_thoughts,omitempty"`
    QuotaRefreshEnabled         *bool             `json:"quota_refresh_enabled,omitempty"`           // 缺省 true
    QuotaRefreshIntervalMinutes *int              `json:"quota_refresh_interval_minutes,omitempty"` // 缺省 60，范围 [3, 60]
    QuotaGateEnabled            *bool             `json:"quota_gate_enabled,omitempty"`             // 缺省 true（2026-10-21 已确认，D1=B）；独立硬门禁止血开关
    ConfigRevision             int64             `json:"config_revision,omitempty"`               // 单调递增，用于跨实例拒旧
}
```
- **`quota_gate_enabled` 语义（本轮新增）**：仅控制"是否用配额事实阻断调度与影响软评分"，不改写、不删除已落库快照，不影响本地 TTL/reset 到期维护，不影响 `quota_refresh_enabled` 的采集行为。关闭后硬门禁立即失效（用于契约异常时止血），重新开启则按现有事实立即生效。该开关与"硬门禁即时生效"不冲突：即时性描述的是无防抖延迟，不是不可关闭。

### 8.2 API 交互、前端界面与并发安全保存（落地要求）
- **GET 请求**（`/api/admin/settings/antigravity`）：返回当前值及默认值回退（`quota_refresh_enabled=true, interval=60, quota_gate_enabled=true`）。
- **PUT 接口必须采用 CAS 合并保存（修正现有内存覆盖缺陷）**：
  1. 校验请求 presence、类型与范围；仅含新 quota 字段也合法。新增字段缺失保留，null 重置默认（enabled=true、interval=60、gate=true），false 必须保留。非法类型或越界返回 400，不保存、不唤醒。
  2. 由于 `system_settings` **没有 revision/updated_at 列**（[database/sqlite.go:278](<database/sqlite.go#L278>)、[database/postgres.go:1347](<database/postgres.go#L1347>)），采用**整 JSON CAS + 内嵌单调 `config_revision`**，无需新增列或迁移：先读当前 `antigravity_config`，在 Go 侧合并出现字段并 `revision+1`，执行 `UPDATE system_settings SET antigravity_config=$new WHERE id=1 AND antigravity_config=$old`；影响 0 行即重读重试（有界重试 3 次后返回 409）。不使用本机旧缓存覆盖其他配置，也不先无锁读后整包 Save。
  3. 成功提交返回该事务生成的新 `config_revision` 与完整生效配置，本机按 revision 单调应用后再唤醒；失败不发布。远端 consumer 也按 revision 拒旧（设置事务本身已触发 `scheduler_outbox_settings_update`，见 [database/scheduler_outbox.go:274](<database/scheduler_outbox.go#L274>)）。若坚持使用独立 `settings_revision` 列，则必须配 SQLite/PG 迁移并在 §9 落点中补列，不能只写"revision"而不落数据模型。
  4. Parse/Normalize/Copy/Encode 包含新增字段，空对象早退不能丢失显式 false 或非默认 interval；null 重置不顺带改变旧字段 API。
  5. **revision 回退检测（本轮新增）**：consumer 与本机应用侧都必须区分"更旧"与"缺失"。读到 `config_revision` 缺失或小于本机已应用值时，**不得静默拒绝后续所有更新**，而应记录 `antigravity_config_revision_rollback` 告警、把已应用基线抬到 `max(本机值, DB 值) + 1` 并强制应用 DB 内容，避免混布窗口把配置清零后新副本永久拒绝合法更新（成因见 §3.1 混合版本写入危害）。基线重建后，若本机仍持有被旧副本抹掉的 `quota_*` 字段，必须以一次受控整 JSON CAS 把它们合并回 DB 并告警——否则止血开关的丢失会被"缺失即默认 true"掩盖，形成无痕的字段级回退。
- **Settings Outbox Consumer 明确增补落地（修正现有缺失）**：
  - 改造 [auth/scheduler_outbox_consumer.go](<auth/scheduler_outbox_consumer.go>)，在处理 settings 事件时补齐 `reloadAntigravityConfig(ctx)`：从 DB 读取最新配置、归一化、版本单调比对并调用 `auth.SetConfiguredAntigravitySettings`，唤醒本地调度器。
- **前端设置页面**：Admin 控制台设置页（[frontend/src/pages/Settings.tsx](<frontend/src/pages/Settings.tsx>)）Antigravity 分区新增独立卡片，提供自动刷新开关、硬门禁开关与 3..60 分钟数字输入框；保存成功后本机 $\le 1\text{s}$ 重排，跨副本通过 outbox 正常 $\le 5\text{s}$ 生效。界面明确提示“关闭仅停止自动配额查询，模型目录维护仍独立”。
- **运维止血操作（本轮修订：补齐并发失效屏障）**：提供 `POST /api/admin/antigravity/quota/clear`（支持按账号 ID 批量，最多 100 个），语义为"在**同一短事务内**删除该账号所有 quota source 事实与定时器、**递增 `quota_epoch`**、清空门禁，保留凭据与 models"，写入审计日志并触发一次优先补查；用于真实契约异常或误封时立即止血。**递增 epoch 是本操作不可省略的组成部分**：clear 保留凭据，generation 与 scope 都不变，若只删事实而不设屏障，则清除前已联网的在途探测会以"generation/scope 仍匹配 + 更大 `mySeq`"合法提交，把刚解除的封禁原样写回，甚至反杀 clear 自己触发的优先补查（详见 §4.1 失效纪元）。空事实清除必须携带新 epoch 才能单调覆盖远端副本内存中的旧 `known_zero`（§4.3）。前端在账号详情与批量操作中提供入口（二次确认）。该操作与 `quota_gate_enabled=false` 的区别：前者清除事实、后者仅暂停判定，二者都必须可用。

### 8.3 启停与变更联动
- **关闭行为（修订）**：关闭 `quota_refresh_enabled` 仅停止新自动配额查询并取消 groups 流待执行槽位，重新武装共享 timer 到其他仍启用任务；已生效的硬门禁按 §3.3 继续有效（`resetAt` 到期解除、无 reset 按 TTL 兜底），**不会**因关闭采集而静默放行；内存每桶 reset 与 TTL/stale 维护持续运行；模型目录 30min 刷新仍独立分散运行；管理员手动同步保持可用。
- **门禁止血**：`quota_gate_enabled=false` 立即停止门禁与软评分使用配额事实（不删数据），用于契约异常时快速恢复调度；重新开启按现有事实立即生效。门禁默认 `true`（D1=B）且**无影子期**（D2=B），因此首次上线前必须完成 §10.1 #27 的真实脱敏 payload 验证；上线后以本开关与清除事实作为唯一兜底。
- **间隔变更与账号变动**：保存新间隔触发重排防抖，新观测写入新 TTL；账号增删或状态变更触发防抖合并与即时重排。

---

## 9. 涉及代码落点清单

- **客户端与核心类型**：
  - [auth/antigravity_client.go](<auth/antigravity_client.go>)：改造透传 status 与 `Retry-After`；实现 `fetchQuotaSummary` 四态判定；`Sync` 记录子调用时间戳、presence 与 scope；`ResetTime` 解析增加时区与"合理上限"校验（§3.3）。
  - [auth/antigravity_types.go](<auth/antigravity_types.go>)：**就地扩展**现有 `AntigravityQuotaSnapshot` / `AntigravityQuotaBucket` / `AntigravityModelQuota`（`remaining_fraction` → `*float64` + `fraction_present` + `source_metadata` + `schema_version`），不新增并行类型；同步修正全仓读点（见 §3.1）。
  - [auth/antigravity_settings.go](<auth/antigravity_settings.go>)：扩展配置字段（含 `quota_gate_enabled`、`config_revision`）、解析、归一化与 deep copy。
  - `auth/clock.go`（*拟新增*）：可注入时钟 seam。当前 auth 侧**没有** Clock 抽象/FakeClock 基建，§10 的 FakeClock 用例依赖此改造。
- **调度协调、防抖与门禁**：
  - `auth/antigravity_coordinator.go`（*拟新增*）：Store 级 AG 控制面执行协调入口、单账号 `controlPlaneFlight` 单飞、**后台 4 许可 + 交互通道**（§4.2）与操作优先级队列。
  - `auth/antigravity_quota_refresh.go`（*拟新增*）：配额流（$I/N$）与目录流（$C/N$）的双流连续均匀平滑派发最小堆调度器，含 stale/reset 到期优先补查。
  - `auth/antigravity_quota_debounce.go`（*拟新增*）：计划重排防抖（100ms/1s）；软权重防抖按 §7.5.2 的建议实现为读时计算 + 可选缓存，若保留状态机则只作为派生缓存。
  - [auth/antigravity_balance.go](<auth/antigravity_balance.go>)：以 `quotaPressure` 替换 `usage` 打分项（复用 `AntigravityThrottleWeights` 的 usage 权重位，见 [auth/antigravity_ratelimit.go:53](<auth/antigravity_ratelimit.go#L53>)）；保留 load、penalty、分层与 tie-break；读锁原子读取。
  - [auth/scheduling_usage_key.go](<auth/scheduling_usage_key.go>)：保持既有数值聚合/排序公式不变，新增 AG Fresh pressure helper（含 models 回退）；V2 时效在 groups 存在时用 groups 元数据，否则用 models 元数据，models-only 更新不能通过总 `UpdatedAt` 给旧 groups 续命；前端按来源显示新鲜度。
  - [auth/store.go](<auth/store.go>)：挂载协调器与生命周期，在 `isAvailableLocked`、`takeByIDModeWithCapacity` 及 slot acquire、reclaim 成功返回与 401 窄恢复之后植入线性化硬门禁，续链旁路不得豁免；新增 `ErrQuotaExhausted`、429/`Retry-After` 映射与门禁状态变化时的 `notifySchedulerAvailability`；`isAvailableLocked` 等底层判定返回**类型化阻断原因**（配额/容量/鉴权可区分）供 proxy 分流，不得只返回 nil；新增 `Store.SyncAntigravityAccountForAdmin` 供管理端统一纳管。
  - [auth/fast_scheduler.go](<auth/fast_scheduler.go>)：候选过滤与 acquire 线性化门禁。
  - [auth/antigravity_catalog.go](<auth/antigravity_catalog.go>)：废除独立 4-worker 批池，接入统一协调器与 per-source 协议，平滑派发且不洗新 groups。
- **存储与 Outbox 同步**：
  - `database/antigravity_quota_source.go`（*拟新增*）：DB 无关 per-source 短事务 CAS 预约与提交协议（**逐 source 紧贴子请求预约**、Latest-Success-Wins）、`quota_epoch` 失效纪元（credentials JSON 的 `antigravity_quota_epoch` 键，与 `antigravity_quota` 同事务）的持久化与三方校验（预约绑定/提交校验/内存投影），支持 UpdateCAS 同事务受控 carryforward、ReplaceCAS 身份重置与 clear 同事务递增 epoch。
  - [database/grok_state.go](<database/grok_state.go>)：`UpdateAccountCredentialsCAS` 是 Codex/Grok/AG 共用实现，AG carryforward 必须按 `upstream_type` 条件化（或加 AG 专用包装），不得改变 Grok 语义；`MergeAccountCredentialsForGeneration` 的允许键需同步纳入 V2 字段。
  - [database/scheduler_outbox.go](<database/scheduler_outbox.go>)：账号触发器补齐 `antigravity_quota` **与 `credential_generation`**（SQLite/PG 双方言）。
  - [auth/scheduler_outbox_consumer.go](<auth/scheduler_outbox_consumer.go>)：增补 `antigravity_config` 重载与已有 Account 对象的配额投影（含门禁即时重算）。
  - [database/antigravity_settings.go](<database/antigravity_settings.go>)：新增整 JSON CAS 保存 helper（`SaveAntigravityConfigCAS(ctx, oldRaw, newRaw)`），沿用 `withSQLiteWriteLock`。
- **管理层与代理层**：
  - [admin/antigravity_settings.go](<admin/antigravity_settings.go>)：PUT 接口实现基于整 JSON CAS 的 merge-and-save（§8.2.2），不读本机缓存作为基准。
  - [admin/antigravity_accounts.go](<admin/antigravity_accounts.go>)：手工同步改为调用 Store 统一入口（不再直接 `client.Sync`），接入 family lease、有界等待与 per-source 协议；新增 `POST /api/admin/antigravity/quota/clear`（§8.2 止血操作，**同事务递增 `quota_epoch`**）。
  - [admin/antigravity_state.go](<admin/antigravity_state.go>)：状态展示适配四态、`stale` 标记与 metadata。
  - [proxy/handler_gemini.go](<proxy/handler_gemini.go>)、[proxy/errors.go](<proxy/errors.go>)、[proxy/retry_exclusions.go](<proxy/retry_exclusions.go>) 与 [proxy/scheduler_queue_error.go](<proxy/scheduler_queue_error.go>)：`ErrQuotaExhausted` 映射为 429 + `Retry-After`；`nextRetryAccountWithGuard` 依类型化阻断原因跳过队列等待（配额阻断不得进入 30s 等待），`schedulerSelectionAPIError`/`writeSchedulerQueueError` 增补 429 分支作为四协议共用出口；countTokens **仅**在候选集合全部因配额阻断时返回 429，其余无号成因保留既有 [estimateInputTokens](<proxy/count_tokens.go#L31-L43>) 回退（事实更正见 §6.2 #3）。
  - [frontend/src/pages/Settings.tsx](<frontend/src/pages/Settings.tsx>)、[frontend/src/api.ts](<frontend/src/api.ts>) 与 [frontend/src/types.ts](<frontend/src/types.ts>)：配置卡片（含门禁开关）、清除配额事实入口、`remaining_fraction` 可空与 `fraction_present` 类型适配。
- **测试覆盖（拟新增与既有）**：
  - `auth/antigravity_coordinator_test.go`（*拟新增*）：后台 4 许可、交互通道不排队、优先级抢占与 controlPlaneFlight。
  - `auth/antigravity_quota_refresh_test.go`（*拟新增*）：连续均匀分散调度器、名义槽位计算、退避与 stale 补查。
  - `auth/antigravity_quota_dispatch_test.go`（*拟新增*）：全路径硬门禁、429/`Retry-After` 契约与 strict 硬零借号语义（D4=C）；配额阻断不进入队列等待、四协议 429 出口与续链 `quota_gate_bound`（§6.2 #2）。
  - `auth/antigravity_balance_quota_test.go`（*拟新增*）：软打分替换 usage、1:1 权重、absent 省略、中性分与 models 回退。
  - `auth/antigravity_quota_debounce_test.go`（*拟新增*）：FakeClock 下重排防抖与（若保留）权重防抖。
  - `auth/antigravity_quota_gate_test.go`（*拟新增*）：已知零 TTL 不解除、reset 到期解除、stale 标记、门禁止血开关与清除事实。
  - `database/antigravity_quota_source_test.go`（*拟新增*）：短事务 CAS 预约、Latest-Success-Wins、受控 carryforward 与身份重置；逐 source 预约与 `observed_at` 同序、`quota_epoch` 屏障（迟到响应不得复活已清除事实）、RT 冲突三分支（§4.1 #5）。
  - `database/scheduler_outbox_test.go`（*拟新增或扩展*）：`antigravity_quota` 与 `credential_generation` 变化的触发器事件（SQLite/PG 双方言）。
  - [auth/antigravity_settings_test.go](<auth/antigravity_settings_test.go>)：配置解析、归一化、整 JSON CAS 并发保存与 revision 单调。
  - [auth/antigravity_client_test.go](<auth/antigravity_client_test.go>)：四态解析、白名单识别与 `reset_time` 校验。

---

## 10. 验收测试矩阵与验证规范

### 10.1 验收测试矩阵
1. **数值解析四态覆盖**：
   - *Given*: 模拟上游返回各类 fraction 数值；
   - *When*: 调用 V2 parser；
   - *Then*: 合法正数、极小正数（0.001）为 `known_positive`；0.0 经 presence 为 `known_zero`；非法值（<0, >1, NaN, Inf, 缺失）为 `unknown`。
2. **旧 Schema 兼容防误封**：
   - *Given*: 存在 legacy 字段无 `schema_version = 2` 或无 metadata 的旧快照，且 fraction 记录为 0.0；
   - *When*: 调度器执行门禁判定；
   - *Then*: 判定为不阻断，仅供前端展示与软排序，绝不触发硬回避。
3. **结构残缺与坏桶隔离**：
   - *Given*: 上游响应返回 200 但 body 为空或 groups 字段丢失，或部分桶 fraction 损坏；
   - *When*: 执行结果提交短事务；
   - *Then*: 损坏桶覆盖为 unknown 且不复活旧零；整体残缺时不推进 last_success_at 并保留历史可信快照至 TTL。
4. **Weekly 为 0 且无 5h 阻断**：
   - *Given*: 账号经验证周配额归零且无 5h 限制；
   - *When*: 业务请求到达进入各调度器选号；
   - *Then*: 全路径判定 `quota_exhausted`，整账号立即硬回避。
5. **5h 为 0 且 Weekly > 0 阻断**：
   - *Given*: 账号周配额充足，但 5h 窗口确认存在且归零；
   - *When*: 业务请求到达；
   - *Then*: 整账号立即硬回避，不因周配额充足而放行。
6. **窗口白名单识别**：
   - *Given*: 上游返回包含 "5h", "weekly" 以及未定义窗口如 "daily"；
   - *When*: 解析窗口并匹配门禁；
   - *Then*: 仅白名单内的 5h 和 weekly 参与门禁判定，未定义窗口严格忽略。
7. **已知零的 reset 到期与 TTL 兜底（本轮修订）**：
   - *Given*: 桶为 `known_zero`、`resetAt` 在 10 分钟后、TTL 为 2 小时；
   - *When*: 时间推进过 10 分钟；
   - *Then*: 仅该桶因窗口滚动转 unknown；没有其他有效 weekly/5h 零桶时才解除本配额门禁，其他 429/管理限制不变；interval 不延长旧桶寿命。
   - *And*: 若 `resetAt` 在 3 天后，时间推进 2 小时（TTL 到点）时**仍须阻断**，只把桶标记为 `stale` 并排入优先补查；TTL 不得成为放行条件。
8. **Catalog 与 Summary 共享并发与平滑派发**：
   - *Given*: 注册 $N$ 个 AG 账号，catalog 周期 30m，summary 周期 60m；
   - *When*: 调度器稳态运行；
   - *Then*: 后台两流并发总和严格 $\le 4$（交互通道不计入该上限）；catalog 按 $C/N$、summary 按 $I/N$ 均匀派发；catalog 成功绝不推进 groups 时效。
9. **Manual / 401 成功不重复 Summary**：
   - *Given*: 管理员点击手工刷新或请求遇 401 完成恢复并取得有效 summary；
   - *When*: 成功落库后；
   - *Then*: 同事务提交新 gen 并消费本地该账号本轮 slot，不发起无条件二次网络 summary。
10. **同身份 UpdateCAS 受控 Carryforward**：
    - *Given*: 账号执行常规例行 token 轮换，verified subject/project/family/scope 均未变；
    - *When*: 执行 UpdateCAS generation 递增；
    - *Then*: 同事务继承旧有效 known_zero 与时效，无调度阻断空窗；若为 ReplaceCAS 或主体改变则彻底清空。
11. **单账号真网络单飞与防死锁**：
    - *Given*: 某账号后台配额刷新正在联网；
    - *When*: 管理员同时触发 manual 同步，且请求中途遇到 401 恢复；
    - *Then*: 同账号共用 `controlPlaneFlight` 单飞，manual 有界等待；同一 owner token 承载 401 恢复不产生嵌套死锁。
12. **Lazy 模式与配额开关矩阵**：
    - *Given*: 系统配置 lazy 模式或配额开关关闭；
    - *When*: 检查调度器行为；
    - *Then*: lazy 下保持低频配额观测与必要恢复；配额开关关闭仅停止自动 groups 查询，本地 reset/TTL/stale 维护与门禁常驻，catalog 独立运行。
13. **Settings PUT 并发 CAS 与 Outbox 真生效**：
    - *Given*: 管理员 A 与 B 并发修改 interval 与 redirects；
    - *When*: 触发 PUT 接口；
    - *Then*: 整 JSON CAS（`WHERE antigravity_config = old`）保证无覆盖丢失，0 行即重读重试并有界失败；本机按 `config_revision` 单调应用；远端副本经 settings outbox consumer 真实重载配置并重排。
14. **重启 Fresh Coverage 与槽位保护**：
    - *Given*: 实例重启；
    - *When*: 调度器启动初始化最小堆；
    - *Then*: 用持久化observedAt/lastSuccessAt恢复覆盖窗，lastAttemptAt恢复等待锚与失败轮次，notBefore恢复退避；fresh已覆盖不重复联网，已执行失败轮不重放。
15. **安全换票 CriticalContext 4min 例外与优雅停机**：
    - *Given*: 探测任务遇到 401 触发换票临界区；
    - *When*: 普通 caller 30s 超时或服务发出 SIGTERM；
    - *Then*: 换票在 CriticalContext 下允许最长 4min lease hold 收尾落库；Store.Stop 等待 join 安全退出。
16. **多实例局部 Round 独立性**：
    - *Given*: 多节点集群部署；
    - *When*: 各节点启动配额调度器；
    - *Then*: 各实例独立随机初始化相位 $\theta$ 错峰运行，局部维护槽位消费，不设跨副本 slot 账本，通过 observationID 幂等去重。
17. **Sync Final-Bearer 观测原子提交**：
    - *Given*: Sync 在取得最终 token 后获取 summary；
    - *When*: 提交短事务；
    - *Then*: 新凭据与有效观测在同一短事务内提交并关联新 gen；更旧的**成功** `mySeq <= applied_seq` 或过期 gen 被拒；后发**失败**不影响先发成功的提交（Latest-Success-Wins）。
18. **Strict 会话遇硬零的既有语义（D4=C）**：
    - *Given*: strict 粘性会话已绑定账号 A，此时 A 因配额硬零被门禁拒绝，且池内仍有账号 B 可用；
    - *When*: 会话发起**新请求**（即 `preserveBinding=false`；`preserveBinding=true` 的续链场景见 #38，二者互不替代）；
    - *Then*: 按既有语义借号并重绑到 B，不返回 429；健康且未被硬零拒绝的绑定账号仍直接复用、完全忽略软权重；仅当整池无可用账号时才返回 429 + `Retry-After`。
19. **配额软权重替换 usage 与防双罚**：
    - *Given*: 开启 AG 加权选号；
    - *When*: 计算候选账号 score；
    - *Then*: `quotaPressure` 完全替换原 usage 项，周与 5h 1:1 加权；`absent`/存在性未知省略而非 0.5 惩罚；仅"证实存在但数值不可用"计 0.5；groups 全不可用时回退 models 压力，绝不出现恒定 0.5。
20. **双重防抖与硬状态即刻生效**：
    - *Given*: 账号配额由正变 0，或发生微变 0.81 -> 0.815；
    - *When*: 观测发布到内存；
    - *Then*: 变为 0 时硬门禁即刻生效并原子取消旧 timer，不等待任何防抖；重排事件合并在 100ms/1s 内完成；若保留权重防抖则微变在 1s/5s 后发布，若采用 §7.5.2 的读时计算则直接验证下一次选号即使用最新值。
21. **Filter 单次求值与 Handled 保护**：
    - *Given*: Fresh 选号流程；
    - *When*: 遍历候选快照；
    - *Then*: 每个候选账号至多求值一次 filter；加权接管后若无可用账号直接返回 nil，不回退重扫。
22. **FakeClock 稳态分布与防雪崩**：
    - *Given*: FakeClock 推进时间，设置不同账号数量 $N$；
    - *When*: 调度器运行；
    - *Then*: $N=0$ 空闲挂起；$N=1$ 严格按 $I$ 触发；仅在FakeClock理想稳态、无退避/无重排/总容量充足且精度足够时按来源分别验证Δ±ε；不要求叠加双流HTTP严格等距，启动重排不全池补刷。
23. **FakeClock Churn 扰动与防饥饿**：
    - *Given*: 每 200ms 动态加入新账号；
    - *When*: 调度器重排；
    - *Then*: 即时stub、无退避且总共享容量充足时既有账号间隔≤2I（models用2C）；未尝试用首次入池锚，epoch/phase不重置，重排不能后移期限；过载记录lag/skipped/等待年龄并公平顺延。
24. **配置缩短专项**：
    - *Given*: 管理员将间隔从 60 分钟修改为 3 分钟；
    - *When*: 保存配置；
    - *Then*: gate 一次性收紧，新派发不晚于生效后 $2I$，不得集中补发历史任务。
25. **原生 Gemini 与 countTokens 配额阻断行为（本轮修订）**：
    - *Given*: 本次请求候选集合内的账号全部因配额硬回避被阻断；
    - *When*: 收到 native countTokens 请求；
    - *Then*: 严禁突破门禁发送网络请求；与生成一致返回 429（含 `Retry-After`），**不得**落入既有的 `200 + estimateInputTokens` 静默回退（D8=A 是对既有行为的变更，而非"未实现估算器"，见 §6.2 #3 事实更正）；
    - *And*: 候选集合并非全部因配额阻断的其他无号成因（选号超时、队列准入拒绝、上游 404、不可重试传输错误）保持既有回退行为不变。
26. **异常状态保留**：
    - *Given*: 上游探测返回 403/404/5xx 或网络超时；
    - *When*: 记录诊断；
    - *Then*: 计入退避重试，不作为配额归零依据；历史有效零继续阻断直至 `resetAt`（无可信 reset 时至 TTL 兜底）。
27. **脱敏真实验证作为唯一上线门槛（D2=B）**：
    - *Given*: 准备上线发布，且门禁默认开启（D1=B）；
    - *When*: 执行发布前验收核对；
    - *Then*: 必须完成真实 Google 脱敏 payload 验证（独立授权事项），并留下以下证据：(a) 至少一份真实 `retrieveUserQuotaSummary` 成功响应（脱敏），确认 `groups[]/buckets[]/window/reset_time` 的实际语义与格式；(b) 实际出现的窗口字符串全量清单，核对 §3.2.3 白名单是否覆盖 `weekly`/`5h` 的线上写法；(c) `remainingFraction` 的 presence 缺失情况与取值分布；(d) 若验证不通过，则门禁必须保持关闭，不得以"影子期观察"替代（本规格不设影子期）。
28. **Latest-Success-Wins 反例**：
    - *Given*: A 00:02 发起（慢）、B 00:05 发起并失败、A 00:07 成功；
    - *When*: A 提交；
    - *Then*: A 的成功被接受并推进 `applied_seq`；B 的失败只更新诊断，不得否决 A；随后的更旧成功被拒。
29. **已知零 TTL 不静默放行（stale）**：
    - *Given*: weekly known_zero、`resetAt` 在 3 天后、TTL 2 小时、无新观测；
    - *When*: 时间推进 2 小时以上；
    - *Then*: 仍阻断调度，桶标记 `stale`、计数并排入优先补查；`quota_gate_enabled=false` 时才立即恢复调度。
30. **门禁止血开关与清除事实**：
    - *Given*: 契约异常导致批量误封；
    - *When*: 关闭 `quota_gate_enabled` 或调用 `/api/admin/antigravity/quota/clear`；
    - *Then*: 前者立即停止门禁与软评分使用事实但不删数据；后者清空该账号事实、门禁与定时器并触发补查；两者均写入审计与指标，且不影响凭据与 models。
31. **`ErrQuotaExhausted` → 429 契约**：
    - *Given*: 全部候选账号（含当前绑定账号）均因配额阻断；
    - *When*: 发起请求；
    - *Then*: 返回 429 + `Retry-After`（取最早可信 resetAt，上限 3600s，无 reset 取 60s），不进入长时间等待队列，不误报 503，并在指标中标记 `reason=quota_gate`。
32. **models-only 与 groups 缺失回退**：
    - *Given*: 账号无 summary/groups，仅有 models 剩余比例；
    - *When*: 计算 quotaPressure；
    - *Then*: 使用 models 平均剩余（只计 presence 有效条目），结果随数值变化而非恒定 0.5；models 也缺失时才取中性 0.5。
33. **outbox 覆盖 quota、generation 与 epoch 变化（双方言）**：
    - *Given*: 仅 `antigravity_quota` 变化；仅 `credential_generation` 变化而 token 未变；仅 `quota_epoch` 变化而 quota 内容等价重写（clear 场景）；
    - *When*: 提交事务；
    - *Then*: SQLite 与 PG 均产生 account 事件，consumer 投影新事实与 epoch 并立即重算门禁；**仅 epoch 变化时也必须投影**，否则远端内存 epoch 落后，其后续新观测会在本地发布阶段被单调规则丢弃；两种方言各一条触发器测试。
34. **混合版本与回滚**：
    - *Given*: 旧副本读取 V2 快照（含 `remaining_fraction: null`）；
    - *When*: 参与选号；
    - *Then*: 解码不报错、仅按 0 参与软排序，不执行硬门禁；发布说明注明"先升级全量副本，再依赖门禁生效"（默认 true，D1=B）。
35. **退避在默认间隔下真实生效**：
    - *Given*: `I=60min` 且连续失败 k=1..3；
    - *When*: 计算 `not_before`；
    - *Then*: 退避为 2h/4h 封顶（而非恒定 60min），且不与 reset 到期优先补查冲突（优先补查仍受 `not_before` 约束）。
36. **清除事实对迟到探测的屏障（本轮新增）**：
    - *Given*: 账号 weekly 已确认归零并正被硬门禁阻断，某实例已对 groups 预约并联网（`mySeq=8`）；管理员在其响应返回之前调用 `/quota/clear` 并完成清除；
    - *When*: 迟到的零余额响应尝试提交；
    - *Then*: 因 `quota_epoch` 不匹配被拒，门禁保持已解除状态；clear 触发的优先补查（新 epoch 下 `mySeq=1`）正常提交成功——**旧 epoch 的更大序号不得否决新 epoch 的观测**；本机与远端副本内存均被携带新 epoch 的空事实覆盖，不残留 `known_zero`。
37. **逐 source 预约与观测同序（本轮新增）**：
    - *Given*: 副本 A 执行 `Sync`（identity/models 较慢，于 T+100s 才发起 summary），副本 B 于 T+50s 独立发起周期 summary 并已提交正值；
    - *When*: A 随后取回零余额并提交；
    - *Then*: A 的 `mySeq` 大于 B 已落库的 `applied_seq` 而被接受（更晚发起的成功观测胜出）；确认预约发生在各子请求联网前，且序号序与 `observed_at` 同序，不存在"先预约、后观测"导致的合法拒绝。
38. **配额阻断不进入队列等待与 429 传播（本轮新增）**：
    - *Given*: 本次请求候选集合全部因配额阻断；
    - *When*: 经 `nextRetryAccountWithGuard` 选号；
    - *Then*: 不进入 `waitForRetryAccountAvailableWithGuard` 的 30s 队列等待；`schedulerSelectionAPIError` 识别 `ErrQuotaExhausted`，`writeSchedulerQueueError` 在 Gemini/Anthropic/Chat/Responses 四协议下写出 429 + `Retry-After` 与 `reason=quota_gate`；混合原因场景仍按 503/既有路径处理；续链 `preserveBinding=true` 遇绑定账号被阻断时保绑定、不重绑，返回 `reason=quota_gate_bound` 的 429；全程不得对候选集合二次求值 filter。
39. **RT 轮换冲突三分支（本轮新增）**：
    - *Given*: `Sync` 已消费 Google RT 轮换，凭据 CAS 分别因 (a) durable winner 已含更新的 access token、(b) durable 行仍等于本次读到的预轮换凭据、(c) family/subject 已改变或账号已删除 三种原因失败；
    - *When*: 按 §4.1 #5 分流处置；
    - *Then*: (a) 信任并 reload winner，绝不回写本次 token；(b) 仅在仍持 family lease 且 expected generation 未变时重放**一次不含任何 source 观测**的凭据 UpdateCAS；(c) 禁止把本次 RT 写入当前身份，按既有隔离/永久失败策略落库并写审计。三种分支均不得顺带接纳来源不匹配的配额观测。

### 10.2 验证规范与执行命令说明
*注：本轮任务仅修订设计规格文档，不执行测试构建与代码修改。未来实现后执行*：
- 后端测试：`go test -v -race ./auth ./database ./proxy ./admin`
- 前端测试：`pnpm run test` 与 `pnpm run typecheck`
- 脱敏真实验证：真实上游 payload 作为独立授权前置事项。

---

## 11. 用户已明确事项与待确认建议

### 11.1 用户已明确事项（本次已落实入规格）
1. **默认间隔与可配置**：Antigravity 配额自动刷新默认刷新间隔为 **60 分钟（1 小时）**，可在管理端设置页修改；3..60min范围仍为草案建议，不视为用户已批准上限。
2. **周期均匀铺满**：刷新触发将所有具备控制面刷新资格的可用账户尽可能均匀铺满整个刷新周期，消除突发风暴。
3. **增强现有加权选号**：周剩余与存在的 5h 剩余加入调度权重，有亲和的账户忽略此权重；增强现有加权选号，不新增 scheduler mode。
4. **两处全覆盖防抖**：防抖机制明确同时覆盖“刷新计划重排”与“额度权重更新”两处（软侧已按 §11.2 #9 采用读时计算 + 可选缓存，需求目标不变）。
5. **硬门禁即时生效**：周 0 或（存在 5h 且 5h 0）整账户硬门禁立即生效，绝不因防抖产生任何延迟。
6. **健康亲和优先**：健康亲和绑定账号优先复用，绝不因配额评分偏低而发生主动迁号或借号。

### 11.2 设计取舍确认状态（2026-10-21）
图例：**✅ 用户已确认**（括号内为所选方案）；**◻ 默认采纳**（按现写法生效，用户未提出异议）。条款正文即生效文本。

1. ✅ **整账号回避代价（A）**：上游 summary 缺乏权威 Model-to-Group 映射，任一 weekly 零桶、或（存在 5h 且 5h 零桶）即阻断整账号；接受跨模型组借道成本。
2. ◻ **独立频率与统一协调器**：保留 models 目录 30min 与 groups 配额 60min 独立周期，Store 级统一协调器 + 后台共享 4 许可（交互通道另计，见 #10），废除 catalog 独立 4-worker 池。
3. ◻ **单账号真网络单飞与有界等待**：每账号 `controlPlaneFlight`，manual 遇在途任务 90/100s 有界等待，admin 换票入 family lease。
4. ◻ **同身份 UpdateCAS 受控 Carryforward**：例行 token 轮换且已验主体/项目/auth_kind/family 未变时，同事务继承有效观测与 known_zero；ReplaceCAS/身份变动彻底失效。
5. ◻ **复用 Sync Final-Bearer 观测**：`Sync` 最终 bearer 建立后取得的真实观测可在凭据 CAS 同事务原子提交，不无条件重发 summary。
6. ◻ **防抖与软评分参数**：重排 100ms/1s；软权重 1s/5s（阈值 0.01）；1:1 平均、中性分 0.5（聚合方式见 #11）；lazy 下保留低频配额观测与必要认证恢复例外。
7. ◻ **配置保存 CAS 与 Consumer 补齐**：PUT 采用整 JSON CAS + 内嵌 `config_revision`（无需新增列），settings outbox consumer 增补 AG 配置重载。
8. ✅ **strict 硬零语义（C）**：不设"硬零禁止借号"特例；`preserveBinding=false` 的 strict/bounded/off 沿用既有借号与重绑语义，`preserveBinding=true` 的续链仍返回 nil 保绑定；硬零借号不视为评分迁号。
9. ◻ **软权重防抖形式**：降级为读时计算 + 可选 1s 缓存（去掉 per-account timer 状态机）；"两处防抖"需求目标保留，仅改变实现形式。
10. ◻ **双通道许可模型**：后台流共享 4 许可；401/lazy/manual 不占该许可，仅受 flight + family lease 约束。
11. ✅ **周/5h 聚合方式（A）**：采用 1:1 平均；显式接受瓶颈稀释（周剩 .01 + 5h 剩 1 → 压力 .495），不采用 `max` 或按窗口长度加权。
12. ✅ **countTokens 无可用账号行为（A）**：与生成一致返回 429 + `Retry-After`；**不沿用**既有 [estimateInputTokens](<proxy/count_tokens.go#L31-L43>) 的 `200 + 估算值` 回退——估算器本身已存在且当前在用，本决议只改变"候选集合全部因配额阻断"这一分支的对外状态，其余无号成因保留现状（事实更正与范围见 §6.2 #3、§10.1 #25）。
13. ◻ **门禁 kill switch 与清除事实**：新增 `quota_gate_enabled` 与 `POST /api/admin/antigravity/quota/clear`，作为契约异常时的两条独立止血路径。
14. ✅ **门禁首版默认值（B）**：`quota_gate_enabled` 默认 `true`，升级完成即生效；滚动升级期间旧副本不门禁，发布说明注明"先升级全量副本再依赖门禁生效"。
15. ✅ **不设影子期（D2=B）**：不做"只观测不阻断"阶段；以 §10.1 #27 的真实脱敏 payload 验证（含窗口字符串清单与 presence 分布）作为门禁默认开启前的唯一门槛，验证不通过则门禁保持关闭。
16. ◻ **续链遇配额阻断的对外状态**：`preserveBinding=true` 的续链绑定账号被硬门禁阻断时保留绑定、不借号、不进入队列等待，返回 429 + `Retry-After` 且 `reason=quota_gate_bound`；与整池耗尽（`reason=quota_gate`）和容量类 503 三者可区分（§6.2 #2、§6.3、§10.1 #38）。
17. ◻ **RT 冲突第二分支的受控补写**：`Sync` 已消费 RT 轮换且 CAS 失败时，若 reload 显示持久化行仍等于预轮换凭据（无人抢先更新），在仍持 family lease 且 expected generation 未变的前提下允许**一次**仅含凭据字段的 UpdateCAS 重放，不携带任何 source 观测；winner 已更新或谱系断裂两分支一律禁止回写（§4.1 #5、§10.1 #39）。

---

## 12. 第一轮审查修订记录（2026-10-21）

| 审查意见项 | 核心缺陷与问题描述 | 本次修订解决处与落地策略 |
| :--- | :--- | :--- |
| **A. 统筹现有刷新策略** | 原草案将 quota 作为孤立循环，未统筹既有 30m 目录、401 恢复、lazy 及手动刷新，存在并发风暴与旧 groups 洗新风险 | 建立 §5.1“现有刷新策略协调矩阵”；废除 catalog 独立 4-worker 池，统筹入 Store 协调器与后台共享 4 许可（交互通道独立），两流独立平滑派发且不洗新（§5.1, §5.2） |
| **B. 真网络单飞与死锁规避** | 原依赖 DB CAS 防并发导致多重网络浪费；admin 手工刷新无 lease；嵌套 401 易递归死锁 | 引入 `controlPlaneFlight` 真单飞；admin 手工加入 family lease 并有界等待；同一 owner token 承载 401 恢复防死锁（§4.2） |
| **C. 代际变迁与 Carryforward** | 原规定代际改变一律作废导致 token 轮换时配额空窗；或无条件继承旧状态破坏隔离 | 建立受控 carryforward：UpdateCAS 在已验证同一身份下同事务继承有效事实与已知零；ReplaceCAS 严格失效（§4.1） |
| **D. Sync 观测原子复用** | 原规定 401 恢复副产品 summary 绝对不可用、必须无条件重发，造成重复网络请求 | 允许复用最终 bearer 下验证的观测，在凭据 CAS 同一短事务内原子提交新 gen 与有效观测，杜绝无条件二次 summary（§4.2） |
| **E. 槽位局部覆盖与重启恢复** | 原缺乏对手动/恢复观测的槽位消费界定；多实例跨副本 slot 账本不切实际 | 建立局部覆盖窗 `(due-周期, due]`，成功观测消费本地本轮 slot；重启用持久化时标恢复覆盖，不设全局 slot 账本（§4.3, §5.4） |
| **F. Lazy 与开关矩阵诚实化** | 误称 lazy 下不开巡检就永久封禁；未界定 quota 关闭时 catalog 状态 | 澄清本地到期维护与门禁常驻（已确认零按 resetAt 解除）；明确 quota_refresh 仅控制 groups，关闭后 catalog 仍独立运行；lazy 下保留低频观测与必要恢复（§3.3, §5.1, §8.3） |
| **G. 配置并发覆盖与 Consumer 落地** | admin PUT 内存 merge 后覆盖易回退并发配置；settings consumer 缺失 AG 配置重载 | PUT 改为整 JSON CAS merge-and-save；consumer 明确增补 `reloadAntigravityConfig` 落点（§8.2） |
| **H. 超时预算与生命周期诚实化** | 谎称所有操作严格受限 30s 且 30s 后仅 DB 收尾，忽视既有 4min family lease 事实 | 诚实承认严格普通预算不覆盖既有安全换票临界段，允许 CriticalContext 最长 4min lease hold 并等待 join（§5.5） |

---

## 13. 第二轮审查修订记录（2026-10-21）

> 本轮修订依据源码复核（基点 `1bdfc85c`）与设计审查结论。事实性更正直接落地；产品取舍保留为 §11.2 待确认项。

| 审查项 | 问题 | 本轮修订 |
| :--- | :--- | :--- |
| **事实：周期重建** | §1.1 #4 误称"5 分钟 reconcile" | 改为 10 分钟 FastScheduler 重建（[auth/store.go:6704](<auth/store.go#L6704>)），并补充触发器列清单、明文可见性与 consumer 未投影的事实引用（§1.1 #2） |
| **事实：并发/迁移落点** | §4.1 声称"废弃混合 SQL"但代码中不存在；AG carryforward 落在 Grok 共用函数却未列落点 | 改为"不采用上一版草案 SQL 方案"，明确沿用现有 Go 侧 merge + `FOR UPDATE`；carryforward 必须按 `upstream_type` 条件化，§9 补列 [database/grok_state.go](<database/grok_state.go>) 与 `MergeAccountCredentialsForGeneration` |
| **P0：已知零被 TTL 放行** | `valid_until = min(resetAt, ttl)` 使确认零额度 2h 后静默复活，与硬回避目标矛盾 | 改为按桶状态分流：`known_zero` 阻断至可信 `resetAt`，无 reset 才用 TTL 兜底；超龄观测标记 `stale` 并优先补查，不静默放行（§3.3、§6.1、§7.5、§10 #7/#29） |
| **P0：缺止血通道** | 契约异常时门禁无法关闭，只能改库 | 新增独立 `quota_gate_enabled`（默认 true）与 `POST /api/admin/antigravity/quota/clear`；明确二者语义与审计（§8.1-§8.3、§9、§10 #30、§11.2 #13） |
| **P0：提交条件丢数据** | `current_attempt_seq == mySeq` 让后发失败否决先发成功，已确认零被推迟一个周期 | 改为 Latest-Success-Wins：提交只要求 `mySeq > applied_seq` + gen/scope 匹配；附反例（§4.1、§10 #28） |
| **P0：错误契约缺失** | `quota_exhausted` 与 503/429 混用，且会进入长时间等待 | 定义 `ErrQuotaExhausted` → 429 + `Retry-After`；不进入 `WaitForDispatchAvailable`；绑定账号被阻断时按 D4=C 借号重绑，仅整池无号才 429；门禁变化通知等待者（§6.2-§6.3、§9、§10 #18/#31） |
| **P0：软评分丢回退** | groups 缺失时压力恒 0.5，原 usage 信号被移除，加权避让静默失效 | 新增 models 级回退（presence 感知），缺失才用 0.5；示例与验收同步（§7.2、§7.6、§10 #19/#32） |
| **P1：请求路径排队** | 401/lazy/manual 抢占后台 4 许可会给用户请求引入延迟 | 改为后台 4 许可 + 交互通道；manual 批量保持顺序；明确 100s 遇 4min 临界段的返回语义（§4.2、§5.1、§5.2、§10 #8、§11.2 #10） |
| **P1：V2 双表示** | 并行 V2 类型与现有 `quota_groups` 同键不同型会漂移 | 改为就地扩展现有类型并列出全部读点；补混合版本/回滚语义（§3.1、§9、§10 #34） |
| **P1：outbox 覆盖不足** | 只补 `antigravity_quota` 会漏掉 generation 变化 | 同时纳入 `credential_generation`，要求双方言触发器测试（§4.3、§9、§10 #33） |
| **P1：软权重防抖收益不明** | 无订阅者却引入 per-account timer 与 Stop 竞态 | 保留需求目标，建议降级为读时计算 + 可选缓存，列为待确认（§7.4、§7.5、§9、§10 #20、§11.2 #9） |
| **P1：跨包 ownership** | admin 直接 `client.Sync`，flight/许可只存在于 Store | 明确 admin 改调 `Store.SyncAntigravityAccountForAdmin`，落点入清单（§4.2、§9） |
| **P2 一致性** | weekly absent 给 0.5、优先级与等待锚冲突、I=60 退避退化、config revision 无迁移落点、**曾误称 countTokens 估算器不存在**（实际已存在，更正见 §14/§16）、FakeClock 基建缺失 | 分别修订：absent 省略、优先级类别为第一关键字、退避改 `max(4I, 60min)`、改用整 JSON CAS（无需迁移）、countTokens 配额阻断改 429 并保留其余回退、新增时钟 seam 落点（§5.2-§5.3、§7.2、§8.2、§9、§10） |
| **文档自洽性（第三轮补丁）** | 门禁默认值与"先升副本再开门禁"自相矛盾；§4.4 并发上限未随双通道更新；`summary_integrity` 无置真规则；§6.1 伪代码未反映 §3.3 分流；§12 标题与 §13 不对称 | 统一为"升级后依赖门禁生效 + 默认 true（D1=B）"；§4.4 分开表述后台/交互并发；§3.1 补 `summary_integrity` 置真规则（契约验证前恒 false，`absent` 不可达）；§6.1 指向 §3.3 分流；§12 改称"第一轮"；§11.2 新增 #14、#15 |
| **2026-10-21 决议落地** | 6 项取舍已拍板：D1=B（门禁默认 true）、D2=B（不设影子期，真实 payload 为唯一门槛）、D3=A（任一零桶即整账号阻断）、D4=C（strict 硬零沿用借号/重绑，不设禁止借号特例）、D6=A（1:1 平均）、D8=A（countTokens 返回 429） | 已改：§1.2 #6、§3.1 混合版本、§6.2.2、§6.3、§7.2、§7.6 示例 4、§8.1、§8.3、§10 #18/#25/#27/#31/#34、§11.1 #4；§11.2 全节改为"确认状态"（✅ 6 项已确认、◻ 9 项默认采纳）；其余 6 项审查修正（A1-A6）标记为已写入 |
| **复核后措辞修订（2026-10-21）** | 复核确认成立 5 处表述/提示项 | 精确化 §3.1 legacy 软排序语义、§4.2 `lazyRefreshInFlight` 不可直接复用与交互通道故障态风险、§4.4 速率为上界、§5.5 lazy 周期取 $I$（落点 §3.1/§4.2/§4.4/§5.5） |

---

## 14. 第三轮审查修订记录（2026-10-21）

> 本轮为逐条复核确认后的直接修复：除 §11.2 #16/#17 两项新增约定外，其余为事实更正与规格自洽性/并发契约闭合，**未改变任何已确认的产品取舍**。严重度经复核调整一项：原报告 P1 的"预约序与观测序逆序"仅在多副本可达，降为 P2；其余五项维持原级。

| 复核项 | 确认的问题 | 修复处与落地策略 |
| :--- | :--- | :--- |
| **P1：clear 缺并发失效屏障** | clear 保留凭据（generation/scope 不变）且只删事实，清除前已联网的在途探测以"更大 `mySeq`"合法提交并写回封禁，甚至反杀 clear 自身触发的优先补查；删除后序号空间重新初始化，旧序号反而更大 | 引入持久化 `quota_epoch` 失效纪元：预约绑定、提交校验、内存与 Outbox 投影单调；**空事实必须携带新 epoch 才生效**，否则远端内存的 `known_zero` 无法被删除动作覆盖（§3.1、§4.1 失效纪元、§4.3、§8.2、§9、§10.1 #36） |
| **P2：预约序与观测序可逆序** | 操作级一次性预约叠加 `Sync` 顺序子请求，使先预约者经慢子请求后观测，其更晚观测持较小序号而被合法拒绝。**适用范围经复核收窄**：单实例由每账号 flight 串行化，不受影响；仅多副本（§4.4 明示不宣称分布式单飞）可达 | 预约改为逐 source 紧贴其网络子请求；`observed_at` 锚定预约事务提交时刻，使序号序＝提交时刻＝观测时刻三者同序；跨副本预约在同一 DB 行锁上串行，无需比较跨节点时钟（§3.1、§4.1 #1/#4、LSW 多副本推论、§10.1 #37） |
| **P1：RT 冲突规则不自洽** | "CAS 竞争失败必须持久化新 RT" 与 "禁止更旧覆盖更新" 互阻且无判定信号（RT 不透明、不可比新旧），并且偏离现有代码行为（reload winner / `RemoveAccount`） | 定性修正为"规格指令与现有行为自相矛盾"；改为按 reload 结果三分支互斥分流：winner 已更新→信任且绝不回写；durable 仍为预轮换→持 lease 且有界一次的凭据-only 重放；身份或谱系断裂→禁止写入并隔离告警。三分支之外禁止任何覆盖写入（§4.1 #5、§10.1 #39） |
| **P1：429 传播落点缺失** | 底层选号仅返回 nil，`nextRetryAccountWithGuard` 无法区分配额与容量而进入 30s 等待，`schedulerSelectionAPIError` 不认识配额原因；§9 未列这两处共享链路 | 底层判定返回类型化阻断原因，经既有四协议共用出口统一映射 429 + `Retry-After`；补列 [proxy/retry_exclusions.go](<proxy/retry_exclusions.go>) 与 [proxy/scheduler_queue_error.go](<proxy/scheduler_queue_error.go>)；明确分类口径为"本请求候选集合"，并禁止二次求值 filter 推断原因（§6.2 #2、§9、§10.1 #38） |
| **P2：混布写侧危害** | 旧副本 PUT 以本机缓存 merge 后**无 guard 整包写库**，一次无关的 redirects 保存即可静默清除 `quota_gate_enabled=false` 止血开关（缺失即回落默认 `true`）并把 `config_revision` 归零，进而令新副本永久拒绝合法更新 | 明确"读侧 null 兼容不等于可混布"；新增发布约束（混布期冻结设置写入/仅路由到新版副本）与 revision 回退检测＋基线重建规则（§3.1、§8.2 #5） |
| **P2：估算器事实错误** | 断言"仓库当前不存在本地 token 估算器"，实际 [estimateInputTokens](<proxy/count_tokens.go#L31-L43>) 已存在，且 native countTokens 在三条路径以 `200 + 估算值` 回退 | 更正事实，并把 D8=A 重述为"仅候选集合全部因配额阻断时改返回 429"，其余无号成因保留既有回退；§6.2 #3、§9、§10.1 #25 同步 |
| **新增默认采纳项** | 续链绑定被硬门禁阻断时的对外状态未定义（既非整池 429 也非借号成功） | §11.2 #16：保绑定、不排队，返回 429 + `reason=quota_gate_bound`，与 `quota_gate`/503 三者可区分（§6.2 #2、§10.1 #38） |

---

## 15. 第三轮复核后补丁（2026-10-21）

> 第三轮修复落地后的定向复核发现：6 项修复本体成立，但引入了 1 处对外口径冲突、2 处设计闭环缺口与 4 处轻微问题。本节记录对应补丁。补丁未改变任何已确认取舍；新增约定仅 §11.2 #17。

| 复核发现 | 问题实质 | 补丁落点 |
| :--- | :--- | :--- |
| **A1（P1）续链口径冲突** | §6.2 #2 无条件写"绑定账号被阻断+池内仍有可用账号→不返回 429"，与 #16 的"`preserveBinding=true`→429"在同一场景给出互斥答案 | §6.2 #2 该条按 `preserveBinding` 分流：false 借号重绑不返回 429；true 走续链条目的 429；并声明 503 仅用于容量类与选号超时 |
| **A2（P1）§6.3 未同步** | §6.3 仍写"返回 nil 并保留绑定"并隐含"等待/503"，与 #16"不排队、立即 429"相左 | §6.3 新增"绑定账号被配额阻断的对外状态"条目：选号层 nil 保绑定 → 边缘层 429 `quota_gate_bound`，不进 30s 排队、不适用 503 |
| **B（P1）epoch 传播闭环** | epoch 存放位置未定；§4.3 触发器清单与 consumer 投影均未含 epoch；内存 epoch 落后导致观测被丢弃时无自愈路径；测试矩阵无"仅 epoch 变化"用例 | 明确 `antigravity_quota_epoch` 存于 credentials JSON（与 `antigravity_quota` 同事务）；触发器 WHEN 补 epoch；consumer 投影补 epoch 并写明自愈义务；§10.1 #33 增第三组 Given/Then |
| **C（P2）§14 自述不符** | §14 称"除 #16 外全部是事实更正"，但 §4.1 #5 第二分支受控补写是新增凭据写行为 | 受控补写登记为 §11.2 #17 ◻；§14 前言改为"除 #16/#17 两项新增约定外" |
| **D1** §2.1 #7 未同步 | 目标清单仍写"基于 `attempt_seq` 与 generation"，未反映逐 source 预约与 epoch | §2.1 #7 改写为逐 source `attempt_seq` + generation + `quota_epoch`，并点明触发器需含 epoch |
| **D2** 行(c) 引用不精确 | 以 `casErr` 的 `RemoveAccount` 行号说明"永久失败标记"，行号与行为不对应 | §4.1 #5 行(c) 分别引用 `antigravity_permanent_refresh_error` 常量（`auth/store.go:12462`）与 `RemoveAccount`（`auth/store.go:12796-12801`） |
| **D3** 混布恢复不完整 | 基线重建只解决 revision 拒旧，未处理被旧副本抹掉的 `quota_*` 字段；止血开关丢失会被默认值掩盖 | §8.2 PUT 第 5 项追加：基线重建后以受控整 JSON CAS 把本机仍持有的 `quota_*` 合并回 DB 并告警 |
| **D4** 术语分叉 | §5.4 的 `startedAt` 与 §3.1 的 `observed_at` 指同一时刻，未点明 | §5.4 覆盖窗条目内标注二者同值（均取该 source 预约事务提交时刻） |

---

## 16. 第四轮全文档源码复核补丁（2026-10-21）

> 复核范围：第三轮补丁后的 784 行版本 × 现有源码（基点 `1bdfc85c`），抽查约 60 处 `file:line` 锚点与行为断言（§1.1 七条现状、§5.1 协调矩阵九行、§7.1/§8.2/§9 落点、路由与端点语义），锚点全部命中；本节记录复核发现的 1 项 P2 与 4 项 P3。未改变任何已确认取舍。

| 复核发现 | 问题实质 | 补丁落点 |
| :--- | :--- | :--- |
| **F1（P2）§11.2 #12 与事实更正冲突** | "不实现本地近似估算"与 §6.2 #3 的"估算器已存在且在用"相矛盾；§11.2 是取舍登记处，只读该节会得到错误前提 | 改为"**不沿用**既有 `estimateInputTokens` 的 `200+估算值` 回退"，明确 D8=A 只改变"候选集合全部因配额阻断"分支，其余无号成因保留现状 |
| **F2（P3）§13 历史表断言错误前提** | 把"countTokens 估算器不存在"写成已修订的问题，历史记录留存错误 | 标注"**曾误称** countTokens 估算器不存在（实际已存在，更正见 §14/§16）"，修复列表改为"配额阻断改 429 并保留其余回退" |
| **F3（P3）§1.1 #4 "`/sync` 为全量"与路由不符** | 全量 `client.Sync` 在 `POST …/antigravity/refresh`；`POST …/sync` 是控制面状态刷新（100s）；`GET …/state` 只读本地（10s） | 按 [admin/handler.go:1184-1187](<admin/handler.go#L1184-L1187>) 实际路由改写，并补 [state.go:215/244](<admin/antigravity_state.go#L215>) 行号锚点 |
| **F4（P3）§5.2 "10s..30s 启动窗口"无源码锚点** | 现有代码只有 10s 首批目录触发（[auth/store.go:6698](<auth/store.go#L6698>)），全仓无 30s 启动窗口 | 改为"沿用现有 10s 首批节奏"，并声明**不宣称 30s 窗口**；更大错峰窗口属新调度器设计选择，须实现时显式定义 |
| **F5（P3）§10.1 #18 strict 与 preserveBinding 易混** | `DispatchPolicy`（strict/bounded/off）与 `preserveBinding` 是正交维度，读者易把"strict 粘性会话"误读为续链 | #18 的 *When* 补注"新请求即 `preserveBinding=false`；`preserveBinding=true` 续链场景见 #38，二者互不替代" |
