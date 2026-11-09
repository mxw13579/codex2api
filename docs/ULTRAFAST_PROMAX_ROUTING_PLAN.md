# Ultrafast 账号资格限制：简化方案

状态：已完成本地实现与测试；已获授权进行新号池蓝绿发布，实际部署结果以 `docs/POOL_DEPLOYMENT_HANDOFF.md` 的最新发布补记为准。

实现集中在 `proxy/ultrafast_routing.go`，复用原有选号过滤；各入口接入，执行器在 Payload 之后及实际发送前兜底，不新增配置开关或数据库迁移。

本版按用户最新说明重写，取代先前“F/U 两个互斥账号池”的错误理解。

## 1. 需求

| 账号套餐 | ultrafast | fast / priority | 默认档 |
| --- | --- | --- | --- |
| ProMax | 允许 | 允许 | 允许 |
| Pro | 不允许 | 允许 | 允许 |
| ProLite | 不允许 | 允许 | 允许 |
| Plus | 不允许 | 允许 | 允许 |
| Team、其他或未知套餐 | 不允许 | 沿用既有规则 | 沿用既有规则 |

“允许”仍受原有模型支持、Key 权限、额度、账号状态和并发限制约束。未提到的套餐不擅自新增 fast/默认权限或限制，沿用已有规则；ultrafast 一律要求明确识别为 ProMax。

核心只有一条新增限制：**ultrafast 只能选择 ProMax；fast 和默认档维持现有调度，不排除 ProMax。**

同一 API Key、同一模型、同一个号池内部完成，不要求客户端换 Key。无 ProMax 可用时等待或报错，不借 Pro/ProLite/Plus，不自动降成 fast。

## 2. 最小实现

### A. 复用请求级账号过滤

在原有候选条件上叠加：

```text
if 请求档位是 ultrafast:
    仅允许原始套餐为 ProMax 的账号
else:
    不增加套餐限制
```

使用 `Account.GetPlanType()` 及明确别名识别 ProMax；不能使用会把 ProMax 和 ProLite 都折成 Pro 的 `auth.NormalizePlanType()`。

同一过滤条件贯穿首次选号、重试、排队、粘性复用、借号、preferred 和外部指定账号。已有 `AccountFilter` 能承载这条限制，不重写调度器。

### B. 实际发送前再检查一次

统一执行路径检查最终请求体：如果将发出的档位是 ultrafast，但当前账号不是 ProMax，拒绝发送，释放本次租约。

这层覆盖 Payload 规则改档、保留账号的 WS→HTTP 回退，以及选号后套餐变化，避免只检查首次选号。

对显式请求 ultrafast，不以自动改成 fast 的方式解决冲突。Payload 规则若与该要求冲突，应明确报错；不是禁止全部 Payload 规则，也不为此引入候选账号逐个重写整个正文的系统。规则把普通请求改成 ultrafast、而已选账号不合格时，同样在本地拒绝，不能先试发给 Pro。

### C. 接入实际使用的入口并做回归

覆盖 Responses HTTP、Chat Completions、Messages 转 Codex、Responses WebSocket 每轮请求。Messages 的档位提取和最终翻译体要一致。接受或继承 ultrafast 的压缩、续想等派生路径也不能跳过资格检查；不支持此档位的路径明确拒绝，不静默忽略。

不向无法证明套餐的 relay 发送 ultrafast。fast 和默认档的既有 relay 行为不变。

## 3. 保持原样的部分

- ProMax 可以继续服务 fast 和默认档，保留现有打分、优先级、负载及会话粘性。
- 不拆 F/U 互斥池，不新增按档位划分的会话命名空间。
- 不新增独立策略平台、配置代次表、套餐能力数据库或逐请求网络探测。
- 不新增任意套餐映射 UI，不顺带改计费、普通重试预算或缓存机制。
- 复用现有套餐刷新与请求/实际/计费档位日志；必要时只补明确的拒绝原因。

## 4. 必须保留的少量边界

1. **无 ProMax 可用**：Key 被允许的账号范围内根本没有 ProMax 时立即返回 HTTP 503（OpenAI 错误码 `tier_pool_unavailable`，Messages 按 Anthropic 错误格式，WS 按错误帧），不进入上游调用；有 ProMax 但忙、冷却或额度耗尽时沿用现有有界等待/错误机制，始终不放宽套餐过滤。外部指定账号继续使用原有 pinned 错误语义，不内部换号。
2. **从绑定 Pro 的会话切到 ultrafast**：普通自包含请求可换 ProMax；依赖原账号状态且不能安全恢复的续链明确报错，不能丢历史或用 Pro 降档执行。fast/默认之间不新增限制。
3. **套餐信息**：以号池当前识别的套餐为依据，未知不能当 ProMax。发送前复检只能防止本地已知变化，不能保证上游尚未通知的套餐变化，也不能保证上游必然兑现 ultrafast。
4. **档位必须到达号池**：前置网关若删掉 tier，号池无法猜测原意。
5. **蓝绿发布**：继续使用既有平滑发布流程，不强断旧连接。旧程序未排空期间不能声称所有旧 WS 后续请求都已受新限制；不为此额外建设一套发布平台。

## 5. 最小验收清单

- 同 Key 同模型混合四类账号：ultrafast 实际发送只到 ProMax。
- fast/默认：四类账号都保留原有资格；只有 ProMax 的池仍可服务 fast/默认。
- ProMax 全满、冷却或报错：ultrafast 等待/失败，Pro 等账号的上游发送数为零。
- 粘性绑定 Pro 后请求 ultrafast：安全换号或明确续链冲突，不能绕过限制。
- 各入口、重试、preferred、pinned、WS→HTTP 复用均遵守相同判断。
- 最终 Payload 变成 ultrafast 或本地套餐改变：发前检查生效；拒绝后租约正常释放。
- fast/默认的调度、重试和缓存回归不变。

## 6. 本地验证

- `proxy/ultrafast_routing_test.go`：套餐白名单（包括 Team、Free、未知）、Key 分组/套餐权限、四个 HTTP 入口、ProMax 仍可服务 fast/默认、重试、Payload 冲突、pinned、保留租约后套餐变化、WS 每轮隔离。
- `proxy/ultrafast_continuation_test.go`：Payload 升档后的完整历史展开、丢失历史时拒绝（含 fail-open 后重试升档）、忙/冷却不借其他套餐。
- `proxy/fast_tier_transport_test.go` / `proxy/ultrafast_test.go`：加速档最终经 HTTP，routing hint 与档位一致。
- 全量 `GOMAXPROCS=2 GOGC=50 go test -p 1 -count=1 -timeout 8m ./...` 通过；最后一次续链边界补丁后，针对性测试及 `./proxy ./proxy/wsrelay` 全包测试再次通过。
- 不涉及前端改动；上述测试为本地验证，不代表生产结果，发版另行验证。

结论：这是一个**新增 ultrafast 套餐资格过滤，并在发送前兜底**的局部改动；不是两个互斥账号池的重构。
