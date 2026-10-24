# Antigravity 账户配额自动刷新与耗尽回避 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立 Store 级 Antigravity 配额自动刷新机制（默认 60min，支持 3..60min 平滑派发），在全调度路径对周配额归零或 5h 配额归零实施整账户硬回避，增强加权选号（以 quotaPressure 替换旧 usage 防双罚），建立双重防抖与多实例 CAS/Outbox 最终一致机制。

**Architecture:** 单实例单 Store 级统一控制面协调器（4 共享后台并发许可 + 独立交互通道），每账号 `controlPlaneFlight` 单飞；双流平滑派发最小堆调度器（groups $I/N$, catalog $C/N$）；逐 source 短事务 CAS 预约与提交协议（Latest-Success-Wins，`quota_epoch` 失效纪元）；内存态原子线性化硬门禁，结合 resetAt 分流与 stale 兜底；类型化阻断原因透传至 proxy 统一映射 429 + `Retry-After`。

**Tech Stack:** Go 1.2x (gin, modernc/sqlite, jackc/pgx/v5, tidwall/gjson), React + TypeScript (Vite, react-i18next).

**Spec:** `docs/superpowers/specs/2026-10-21-antigravity-quota-refresh-and-avoidance-design.md`

---

## Global Constraints & Execution Protocol

### 1. 执行角色、模型与并发上限
- **编写角色（Implementer）**：模型 `local/qwen`，推理强度 `medium`，并发上限 `1`。
- **审查角色（主力，Reviewer）**：模型 `glm-coding/glm-5.3-flash`，推理强度 `high`，并发上限 `1`。
- **审查角色（备用，Fallback Reviewer）**：模型 `deepseek-official/deepseek-flash`，并发上限 `1`。
- **全局并发与调度原则**：全局同时运行子代理总数 $\le 5$。在不产生文件/编译冲突的前提下尽量填满。
- **提示词首行强制要求**：每一份派给子代理的提示词首行必须写：
  `请使用中文思考，不允许再派发子代理`
- **单任务原则（One-Shot Subagent）**：每个子代理只执行一次任务；完成后归档，后续任务必须新建子代理，不得向已完成代理 `send_message` 追加新工作。
- **禁止轮询（No-Polling）**：派发后台子代理后，不得用 `list_agents` / `git status` / `job_list` 反复查询状态；不得使用 DSH 的目标功能进行轮询；应终止当前回合静候完成回调。仅在确实有独立可推进工作时才继续工具调用。

### 2. 编译与测试环境要求
- 在当前沙箱环境中，运行 Go 测试必须显式前缀环境变量：
  `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ...`
- 前端检查命令：`cd frontend && pnpm run typecheck && pnpm run test`。
- 每次任务完成后，必须提交 Git 记录。

### 3. Spec 核心设计契约（不可妥协约束）
- **硬门禁准则**：仅限 AG OAuth；任一未过期 weekly 零桶，或确认存在且未过期的 5h 零桶即阻断整账号；正余额判定 `valid_until = min(resetAt, ttl)`，已知零持续至可信 `resetAt`，无 resetAt 用 TTL 兜底，超龄标记 `stale` 绝不静默放行。
- **防双罚**：以 `quotaPressure` 替换原公式的 `w_u * usage` 项，绝不与旧 usage 叠加。周与 5h 按 1:1 平均；absent/存在性未知省略；中性分 0.5 仅用于存在但数值未知或全无事实；groups 缺失回退 models 级平均。
- **会话粘性**：健康亲和账号完全豁免软评分，绝不主动换号；绑定账号被硬门禁阻断时：`preserveBinding=true` 续链返回 nil 保留绑定映射 429（`reason=quota_gate_bound`），`preserveBinding=false` 沿用既有借号重绑语义（D4=C）。
- **错误映射**：候选集合全部因配额阻断返回 429 + `Retry-After`（`reason=quota_gate`），不得进入 30s 等待队列；native countTokens 仅在候选集合全部因配额阻断时返回 429，其余保留既有本地估算回退。
- **短事务与失效纪元**：逐 source 紧贴网络前预约；Latest-Success-Wins（`mySeq > applied_seq`）；凭据 JSON 内嵌 `antigravity_quota_epoch`；清除事实同事务递增 `quota_epoch`；RT 冲突按 reload winner 三分支分流。

---

## File Structure

| 文件路径 | 职责定位 |
|---|---|
| `auth/clock.go`（新建） | 可注入时钟抽象（Clock 接口、RealClock、FakeClock 基础支撑） |
| `auth/clock_test.go`（新建） | FakeClock 步进、Timer 唤醒与取消单元测试 |
| `auth/antigravity_types.go`（修改） | 就地扩展 V2 Snapshot / Bucket / ModelQuota / SourceMeta，`RemainingFraction` 改为 `*float64` + `FractionPresent` |
| `auth/scheduling_usage_key.go`（修改） | 适配 `RemainingFraction` 可空指针读点，避免空指针解引用 |
| `auth/antigravity_catalog.go`（修改） | 适配 `RemainingFraction` 指针读点与 Catalog 观测更新，不洗新 groups |
| `auth/antigravity_client.go`（修改） | `fetchQuotaSummary` 四态判定、白名单、presence、`ResetTime` 合理上限、Sync 元数据采集 |
| `auth/antigravity_client_quota_test.go`（新建） | 四态解析、坏桶隔离、白名单过滤、时区/上限单元测试 |
| `auth/antigravity_settings.go`（修改） | `AntigravitySettings` 扩展 `QuotaRefreshEnabled`、`QuotaRefreshIntervalMinutes`、`QuotaGateEnabled`、`ConfigRevision` |
| `database/antigravity_settings.go`（修改） | 新增 `SaveAntigravityConfigCAS` 整 JSON CAS 保存 helper 与 `GetAntigravityConfigRaw` 读取 helper |
| `database/antigravity_settings_cas_test.go`（新建） | 整 JSON CAS 双方言测试（过期快照不得覆盖新值） |
| `admin/antigravity_settings.go`（修改） | PUT 接口整 JSON CAS merge-and-save，单调 revision 与回退基线重建 |
| `database/antigravity_quota_source.go`（新建） | DB 无关逐 source 预约、Latest-Success-Wins 提交、UpdateCAS 受控 carryforward、清空与 `quota_epoch` 屏障、RT 冲突三分支 |
| `database/antigravity_quota_source_test.go`（新建） | 短事务 CAS 预约提交、epoch 屏障、carryforward 与 RT 冲突测试 |
| `database/grok_state.go`（修改） | `UpdateAccountCredentialsCAS` 与 `MergeAccountCredentialsForGeneration` 条件化适配 AG |
| `database/scheduler_outbox.go`（修改） | accounts 更新触发器 WHEN 条件增补 `antigravity_quota`、`credential_generation` 与 `quota_epoch`（SQLite/PG） |
| `auth/scheduler_outbox_consumer.go`（修改） | consumer 增补 `reloadAntigravityConfig` 与账号配额/epoch 内存投影及自愈 |
| `auth/antigravity_coordinator.go`（新建） | Store 级单实例协调器，后台 4 许可 + 交互通道，单账号 `controlPlaneFlight`，操作优先级队列与防死锁 |
| `auth/antigravity_coordinator_test.go`（新建） | 协调器并发控制、flight 单飞、交互通道不排队与优先级抢占测试 |
| `auth/antigravity_quota_refresh.go`（新建） | 双流平滑派发最小堆调度器，资格池 $\mathcal{S}$，共有相位 $\theta$，`not_before` 门禁，指数退避，局部覆盖窗与重启恢复 |
| `auth/antigravity_quota_refresh_test.go`（新建） | 调度器槽位分布、名义时间、覆盖窗与退避测试 |
| `auth/antigravity_quota_debounce.go`（新建） | 计划重排防抖（100ms/1s）与软权重读时计算/可选缓存 |
| `auth/antigravity_quota_debounce_test.go`（新建） | FakeClock 下重排防抖合并与即刻生效测试 |
| `auth/antigravity_balance.go`（修改） | `quotaPressure` 替换 usage，1:1 加权，absent 省略，models 回退，健康亲和豁免软评分 |
| `auth/antigravity_balance_quota_test.go`（新建） | 软评分计算、防双罚、models-only 回退与亲和隔离测试 |
| `auth/antigravity_quota_gate.go`（新建） | 纯内存只读硬门禁谓词、resetAt/TTL 分流、stale 标记、到期通知与止血开关 |
| `auth/antigravity_quota_gate_test.go`（新建） | 硬门禁判定、已知零阻断、stale 补查与 kill switch 测试 |
| `auth/store.go`（修改） | 挂载协调器与生命周期，`isAvailableLocked`、`takeByIDModeWithCapacity` 原子线性化硬门禁，返回类型化阻断原因，`Store.SyncAntigravityAccountForAdmin` |
| `auth/fast_scheduler.go`（修改） | 候选过滤与 acquire 线性化门禁 |
| `proxy/retry_exclusions.go`（修改） | 配额阻断跳过 30s 队列等待 |
| `proxy/scheduler_queue_error.go`（修改） | 识别 `ErrQuotaExhausted` 映射 429 + `Retry-After`（四协议共用出口） |
| `proxy/handler_gemini.go`（修改） | native countTokens 仅在候选集合全因配额阻断时返回 429，其余保留本地估算回退 |
| `proxy/antigravity_quota_proxy_test.go`（新建） | 代理层 429 映射、跳过排队、续链 `quota_gate_bound` 与 countTokens 阻断测试 |
| `admin/antigravity_accounts.go`（修改） | 手动同步改调 Store 统一入口有界等待；新增 `POST /api/admin/antigravity/quota/clear` 止血接口 |
| `admin/antigravity_state.go`（修改） | 状态接口展示 V2 四态、stale 标记、metadata 与 epoch |
| `frontend/src/types.ts`（修改） | 前端类型适配可空 fraction 与新设置字段 |
| `frontend/src/api.ts`（修改） | 前端 API 新增配额清除与设置接口 |
| `frontend/src/pages/Settings.tsx`（修改） | Antigravity 渠道设置卡片（自动刷新开关、硬门禁开关、3-60min 输入框） |
| `frontend/src/pages/AntigravityAccounts.tsx`（修改） | 账号列表与详情增补清除配额事实按钮及二次确认 |
| `frontend/src/locales/*.json`（修改） | 中英繁三语国际化文案 |
| `auth/antigravity_quota_integration_test.go`（新建） | 39 条验收测试全量对齐端到端验证 |

---

## Implementation Tasks

### Task 1: 可注入时钟抽象（Clock Seam）与 FakeClock 基建

**Files:**
- Create: `auth/clock.go`
- Create: `auth/clock_test.go`

**Interfaces:**
- Produces:
  - `type Clock interface { Now() time.Time; Sleep(d time.Duration); Since(t time.Time) time.Duration; Until(t time.Time) time.Duration; NewTimer(d time.Duration) Timer }`
  - `type Timer interface { C() <-chan time.Time; Stop() bool; Reset(d time.Duration) bool }`
  - `type RealClock struct{}`
  - `type FakeClock struct` 提供 `NewFakeClock(initial time.Time) *FakeClock`, `Advance(d time.Duration)`
  - `var DefaultClock Clock = RealClock{}`

- [ ] **Step 1: Write the failing test**

```go
// auth/clock_test.go
package auth

import (
	"testing"
	"time"
)

func TestFakeClock_AdvanceAndTimer(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	if !clk.Now().Equal(start) {
		t.Fatalf("expected now %v, got %v", start, clk.Now())
	}

	timer := clk.NewTimer(10 * time.Minute)
	select {
	case <-timer.C():
		t.Fatal("timer triggered prematurely")
	default:
	}

	clk.Advance(5 * time.Minute)
	select {
	case <-timer.C():
		t.Fatal("timer triggered at 5m")
	default:
	}

	clk.Advance(5 * time.Minute)
	select {
	case fireTime := <-timer.C():
		expected := start.Add(10 * time.Minute)
		if !fireTime.Equal(expected) {
			t.Fatalf("expected fire time %v, got %v", expected, fireTime)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timer failed to fire after reaching deadline")
	}
}

func TestFakeClock_TimerResetAndStop(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	timer := clk.NewTimer(10 * time.Minute)
	clk.Advance(5 * time.Minute)
	stopped := timer.Stop()
	if !stopped {
		t.Fatal("expected timer.Stop() to return true")
	}

	clk.Advance(10 * time.Minute)
	select {
	case <-timer.C():
		t.Fatal("stopped timer fired")
	default:
	}

	timer.Reset(2 * time.Minute)
	clk.Advance(2 * time.Minute)
	select {
	case <-timer.C():
	case <-time.After(50 * time.Millisecond):
		t.Fatal("reset timer failed to fire")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestFakeClock_' -count=1`
Expected: FAIL with `undefined: NewFakeClock`

- [ ] **Step 3: Write minimal implementation**

```go
// auth/clock.go
package auth

import (
	"sync"
	"time"
)

type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	Since(t time.Time) time.Duration
	Until(t time.Time) time.Duration
	NewTimer(d time.Duration) Timer
}

type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) Sleep(d time.Duration)                 { time.Sleep(d) }
func (RealClock) Since(t time.Time) time.Duration       { return time.Since(t) }
func (RealClock) Until(t time.Time) time.Duration       { return time.Until(t) }
func (RealClock) NewTimer(d time.Duration) Timer        { return &realTimer{Timer: time.NewTimer(d)} }

type realTimer struct {
	*time.Timer
}

func (r *realTimer) C() <-chan time.Time {
	return r.Timer.C
}

var DefaultClock Clock = RealClock{}

type fakeTimer struct {
	clk      *FakeClock
	ch       chan time.Time
	deadline time.Time
	active   bool
}

func (ft *fakeTimer) C() <-chan time.Time {
	return ft.ch
}

func (ft *fakeTimer) Stop() bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()
	wasActive := ft.active
	ft.active = false
	return wasActive
}

func (ft *fakeTimer) Reset(d time.Duration) bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()
	wasActive := ft.active
	ft.deadline = ft.clk.now.Add(d)
	ft.active = true
	return wasActive
}

type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func NewFakeClock(initial time.Time) *FakeClock {
	return &FakeClock{now: initial}
}

func (fc *FakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.now
}

func (fc *FakeClock) Sleep(d time.Duration) {
	fc.Advance(d)
}

func (fc *FakeClock) Since(t time.Time) time.Duration {
	return fc.Now().Sub(t)
}

func (fc *FakeClock) Until(t time.Time) time.Duration {
	return t.Sub(fc.Now())
}

func (fc *FakeClock) NewTimer(d time.Duration) Timer {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	ft := &fakeTimer{
		clk:      fc,
		ch:       make(chan time.Time, 1),
		deadline: fc.now.Add(d),
		active:   true,
	}
	fc.timers = append(fc.timers, ft)
	return ft
}

func (fc *FakeClock) Advance(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = fc.now.Add(d)
	for _, ft := range fc.timers {
		if ft.active && !fc.now.Before(ft.deadline) {
			ft.active = false
			select {
			case ft.ch <- ft.deadline:
			default:
			}
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestFakeClock_' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/clock.go auth/clock_test.go
git commit -m "feat(auth): add Clock interface and FakeClock for deterministic time testing"
```

---

### Task 2: V2 配额数据契约就地扩展与全仓读点适配

**Files:**
- Modify: `auth/antigravity_types.go:280-335`
- Modify: `auth/scheduling_usage_key.go:80-110`
- Modify: `auth/antigravity_catalog.go:48-55`
- Create: `auth/antigravity_types_v2_test.go`

**Interfaces:**
- Produces:
  - `AntigravityQuotaSnapshot.SchemaVersion int` (必须为 2)
  - `AntigravityQuotaSnapshot.SourceMetadata map[string]SourceMeta`
  - `AntigravityQuotaBucket.RemainingFraction *float64`
  - `AntigravityQuotaBucket.FractionPresent bool`
  - `AntigravityQuotaBucket.ObservedAt time.Time`
  - `AntigravityQuotaBucket.ObservationSeq int64`
  - `AntigravityQuotaBucket.TTLSeconds int`
  - `AntigravityModelQuota.RemainingFraction *float64`（models 读点同步改可空，Spec §3.1"models[] 的 RemainingFraction 同样补 presence"）
  - `AntigravityModelQuota.FractionPresent bool`
  - `SourceMeta` 结构体契约
  - `Account.AntigravityQuota AntigravityQuotaSnapshot`（新增内存投影字段，受 `Account.mu` 保护；装载与 outbox 投影时从 credentials JSON 的 `antigravity_quota` 键填充，Task 11/12 的门禁与选号测试依赖该字段）
- Preserves: 现有 `(*AntigravityQuotaSnapshot).UnmarshalJSON`（`auth/antigravity_types.go:340-357`）对旧 `groups` 键的反序列化兼容必须原样保留——V2 写入 canonical `quota_groups`，读取同时接受 `groups`/`quota_groups`（Spec §3.1"兼容旧 `groups` 仅作为反序列化只读兼容"）。修改字段类型后不得删除该兼容逻辑。
- 全仓读点清单（必须全部同步适配 `*float64`，缺一编译失败）：`auth/scheduling_usage_key.go` 的 `antigravitySchedulingUsedPercent`（groups 桶循环与 models 平均循环两处）、`auth/antigravity_catalog.go:53` 的 groups/credits 拼回（无需改数值，仅保留对象拷贝）、`admin` 投影与 `frontend/src/types.ts` 的 `remaining_fraction: number | null` + `fraction_present: boolean`。

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_types_v2_test.go
package auth

import (
	"encoding/json"
	"testing"
	"time"
)

func TestAntigravityQuotaSnapshot_V2SchemaAndCompatibility(t *testing.T) {
	legacyJSON := `{"groups":[{"display_name":"Weekly","buckets":[{"window":"weekly","remaining_fraction":0.0}]}]}`
	var snap AntigravityQuotaSnapshot
	if err := json.Unmarshal([]byte(legacyJSON), &snap); err != nil {
		t.Fatalf("failed to unmarshal legacy JSON: %v", err)
	}
	if snap.SchemaVersion != 0 {
		t.Fatalf("expected legacy schema_version 0, got %d", snap.SchemaVersion)
	}
	if len(snap.Groups) != 1 || len(snap.Groups[0].Buckets) != 1 {
		t.Fatalf("expected 1 group with 1 bucket, got %+v", snap.Groups)
	}
	b := snap.Groups[0].Buckets[0]
	if b.FractionPresent {
		t.Fatal("legacy bucket should not have FractionPresent=true")
	}
	if b.RemainingFraction == nil || *b.RemainingFraction != 0.0 {
		t.Fatalf("expected remaining_fraction 0.0, got %v", b.RemainingFraction)
	}

	val := 0.75
	v2 := AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {
				Producer:         "summary_loop",
				PresenceVerified: true,
				AppliedSeq:       10,
				ObservedAt:       time.Now().UTC(),
			},
		},
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{
						Window:            "weekly",
						RemainingFraction: &val,
						FractionPresent:   true,
						TTLSeconds:        3600,
					},
				},
			},
		},
	}
	raw, err := json.Marshal(v2)
	if err != nil {
		t.Fatalf("marshal V2 failed: %v", err)
	}
	var decoded AntigravityQuotaSnapshot
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal V2 failed: %v", err)
	}
	if decoded.SchemaVersion != 2 {
		t.Fatalf("expected SchemaVersion 2, got %d", decoded.SchemaVersion)
	}
	if !decoded.Groups[0].Buckets[0].FractionPresent || *decoded.Groups[0].Buckets[0].RemainingFraction != 0.75 {
		t.Fatalf("mismatched V2 bucket: %+v", decoded.Groups[0].Buckets[0])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestAntigravityQuotaSnapshot_V2SchemaAndCompatibility' -count=1`
Expected: FAIL with `unknown field SchemaVersion` or `mismatched type *float64`

- [ ] **Step 3: Write minimal implementation**

在 `auth/antigravity_types.go` 就地扩展：
```go
type SourceMeta struct {
	Producer           string    `json:"producer"`
	Trigger            string    `json:"trigger"`
	CredentialFamilyID string    `json:"credential_family_id"`
	QuotaIdentityScope string    `json:"quota_identity_scope"`
	Generation         int64     `json:"generation"`
	CurrentAttemptSeq  int64     `json:"current_attempt_seq"`
	AppliedSeq         int64     `json:"applied_seq"`
	ObservedAt         time.Time `json:"observed_at"`
	LastAttemptAt      time.Time `json:"last_attempt_at"`
	LastSuccessAt      time.Time `json:"last_success_at"`
	FailureCount       int       `json:"failure_count"`
	NotBefore          time.Time `json:"not_before"`
	PresenceVerified   bool      `json:"presence_verified"`
	ContractVersion    string    `json:"contract_version,omitempty"`
	SummaryIntegrity   bool      `json:"summary_integrity"`
}

type AntigravityQuotaBucket struct {
	BucketID          string    `json:"bucket_id,omitempty"`
	Window            string    `json:"window,omitempty"`
	RemainingFraction *float64  `json:"remaining_fraction"`
	FractionPresent   bool      `json:"fraction_present"`
	ResetTime         string    `json:"reset_time,omitempty"`
	DisplayName       string    `json:"display_name,omitempty"`
	Description       string    `json:"description,omitempty"`
	ObservedAt        time.Time `json:"observed_at"`
	ObservationSeq    int64     `json:"observation_seq"`
	TTLSeconds        int       `json:"ttl_seconds"`
}

// AntigravityModelQuota 同步就地扩展（Spec §3.1：models[] 的 RemainingFraction
// 同样补 presence）；DisplayName 等其余既有字段保持不变。
type AntigravityModelQuota struct {
	ModelID           string   `json:"model_id"`
	RemainingFraction *float64 `json:"remaining_fraction"`
	FractionPresent   bool     `json:"fraction_present"`
	// ...其余既有字段（RemainingPercent/ResetTime/SupportsImages 等）原样保留。
}

type AntigravityQuotaSnapshot struct {
	SchemaVersion        int                     `json:"schema_version"`
	CatalogModelIDs      []string                `json:"catalog_model_ids,omitempty"`
	InternalModelIDs     []string                `json:"internal_model_ids,omitempty"`
	Models               []AntigravityModelQuota `json:"models"`
	Groups               []AntigravityQuotaGroup `json:"quota_groups,omitempty"`
	ModelForwardingRules map[string]string       `json:"model_forwarding_rules,omitempty"`
	AICredits            *AntigravityAICredits   `json:"ai_credits,omitempty"`
	Forbidden            bool                    `json:"forbidden,omitempty"`
	SourceMetadata       map[string]SourceMeta   `json:"source_metadata,omitempty"`
	UpdatedAt            time.Time               `json:"updated_at"`
}
```
并在 `auth/scheduling_usage_key.go` 与 `auth/antigravity_catalog.go` 修复全部读点（两处数值循环都必须判空，`antigravitySchedulingUsedPercent` 现有 `clampRemainingFraction(bucket.RemainingFraction)` 直接解引用 float64，改指针后必须先判 nil）：
```go
// auth/scheduling_usage_key.go — groups 桶循环与 models 平均循环同样处理：
if bucket.FractionPresent && bucket.RemainingFraction != nil {
    remaining := clampRemainingFraction(*bucket.RemainingFraction)
    ...
}
// models 平均循环：
if model.FractionPresent && model.RemainingFraction != nil {
    sum += clampRemainingFraction(*model.RemainingFraction)
}
```
同时在 `Account`（`auth/store.go:158`）新增内存投影字段并在装载/同步/投影链路填充：
```go
// AntigravityQuota 是 credentials JSON 中 antigravity_quota 的内存 V2 投影，
// 受 a.mu 保护；由账号装载、client.Sync 落库后与 outbox consumer 投影共同更新。
AntigravityQuota AntigravityQuotaSnapshot
```
注意：既有 `(*AntigravityQuotaSnapshot).UnmarshalJSON`（对旧 `groups` 键的兼容）必须原样保留，本 Task 的 legacy JSON 测试依赖它；`AntigravityModelQuota` 的 JSON 键 `remaining_fraction` 与桶相同，旧数据中数值型 `remaining_fraction` 会被解码为 nil 指针 + `FractionPresent=false`，符合"legacy 不可信、不得硬封"的契约。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestAntigravityQuotaSnapshot_V2SchemaAndCompatibility' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_types.go auth/scheduling_usage_key.go auth/antigravity_catalog.go auth/antigravity_types_v2_test.go
git commit -m "feat(auth): extend AntigravityQuotaSnapshot to V2 with pointer fraction and presence"
```

---

### Task 3: 上游 Client 配额解析四态收敛与白名单判定

**Files:**
- Modify: `auth/antigravity_client.go:1076-1115`
- Create: `auth/antigravity_client_quota_test.go`

**Interfaces:**
- Consumes: `AntigravityQuotaSnapshot` V2, `AntigravityQuotaBucket`, `SourceMeta`
- Produces:
  - `NormalizeAntigravityWindow(raw string) (string, bool)`（识别 5h 与 weekly，其余白名单外返回 false）
  - `ClassifyAntigravityFraction(value *float64, present bool) string`（§3.2 四态收敛：`known_positive` / `known_zero` / `unknown`；`>1`、负数、`nil`、缺失、`NaN`、`Inf` 统一 `unknown`；验收 #1）
  - `ApplyQuotaGroupsV2(prev *AntigravityQuotaSnapshot, incoming []AntigravityQuotaGroup, summaryIntegrity bool, observedAt time.Time, ttlSeconds int, seq int64) (AntigravityQuotaSnapshot, bool)`（§3.2 结构完整性与坏桶隔离：空响应不推进成功、坏桶覆盖为 unknown 绝不复活旧零、未返回旧桶原样保留 `observed_at`/`observation_seq`、仅完整权威清单才整体替换；验收 #3）
  - `fetchQuotaSummary` 填入 `FractionPresent`、`RemainingFraction` 真实值（nil 不强行置 0.0）
  - `ValidateResetTime(resetStr string, observedAt time.Time, window string) (time.Time, bool)`（时区解析，5h $\le 6$h，weekly $\le 8$d 上限检查）

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_client_quota_test.go
package auth

import (
	"math"
	"testing"
	"time"
)

func TestNormalizeAntigravityWindow(t *testing.T) {
	cases := []struct {
		input string
		want  string
		ok    bool
	}{
		{"5h", "5h", true},
		{"FIVE_HOUR", "5h", true},
		{" 5_hour ", "5h", true},
		{"fivehour", "5h", true},
		{"weekly", "weekly", true},
		{"WEEK", "weekly", true},
		{"7d", "weekly", true},
		{"seven_day", "weekly", true},
		{"daily", "", false},
		{"monthly", "", false},
		{"unknown_window", "", false},
	}
	for _, tc := range cases {
		got, ok := NormalizeAntigravityWindow(tc.input)
		if ok != tc.ok || got != tc.want {
			t.Errorf("NormalizeAntigravityWindow(%q) = (%q, %v), want (%q, %v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

func TestValidateResetTime(t *testing.T) {
	observed := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	// 合法 5h
	valid5h := observed.Add(4 * time.Hour).Format(time.RFC3339)
	if rt, ok := ValidateResetTime(valid5h, observed, "5h"); !ok || rt.IsZero() {
		t.Fatalf("expected valid 5h reset time, got ok=%v", ok)
	}
	// 越界 5h (>6h)
	invalid5h := observed.Add(8 * time.Hour).Format(time.RFC3339)
	if _, ok := ValidateResetTime(invalid5h, observed, "5h"); ok {
		t.Fatal("expected 8h reset time to fail for 5h window")
	}
	// 早于 observed
	past := observed.Add(-1 * time.Hour).Format(time.RFC3339)
	if _, ok := ValidateResetTime(past, observed, "weekly"); ok {
		t.Fatal("expected past reset time to fail")
	}
}

func TestClassifyAntigravityFraction_FourStates(t *testing.T) {
	tiny, zero, neg, over := 0.001, 0.0, -0.5, 1.5
	nan := math.NaN()
	inf := math.Inf(1)
	cases := []struct {
		name    string
		value   *float64
		present bool
		want    string
	}{
		{"tiny positive", &tiny, true, "known_positive"},
		{"exact one", &over, false, "unknown"}, // present=false 永远 unknown
		{"zero with presence", &zero, true, "known_zero"},
		{"missing pointer", nil, true, "unknown"},
		{"absent flag", &zero, false, "unknown"},
		{"negative", &neg, true, "unknown"},
		{"over one", &over, true, "unknown"},
		{"nan", &nan, true, "unknown"},
		{"inf", &inf, true, "unknown"},
	}
	for _, tc := range cases {
		if got := ClassifyAntigravityFraction(tc.value, tc.present); got != tc.want {
			t.Errorf("%s: ClassifyAntigravityFraction = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestApplyQuotaGroupsV2_BadBucketIsolationAndMerge(t *testing.T) {
	oldZero := 0.0
	prevObserved := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	prev := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{{
			DisplayName: "Weekly",
			Buckets: []AntigravityQuotaBucket{{
				BucketID: "w1", Window: "weekly", RemainingFraction: &oldZero, FractionPresent: true,
				ObservedAt: prevObserved, ObservationSeq: 7, TTLSeconds: 3600,
			}},
		}},
	}
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	findBucket := func(snap AntigravityQuotaSnapshot, window string) (AntigravityQuotaBucket, bool) {
		for _, g := range snap.Groups {
			for _, b := range g.Buckets {
				if b.Window == window {
					return b, true
				}
			}
		}
		return AntigravityQuotaBucket{}, false
	}

	// 1. 空/nil groups 不是"窗口不存在"的证据：保留历史快照且不推进成功时标。
	next, advanced := ApplyQuotaGroupsV2(prev, nil, false, now, 3600, 8)
	if advanced {
		t.Fatal("empty response must not advance success")
	}
	if b, ok := findBucket(next, "weekly"); !ok || b.ObservationSeq != 7 || !b.ObservedAt.Equal(prevObserved) {
		t.Fatalf("empty response must retain previous bucket unchanged, got %+v", b)
	}

	// 2. 部分清单中仅返回坏桶：坏桶覆盖为 unknown（绝不复活旧零），未返回旧桶保留原时标。
	incoming := []AntigravityQuotaGroup{{
		DisplayName: "FiveHour",
		Buckets:     []AntigravityQuotaBucket{{BucketID: "h1", Window: "5h", RemainingFraction: nil, FractionPresent: false}},
	}}
	next, advanced = ApplyQuotaGroupsV2(prev, incoming, false, now, 3600, 8)
	if !advanced {
		t.Fatal("non-empty response must advance")
	}
	bad, ok := findBucket(next, "5h")
	if !ok || bad.FractionPresent || bad.RemainingFraction != nil || bad.ObservationSeq != 8 {
		t.Fatalf("bad bucket must be isolated as unknown with new seq, got %+v", bad)
	}
	kept, ok := findBucket(next, "weekly")
	if !ok || kept.ObservationSeq != 7 || !kept.ObservedAt.Equal(prevObserved) {
		t.Fatalf("unreturned previous bucket must keep original timestamps, got %+v", kept)
	}

	// 3. 完整权威清单（summaryIntegrity=true）才允许整体替换未返回桶。
	next, _ = ApplyQuotaGroupsV2(prev, incoming, true, now, 3600, 9)
	if _, ok := findBucket(next, "weekly"); ok {
		t.Fatal("authoritative full list must replace buckets absent from the response")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestNormalizeAntigravityWindow|TestValidateResetTime|TestClassifyAntigravityFraction|TestApplyQuotaGroupsV2' -count=1`
Expected: FAIL with `undefined: NormalizeAntigravityWindow`

- [ ] **Step 3: Write minimal implementation**

在 `auth/antigravity_client.go` 中新增白名单规整与校验逻辑：
```go
func NormalizeAntigravityWindow(raw string) (string, bool) {
	norm := strings.ToLower(strings.TrimSpace(raw))
	switch norm {
	case "5h", "five_hour", "5_hour", "fivehour":
		return "5h", true
	case "weekly", "week", "7d", "seven_day":
		return "weekly", true
	default:
		return "", false
	}
}

func ValidateResetTime(resetStr string, observedAt time.Time, window string) (time.Time, bool) {
	if strings.TrimSpace(resetStr) == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(resetStr))
	if err != nil {
		return time.Time{}, false
	}
	if !parsed.After(observedAt) {
		return time.Time{}, false
	}
	diff := parsed.Sub(observedAt)
	switch window {
	case "5h":
		if diff > 6*time.Hour {
			return time.Time{}, false
		}
	case "weekly":
		if diff > 8*24*time.Hour {
			return time.Time{}, false
		}
	default:
		return time.Time{}, false
	}
	return parsed, true
}

// ClassifyAntigravityFraction 按 Spec §3.2 把单个桶数值收敛为四态（absent 由
// summary_integrity 权威清单在合并层判定，解析层只区分 known_positive/known_zero/unknown）。
func ClassifyAntigravityFraction(value *float64, present bool) string {
	if !present || value == nil {
		return "unknown"
	}
	v := *value
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0.0 || v > 1.0 {
		return "unknown"
	}
	if v == 0.0 {
		return "known_zero"
	}
	return "known_positive"
}

// ApplyQuotaGroupsV2 按 Spec §3.2 合并一次 groups 观测：
//   - incoming 为空：整体残缺，不得推进成功（返回 advanced=false），保留 prev；
//   - incoming 非空：替换所有已返回桶（坏桶按 presence 判 unknown，绝不复活旧零），
//     未返回旧桶原样保留 observed_at/observation_seq（不洗新）；
//   - 仅 summaryIntegrity=true（完整权威清单）才删除未返回桶。
func ApplyQuotaGroupsV2(prev *AntigravityQuotaSnapshot, incoming []AntigravityQuotaGroup, summaryIntegrity bool, observedAt time.Time, ttlSeconds int, seq int64) (AntigravityQuotaSnapshot, bool) {
	if len(incoming) == 0 {
		if prev == nil {
			return AntigravityQuotaSnapshot{SchemaVersion: 2}, false
		}
		return *prev, false
	}
	next := AntigravityQuotaSnapshot{SchemaVersion: 2}
	if prev != nil {
		next.SourceMetadata = prev.SourceMetadata
		next.Models = prev.Models
		next.UpdatedAt = prev.UpdatedAt
	}
	returned := make(map[string]bool)
	for _, group := range incoming {
		g := group
		for i := range g.Buckets {
			b := g.Buckets[i]
			if w, known := NormalizeAntigravityWindow(b.Window); known {
				b.Window = w
			}
			b.ObservedAt = observedAt
			b.ObservationSeq = seq
			b.TTLSeconds = ttlSeconds
			if !b.FractionPresent {
				b.RemainingFraction = nil // 坏桶隔离：unknown，绝不沿用旧数值
			}
			returned[b.BucketID+"|"+b.Window] = true
			g.Buckets[i] = b
		}
		next.Groups = append(next.Groups, g)
	}
	if prev != nil && !summaryIntegrity {
		for _, group := range prev.Groups {
			var retained []AntigravityQuotaBucket
			for _, b := range group.Buckets {
				if !returned[b.BucketID+"|"+b.Window] {
					retained = append(retained, b)
				}
			}
			if len(retained) > 0 {
				next.Groups = append(next.Groups, AntigravityQuotaGroup{DisplayName: group.DisplayName, Description: group.Description, Buckets: retained})
			}
		}
	}
	return next, true
}
```
并在 `fetchQuotaSummary`（L1098-1107）保留真实的 `RemainingFraction` 指针和 `FractionPresent = (bucket.RemainingFraction != nil)`，杜绝 `nil -> 0.0` 强行转换；成功响应在提交时经 `ApplyQuotaGroupsV2` 合并（提交层由 Task 5 的短事务调用），`groups` 观测的 `observed_at` 取该 source 预约事务提交时刻（Spec §3.1 观测时间）。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestNormalizeAntigravityWindow|TestValidateResetTime|TestClassifyAntigravityFraction|TestApplyQuotaGroupsV2' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_client.go auth/antigravity_client_quota_test.go
git commit -m "feat(auth): implement quota window normalization and reset_time bounds check"
```

---

### Task 4: 系统配置契约、整 JSON CAS 持久化与 Settings API

**Files:**
- Modify: `auth/antigravity_settings.go`
- Modify: `database/antigravity_settings.go`
- Modify: `admin/antigravity_settings.go`
- Create: `auth/antigravity_settings_v2_test.go`
- Create: `database/antigravity_settings_cas_test.go`

**Interfaces:**
- Produces:
  - `AntigravitySettings.QuotaRefreshEnabled *bool`（缺省 true）
  - `AntigravitySettings.QuotaRefreshIntervalMinutes *int`（缺省 60，[3, 60]）
  - `AntigravitySettings.QuotaGateEnabled *bool`（缺省 true）
  - `AntigravitySettings.ConfigRevision int64`
  - `database.SaveAntigravityConfigCAS(ctx, oldRaw, newRaw string) (bool, error)`

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_settings_v2_test.go
package auth

import (
	"encoding/json"
	"testing"
)

func TestAntigravitySettings_V2DefaultsAndNormalization(t *testing.T) {
	raw := `{}`
	var s AntigravitySettings
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	norm, err := NormalizeAntigravitySettings(s)
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if !norm.GetQuotaRefreshEnabled() {
		t.Fatal("expected default QuotaRefreshEnabled=true")
	}
	if norm.GetQuotaRefreshIntervalMinutes() != 60 {
		t.Fatalf("expected default interval 60, got %d", norm.GetQuotaRefreshIntervalMinutes())
	}
	if !norm.GetQuotaGateEnabled() {
		t.Fatal("expected default QuotaGateEnabled=true")
	}

	// 越界间隔钳位到 [3, 60]
	badInterval := 1
	s.QuotaRefreshIntervalMinutes = &badInterval
	norm2, err := NormalizeAntigravitySettings(s)
	if err != nil {
		t.Fatalf("normalize with interval failed: %v", err)
	}
	if norm2.GetQuotaRefreshIntervalMinutes() != 3 {
		t.Fatalf("expected clamped interval 3, got %d", norm2.GetQuotaRefreshIntervalMinutes())
	}

	// 显式 false 必须保留（Parse/Normalize/Copy/Encode 全链路不丢字段，Spec §8.2 #4）
	gateOff := false
	s.QuotaGateEnabled = &gateOff
	norm3, err := NormalizeAntigravitySettings(s)
	if err != nil {
		t.Fatalf("normalize with gate failed: %v", err)
	}
	if norm3.GetQuotaGateEnabled() {
		t.Fatal("explicit quota_gate_enabled=false must be preserved by normalization")
	}
	encoded, err := EncodeAntigravitySettings(norm3)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	parsed, err := ParseAntigravitySettings(encoded)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if parsed.GetQuotaGateEnabled() || parsed.GetQuotaRefreshIntervalMinutes() != 3 {
		t.Fatalf("round-trip lost explicit false or interval: %+v", parsed)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestAntigravitySettings_V2DefaultsAndNormalization' -count=1`
Expected: FAIL with `undefined: GetQuotaRefreshEnabled`

- [ ] **Step 3: Write minimal implementation**

在 `auth/antigravity_settings.go` 扩充结构体与访问器：
```go
type AntigravitySettings struct {
	ModelRedirects              map[string]string `json:"model_redirects,omitempty"`
	RedirectOverridesEffort     bool              `json:"redirect_overrides_effort,omitempty"`
	ExposeThoughts              bool              `json:"expose_thoughts,omitempty"`
	QuotaRefreshEnabled         *bool             `json:"quota_refresh_enabled,omitempty"`
	QuotaRefreshIntervalMinutes *int              `json:"quota_refresh_interval_minutes,omitempty"`
	QuotaGateEnabled            *bool             `json:"quota_gate_enabled,omitempty"`
	ConfigRevision              int64             `json:"config_revision,omitempty"`
}

func (s AntigravitySettings) GetQuotaRefreshEnabled() bool {
	if s.QuotaRefreshEnabled == nil {
		return true
	}
	return *s.QuotaRefreshEnabled
}

func (s AntigravitySettings) GetQuotaRefreshIntervalMinutes() int {
	if s.QuotaRefreshIntervalMinutes == nil {
		return 60
	}
	v := *s.QuotaRefreshIntervalMinutes
	if v < 3 {
		return 3
	}
	if v > 60 {
		return 60
	}
	return v
}

func (s AntigravitySettings) GetQuotaGateEnabled() bool {
	if s.QuotaGateEnabled == nil {
		return true
	}
	return *s.QuotaGateEnabled
}
```
**同步修复 `auth/antigravity_settings.go:38-48` 的 `ConfiguredAntigravitySettings()`**：现有副本构造只拷贝旧三个字段，新增的 quota 字段若不加入拷贝将永远回落默认值、设置完全不生效，必须扩展为：
```go
func ConfiguredAntigravitySettings() AntigravitySettings {
	v, _ := configuredAntigravitySettings.Load().(AntigravitySettings)
	out := AntigravitySettings{
		RedirectOverridesEffort:     v.RedirectOverridesEffort,
		ExposeThoughts:              v.ExposeThoughts,
		QuotaRefreshEnabled:         v.QuotaRefreshEnabled,
		QuotaRefreshIntervalMinutes: v.QuotaRefreshIntervalMinutes,
		QuotaGateEnabled:            v.QuotaGateEnabled,
		ConfigRevision:              v.ConfigRevision,
	}
	if len(v.ModelRedirects) > 0 {
		out.ModelRedirects = make(map[string]string, len(v.ModelRedirects))
		for key, value := range v.ModelRedirects {
			out.ModelRedirects[key] = value
		}
	}
	return out
}
```
同时 `ParseAntigravitySettings` / `EncodeAntigravitySettings` / 深拷贝（Copy）与 `NormalizeAntigravitySettings` 必须携带新字段：空对象早退不能丢失显式 `false` 或非默认 interval；`null` 重置默认（enabled=true、interval=60、gate=true）（Spec §8.2 #4）。

在 `database/antigravity_settings.go` 新增整 JSON CAS：
```go
func (db *DB) SaveAntigravityConfigCAS(ctx context.Context, oldRaw, newRaw string) (bool, error) {
	var applied bool
	err := db.withSQLiteWriteLock(ctx, func() error {
		if _, err := db.conn.ExecContext(ctx, `
			INSERT INTO system_settings (id) VALUES (1)
			ON CONFLICT (id) DO NOTHING
		`); err != nil {
			return err
		}
		res, err := db.conn.ExecContext(ctx, `
			UPDATE system_settings
			SET antigravity_config = $1
			WHERE id = 1 AND COALESCE(antigravity_config, '{}') = $2
		`, newRaw, oldRaw)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		applied = rows > 0
		return nil
	})
	return applied, err
}
```
在 `admin/antigravity_settings.go` 中，PUT 接口改为**读 DB 当前 raw**（不读本机内存缓存作为基准，修正现有内存覆盖缺陷，见 `admin/antigravity_settings.go:69-92` 现状），Go 侧 merge 出现字段，`ConfigRevision = max(dbRevision, 本机已应用 revision) + 1`，执行 `SaveAntigravityConfigCAS(ctx, oldRaw, newRaw)`，影响 0 行即重读重试，有界重试 3 次后返回 409。

**revision 回退检测与基线重建（Spec §8.2 #5，本 Task 必须落地）**：本机与 consumer 读到 `config_revision` 缺失或小于本机已应用值时，不得静默拒绝后续所有更新：记录 `antigravity_config_revision_rollback` 告警，把已应用基线抬到 `max(本机值, DB 值) + 1` 并强制应用 DB 内容；基线重建后，若本机仍持有被旧副本抹掉的 `quota_*` 字段，必须以一次受控整 JSON CAS（基准 raw 取 DB 当前值）把它们合并回 DB 并再次告警——否则混布窗口（Spec §3.1 混布写侧危害）中 `quota_gate_enabled=false` 止血开关会被"缺失即默认 true"无痕掩盖。

新增 `database/antigravity_settings_cas_test.go` 验证 CAS 语义（双方言，PG 无 DSN 时跳过，模式与 `database/auto_reset_exhausted_test.go:9-20` 一致）：
```go
// database/antigravity_settings_cas_test.go
package database

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveAntigravityConfigCAS_WholeJSONCompareAndSwap(t *testing.T) {
	for _, driver := range []string{"sqlite", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			dsn := filepath.Join(t.TempDir(), "settings.db")
			if driver == "postgres" {
				dsn = os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("requires isolated CODEX2API_TEST_POSTGRES_DSN")
				}
			}
			db, err := New(driver, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()

			v1 := `{"quota_refresh_enabled":true,"config_revision":1}`
			if err := db.SaveAntigravityConfig(ctx, v1); err != nil {
				t.Fatalf("seed v1 failed: %v", err)
			}

			v2 := `{"quota_refresh_enabled":true,"quota_gate_enabled":false,"config_revision":2}`
			applied, err := db.SaveAntigravityConfigCAS(ctx, v1, v2)
			if err != nil || !applied {
				t.Fatalf("expected CAS apply, applied=%v err=%v", applied, err)
			}

			// 基于过期快照的写入必须整包失败（0 行），不得覆盖 v2。
			stale := `{"quota_refresh_interval_minutes":3,"config_revision":3}`
			applied, err = db.SaveAntigravityConfigCAS(ctx, v1, stale)
			if err != nil {
				t.Fatalf("stale CAS returned error: %v", err)
			}
			if applied {
				t.Fatal("stale CAS must not apply")
			}
			got, err := db.GetAntigravityConfigRaw(ctx)
			if err != nil {
				t.Fatalf("read back failed: %v", err)
			}
			if !strings.Contains(got, `"quota_gate_enabled":false`) || strings.Contains(got, `"quota_refresh_interval_minutes":3`) {
				t.Fatalf("stale write leaked into stored config: %s", got)
			}
		})
	}
}
```
配套在 `database/antigravity_settings.go` 新增读取 helper `GetAntigravityConfigRaw(ctx) (string, error)`（返回 `antigravity_config` 列原文，空列返回 `'{}'`），PUT 的读-合并-写循环以它为基准。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestAntigravitySettings_V2DefaultsAndNormalization' -count=1 && GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./database/ -run 'TestSaveAntigravityConfigCAS_WholeJSONCompareAndSwap' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_settings.go database/antigravity_settings.go admin/antigravity_settings.go auth/antigravity_settings_v2_test.go database/antigravity_settings_cas_test.go
git commit -m "feat(settings): support Antigravity quota settings with whole-JSON CAS and revision rollback detection"
```

---

### Task 5: 存储短事务 CAS、逐 Source 预约与 quota_epoch 失效纪元

**Files:**
- Create: `database/antigravity_quota_source.go`
- Create: `database/antigravity_quota_source_test.go`
- Modify: `database/grok_state.go:821-895`

**Interfaces:**
- Produces:
  - `ReserveAntigravityQuotaSource(ctx context.Context, accountID int64, source string, expectedGen int64, expectedScope string) (seq int64, epoch int64, err error)`
  - `CommitAntigravityQuotaSource(ctx context.Context, accountID int64, source string, mySeq int64, epoch int64, expectedGen int64, expectedScope string, snapshotJSON string) (applied bool, err error)`（`snapshotJSON` 为 V2 快照原文，Latest-Success-Wins：仅要求 `mySeq > applied_seq` 且 gen/scope/epoch 匹配，后发失败不得否决先发成功）
  - `ClearAntigravityQuotaSourcesCAS(ctx context.Context, accountID int64) (newEpoch int64, err error)`（同一短事务删除 quota source 事实与定时器并递增 `quota_epoch`，保留凭据与 models）
  - `GetAntigravityQuotaEpoch(ctx context.Context, accountID int64) (int64, error)`（读取 credentials JSON 的 `antigravity_quota_epoch` 键，缺省 0）
  - `ApplyAntigravityCarryforwardOnUpdateCAS(merged map[string]any, prevRaw string)`（`UpdateAccountCredentialsCAS` 的 AG 受控 carryforward：同已验证身份轮换时把旧 `antigravity_quota` facts 原样盖到新 credentials，严格保留 `observed_at`/`reset`/`ttl`/`applied_seq`/`known_zero`，不洗新不解除；`antigravity_quota_epoch` 原值保留不递增）
  - RT 冲突 reload 三分支（Spec §4.1 #5、验收 #39）：`type RTConflictOutcome int`（`RTConflictTrustWinner` / `RTConflictReplayCredentialOnly` / `RTConflictIsolateIdentity`）与 `HandleAntigravityRTConflict(ctx context.Context, accountID int64, expectedGen int64, preRotationCredentials string, rotated map[string]any) (RTConflictOutcome, error)`：
    - reload 后 durable access token 已不同于 `preRotationCredentials` 中读到的旧值 → `RTConflictTrustWinner`，丢弃本次 token 与全部 source 观测，绝不回写；
    - durable 行仍等于预轮换凭据且身份/family 未变 → `RTConflictReplayCredentialOnly`：在调用方仍持 family lease 的前提下重放一次仅含凭据字段的 UpdateCAS，不携带任何 source 观测（不循环重试）；
    - family/subject/project 改变或账号已删除/隔离 → `RTConflictIsolateIdentity`：禁止把本次 RT 写入当前身份，落 `antigravity_permanent_refresh_error` 并写审计。

- [ ] **Step 1: Write the failing test**

```go
// database/antigravity_quota_source_test.go
package database

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newAntigravityQuotaTestDB 按方言创建测试库（Postgres 需隔离 DSN，未配置则跳过，
// 模式与 database/auto_reset_exhausted_test.go 一致），并插入一个 AG 账号。
func newAntigravityQuotaTestDB(t *testing.T, ctx context.Context, creds string) (*DB, int64) {
	t.Helper()
	driver := "sqlite"
	dsn := filepath.Join(t.TempDir(), "quota.db")
	if os.Getenv("CODEX2API_TEST_POSTGRES_DSN") != "" {
		driver = "postgres"
		dsn = os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
	}
	db, err := New(driver, dsn)
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	res, err := db.conn.ExecContext(ctx,
		`INSERT INTO accounts (name, platform, type, credentials, status, enabled, locked, credential_generation) VALUES ('ag', 'antigravity', 'oauth', ?, 'active', 1, 0, 1)`,
		creds)
	if err != nil {
		t.Fatalf("insert account failed: %v", err)
	}
	id, lastIDErr := res.LastInsertId()
	if lastIDErr != nil {
		if scanErr := db.conn.QueryRowContext(ctx, `SELECT id FROM accounts WHERE name = ?`, "ag").Scan(&id); scanErr != nil {
			t.Fatalf("resolve account id failed: %v (lastInsertId: %v)", scanErr, lastIDErr)
		}
	}
	return db, id
}

func TestAntigravityQuotaSource_LatestSuccessWinsAndEpochBarrier(t *testing.T) {
	ctx := context.Background()
	db, accID := newAntigravityQuotaTestDB(t, ctx,
		`{"access_token":"token1","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`)

	// 1. A 先预约（慢请求）、B 后预约并提交
	seqA, epochA, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam1")
	if err != nil {
		t.Fatalf("reserve A failed: %v", err)
	}
	seqB, epochB, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", 1, "fam1")
	if err != nil {
		t.Fatalf("reserve B failed: %v", err)
	}
	if seqB <= seqA {
		t.Fatalf("expected seqB > seqA, got %d <= %d", seqB, seqA)
	}

	// B 提交成功：推进 applied_seq（实现负责把 source_metadata.groups.applied_seq 写为 mySeq）
	okB, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB, epochB, 1, "fam1", `{"schema_version":2}`)
	if err != nil || !okB {
		t.Fatalf("commit B failed: ok=%v, err=%v", okB, err)
	}

	// 2. A 迟到提交：seqA < applied_seq，被 Latest-Success-Wins 拒绝（更旧成功被拒）
	okA, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqA, epochA, 1, "fam1", `{"schema_version":2}`)
	if err != nil {
		t.Fatalf("commit A returned error: %v", err)
	}
	if okA {
		t.Fatal("expected slower A with smaller seq to be rejected by Latest-Success-Wins")
	}

	// 3. Clear 同事务递增 quota_epoch
	newEpoch, err := db.ClearAntigravityQuotaSourcesCAS(ctx, accID)
	if err != nil {
		t.Fatalf("clear quota failed: %v", err)
	}
	if newEpoch != epochB+1 {
		t.Fatalf("expected newEpoch %d, got %d", epochB+1, newEpoch)
	}

	// 4. 清除后使用旧 epoch 的迟到提交必须被屏障拦截（验收 #36）
	okLate, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB+10, epochB, 1, "fam1", `{"schema_version":2}`)
	if err != nil {
		t.Fatalf("late commit returned error: %v", err)
	}
	if okLate {
		t.Fatal("expected late commit with old epoch to be blocked by epoch barrier")
	}

	// 5. GetAntigravityQuotaEpoch 读取 credentials JSON 内嵌纪元
	got, err := db.GetAntigravityQuotaEpoch(ctx, accID)
	if err != nil || got != newEpoch {
		t.Fatalf("GetAntigravityQuotaEpoch = (%d, %v), want (%d, nil)", got, err, newEpoch)
	}
}

func TestAntigravityQuotaSource_UpdateCASSameIdentityCarryforward(t *testing.T) {
	ctx := context.Background()
	quotaFacts := `{"schema_version":2,"source_metadata":{"groups":{"applied_seq":5,"observed_at":"2026-10-21T10:00:00Z"}},"quota_groups":[{"display_name":"Weekly","buckets":[{"window":"weekly","remaining_fraction":0,"fraction_present":true,"observed_at":"2026-10-21T10:00:00Z","observation_seq":5,"ttl_seconds":3600}]}]}`
	creds := `{"access_token":"old-at","refresh_token":"old-rt","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1,"antigravity_quota":` + jsonString(quotaFacts) + `}`
	db, accID := newAntigravityQuotaTestDB(t, ctx, creds)

	// 例行同身份 token 轮换（UpdateCAS 递增 generation）
	newGen, applied, err := db.UpdateAccountCredentialsCAS(ctx, accID, 1, map[string]any{
		"access_token": "new-at", "refresh_token": "new-rt",
	})
	if err != nil || !applied || newGen != 2 {
		t.Fatalf("UpdateAccountCredentialsCAS = (gen=%d, applied=%v, err=%v)", newGen, applied, err)
	}

	var raw string
	if err := db.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accID).Scan(&raw); err != nil {
		t.Fatalf("reload credentials failed: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decode credentials failed: %v", err)
	}
	quotaRaw, _ := doc["antigravity_quota"].(string)
	if quotaRaw == "" {
		t.Fatal("carryforward must keep antigravity_quota facts on same-identity rotation")
	}
	var carried AntigravityQuotaSnapshotLite
	if err := json.Unmarshal([]byte(quotaRaw), &carried); err != nil {
		t.Fatalf("decode carried quota failed: %v", err)
	}
	if carried.SourceMetadata["groups"].AppliedSeq != 5 {
		t.Fatalf("carryforward must preserve applied_seq, got %+v", carried.SourceMetadata)
	}
	bucket := carried.Groups[0].Buckets[0]
	if bucket.RemainingFraction == nil || *bucket.RemainingFraction != 0 || !bucket.FractionPresent {
		t.Fatalf("carryforward must keep known_zero bucket, got %+v", bucket)
	}
	if !bucket.ObservedAt.Equal(time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("carryforward must not refresh observed_at, got %v", bucket.ObservedAt)
	}
	if got, _ := doc["antigravity_quota_epoch"].(float64); got != 1 {
		t.Fatalf("routine rotation must not bump quota_epoch, got %v", doc["antigravity_quota_epoch"])
	}
}

// jsonString 把 JSON 文本编码为可嵌入另一 JSON 字符串字面量的引号形式。
func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// AntigravityQuotaSnapshotLite 是测试用最小解码视图，避免测试耦合 auth 包类型。
type AntigravityQuotaSnapshotLite struct {
	Groups []struct {
		Buckets []struct {
			Window            string     `json:"window"`
			RemainingFraction *float64   `json:"remaining_fraction"`
			FractionPresent   bool       `json:"fraction_present"`
			ObservedAt        time.Time  `json:"observed_at"`
			ObservationSeq    int64      `json:"observation_seq"`
		} `json:"buckets"`
	} `json:"quota_groups"`
	SourceMetadata map[string]struct {
		AppliedSeq int64 `json:"applied_seq"`
	} `json:"source_metadata"`
}

func TestAntigravityQuotaSource_RTConflictThreeBranches(t *testing.T) {
	ctx := context.Background()
	pre := `{"access_token":"pre-at","refresh_token":"pre-rt","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`
	rotated := map[string]any{"access_token": "rotated-at", "refresh_token": "rotated-rt"}

	// 分支 (a)：durable winner 已完成同身份换票 → 信任 winner，绝不回写。
	dbA, accA := newAntigravityQuotaTestDB(t, ctx, pre)
	if _, err := dbA.conn.ExecContext(ctx, `UPDATE accounts SET credentials = ? WHERE id = ?`,
		`{"access_token":"winner-at","refresh_token":"winner-rt","credential_family_id":"fam1","upstream_type":"antigravity","antigravity_quota_epoch":1}`, accA); err != nil {
		t.Fatalf("simulate winner failed: %v", err)
	}
	outcome, err := dbA.HandleAntigravityRTConflict(ctx, accA, 1, pre, rotated)
	if err != nil || outcome != RTConflictTrustWinner {
		t.Fatalf("branch (a) = (%v, %v), want trustWinner", outcome, err)
	}
	var rawA string
	if err := dbA.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accA).Scan(&rawA); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if !strings.Contains(rawA, `"winner-at"`) || strings.Contains(rawA, `"rotated-at"`) {
		t.Fatalf("branch (a) must keep winner tokens and never write rotated RT: %s", rawA)
	}

	// 分支 (b)：durable 行仍等于预轮换凭据（无人抢先）→ 受控补写一次凭据-only CAS。
	dbB, accB := newAntigravityQuotaTestDB(t, ctx, pre)
	outcome, err = dbB.HandleAntigravityRTConflict(ctx, accB, 1, pre, rotated)
	if err != nil || outcome != RTConflictReplayCredentialOnly {
		t.Fatalf("branch (b) = (%v, %v), want replayCredentialOnly", outcome, err)
	}
	var rawB, genB string
	// 一次成功补写后凭据落库；quota 事实（此处无）与 epoch 不被该重放触碰。
	if err := dbB.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accB).Scan(&rawB); err != nil {
		t.Fatalf("reload branch (b) failed: %v", err)
	}
	_ = dbB.conn.QueryRowContext(ctx, `SELECT credential_generation FROM accounts WHERE id = ?`, accB).Scan(&genB)
	if !strings.Contains(rawB, `"rotated-at"`) {
		t.Fatalf("branch (b) must persist the consumed RT exactly once: %s", rawB)
	}

	// 分支 (c)：family 已更换（身份谱系断裂）→ 禁止写入，落隔离标记。
	dbC, accC := newAntigravityQuotaTestDB(t, ctx, pre)
	if _, err := dbC.conn.ExecContext(ctx, `UPDATE accounts SET credential_family_id = 'fam2' WHERE id = ?`, accC); err != nil {
		t.Fatalf("simulate family change failed: %v", err)
	}
	outcome, err = dbC.HandleAntigravityRTConflict(ctx, accC, 1, pre, rotated)
	if err != nil || outcome != RTConflictIsolateIdentity {
		t.Fatalf("branch (c) = (%v, %v), want isolateIdentity", outcome, err)
	}
	var rawC string
	if err := dbC.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id = ?`, accC).Scan(&rawC); err != nil {
		t.Fatalf("reload branch (c) failed: %v", err)
	}
	if strings.Contains(rawC, `"rotated-at"`) || !strings.Contains(rawC, "antigravity_permanent_refresh_error") {
		t.Fatalf("branch (c) must isolate identity without writing rotated RT: %s", rawC)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./database/ -run 'TestAntigravityQuotaSource_' -count=1`
Expected: FAIL with `undefined: ReserveAntigravityQuotaSource`

- [ ] **Step 3: Write minimal implementation**

在 `database/antigravity_quota_source.go` 中实现短事务锁行与 JSON 合并（锁行模式与 `database/grok_state.go:826-840` 现有写法一致：事务内 `SELECT ...`，仅 `!db.isSQLite()` 追加 `FOR UPDATE`，SQLite 依赖 `withSQLiteWriteLock` 串行化——SQLite 方言不支持 `FOR UPDATE`）：
- `ReserveAntigravityQuotaSource`：事务内锁行读取 `credentials, credential_generation, credential_family_id`；校验未删除、`expectedGen == credential_generation`、`expectedScope == credential_family_id`；按 source 读改 credentials JSON 的 `source_metadata.{source}.current_attempt_seq`（单调 +1）与 `last_attempt_at`，同时读取 `antigravity_quota_epoch`；提交后返回最新序号与 epoch。未取得执行资格的调用方不得调用本函数空耗序号（Spec §4.1 #1 逐 source 紧贴网络前预约）。
- `CommitAntigravityQuotaSource`：同一短事务内锁行复验 `epoch == antigravity_quota_epoch`、`expectedGen == credential_generation`、`expectedScope == credential_family_id`；要求 `mySeq > applied_seq`（**不得要求 `current_attempt_seq == mySeq`**，后发失败不得否决先发成功，验收 #28）；合并 `snapshotJSON` 进 `antigravity_quota`（保留未返回旧桶原时标、坏桶按 Task 3 `ApplyQuotaGroupsV2` 语义隔离），把 `source_metadata.{source}.applied_seq` 推进为 `mySeq` 并写入成功时标。任一 fence 不匹配返回 `applied=false, err=nil`。
- `ClearAntigravityQuotaSourcesCAS`：锁定账号行，删除 `antigravity_quota` 内全部 source 事实（保留 `models` 等目录分区），**同一事务内** `antigravity_quota_epoch++` 并返回新 epoch（Spec §4.1 失效纪元、验收 #36）。
- `GetAntigravityQuotaEpoch`：只读解析 credentials JSON 的 `antigravity_quota_epoch`，缺失返回 0。
- `database/grok_state.go` 的 `UpdateAccountCredentialsCAS`：在事务内 merge 后、写回前调用 `ApplyAntigravityCarryforwardOnUpdateCAS(merged, prevRaw)`——仅当 credentials JSON 的 `upstream_type == "antigravity"` 且 `credential_family_id` 与上一代一致时，把 `prevRaw` 中的 `antigravity_quota` 原样搬入新 merged（严格保留 `observed_at`/`reset`/`ttl`/`applied_seq` 与 known_zero，`antigravity_quota_epoch` 原值保留不递增）；身份/family 变化时不搬运（ReplaceCAS 语义不受影响）。该逻辑按 `upstream_type` 条件化，**不得改变 Grok 既有语义**（Spec §4.1 落点注意、验收 #10）。
- `HandleAntigravityRTConflict`：在同一 family lease 调用约定下 reload 持久化行（不复用本方内存态），按 Interfaces 列出的三分支互斥分流；分支 (b) 的受控补写复用 `UpdateAccountCredentialsCAS` 且 updates 仅含凭据字段（不携带 `antigravity_quota`），再失败即转分支 (c) 处置，不循环重试（Spec §4.1 #5、验收 #39）。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./database/ -run 'TestAntigravityQuotaSource_' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add database/antigravity_quota_source.go database/antigravity_quota_source_test.go database/grok_state.go
git commit -m "feat(database): implement per-source CAS reservation, Latest-Success-Wins and quota_epoch barrier"
```

---

### Task 6: Scheduler Outbox 触发器扩展与 Consumer 传播落地

**Files:**
- Modify: `database/scheduler_outbox.go:191-221,336-364`
- Modify: `auth/scheduler_outbox_consumer.go:317-336,464-608`
- Create: `database/scheduler_outbox_antigravity_test.go`

**Interfaces:**
- Produces:
  - SQLite 与 PostgreSQL 触发器 WHEN 条件包含 `antigravity_quota`、`credential_generation` 与 `antigravity_quota_epoch`
  - Consumer 在 settings 变更时调用 `reloadAntigravityConfig(ctx)`
  - Consumer 在 accounts 变更时对已有内存 `Account` 对象执行配额反序列化、epoch 比对与门禁重算

- [ ] **Step 1: Write the failing test**

```go
// database/scheduler_outbox_antigravity_test.go
package database

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestSchedulerOutbox_TriggersOnQuotaAndEpochChange 覆盖 Spec §10.1 #33 的三组场景，
// SQLite 与 Postgres 各自执行一条触发器测试（PG 未配置隔离 DSN 时跳过）。
func TestSchedulerOutbox_TriggersOnQuotaAndEpochChange(t *testing.T) {
	for _, driver := range []string{"sqlite", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			dsn := filepath.Join(t.TempDir(), "outbox.db")
			if driver == "postgres" {
				dsn = os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("requires isolated CODEX2API_TEST_POSTGRES_DSN")
				}
			}
			db, err := New(driver, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()

			insert := func(name, creds string) int64 {
				res, err := db.conn.ExecContext(ctx,
					`INSERT INTO accounts (name, platform, type, credentials, status, enabled, locked, credential_generation) VALUES (?, 'antigravity', 'oauth', ?, 'active', 1, 0, 1)`,
					name, creds)
				if err != nil {
					t.Fatalf("insert account failed: %v", err)
				}
				id, lastIDErr := res.LastInsertId()
				if lastIDErr != nil {
					if scanErr := db.conn.QueryRowContext(ctx, `SELECT id FROM accounts WHERE name = ?`, name).Scan(&id); scanErr != nil {
						t.Fatalf("resolve id failed: %v", scanErr)
					}
				}
				return id
			}
			accountEventsAfterBaseline := func() map[int64]string {
				watermark, err := db.SchedulerOutboxHighWatermark(ctx)
				if err != nil {
					t.Fatalf("read watermark failed: %v", err)
				}
				events, err := db.ListSchedulerOutboxEventsAfter(ctx, watermark, 100)
				if err != nil {
					t.Fatalf("list events failed: %v", err)
				}
				out := map[int64]string{}
				for _, e := range events {
					if e.EntityType == SchedulerEntityAccount {
						out[e.EntityID] = e.EventType
					}
				}
				return out
			}

			// 场景 1：仅 antigravity_quota 变化。
			quotaID := insert("ag-quota", `{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":1}`)
			if _, err := db.conn.ExecContext(ctx, `UPDATE accounts SET credentials = ? WHERE id = ?`,
				`{"upstream_type":"antigravity","antigravity_quota":"{\"schema_version\":2}","antigravity_quota_epoch":1}`, quotaID); err != nil {
				t.Fatalf("update quota failed: %v", err)
			}
			if ev := accountEventsAfterBaseline(); ev[quotaID] != "updated" {
				t.Fatalf("expected outbox event on antigravity_quota change, got %v", ev)
			}

			// 场景 2：仅 credential_generation 变化而 token 未变（验证 AFTER UPDATE OF 列清单）。
			genID := insert("ag-gen", `{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":1}`)
			if _, err := db.conn.ExecContext(ctx, `UPDATE accounts SET credential_generation = credential_generation + 1 WHERE id = ?`, genID); err != nil {
				t.Fatalf("bump generation failed: %v", err)
			}
			if ev := accountEventsAfterBaseline(); ev[genID] != "updated" {
				t.Fatalf("expected outbox event on credential_generation change, got %v", ev)
			}

			// 场景 3：仅 quota_epoch 变化而 quota 内容等价重写（clear 场景）。
			epochID := insert("ag-epoch", `{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":1}`)
			if _, err := db.conn.ExecContext(ctx, `UPDATE accounts SET credentials = ? WHERE id = ?`,
				`{"upstream_type":"antigravity","antigravity_quota":"{}","antigravity_quota_epoch":2}`, epochID); err != nil {
				t.Fatalf("bump epoch failed: %v", err)
			}
			if ev := accountEventsAfterBaseline(); ev[epochID] != "updated" {
				t.Fatalf("expected outbox event on antigravity_quota_epoch change, got %v", ev)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./database/ -run 'TestSchedulerOutbox_TriggersOnQuotaAndEpochChange' -count=1`
Expected: FAIL with `expected outbox event on antigravity_quota_epoch change`

- [ ] **Step 3: Write minimal implementation**

在 `database/scheduler_outbox.go` 修改 SQLite（`installSQLiteSchedulerOutboxTriggers`，L164-221）与 PG（`installPostgresSchedulerOutboxTriggers`，L314-364）两侧的 accounts update 触发器。**必须同时修改两处，缺一不可**：
1. `AFTER UPDATE OF` 列清单追加 `credential_generation`（accounts 独立列，当前清单缺失）——否则"generation 变而 token 未变"的 UPDATE 语句根本不会评估触发器，WHEN 条件补得再全也无从触发；
2. WHEN 条件追加三个比较（SQLite 用 `json_extract` + `IS NOT`，PG 用 `->>' + IS DISTINCT FROM`）：
```sql
-- SQLite
OR COALESCE(json_extract(OLD.credentials,'$.antigravity_quota'),'') IS NOT COALESCE(json_extract(NEW.credentials,'$.antigravity_quota'),'')
OR COALESCE(json_extract(OLD.credentials,'$.antigravity_quota_epoch'),'') IS NOT COALESCE(json_extract(NEW.credentials,'$.antigravity_quota_epoch'),'')
OR OLD.credential_generation IS NOT NEW.credential_generation
-- PostgreSQL
OR COALESCE(OLD.credentials->>'antigravity_quota','') IS DISTINCT FROM COALESCE(NEW.credentials->>'antigravity_quota','')
OR COALESCE(OLD.credentials->>'antigravity_quota_epoch','') IS DISTINCT FROM COALESCE(NEW.credentials->>'antigravity_quota_epoch','')
OR OLD.credential_generation IS DISTINCT FROM NEW.credential_generation
```
`antigravity_quota` 与 `antigravity_quota_epoch` 均为明文存储（`database/credential_crypto.go:34-42` 不在加密字段集合），可直接 JSON 比较（Spec §4.3 #1；仅 epoch 变化也必须投影，否则远端内存 epoch 永久落后，Spec §10.1 #33）。

在 `auth/scheduler_outbox_consumer.go`（函数名以现有代码为准）：
1. `applySchedulerOutboxEvent`（L318-338）的 `SchedulerEntitySettings` 分支（现状仅调用 `reloadSchedulerEngineSetting`）追加 `reloadAntigravityConfig(ctx)`：从 DB 读取最新 `antigravity_config`、归一化、按 `config_revision` 单调比对（缺失/回退按 Task 4 的基线重建规则处理）、调用 `auth.SetConfiguredAntigravitySettings` 并唤醒本地调度器（Spec §8.2"Settings Outbox Consumer 明确增补落地"）。
2. `applyPersistentAccountSnapshot`（L464-608）反序列化 `antigravity_quota`（V2 快照）与 `antigravity_quota_epoch`，投影到 `dst.AntigravityQuota`（Task 2 新增字段）并持久化 epoch 基线；按 Spec §4.3 单调规则发布：epoch 与内存一致且 `applied_seq >=` 内存序列才原位合并；**epoch 更新的空事实直接覆盖清空门禁**（否则远端 `known_zero` 永远无法被删除动作覆盖），epoch 更旧或相同的空事实一律丢弃；epoch 变化时即刻重算门禁并按需 `notifySchedulerAvailability`。**自愈义务**：本机因内存 epoch 落后而被发布规则丢弃的观测，必须在该 outbox 事件到达时以新 epoch 基线重新投影一次。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./database/ -run 'TestSchedulerOutbox_TriggersOnQuotaAndEpochChange' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add database/scheduler_outbox.go auth/scheduler_outbox_consumer.go database/scheduler_outbox_antigravity_test.go
git commit -m "feat(outbox): add quota and epoch triggers and propagate to consumer"
```

---

### Task 7: Store 级控制面协调器与真网络单飞（Coordinator & Flight）

**Files:**
- Create: `auth/antigravity_coordinator.go`
- Create: `auth/antigravity_coordinator_test.go`

**Interfaces:**
- Produces:
  - `type AntigravityCoordinator struct`
  - `NewAntigravityCoordinator(sharedBackgroundLimit int) *AntigravityCoordinator`（默认 limit=4）
  - `AcquireFlight(ctx context.Context, accountID int64, opType string) (*FlightLease, error)`（支持 Context 取消，单账号互斥）
  - `AcquirePermit(ctx context.Context, opType string) (func(), error)`（后台任务申请 4 许可；`isInteractiveOpType(opType) == true` 的交互操作——`http_401_recovery`/`lazy_refresh`/`manual_refresh`——不占 4 许可直接放行）
  - `isInteractiveOpType(opType string) bool`（通道划分依据）
  - 通道与优先级职责边界：本 Task 只负责通道划分与单飞互斥；Spec §5.2.3 的操作类别仲裁（安全状态 > 401 恢复 > manual > 逾期任务 > 常规 summary > 常规 catalog，优先级类别为第一关键字）由 Task 8 调度器的最小堆分层排序实现，不在本 Task 重复建设。

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_coordinator_test.go
package auth

import (
	"context"
	"testing"
	"time"
)

func TestAntigravityCoordinator_DualChannelAndFlightMutualExclusion(t *testing.T) {
	coord := NewAntigravityCoordinator(4)
	ctx := context.Background()

	// 1. 同一账号 flight 互斥
	lease1, err := coord.AcquireFlight(ctx, 101, "background_summary")
	if err != nil {
		t.Fatalf("acquire lease1 failed: %v", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = coord.AcquireFlight(timeoutCtx, 101, "manual_refresh")
	if err == nil {
		t.Fatal("expected second acquire on same account to block and timeout")
	}
	lease1.Release()

	// 2. 交互通道不占后台 4 并发许可
	// 占满 4 个后台许可
	var releases []func()
	for i := 0; i < 4; i++ {
		rel, err := coord.AcquirePermit(ctx, "background_summary")
		if err != nil {
			t.Fatalf("acquire background permit %d failed: %v", i, err)
		}
		releases = append(releases, rel)
	}

	// 第 5 个后台许可应阻塞（用新鲜的短超时 context，验证许可饱和阻塞而非 context 取消）
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer blockedCancel()
	_, err = coord.AcquirePermit(blockedCtx, "background_summary")
	if err == nil {
		t.Fatal("expected 5th background permit to block until timeout")
	}

	// 交互通道不受后台 4 许可限制，可立即获取
	relInteractive, err := coord.AcquirePermit(ctx, "http_401_recovery")
	if err != nil {
		t.Fatalf("interactive permit should not be blocked: %v", err)
	}
	relInteractive()

	for _, rel := range releases {
		rel()
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestAntigravityCoordinator_' -count=1`
Expected: FAIL with `undefined: NewAntigravityCoordinator`

- [ ] **Step 3: Write minimal implementation**

```go
// auth/antigravity_coordinator.go
package auth

import (
	"context"
	"sync"
)

type FlightLease struct {
	coord     *AntigravityCoordinator
	accountID int64
}

func (fl *FlightLease) Release() {
	fl.coord.releaseFlight(fl.accountID)
}

type AntigravityCoordinator struct {
	mu           sync.Mutex
	flightMap    map[int64]chan struct{}
	bgSem        chan struct{}
}

func NewAntigravityCoordinator(sharedBackgroundLimit int) *AntigravityCoordinator {
	if sharedBackgroundLimit <= 0 {
		sharedBackgroundLimit = 4
	}
	return &AntigravityCoordinator{
		flightMap: make(map[int64]chan struct{}),
		bgSem:     make(chan struct{}, sharedBackgroundLimit),
	}
}

func (c *AntigravityCoordinator) AcquireFlight(ctx context.Context, accountID int64, opType string) (*FlightLease, error) {
	for {
		c.mu.Lock()
		lockCh, exists := c.flightMap[accountID]
		if !exists {
			ch := make(chan struct{})
			c.flightMap[accountID] = ch
			c.mu.Unlock()
			return &FlightLease{coord: c, accountID: accountID}, nil
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-lockCh:
		}
	}
}

func (c *AntigravityCoordinator) releaseFlight(accountID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.flightMap[accountID]; ok {
		delete(c.flightMap, accountID)
		close(ch)
	}
}

// isInteractiveOpType 报告操作是否属于交互/恢复通道（Spec §4.2 双通道划分）。
func isInteractiveOpType(opType string) bool {
	switch opType {
	case "http_401_recovery", "lazy_refresh", "manual_refresh":
		return true
	}
	return false
}

func (c *AntigravityCoordinator) AcquirePermit(ctx context.Context, opType string) (func(), error) {
	// 交互通道不占后台 4 许可，仅受每账号 flight 与跨实例 family lease 约束。
	if isInteractiveOpType(opType) {
		return func() {}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.bgSem <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-c.bgSem
			})
		}, nil
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestAntigravityCoordinator_' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_coordinator.go auth/antigravity_coordinator_test.go
git commit -m "feat(auth): implement AntigravityCoordinator with dual-channel permits and flight mutex"
```

---

### Task 8: 连续均匀分散调度算法与最小堆驱动器

**Files:**
- Create: `auth/antigravity_quota_refresh.go`
- Create: `auth/antigravity_quota_refresh_test.go`

**Interfaces:**
- Produces:
  - `type AntigravitySpacedScheduler struct`
  - `NominalSlotTime(j, k int, interval time.Duration, n int, epoch time.Time, theta time.Duration) time.Time`
  - `ComputeBackoff(interval time.Duration, consecutiveFailures int) time.Duration`（`min(I * 2^k, max(4I, 60min))`）
  - `CheckCoverageWindow(nominalDue time.Time, observedAt time.Time, period time.Duration) bool`（`nominalDue - period < observedAt <= nominalDue`）

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_quota_refresh_test.go
package auth

import (
	"testing"
	"time"
)

func TestComputeBackoff(t *testing.T) {
	// I=60min: k=1 -> 120m (2h); k=2 -> 240m (4h); 封顶 max(4I, 60m)=240m
	i60 := 60 * time.Minute
	if b1 := ComputeBackoff(i60, 1); b1 != 2*time.Hour {
		t.Fatalf("expected k=1 backoff 2h, got %v", b1)
	}
	if b2 := ComputeBackoff(i60, 2); b2 != 4*time.Hour {
		t.Fatalf("expected k=2 backoff 4h, got %v", b2)
	}
	if b3 := ComputeBackoff(i60, 3); b3 != 4*time.Hour {
		t.Fatalf("expected k=3 capped backoff 4h, got %v", b3)
	}

	// I=3min: max(4I, 60m)=60m. k=1 -> 6m, k=2 -> 12m, k=3 -> 24m, k=4 -> 48m, k=5 -> 60m 封顶
	i3 := 3 * time.Minute
	if b1 := ComputeBackoff(i3, 1); b1 != 6*time.Minute {
		t.Fatalf("expected k=1 backoff 6m, got %v", b1)
	}
	if b5 := ComputeBackoff(i3, 5); b5 != 60*time.Minute {
		t.Fatalf("expected k=5 capped backoff 60m, got %v", b5)
	}
}

func TestCheckCoverageWindow(t *testing.T) {
	due := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	period := 60 * time.Minute

	// 11:30 在 (11:00, 12:00] 内 -> 消费
	if !CheckCoverageWindow(due, due.Add(-30*time.Minute), period) {
		t.Fatal("expected 11:30 to be within coverage window of 12:00")
	}
	// 10:59 在 11:00 前 -> 不在覆盖窗
	if CheckCoverageWindow(due, due.Add(-61*time.Minute), period) {
		t.Fatal("expected 10:59 to be outside coverage window")
	}
	// 12:01 超过 due -> 不在名义覆盖窗
	if CheckCoverageWindow(due, due.Add(1*time.Minute), period) {
		t.Fatal("expected 12:01 to be outside coverage window")
	}
}

func TestNominalSlotTime_UniformSpacingAndSharedTheta(t *testing.T) {
	epoch := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	interval := 60 * time.Minute

	// 1. N=4、theta=0：j=0..3 的槽位在全周期内均匀等距（(j+0.5)*Δ，Δ=I/N=15m）
	wantOffsets := []time.Duration{7500 * time.Second, 22500 * time.Second, 37500 * time.Second, 52500 * time.Second}
	for j, want := range wantOffsets {
		got := NominalSlotTime(j, 0, interval, 4, epoch, 0)
		if got.Sub(epoch) != want {
			t.Fatalf("N=4 j=%d offset = %v, want %v", j, got.Sub(epoch), want)
		}
	}

	// 2. theta 只整体旋转全部槽位，绝不破坏槽位间 Δ 的均匀恒定间距（Spec §5.3.2）；
	//    mod 周期边界后 j=3 与下一周期 j=0 相接，循环序列仍然等距。
	theta := 7 * time.Minute
	slot := func(j, k int) time.Time { return NominalSlotTime(j, k, interval, 4, epoch, theta) }
	for j := 1; j < 4; j++ {
		if gap := slot(j, 0).Sub(slot(j-1, 0)); gap != 15*time.Minute {
			t.Fatalf("theta must not break uniform spacing, j=%d gap=%v", j, gap)
		}
	}
	if gap := slot(0, 1).Sub(slot(3, 0)); gap != 15*time.Minute {
		t.Fatalf("cyclic spacing across period boundary = %v, want 15m", gap)
	}
	if base0 := slot(0, 0); base0.Sub(NominalSlotTime(0, 0, interval, 4, epoch, 0)) != theta {
		t.Fatal("shared theta must rotate all slots by a constant offset")
	}

	// 3. N=1：Δ=I，槽位仍按公式带 (j+0.5)Δ 偏移（不得退化到无偏移的 epoch+k*I）
	if got := NominalSlotTime(0, 0, interval, 1, epoch, 0); got.Sub(epoch) != 30*time.Minute {
		t.Fatalf("N=1 j=0 offset = %v, want 30m", got.Sub(epoch))
	}

	// 4. 周期推进：T_slot(j,k+1) - T_slot(j,k) == I
	if got := NominalSlotTime(2, 1, interval, 4, epoch, 0).Sub(NominalSlotTime(2, 0, interval, 4, epoch, 0)); got != interval {
		t.Fatalf("period advance = %v, want %v", got, interval)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestComputeBackoff|TestCheckCoverageWindow|TestNominalSlotTime' -count=1`
Expected: FAIL with `undefined: ComputeBackoff`

- [ ] **Step 3: Write minimal implementation**

在 `auth/antigravity_quota_refresh.go` 中实现数学公式：
```go
package auth

import (
	"math"
	"time"
)

func ComputeBackoff(interval time.Duration, consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		return interval
	}
	mult := math.Pow(2, float64(consecutiveFailures))
	backoff := time.Duration(float64(interval) * mult)
	capVal := 4 * interval
	if capVal < 60*time.Minute {
		capVal = 60 * time.Minute
	}
	if backoff > capVal {
		return capVal
	}
	return backoff
}

func CheckCoverageWindow(nominalDue time.Time, observedAt time.Time, period time.Duration) bool {
	windowStart := nominalDue.Add(-period)
	return observedAt.After(windowStart) && !observedAt.After(nominalDue)
}

// NominalSlotTime 按 Spec §5.3.2 公式计算名义槽位：
// T_slot(j,k) = T_epoch + k*I + ((j + 1/2) * Δ + θ) mod I，Δ = I/N。
// 每个名义时间由 (j,k) 独立计算，禁止累加舍入后的 Δ 造成漂移；θ 为单实例
// 周期共有初始相位，只整体旋转槽位，不破坏 Δ 均匀间距。N<=0 视为无资格
// 账号（调度器空闲），退化为 epoch + k*I。N=1 时 Δ=I，(0+0.5)*I + θ 对 I 取模
// 即半周期偏移，与公式一致，不做特判。
func NominalSlotTime(j, k int, interval time.Duration, n int, epoch time.Time, theta time.Duration) time.Time {
	if n <= 0 || interval <= 0 {
		return epoch.Add(time.Duration(k) * interval)
	}
	delta := interval / time.Duration(n)
	offset := (time.Duration(float64(j)+0.5)*delta + theta) % interval
	return epoch.Add(time.Duration(k)*interval + offset)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestComputeBackoff|TestCheckCoverageWindow|TestNominalSlotTime' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_quota_refresh.go auth/antigravity_quota_refresh_test.go
git commit -m "feat(auth): add spaced scheduling slot math, coverage window and backoff"
```

---

### Task 9: 计划重排防抖与配额软权重防抖

**Files:**
- Create: `auth/antigravity_quota_debounce.go`
- Create: `auth/antigravity_quota_debounce_test.go`

**Interfaces:**
- Produces:
  - `type RescheduleDebouncer struct`（`quiet_window = 100ms`, `max_wait = 1s`）
  - `Trigger()`、`Stop()`、绑定可注入 `Clock`
  - 软权重读时计算 + 1s 派生缓存（raw 变 0 立即生效）

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_quota_debounce_test.go
package auth

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRescheduleDebouncer_CoalescesEvents(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	var runCount int32
	debouncer := NewRescheduleDebouncer(clk, 100*time.Millisecond, 1*time.Second, func() {
		atomic.AddInt32(&runCount, 1)
	})

	// 1. 触发一次，未满 100ms 不执行（deadline 未到，无任何异步路径可触发）
	debouncer.Trigger()
	clk.Advance(50 * time.Millisecond)
	if atomic.LoadInt32(&runCount) != 0 {
		t.Fatal("debouncer executed before quiet window")
	}

	// 2. 持续频繁触发，保持 quiet 窗不断刷新，直到 max_wait (1s) 强制执行。
	// max_wait 到点后 Trigger 在调用栈内同步结算（不依赖 timer goroutine 调度），
	// 因此本断言是确定性的：无论 timer 回调 goroutine 与 Trigger#13 谁先拿锁，
	// 都恰好执行一次（consumeLocked 的 fired != d.timer 合并保护兜底）。
	for i := 0; i < 15; i++ {
		debouncer.Trigger()
		clk.Advance(80 * time.Millisecond)
	}
	if atomic.LoadInt32(&runCount) != 1 {
		t.Fatalf("expected max_wait to trigger exactly 1 execution, got %d", atomic.LoadInt32(&runCount))
	}
	debouncer.Stop()
}

func TestRescheduleDebouncer_StopPreventsExecution(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	var runCount int32
	debouncer := NewRescheduleDebouncer(clk, 100*time.Millisecond, 1*time.Second, func() {
		atomic.AddInt32(&runCount, 1)
	})

	debouncer.Trigger()
	debouncer.Stop()
	clk.Advance(2 * time.Second)
	if atomic.LoadInt32(&runCount) != 0 {
		t.Fatal("stopped debouncer must not execute")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestRescheduleDebouncer_' -count=1`
Expected: FAIL with `undefined: NewRescheduleDebouncer`

- [ ] **Step 3: Write minimal implementation**

```go
// auth/antigravity_quota_debounce.go
package auth

import (
	"sync"
	"time"
)

// RescheduleDebouncer 实现 Spec §7.5.1 的计划重排防抖（quiet_window=100ms，
// max_wait=1s）。实现要点：
//   - action 一律在 d.mu 之外同步执行：max_wait 到点的 Trigger 在调用栈内结算，
//     测试与调用方在 Trigger 返回后即可观察副作用，不依赖 goroutine 调度；
//   - 每轮 timer 配对一个 stopCh，被合并/停止/Stop 时 close，等待 goroutine
//     经 select 退出，绝不泄漏（原草案 `go func(){ <-t.C() }` 在 Stop 后永久
//     阻塞，属goroutine 泄漏，已修正）；
//   - timer 回调拿锁后用 consumeLocked(fired) 复核 fired 仍是当前 timer，
//     与并发 Trigger 的合并语义互斥，保证一轮事件恰好执行一次。
type RescheduleDebouncer struct {
	clk         Clock
	quietWindow time.Duration
	maxWait     time.Duration
	action      func()

	mu         sync.Mutex
	timer      Timer
	timerStop  chan struct{}
	firstEvent time.Time
	pending    bool
	stopped    bool
}

func NewRescheduleDebouncer(clk Clock, quiet, max time.Duration, action func()) *RescheduleDebouncer {
	if clk == nil {
		clk = DefaultClock
	}
	return &RescheduleDebouncer{
		clk:         clk,
		quietWindow: quiet,
		maxWait:     max,
		action:      action,
	}
}

// Trigger 记录一次变更事件。账号禁用/删除等必须立即生效的事件不经过本防抖器，
// 由调用方直接执行重排（Spec §7.5.1）。
func (d *RescheduleDebouncer) Trigger() {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	now := d.clk.Now()
	if !d.pending {
		d.pending = true
		d.firstEvent = now
	}

	timeRemaining := d.maxWait - now.Sub(d.firstEvent)
	delay := d.quietWindow
	if timeRemaining < delay {
		delay = timeRemaining
	}
	if delay <= 0 {
		// max_wait 已过：锁内结算，锁外同步执行，调用返回后副作用可见。
		action := d.consumeLocked(nil)
		d.mu.Unlock()
		if action != nil {
			action()
		}
		return
	}

	if d.timerStop != nil {
		close(d.timerStop)
		d.timerStop = nil
	}
	if d.timer != nil {
		d.timer.Stop()
	}
	t := d.clk.NewTimer(delay)
	stopCh := make(chan struct{})
	d.timer = t
	d.timerStop = stopCh
	d.mu.Unlock()

	go func(t Timer, stopCh <-chan struct{}) {
		select {
		case <-t.C():
			d.mu.Lock()
			action := d.consumeLocked(t)
			d.mu.Unlock()
			if action != nil {
				action()
			}
		case <-stopCh:
			// 本轮 timer 已被更新的 Trigger 或 Stop 合并/终止，goroutine 退出。
		}
	}(t, stopCh)
}

// consumeLocked 在持锁状态下结算当前 pending 轮次并返回待执行回调。
// fired 非 nil 时要求它仍是当前 timer，否则说明该轮已被更新的 Trigger 合并。
// 返回 nil 表示无轮次可结算（幂等，重复回调不重复执行）。
func (d *RescheduleDebouncer) consumeLocked(fired Timer) func() {
	if !d.pending {
		return nil
	}
	if fired != nil && fired != d.timer {
		return nil
	}
	d.pending = false
	d.timer = nil
	if d.timerStop != nil {
		close(d.timerStop)
		d.timerStop = nil
	}
	return d.action
}

// Stop 终止防抖器：丢弃 pending 轮次、停止 timer 并释放等待 goroutine。
// 不能只靠 timer.Stop 防竞态（Spec §7.5.3），stopped 标志使后续 Trigger 直接返回。
func (d *RescheduleDebouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopped = true
	if d.timerStop != nil {
		close(d.timerStop)
		d.timerStop = nil
	}
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.pending = false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestRescheduleDebouncer_' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_quota_debounce.go auth/antigravity_quota_debounce_test.go
git commit -m "feat(auth): implement reschedule debouncer with quiet window and max_wait bounds"
```

---

### Task 10: 额度调度权重增强与会话亲和隔离（Balance）

**Files:**
- Modify: `auth/antigravity_balance.go:130-165`
- Create: `auth/antigravity_balance_quota_test.go`

**Interfaces:**
- Produces:
  - `(snap *AntigravityQuotaSnapshot) ComputeQuotaPressure(now time.Time) float64`
  - 1:1 周与 5h 平均，absent 省略，中性分 0.5（证实存在但数值未知），groups 缺失回退 models
  - 加权公式中以 `w_u * quotaPressure` 替换原 `w_u * usage`（防双罚）
  - 已绑定健康账号完全豁免软打分

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_balance_quota_test.go
package auth

import (
	"testing"
	"time"
)

func TestComputeQuotaPressure_Scenarios(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	// 1. 周 0.8, 5h 0.8 -> pressure = (0.2 + 0.2)/2 = 0.2
	w08, f08 := 0.8, 0.8
	snap1 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
					{Window: "5h", RemainingFraction: &f08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap1.ComputeQuotaPressure(now); p != 0.2 {
		t.Fatalf("expected pressure 0.2, got %v", p)
	}

	// 2. 周 0.8, 5h absent -> pressure = 0.2 (absent 省略)
	snap2 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap2.ComputeQuotaPressure(now); p != 0.2 {
		t.Fatalf("expected absent 5h omitted -> pressure 0.2, got %v", p)
	}

	// 3. groups 全无 -> 回退 models
	m06 := 0.6
	snap3 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Models: []AntigravityModelQuota{
			{ModelID: "gemini-2.5-pro", RemainingFraction: &m06, FractionPresent: true},
		},
	}
	if p := snap3.ComputeQuotaPressure(now); p != 0.4 {
		t.Fatalf("expected models fallback pressure 0.4, got %v", p)
	}

	// 4. Spec §7.6 示例 3：周 0.8 + 5h 证实存在但数值不可用 -> (0.2 + 0.5)/2 = 0.35
	//    （中性分 0.5 只给"权威清单证实存在"的窗口；存在性未知仍省略）
	snap4 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {SummaryIntegrity: true},
		},
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
					{Window: "5h", FractionPresent: false}, // 证实存在，数值不可用
				},
			},
		},
	}
	if p := snap4.ComputeQuotaPressure(now); p != 0.35 {
		t.Fatalf("expected Exists window to contribute neutral 0.5 -> pressure 0.35, got %v", p)
	}

	// 5. 桶过期（时效不可用）在权威清单下同样降级为中性 0.5，而不是静默省略
	snap5 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {SummaryIntegrity: true},
		},
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now.Add(-2 * time.Hour), TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap5.ComputeQuotaPressure(now); p != 0.5 {
		t.Fatalf("expected expired bucket to fall back to neutral 0.5, got %v", p)
	}

	// 6. 同窗口多桶取最小剩余（最紧桶决定压力，Spec §7.2）
	f05 := 0.5
	snap6 := &AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &f05, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
					{Window: "weekly", RemainingFraction: &w08, FractionPresent: true, ObservedAt: now, TTLSeconds: 3600},
				},
			},
		},
	}
	if p := snap6.ComputeQuotaPressure(now); p != 0.5 {
		t.Fatalf("expected tightest weekly bucket (0.5) -> pressure 0.5, got %v", p)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestComputeQuotaPressure_' -count=1`
Expected: FAIL with `undefined: ComputeQuotaPressure`

- [ ] **Step 3: Write minimal implementation**

在 `auth/antigravity_balance.go` 实现 `ComputeQuotaPressure`（三态聚合：Present / Exists / 省略；同窗口多桶取最小剩余；时效按桶 `observed_at + ttl_seconds` 判定）：
```go
// aggregateQuotaWindow 聚合一个白名单窗口的桶状态：
//   - present=true：存在 presence 有效、数值合法且未过期的桶，value 取最小剩余；
//   - exists=true：权威完整清单（summary_integrity）证实窗口存在，但数值/时效不可用；
//   - 两者皆 false：缺席或存在性未知（调用方省略，不惩罚）。
func aggregateQuotaWindow(snap *AntigravityQuotaSnapshot, window string, now time.Time) (value float64, present, exists bool) {
	best := math.Inf(1)
	foundAny := false
	for _, g := range snap.Groups {
		for _, b := range g.Buckets {
			w, ok := NormalizeAntigravityWindow(b.Window)
			if !ok || w != window {
				continue
			}
			foundAny = true
			if !b.FractionPresent || b.RemainingFraction == nil {
				continue
			}
			v := *b.RemainingFraction
			if v < 0.0 || v > 1.0 || math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			if !b.ObservedAt.IsZero() && now.After(b.ObservedAt.Add(time.Duration(b.TTLSeconds)*time.Second)) {
				continue // 时效不可用
			}
			if v < best {
				best = v
			}
			present = true
		}
	}
	if present {
		return best, true, false
	}
	if foundAny && snap.SourceMetadata["groups"].SummaryIntegrity {
		return 0, false, true
	}
	return 0, false, false
}

func (snap *AntigravityQuotaSnapshot) ComputeQuotaPressure(now time.Time) float64 {
	if snap == nil {
		return 0.5
	}
	var total, weight float64
	for _, window := range []string{"weekly", "5h"} {
		value, present, exists := aggregateQuotaWindow(snap, window, now)
		switch {
		case present:
			total += 1.0 - value
			weight += 1.0
		case exists:
			total += 0.5 // 证实存在但数值/时效不可用：中性分（Spec §7.2）
			weight += 1.0
		}
		// absent / 存在性未知：省略，不给"无 5h 限制"的账号无端降权（Spec §7.2 修订）
	}
	if weight > 0 {
		return total / weight
	}

	// groups 完全不可用（无 summary、窗口全部未识别、quota 关闭期间）：
	// 回退 models 级压力，只统计 presence 有效的条目，防止加权避让静默退化。
	var modelSum float64
	var modelCount int
	for _, m := range snap.Models {
		if m.FractionPresent && m.RemainingFraction != nil {
			v := *m.RemainingFraction
			if v >= 0.0 && v <= 1.0 && !math.IsNaN(v) && !math.IsInf(v, 0) {
				modelSum += v
				modelCount++
			}
		}
	}
	if modelCount > 0 {
		return 1.0 - (modelSum / float64(modelCount))
	}

	// 全无事实：全局中性先验。
	return 0.5
}
```

**打分替换（防双罚，Spec §7.1、验收 #19）**：`auth/antigravity_balance.go:148-150` 现状 `score := load + usage*usageWeight + penalty*penaltyWeight`（`usage := acc.usagePercentForScheduling() / 100.0`）必须改为读时计算 `quotaPressure` 并**整体替换** usage 项，绝不叠加：
```go
// Fresh 候选循环内（同一 Account.mu 读锁内原子读取，Spec §7.4）：
pressure := acc.antigravityQuotaPressureForScheduling(now) // 读时基于 raw 快照 ComputeQuotaPressure
score := load + pressure*usageWeight + penalty*penaltyWeight
```
`antigravityQuotaPressureForScheduling` 为 Account 新增方法：`a.mu` 读锁内取 `a.AntigravityQuota` 调 `ComputeQuotaPressure(now)`（`SchemaVersion != 2` 或无事实时返回 0.5 中性值）。**健康亲和豁免**：该软分只参与 `nextAccountForFreshAffinityWithDispatch` 的候选排序；既有健康绑定复用分支（`NextForContinuation*` 的有效绑定路径）不调用本方法，天然豁免（Spec §7.3，验收由 Task 12 续链用例与 Task 16 #19 覆盖）。权重沿用 `AntigravityThrottleWeights()` 的 usage 权重位，不新增独立常量（Spec §7.1）。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestComputeQuotaPressure_' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_balance.go auth/antigravity_balance_quota_test.go
git commit -m "feat(auth): compute quota pressure replacing usage without double penalty"
```

---

### Task 11: 内存只读硬门禁谓词与生命周期到期维护（Gate & Expiry）

**Files:**
- Create: `auth/antigravity_quota_gate.go`
- Create: `auth/antigravity_quota_gate_test.go`

**Interfaces:**
- Produces:
  - `(a *Account) IsAntigravityQuotaExhaustedLocked(now time.Time) (exhausted bool, reason string, resetAt time.Time)`
  - 按 §3.3 分流：已知零持续至可信 `resetAt`，无可信 `resetAt` 时按 TTL 兜底；TTL 到期标记 `stale` 绝不静默放行；正余额 `min(resetAt, ttl)`。
  - `quota_gate_enabled=false` 时立即返回 `exhausted=false`。
  - 来源与元数据校验：`SchemaVersion != 2`、groups 元数据缺失 / `applied_seq <= 0` / `observed_at` 零值或在未来时一律返回不阻断（Spec §3.1 元数据可信条件）。
- Depends on: Task 2 新增的 `Account.AntigravityQuota AntigravityQuotaSnapshot` 内存投影字段与 Task 3 的 `NormalizeAntigravityWindow` / `ValidateResetTime`、Task 4 的 `GetQuotaGateEnabled` 访问器。本方法为纯内存只读谓词：严禁任何 I/O 与派生 goroutine（Spec §3.3）。

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_quota_gate_test.go
package auth

import (
	"testing"
	"time"
)

func TestIsAntigravityQuotaExhaustedLocked_Rules(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	zero := 0.0
	pos := 0.5

	// verifiedMeta 构造满足 Spec §3.1 元数据可信条件的 groups 来源元数据。
	verifiedMeta := func(observed time.Time) map[string]SourceMeta {
		return map[string]SourceMeta{
			"groups": {AppliedSeq: 5, ObservedAt: observed, PresenceVerified: true},
		}
	}
	weeklyZero := func(observed, reset time.Time, ttl int) AntigravityQuotaSnapshot {
		return AntigravityQuotaSnapshot{
			SchemaVersion:  2,
			SourceMetadata: verifiedMeta(observed),
			Groups: []AntigravityQuotaGroup{
				{
					Buckets: []AntigravityQuotaBucket{
						{
							Window:            "weekly",
							RemainingFraction: &zero,
							FractionPresent:   true,
							ResetTime:         reset.Format(time.RFC3339),
							ObservedAt:        observed,
							ObservationSeq:    5,
							TTLSeconds:        ttl,
						},
					},
				},
			},
		}
	}

	// 1. 周 0 且 resetAt 在 2h 后：阻断，resetAt 精确上浮
	acc := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: weeklyZero(now, now.Add(2*time.Hour), 3600)}
	exhausted, _, reset := acc.IsAntigravityQuotaExhaustedLocked(now)
	if !exhausted {
		t.Fatal("expected weekly 0 to be exhausted")
	}
	if reset.Sub(now) != 2*time.Hour {
		t.Fatalf("expected reset in 2h, got %v", reset)
	}

	// 2. TTL 到期 (2小时后) 但 resetAt 还在未来 (3天后)：绝不放行，继续阻断并标记 stale
	accStale := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: weeklyZero(now, now.Add(3*24*time.Hour), 3600)}
	// 时间推进 2 小时 (超过 TTL)
	exhaustedStale, reason, _ := accStale.IsAntigravityQuotaExhaustedLocked(now.Add(2 * time.Hour))
	if !exhaustedStale {
		t.Fatal("stale zero must NOT be silently unblocked after TTL")
	}
	if reason != "stale_zero" && reason != "known_zero" {
		t.Fatalf("expected stale_zero or known_zero, got %q", reason)
	}

	// 3. 正余额在 TTL 到期后变为 unknown 不阻断
	snapPos := AntigravityQuotaSnapshot{
		SchemaVersion:  2,
		SourceMetadata: verifiedMeta(now),
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{
						Window:            "weekly",
						RemainingFraction: &pos,
						FractionPresent:   true,
						ObservedAt:        now,
						ObservationSeq:    6,
						TTLSeconds:        3600,
					},
				},
			},
		},
	}
	accPos := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: snapPos}
	exPos, _, _ := accPos.IsAntigravityQuotaExhaustedLocked(now.Add(2 * time.Hour))
	if exPos {
		t.Fatal("positive quota after TTL should become unknown and not block")
	}

	// 4. legacy 快照（无 schema_version=2 / 无可信 metadata）：绝不硬封（验收 #2）
	legacy := weeklyZero(now, now.Add(2*time.Hour), 3600)
	legacy.SchemaVersion = 0
	legacy.SourceMetadata = nil
	accLegacy := &Account{UpstreamType: UpstreamAntigravity, AntigravityQuota: legacy}
	if ex, _, _ := accLegacy.IsAntigravityQuotaExhaustedLocked(now); ex {
		t.Fatal("legacy snapshot without V2 metadata must never hard-block")
	}

	// 5. quota_gate_enabled=false 止血开关立即放行（验收 #30 前半；测后恢复全局设置）
	prev := ConfiguredAntigravitySettings()
	gateOff := false
	SetConfiguredAntigravitySettings(AntigravitySettings{QuotaGateEnabled: &gateOff})
	defer SetConfiguredAntigravitySettings(prev)
	if ex, _, _ := acc.IsAntigravityQuotaExhaustedLocked(now); ex {
		t.Fatal("quota_gate_enabled=false must immediately unblock")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestIsAntigravityQuotaExhaustedLocked_' -count=1`
Expected: FAIL with `undefined: IsAntigravityQuotaExhaustedLocked`

- [ ] **Step 3: Write minimal implementation**

在 `auth/antigravity_quota_gate.go` 中实现判定逻辑：
```go
package auth

import (
	"time"
)

func (a *Account) IsAntigravityQuotaExhaustedLocked(now time.Time) (bool, string, time.Time) {
	if a == nil || a.UpstreamType != UpstreamAntigravity {
		return false, "", time.Time{}
	}
	// 止血开关：若系统设置关闭门禁，立即放行（Spec §8.1，不影响已落库快照）
	settings := ConfiguredAntigravitySettings()
	if !settings.GetQuotaGateEnabled() {
		return false, "", time.Time{}
	}
	// legacy 快照（无 schema_version=2）：旧数据显式 0 不可信，绝不硬封（Spec §3.1）
	if a.AntigravityQuota.SchemaVersion != 2 {
		return false, "", time.Time{}
	}
	// 元数据可信条件（Spec §3.1）：applied_seq > 0、observed_at 非零且不在未来；
	// 缺失即 unknown，不构成阻断依据。
	meta, ok := a.AntigravityQuota.SourceMetadata["groups"]
	if !ok || meta.AppliedSeq <= 0 || meta.ObservedAt.IsZero() || meta.ObservedAt.After(now) {
		return false, "", time.Time{}
	}

	for _, g := range a.AntigravityQuota.Groups {
		for _, b := range g.Buckets {
			w, ok := NormalizeAntigravityWindow(b.Window)
			if !ok || !b.FractionPresent || b.RemainingFraction == nil {
				continue // 白名单外 / 无 presence / 无数值 → unknown，不阻断
			}
			if *b.RemainingFraction != 0.0 {
				continue // known_positive（含极小正数）与解析层未收敛的非法值一律不阻断
			}
			resetAt, hasReset := ValidateResetTime(b.ResetTime, b.ObservedAt, w)
			ttlDeadline := b.ObservedAt.Add(time.Duration(b.TTLSeconds) * time.Second)
			if hasReset {
				if now.Before(resetAt) {
					if now.After(ttlDeadline) {
						// 超龄已知零（stale）：观测年龄超 TTL 但 resetAt 未到，
						// 保持阻断并标记 stale，进入优先补查，绝不静默放行（Spec §3.3）
						return true, "stale_zero", resetAt
					}
					return true, "known_zero", resetAt // 阻断持续到可信 resetAt
				}
				// resetAt 已过：窗口滚动转 unknown，仅解除本桶，不清其他有效零桶
				continue
			}
			// 无可信 resetAt：TTL 兜底阻断，避免永久封禁
			if now.Before(ttlDeadline) {
				return true, "known_zero", ttlDeadline
			}
		}
	}
	return false, "", time.Time{}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestIsAntigravityQuotaExhaustedLocked_' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_quota_gate.go auth/antigravity_quota_gate_test.go
git commit -m "feat(auth): implement pure in-memory read-only quota gate predicate with resetAt and stale check"
```

---

### Task 12: 调度全路径原子线性化与类型化阻断原因（Store & Dispatch）

**Files:**
- Modify: `auth/store.go:7310-7360,7678-7743`
- Modify: `auth/fast_scheduler.go`
- Create: `auth/antigravity_quota_dispatch_test.go`

**Interfaces:**
- Produces:
  - `var ErrQuotaExhausted = errors.New("antigravity account quota exhausted")`
  - `takeByIDModeWithCapacity` / `isAvailableLocked` 锁内线性化阻断，返回类型化原因 `quota_exhausted`
  - `preserveBinding=true` 续链遇阻断保留绑定返回 nil
  - `preserveBinding=false` 允许借号重绑（D4=C）
  - 门禁状态变化时调用 `notifySchedulerAvailability`

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_quota_dispatch_test.go
package auth

import (
	"testing"
	"time"
)

func TestStore_DispatchQuotaGateLinearizationAndSessionBinding(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	zero := 0.0

	newAccount := func(id int64) *Account {
		// 可调度最小构造，参照 auth/antigravity_types_test.go:15-21 的既有模式。
		return &Account{
			DBID:                 id,
			UpstreamType:         UpstreamAntigravity,
			AccessToken:          "at",
			RefreshToken:         "rt",
			AntigravityProjectID: "project-id",
			HealthTier:           HealthTierHealthy,
		}
	}

	// 已耗尽账号 A（weekly known_zero + 可信 metadata）与健康账号 B
	accA := newAccount(1)
	accA.AntigravityQuota = AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {AppliedSeq: 3, ObservedAt: now, PresenceVerified: true},
		},
		Groups: []AntigravityQuotaGroup{{
			Buckets: []AntigravityQuotaBucket{{
				Window: "weekly", RemainingFraction: &zero, FractionPresent: true,
				ResetTime: now.Add(1 * time.Hour).Format(time.RFC3339),
				ObservedAt: now, ObservationSeq: 3, TTLSeconds: 3600,
			}},
		}},
	}
	accB := newAccount(2)

	store := (&Store{accounts: []*Account{accA, accB}}).withMaxConcurrency(2)
	prevAffinity := GetAntigravityBalancedAffinity()
	SetAntigravityBalancedAffinity(true)
	defer SetAntigravityBalancedAffinity(prevAffinity)
	opts := SelectionOptions{Provider: UpstreamAntigravity}

	// 1. 会话先绑定 A（硬零）。新请求（preserveBinding=false 语义）按 D4=C
	//    fallthrough 到 Fresh 选号：硬零只是"不可用原因"之一，与容量满/鉴权失败
	//    并列，借号并重绑到健康账号 B，不返回错误。
	store.bindSessionAffinity("session_key_1", accA, "")
	selected := store.nextAccountForFreshAffinityWithDispatch("session_key_1", 0, nil, nil, opts)
	if selected == nil || selected.DBID != accB.DBID {
		t.Fatalf("expected borrowing and rebinding to healthy accB, got %+v", selected)
	}

	// 2. 续链请求（preserveBinding=true）绑定到 A：遇阻断必须返回 nil 并保留绑定，
	//    不借号、不静默重绑（映射为 429 + reason=quota_gate_bound 的职责在 proxy
	//    边缘层，见 Task 13）。
	store.bindSessionAffinity("session_key_bound_a", accA, "")
	selectedBound, _ := store.NextForContinuationWithDispatchForProvider(opts, "session_key_bound_a", 0, nil, nil)
	if selectedBound != nil {
		t.Fatalf("expected nil for preserveBinding=true with quota exhausted account, got %+v", selectedBound)
	}

	// 3. 门禁关闭（kill switch）后同一 Fresh 选号放行被阻断的账号（止血路径连通性）。
	prevGate := ConfiguredAntigravitySettings()
	gateOff := false
	SetConfiguredAntigravitySettings(AntigravitySettings{QuotaGateEnabled: &gateOff})
	defer SetConfiguredAntigravitySettings(prevGate)
	selectedAfterKillSwitch := store.nextAccountForFreshAffinityWithDispatch("session_key_kill_switch", 0, nil, nil, opts)
	if selectedAfterKillSwitch == nil {
		t.Fatal("quota_gate_enabled=false must restore schedulability for gated accounts")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestStore_DispatchQuotaGateLinearization' -count=1`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

在 `auth/store.go` 中（`isAvailableLocked` 定义于 L1478、Fresh 接管点 `nextAccountForFreshAffinityWithDispatch` 位于 L7763、续链 `NextForContinuationWithDispatchForProvider` 位于 `auth/antigravity_balance.go:216`）：
1. 新增 `ErrQuotaExhausted = errors.New(...)` 与 Account 级类型化阻断查询 `func (a *Account) antigravityQuotaBlockReasonLocked(now time.Time) string`（内部复用 Task 11 的 `IsAntigravityQuotaExhaustedLocked`，返回 `""` / `"quota_exhausted"`）。`isAvailableLocked`（bool 签名不变，避免波及全仓调用点）在其既有 AG 提前返回**之前**插入该检查；`takeByIDModeWithCapacity`、FastScheduler 候选过滤、`tryReclaimSessionSlot` 成功返回前与 401 窄恢复（`antigravityUnauthorizedRecoveryLocked`）之后各自复检，阻断原因随选号结果上浮（Spec §6.2 #2 类型化原因落点）——被阻断账号按"不可选"处理，不计入 `capacityFull`，也**不进入** `WaitForDispatchAvailable` 长时间等待。
2. 在会话粘性匹配分支中：
   - 若 `preserveBinding=true` 且绑定账号被配额阻断，续链返回 nil 并保留绑定（不借号；proxy 边缘层映射为 429 + `reason=quota_gate_bound`）。
   - 若 `preserveBinding=false` 且绑定账号被配额阻断，照常 fallthrough 进入 Fresh 选号流程借号并重绑（D4=C，硬零与容量满/鉴权失败同列，不设 strict 特例）。
3. 挂载协调器与生命周期；门禁状态变化（转为阻断/解除阻断/reset 到期）调用 `notifySchedulerAvailability`，避免排队等待者空等（Spec §6.2 #2）。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestStore_DispatchQuotaGateLinearization' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/store.go auth/fast_scheduler.go auth/antigravity_quota_dispatch_test.go
git commit -m "feat(dispatch): enforce atomic linearization of quota gate and preserveBinding semantics"
```

---

### Task 13: 代理层 429 映射、跳过排队与 countTokens 阻断

**Files:**
- Modify: `proxy/retry_exclusions.go:558-634`
- Modify: `proxy/scheduler_queue_error.go:20-62`
- Modify: `proxy/handler_gemini.go:240-300`
- Create: `proxy/antigravity_quota_proxy_test.go`

**Interfaces:**
- Produces:
  - `auth.QuotaExhaustedError`（`struct { Bound bool; RetryAfter time.Duration }`）：实现 `Error()`，且 `Unwrap/Is` 匹配 `ErrQuotaExhausted`（Task 12 定义）；`RetryAfter` 取被阻断候选中最早可信 `resetAt`（上限 3600s，无可信 reset 取 60s，Spec §6.2 #2），`Bound=true` 表示续链 `preserveBinding=true` 场景（`reason=quota_gate_bound`）
  - `schedulerSelectionAPIError` 识别 `errors.Is(err, auth.ErrQuotaExhausted)` 并返回 429 类 APIError，从 `auth.QuotaExhaustedError` 提取 reason 与 Retry-After
  - `writeSchedulerQueueError` 四协议共用出口增补 429 分支（Gemini/Anthropic/Chat/Responses），并新增 `writeSchedulerQuotaExhaustedError(c *gin.Context, retryAfterSeconds int, reason string)` 直接写出 429 + `Retry-After` + reason（供 countTokens 与续链路径复用）
  - `nextRetryAccountWithGuard` 在识别配额阻断时**不调用** `waitForRetryAccountAvailableWithGuard`，跳过 30s 队列等待
  - native countTokens 仅在候选集合全因配额阻断时返回 429，其余保留既有本地估算回退

- [ ] **Step 1: Write the failing test**

```go
// proxy/antigravity_quota_proxy_test.go
package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/gin-gonic/gin"
)

func TestProxy_QuotaGate429MappingAndSkipQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newContext := func() (*gin.Context, *httptest.ResponseRecorder) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1beta/models/m:generateContent", nil)
		return c, w
	}

	// 1. 整池配额阻断：429 + Retry-After + reason=quota_gate
	c, w := newContext()
	writeSchedulerQuotaExhaustedError(c, 120, "quota_gate")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "120" {
		t.Fatalf("expected Retry-After 120, got %q", got)
	}
	if !strings.Contains(w.Body.String(), "quota_gate") {
		t.Fatalf("expected reason quota_gate in body, got %s", w.Body.String())
	}

	// 2. 续链阻断：reason=quota_gate_bound（与整池耗尽、容量类 503 三者可区分）
	c2, w2 := newContext()
	writeSchedulerQuotaExhaustedError(c2, 60, "quota_gate_bound")
	if w2.Code != http.StatusTooManyRequests || !strings.Contains(w2.Body.String(), "quota_gate_bound") {
		t.Fatalf("expected 429 with quota_gate_bound, got %d %s", w2.Code, w2.Body.String())
	}

	// 3. schedulerSelectionAPIError 识别配额阻断（writeSchedulerQueueError 的 429 分支入口）
	bound := &auth.QuotaExhaustedError{Bound: true, RetryAfter: 90 * time.Second}
	if apiErr := schedulerSelectionAPIError(bound); apiErr == nil {
		t.Fatal("schedulerSelectionAPIError must recognize auth.QuotaExhaustedError")
	}
	// 容量类错误保持既有 503 语义，不与配额 429 混用
	if apiErr := schedulerSelectionAPIError(auth.ErrSchedulerQueueFull); apiErr == nil {
		t.Fatal("existing queue-full mapping must remain")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./proxy/ -run 'TestProxy_QuotaGate429MappingAndSkipQueue' -count=1`
Expected: FAIL with `undefined: writeSchedulerQuotaExhaustedError`

- [ ] **Step 3: Write minimal implementation**

在 `auth` 包（与 Task 12 同处或 `auth/antigravity_quota_gate.go`）定义：
```go
// QuotaExhaustedError 携带对外 429 契约所需的重试与原因信息。
// Bound=true 表示续链 preserveBinding=true 场景（reason=quota_gate_bound）。
type QuotaExhaustedError struct {
	Bound      bool
	RetryAfter time.Duration
}

func (e *QuotaExhaustedError) Error() string {
	if e.Bound {
		return "antigravity bound account quota exhausted"
	}
	return "antigravity account quota exhausted"
}

// Is 使 errors.Is(err, ErrQuotaExhausted) 对两种原因都成立。
func (e *QuotaExhaustedError) Is(target error) bool { return target == ErrQuotaExhausted }
```

在 `proxy/scheduler_queue_error.go`：
- `schedulerSelectionAPIError`（L21-29）增加分支：`case errors.Is(err, auth.ErrQuotaExhausted):` 从 `*auth.QuotaExhaustedError` 提取 reason（`quota_gate` / `quota_gate_bound`）与 Retry-After 秒数（`RetryAfter.Seconds()` 向上取整，缺省 60，上限 3600），返回 429 类 APIError。
- `writeSchedulerQueueError`（L31-62）按 apiErr 状态码分流：429 时调用 `writeSchedulerQuotaExhaustedError(c, retryAfterSeconds, reason)`（Gemini/Anthropic/Chat/Responses 四协议各自在函数内写出差异化错误体），既有 503 分支语义不变。

新增 `writeSchedulerQuotaExhaustedError`（同文件）：
```go
// writeSchedulerQuotaExhaustedError 输出 429 + Retry-After + reason。
// reason 取 "quota_gate"（整池耗尽）或 "quota_gate_bound"（续链保留绑定），
// 与容量类 503、上游真实 429（reason=upstream_429）在日志与响应中可区分。
func writeSchedulerQuotaExhaustedError(c *gin.Context, retryAfterSeconds int, reason string) {
	if retryAfterSeconds <= 0 {
		retryAfterSeconds = 60
	}
	if retryAfterSeconds > 3600 {
		retryAfterSeconds = 3600
	}
	c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))
	message := "Antigravity account quota exhausted (" + reason + ")"
	switch protocol := continuousRetryProtocolOf(c); protocol {
	case continuousRetryProtocolAnthropic:
		sendAnthropicError(c, http.StatusTooManyRequests, "rate_limit_error", message)
	case continuousRetryProtocolChat:
		c.JSON(http.StatusTooManyRequests, gin.H{"error": gin.H{"code": http.StatusTooManyRequests, "type": "quota_gate", "reason": reason, "message": message}})
	default: // Gemini / Responses
		writeGeminiNativeError(c, http.StatusTooManyRequests, message)
	}
}
```
（`continuousRetryProtocolOf` 若无现成探针，则以调用方传入的 protocol 参数替代；不得靠对候选集合重新求值 filter 推断 reason——Spec §6.2 #2 严禁二次求值。）

在 `proxy/retry_exclusions.go` 的 `nextRetryAccountWithGuard`（L558-634）：
- 选号返回 nil 且携带 `ErrQuotaExhausted`（选号层以类型化原因上浮，Task 12）时，**直接返回配额阻断错误**（由调用方经 `schedulerSelectionAPIError` 映射 429），**不得**进入 `waitForRetryAccountAvailableWithGuard` 的 30s 队列等待——`resetAt` 常在数小时后，等待只会白耗预算（Spec §6.2 #2、验收 #38）；混合原因（部分配额、部分容量/冷却）不满足"全部因配额阻断"，仍按既有 503/无可用账号路径处理。

在 `proxy/handler_gemini.go`：
- countTokens 入口（L247 / L278 / L296 三处 `estimateInputTokens` 静默回退）**仅**在选号错误满足 `errors.Is(selectionErr, auth.ErrQuotaExhausted)` 时改为 `writeSchedulerQuotaExhaustedError(c, retrySeconds, "quota_gate")` 并 return；其余无号成因（选号超时、队列准入拒绝、上游 404、不可重试传输错误）**保持既有 200 + 估算回退不变**（D8=A 只改变"候选集合全部因配额阻断"分支，Spec §6.2 #3、验收 #25）。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./proxy/ -run 'TestProxy_QuotaGate429MappingAndSkipQueue' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add proxy/retry_exclusions.go proxy/scheduler_queue_error.go proxy/handler_gemini.go proxy/antigravity_quota_proxy_test.go
git commit -m "feat(proxy): map ErrQuotaExhausted to 429 with Retry-After and skip queue wait"
```

---

### Task 14: 管理端接口改造与运维止血清除操作

**Files:**
- Modify: `admin/antigravity_accounts.go:1966-2117`
- Modify: `admin/antigravity_state.go:215-250`
- Modify: `admin/handler.go`
- Create: `admin/antigravity_quota_admin_test.go`

**Interfaces:**
- Produces:
  - `Store.SyncAntigravityAccountForAdmin(ctx, accountID int64)` 替代原直接 `client.Sync`（入 family lease、90/100s 有界等待）
  - `POST /api/admin/antigravity/quota/clear` 止血接口（清除事实、递增 `quota_epoch`、清空门禁并触发补查）
  - `/state` 响应返回 V2 四态与 metadata

- [ ] **Step 1: Write the failing test**

```go
// admin/antigravity_quota_admin_test.go
package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAdmin_ClearAntigravityQuotaAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newTestAdminDB(t)
	ctx := context.Background()

	// 插入带 quota 事实与 epoch=1 的 AG 账号（helper 见 admin/handler_test.go:3734）
	accountID, err := db.InsertAccountWithCredentials(ctx, "ag-clear", map[string]interface{}{
		"upstream_type":           "antigravity",
		"access_token":            "at",
		"credential_family_id":    "fam1",
		"antigravity_quota":       `{"schema_version":2}`,
		"antigravity_quota_epoch": 1,
	}, "")
	if err != nil {
		t.Fatalf("insert antigravity account failed: %v", err)
	}

	handler := &Handler{db: db}
	body := `{"account_ids":[` + strconv.FormatInt(accountID, 10) + `]}`
	req := httptest.NewRequest(http.MethodPost, "/api/admin/accounts/antigravity/quota/clear", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	handler.ClearAntigravityQuotaHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	// 清除必须同事务递增 quota_epoch（Spec §8.2 运维止血、验收 #30/#36）并删除事实
	epoch, epochErr := db.GetAntigravityQuotaEpoch(ctx, accountID)
	if epochErr != nil || epoch != 2 {
		t.Fatalf("expected epoch 2 after clear, got (%d, %v)", epoch, epochErr)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./admin/ -run 'TestAdmin_ClearAntigravityQuotaAPI' -count=1`
Expected: FAIL with 404 Not Found

- [ ] **Step 3: Write minimal implementation**

在 `admin/antigravity_accounts.go`：
1. 将手工同步改为调用 `store.SyncAntigravityAccountForAdmin`（不再直接 `client.Sync`，现状见 `admin/antigravity_accounts.go:2012`；90/100s 有界等待在途 flight，部分成功只复用已满足来源，Spec §4.2 手动请求有界等待）。
2. 实现 `ClearAntigravityQuotaHandler`：解析 `account_ids`（最多 100 个，超限返回 400），逐个调用 `db.ClearAntigravityQuotaSourcesCAS`（同一短事务删除 quota source 事实、递增 `quota_epoch`，保留凭据与 models），经 Store 投影清空本机内存门禁并触发一次优先补查，写入审计日志（Spec §8.2、验收 #30 后半）。
在 `admin/handler.go` 的 AG 路由组（L1175-1194 的 `api` 组）注册：`api.POST("/accounts/antigravity/quota/clear", h.ClearAntigravityQuotaHandler)`——完整路径 `/api/admin/accounts/antigravity/quota/clear`，与既有 `POST /accounts/antigravity/refresh` 批量风格一致，即 Spec §8.2 `/api/admin/antigravity/quota/clear` 止血操作的落点。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./admin/ -run 'TestAdmin_ClearAntigravityQuotaAPI' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add admin/antigravity_accounts.go admin/antigravity_state.go admin/handler.go admin/antigravity_quota_admin_test.go
git commit -m "feat(admin): implement quota clear kill-switch API and wrap manual sync with Store coordinator"
```

---

### Task 15: 前端类型、设置卡片与配额事实清除交互

**Files:**
- Modify: `frontend/src/types.ts`
- Modify: `frontend/src/api.ts`
- Modify: `frontend/src/pages/Settings.tsx`
- Modify: `frontend/src/pages/AntigravityAccounts.tsx`
- Modify: `frontend/src/locales/zh.json`, `frontend/src/locales/en.json`, `frontend/src/locales/zh-TW.json`
- Create: `frontend/src/lib/antigravityQuota.test.mjs`

**Interfaces:**
- Produces:
  - 前端类型新增 `quota_refresh_enabled`、`quota_refresh_interval_minutes`、`quota_gate_enabled`、`fraction_present`
  - 前端 API 新增 `clearAntigravityQuota(ids: number[])`
  - Settings 页面新增 Antigravity 渠道配额刷新卡片
  - 账号管理页新增“清除配额事实”按钮及二次确认
  - 中英繁三语国际化文案

- [ ] **Step 1: Write the failing test**

```javascript
// frontend/src/lib/antigravityQuota.test.mjs
// 注意：本仓库 locales 为嵌套结构（如 "qualityTest": {...}，见 src/locales/zh.json），
// 新键统一挂在 "antigravityQuota" 命名空间下，不是顶层平铺键。
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const requiredLeafKeys = [
  'refreshEnabled',
  'refreshInterval',
  'gateEnabled',
  'clearAction',
  'clearConfirm',
];

function readNamespace(locale, file) {
  const raw = JSON.parse(fs.readFileSync(`src/locales/${file}`, 'utf8'));
  const ns = raw.antigravityQuota;
  assert.ok(ns && typeof ns === 'object', `missing antigravityQuota namespace in ${file}`);
  return ns;
}

test('Antigravity quota i18n keys present across all locales', () => {
  const namespaces = {
    zh: readNamespace('zh', 'zh.json'),
    en: readNamespace('en', 'en.json'),
    tw: readNamespace('zh-TW', 'zh-TW.json'),
  };

  for (const [locale, ns] of Object.entries(namespaces)) {
    for (const k of requiredLeafKeys) {
      assert.ok(ns[k] && ns[k].length > 0, `missing antigravityQuota.${k} in ${locale}`);
    }
  }
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd frontend && node --test src/lib/antigravityQuota.test.mjs`
Expected: FAIL with `missing antigravityQuota namespace in zh.json`

- [ ] **Step 3: Write minimal implementation**

1. 在 `frontend/src/locales/zh.json`、`en.json`、`zh-TW.json` 顶层各新增 `"antigravityQuota"` 命名空间，含 `refreshEnabled`、`refreshInterval`、`gateEnabled`、`clearAction`、`clearConfirm` 五个叶子键（界面另可扩展 hint 文案，但上述键为测试必需）。
2. 在 `frontend/src/types.ts` 补充 `remaining_fraction: number | null` + `fraction_present: boolean`（与 Task 2 V2 契约对齐）及设置字段 `quota_refresh_enabled`、`quota_refresh_interval_minutes`、`quota_gate_enabled`。
3. 在 `frontend/src/api.ts` 新增 `clearAntigravityQuota(ids: number[])`（POST `/api/admin/accounts/antigravity/quota/clear`）与设置 PUT 透传新字段。
4. 在 `frontend/src/pages/Settings.tsx` 添加配额卡片（自动刷新开关、硬门禁开关、3-60min 数字输入框；保存成功后本机 ≤1s 重排，跨副本经 outbox ≤5s 生效，界面提示"关闭仅停止自动配额查询，模型目录维护仍独立"）。
5. 在 `frontend/src/pages/AntigravityAccounts.tsx` 添加"清除配额事实"操作按钮与二次确认模态框。

- [ ] **Step 4: Run test to verify it passes**

Run: `cd frontend && node --test src/lib/antigravityQuota.test.mjs && pnpm run typecheck`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add frontend/src/
git commit -m "feat(ui): add Antigravity quota settings card, clear facts action and i18n keys"
```

---

### Task 16: 全链路集成与 39 条验收测试全量对齐验证

**Files:**
- Create: `auth/antigravity_quota_integration_test.go`

**Interfaces:**
- Consumes: 全套 Task 1-15 组件
- Produces:
  - `auth/antigravity_quota_integration_test.go` 中 `TestAntigravityQuota_AcceptanceMatrix`（子测试覆盖跨模块组合场景）；
  - Spec §10.1 全部 39 项验收的**完整覆盖矩阵**（专项 Task 测试 + 本集成任务，无遗漏）：

| 验收项 | 覆盖位置（测试函数 / 落点） |
| :--- | :--- |
| #1 数值四态 | Task 3 `TestClassifyAntigravityFraction_FourStates` |
| #2 旧 Schema 兼容防误封 | Task 11 用例 4 + 集成 `s34_legacy_snapshot_soft_only` |
| #3 坏桶隔离 | Task 3 `TestApplyQuotaGroupsV2_BadBucketIsolationAndMerge` |
| #4 Weekly 0 阻断 | Task 11 用例 1 + 集成 `s4_s5_any_effective_zero_bucket_blocks_whole_account` |
| #5 5h 0 阻断 | 集成 `s4_s5_any_effective_zero_bucket_blocks_whole_account` |
| #6 窗口白名单 | Task 3 `TestNormalizeAntigravityWindow` |
| #7 reset 到期与 TTL 兜底 | Task 11 用例 1/2 + 集成 `s7_reset_expiry_drops_only_that_bucket` |
| #8 共享并发与平滑派发 | Task 7 `TestAntigravityCoordinator_DualChannelAndFlightMutualExclusion` + Task 8 `TestNominalSlotTime_UniformSpacingAndSharedTheta` |
| #9 Manual/401 消费 slot 不重复 summary | Task 8 `TestCheckCoverageWindow`（覆盖窗判定单元）+ Task 14 `Store.SyncAntigravityAccountForAdmin`（复用已满足来源，Spec §4.2 有界等待） |
| #10 同身份 carryforward | Task 5 `TestAntigravityQuotaSource_UpdateCASSameIdentityCarryforward` |
| #11 单账号单飞与防死锁 | Task 7 flight 互斥 + owner 复用（Spec §4.2，交互通道 `isInteractiveOpType` 不重入自身资源） |
| #12 Lazy 模式与开关矩阵 | Task 12 用例 3（kill switch 恢复调度）+ Task 7 `isInteractiveOpType`（lazy 走交互通道）+ 集成 `s7`（关闭采集后本地到期维护常驻的谓词侧验证） |
| #13 Settings PUT CAS | Task 4 `TestSaveAntigravityConfigCAS_WholeJSONCompareAndSwap` |
| #14 重启 Fresh coverage | Task 8 `TestCheckCoverageWindow` + 集成 `s34`（持久化时标解码后 coverage 重建输入） |
| #15 CriticalContext 4min 与优雅停机 | Task 7 `FlightLease.Release` + Task 9 `TestRescheduleDebouncer_StopPreventsExecution`（timer 生命周期收尾不依赖 caller cancel；4min lease hold 为 Spec §5.5 既有安全语义，不在测试中模拟真实换票） |
| #16 多实例局部 round 独立 | 集成 `s36_s37_epoch_barrier_and_reservation_order`（observationID/epoch 幂等去重，无跨副本 round 账本） |
| #17 Sync final-bearer 原子提交 | Task 5 `TestAntigravityQuotaSource_LatestSuccessWinsAndEpochBarrier`（LSW 提交协议） |
| #18 Strict 遇硬零借号 (D4=C) | Task 12 用例 1 |
| #19 quotaPressure 替换 usage 防双罚 | Task 10 `TestComputeQuotaPressure_Scenarios`（含打分替换说明） |
| #20 双重防抖与硬状态即刻 | Task 9 两个测试 + Task 11/12 门禁即时断言（防抖不经过硬门禁路径） |
| #21 Filter 单次求值与 Handled 保护 | Task 12 测试（`nextAccountForFreshAffinityWithDispatch` 的 `handled=true` 直接返回路径，auth/store.go:7763-7775 既有注释锚点） |
| #22 FakeClock 稳态分布 | Task 8 NominalSlotTime 测试 + 集成 `s22_fake_clock_steady_state` |
| #23 Churn 扰动防饥饿 | Task 8 theta 旋转/循环等距测试（重排期限仲裁以名义时间函数为可验证单元） |
| #24 配置缩短专项 | 集成 `s24_shortened_interval_no_catch_up` |
| #25 countTokens 阻断 | Task 13 `TestProxy_QuotaGate429MappingAndSkipQueue` + Task 13 实现说明的三处回退点分流 |
| #26 异常状态保留 | Task 3 `TestClassifyAntigravityFraction_FourStates`（403/404/5xx 不产生数值事实）+ Task 11（失败不洗新，gate 只认已提交有效事实） |
| #27 脱敏真实验证（D2=B） | **人工验收检查单（非自动化测试）**，见本 Task Step 3 |
| #28 Latest-Success-Wins 反例 | Task 5 LSW 测试 + 集成 `s36_s37` |
| #29 stale 阻断 | Task 11 用例 2 |
| #30 门禁止血与清除事实 | Task 11 用例 5 + Task 14 `TestAdmin_ClearAntigravityQuotaAPI` |
| #31 ErrQuotaExhausted → 429 契约 | Task 13 `TestProxy_QuotaGate429MappingAndSkipQueue` + 集成 `s4_s5`（整池无可选账号） |
| #32 models-only 回退 | Task 10 场景 3 + 集成 `s34` |
| #33 outbox 三场景双方言 | Task 6 `TestSchedulerOutbox_TriggersOnQuotaAndEpochChange` |
| #34 混合版本与回滚 | 集成 `s34_legacy_snapshot_soft_only` |
| #35 退避真实生效 | Task 8 `TestComputeBackoff`（I=60 时 2h/4h 封顶，非恒定 60min） |
| #36 clear 屏障拦截迟到探测 | Task 5 epoch barrier + 集成 `s36_s37` |
| #37 逐 source 预约与观测同序 | 集成 `s36_s37_epoch_barrier_and_reservation_order` |
| #38 跳过排队与 429 传播 | Task 13（skip queue + 四协议出口）+ Task 12 用例 2（续链 nil 不重绑） |
| #39 RT 冲突三分支 | Task 5 `TestAntigravityQuotaSource_RTConflictThreeBranches` |

- [ ] **Step 1: Write the failing test**

```go
// auth/antigravity_quota_integration_test.go
package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codex2api/database"
)

// TestAntigravityQuota_AcceptanceMatrix 是 Spec §10.1 39 项验收的集成装配层：
// 逐项断言由各专项 Task 的测试覆盖（见上方覆盖矩阵），本文件对必须跨模块
// 组合的场景给出端到端验证。任一子测试失败即整体验收不通过。
func TestAntigravityQuota_AcceptanceMatrix(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	newAccount := func(id int64) *Account {
		return &Account{
			DBID: id, UpstreamType: UpstreamAntigravity,
			AccessToken: "at", RefreshToken: "rt",
			AntigravityProjectID: "project-id", HealthTier: HealthTierHealthy,
		}
	}
	verifiedMeta := func(observed time.Time) map[string]SourceMeta {
		return map[string]SourceMeta{"groups": {AppliedSeq: 3, ObservedAt: observed, PresenceVerified: true}}
	}

	t.Run("s4_s5_any_effective_zero_bucket_blocks_whole_account", func(t *testing.T) {
		zero, pos := 0.0, 0.5
		// #5：weekly 正、5h 零 —— 周配额充足也必须整账号阻断。
		snap := AntigravityQuotaSnapshot{
			SchemaVersion:  2,
			SourceMetadata: verifiedMeta(now),
			Groups: []AntigravityQuotaGroup{{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &pos, FractionPresent: true, ObservedAt: now, ObservationSeq: 3, TTLSeconds: 3600},
					{Window: "5h", RemainingFraction: &zero, FractionPresent: true, ResetTime: now.Add(2 * time.Hour).Format(time.RFC3339), ObservedAt: now, ObservationSeq: 3, TTLSeconds: 3600},
				},
			}},
		}
		acc := newAccount(501)
		acc.AntigravityQuota = snap
		exhausted, reason, _ := acc.IsAntigravityQuotaExhaustedLocked(now)
		if !exhausted || reason != "known_zero" {
			t.Fatalf("5h zero with positive weekly must block, got (%v,%q)", exhausted, reason)
		}

		// #4/#31 组合：唯一账号被阻断时整池 Fresh 选号返回 nil（无可选账号；
		// proxy 层映射 429 + Retry-After 的出口见 Task 13）。
		store := (&Store{accounts: []*Account{acc}}).withMaxConcurrency(2)
		prevAffinity := GetAntigravityBalancedAffinity()
		SetAntigravityBalancedAffinity(true)
		defer SetAntigravityBalancedAffinity(prevAffinity)
		if got := store.nextAccountForFreshAffinityWithDispatch("s4", 0, nil, nil, SelectionOptions{Provider: UpstreamAntigravity}); got != nil {
			t.Fatalf("fully gated pool must select nothing, got %+v", got)
		}
	})

	t.Run("s7_reset_expiry_drops_only_that_bucket", func(t *testing.T) {
		zero := 0.0
		// #7：weekly 零（reset 远期）+ 5h 零（reset 到期）——5h 转 unknown 后
		// weekly 零桶继续阻断（Spec §3.3 账户级阻断与桶隔离）。
		snap := AntigravityQuotaSnapshot{
			SchemaVersion:  2,
			SourceMetadata: verifiedMeta(now),
			Groups: []AntigravityQuotaGroup{{
				Buckets: []AntigravityQuotaBucket{
					{Window: "weekly", RemainingFraction: &zero, FractionPresent: true, ResetTime: now.Add(72 * time.Hour).Format(time.RFC3339), ObservedAt: now, ObservationSeq: 3, TTLSeconds: 7200},
					{Window: "5h", RemainingFraction: &zero, FractionPresent: true, ResetTime: now.Add(10 * time.Minute).Format(time.RFC3339), ObservedAt: now, ObservationSeq: 3, TTLSeconds: 7200},
				},
			}},
		}
		acc := newAccount(502)
		acc.AntigravityQuota = snap
		exhausted, reason, reset := acc.IsAntigravityQuotaExhaustedLocked(now.Add(11 * time.Minute))
		if !exhausted || reason != "known_zero" || !reset.Equal(now.Add(72*time.Hour)) {
			t.Fatalf("weekly zero must keep blocking after 5h reset expiry, got (%v,%q,%v)", exhausted, reason, reset)
		}
	})

	t.Run("s22_fake_clock_steady_state", func(t *testing.T) {
		// #22：N=0 空闲退化；N=1 严格按 I（含半间隔相位偏移，与公式一致）。
		epoch := now
		if got := NominalSlotTime(0, 0, 60*time.Minute, 0, epoch, 0); !got.Equal(epoch) {
			t.Fatalf("N=0 must idle at epoch, got %v", got)
		}
		if got := NominalSlotTime(0, 1, 60*time.Minute, 1, epoch, 0); got.Sub(epoch) != 90*time.Minute {
			t.Fatalf("N=1 slot (k=1) = %v, want epoch+90m", got)
		}
	})

	t.Run("s24_shortened_interval_no_catch_up", func(t *testing.T) {
		// #24：60m -> 3m 后首个新周期槽位必须全部落在 (now, now+2I] 内，
		// 不得集中补发历史任务（Spec §5.3 无历史补偿）。
		interval := 3 * time.Minute
		for j := 0; j < 4; j++ {
			slot := NominalSlotTime(j, 1, interval, 4, now, 0)
			if !slot.After(now) {
				t.Fatalf("j=%d slot %v not in the future (catch-up forbidden)", j, slot)
			}
			if slot.Sub(now) > 2*interval {
				t.Fatalf("j=%d slot %v later than 2I after shortening", j, slot)
			}
		}
	})

	t.Run("s34_legacy_snapshot_soft_only", func(t *testing.T) {
		// #34/#2：旧 JSON 无 schema_version/metadata，显式 0 不可信——绝不硬封。
		// legacy models 条目缺 fraction_present → 不计入回退平均，整体回落中性
		// 0.5（Spec §3.1 legacy 软排序语义）；presence 有效的 models 回退见 Task 10 场景 3。
		legacyJSON := `{"remaining_fraction":0,"models":[{"model_id":"m","remaining_fraction":0.8}]}`
		var snap AntigravityQuotaSnapshot
		if err := json.Unmarshal([]byte(legacyJSON), &snap); err != nil {
			t.Fatalf("legacy decode must not fail: %v", err)
		}
		acc := newAccount(503)
		acc.AntigravityQuota = snap
		if ex, _, _ := acc.IsAntigravityQuotaExhaustedLocked(now); ex {
			t.Fatal("legacy explicit zero must never hard-block")
		}
		if p := snap.ComputeQuotaPressure(now); p != 0.5 {
			t.Fatalf("legacy snapshot without presence must stay neutral 0.5, got %v", p)
		}
	})

	t.Run("s36_s37_epoch_barrier_and_reservation_order", func(t *testing.T) {
		// #36/#37/#28/#16 跨模块（auth + database）：预约序单调、LSW 拒绝更早
		// 预约、clear epoch 屏障拦截迟到探测、新 epoch 观测正常提交。
		driver := "sqlite"
		dsn := filepath.Join(t.TempDir(), "it.db")
		if os.Getenv("CODEX2API_TEST_POSTGRES_DSN") != "" {
			driver = "postgres"
			dsn = os.Getenv("CODEX2API_TEST_POSTGRES_DSN")
		}
		db, err := database.New(driver, dsn)
		if err != nil {
			t.Fatalf("open db failed: %v", err)
		}
		defer db.Close()

		accID, err := db.InsertAccountWithCredentials(ctx, "ag-integration", map[string]interface{}{
			"upstream_type": "antigravity", "access_token": "at", "credential_family_id": "fam1",
			"antigravity_quota_epoch": 1,
		}, "")
		if err != nil {
			t.Fatalf("insert account failed: %v", err)
		}
		row, err := db.GetAccountByID(ctx, accID)
		if err != nil || row == nil {
			t.Fatalf("load account failed: %v", err)
		}
		gen, fam := row.CredentialGeneration, row.CredentialFamilyID

		seqA, epochA, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", gen, fam)
		if err != nil {
			t.Fatalf("reserve A failed: %v", err)
		}
		seqB, epochB, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", gen, fam)
		if err != nil {
			t.Fatalf("reserve B failed: %v", err)
		}
		if seqA >= seqB || epochA != epochB {
			t.Fatalf("per-source reservations must be monotonic within one epoch, got (%d,%d,%d,%d)", seqA, seqB, epochA, epochB)
		}

		// 更晚预约的 B 提交成功（LSW）；更早预约的 A 被拒绝。
		if ok, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB, epochB, gen, fam, `{"schema_version":2}`); err != nil || !ok {
			t.Fatalf("later reservation must win: ok=%v err=%v", ok, err)
		}
		if ok, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqA, epochA, gen, fam, `{"schema_version":2}`); err != nil || ok {
			t.Fatalf("earlier reservation must be rejected by LSW: ok=%v err=%v", ok, err)
		}

		// clear 同事务递增 epoch；旧 epoch 的迟到探测被屏障拒绝；
		// 新 epoch 的预约/提交不受旧序号影响。
		newEpoch, err := db.ClearAntigravityQuotaSourcesCAS(ctx, accID)
		if err != nil || newEpoch != epochB+1 {
			t.Fatalf("clear must bump epoch exactly once: (%d,%v)", newEpoch, err)
		}
		if ok, _ := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqB+100, epochB, gen, fam, `{"schema_version":2}`); ok {
			t.Fatal("late probe with stale epoch must be rejected by epoch barrier")
		}
		seqNew, epochNew, err := db.ReserveAntigravityQuotaSource(ctx, accID, "groups", gen, fam)
		if err != nil {
			t.Fatalf("reserve after clear failed: %v", err)
		}
		if epochNew != newEpoch {
			t.Fatalf("post-clear reservation must bind new epoch: %d != %d", epochNew, newEpoch)
		}
		if ok, err := db.CommitAntigravityQuotaSource(ctx, accID, "groups", seqNew, epochNew, gen, fam, `{"schema_version":2}`); err != nil || !ok {
			t.Fatalf("post-clear probe must commit under new epoch: ok=%v err=%v", ok, err)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ -run 'TestAntigravityQuota_AcceptanceMatrix' -count=1`
Expected: FAIL with `undefined: NominalSlotTime`（首个未实现符号）

- [ ] **Step 3: Write minimal implementation**

本 Task 不新增生产代码（组件已在 Task 1-15 实现），集成层只做装配验证与两件收尾事项：
1. **覆盖矩阵核对**：按 Interfaces 的矩阵逐项确认对应测试已存在且通过；任何一项缺失即视为本 Task 未完成，必须在对应专项 Task 补测试后回到本步骤。
2. **#27 脱敏真实验证检查单（人工验收，非自动化测试）**：门禁默认开启（D1=B）且无影子期（D2=B）的唯一上线门槛，执行时须留档：(a) 至少一份真实 `retrieveUserQuotaSummary` 成功响应（脱敏），确认 `groups[]/buckets[]/window/reset_time` 实际语义与格式；(b) 线上出现的窗口字符串全量清单，核对 Task 3 白名单是否覆盖；(c) `remainingFraction` 的 presence 缺失情况与取值分布；(d) 验证不通过则 `quota_gate_enabled` 保持 `false` 发布，不得以影子期观察替代（Spec §10.1 #27）。该项为独立授权事项，不阻塞其余 38 项的自动化验收。

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=/tmp/go-build GOPATH=/tmp/go go test ./auth/ ./database/ ./proxy/ -run 'TestAntigravityQuota_|TestAntigravityQuotaSource_|TestSchedulerOutbox_TriggersOnQuotaAndEpochChange|TestProxy_QuotaGate429MappingAndSkipQueue' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add auth/antigravity_quota_integration_test.go
git commit -m "test(integration): add comprehensive acceptance test suite covering all 39 spec scenarios"
```

---

## Plan Self-Review Checklist

1. **Spec Coverage**:
   - §3 数据契约与四态判定 $\rightarrow$ Task 2（含 `AntigravityModelQuota` 与 `Account.AntigravityQuota` 投影）, Task 3（含四态与坏桶隔离）
   - §3.3 resetAt 到期与 TTL stale $\rightarrow$ Task 11（含 metadata 校验与 kill switch）
   - §4 短事务 CAS、逐 source 预约、carryforward、epoch 屏障、RT 三分支 $\rightarrow$ Task 5（含 LSW / carryforward / RT 三分支测试）
   - §4.2 统一协调器与真网络单飞 $\rightarrow$ Task 7（双通道 + flight；类别仲裁归 Task 8）
   - §4.3 Outbox 触发器（含 `AFTER UPDATE OF` 列清单）与 Consumer 投影/自愈 $\rightarrow$ Task 6
   - §5 连续均匀分散调度算法（名义槽位公式、退避、覆盖窗）$\rightarrow$ Task 8
   - §6 全局硬门禁与 429 映射、strict 借号 (D4=C)、类型化阻断原因 $\rightarrow$ Task 11, Task 12, Task 13（`QuotaExhaustedError` 贯穿 auth→proxy）
   - §7 额度调度权重增强（三态聚合 / models 回退 / 防双罚）与双重防抖 $\rightarrow$ Task 9, Task 10
   - §8 系统配置 CAS、revision 回退基线重建与止血 clear 操作 $\rightarrow$ Task 4, Task 14
   - §10 39 项验收测试矩阵 $\rightarrow$ Task 16 覆盖矩阵表（专项 Task + 集成子测试；#27 为人工验收检查单）
2. **No Placeholders**: 所有步骤均提供具体代码和确切执行命令，无 TODO、TBD、`(...)` 省略号签名或"已在某 Task 验证"式占位；#27 为 Spec 明示的人工验收事项而非代码占位。
3. **Type Consistency**: 函数签名、结构体字段全流程保持一致（`AntigravityQuotaSnapshot.SchemaVersion`, `RemainingFraction *float64` + `FractionPresent`, `CommitAntigravityQuotaSource(..., snapshotJSON string)`, `quota_epoch`, `QuotaExhaustedError{Bound, RetryAfter}`, `RescheduleDebouncer.Stop()`）；测试只引用真实存在的符号（`&Store{accounts: ...}` + `withMaxConcurrency`、`database.New` + `InsertAccountWithCredentials`、`newTestAdminDB`、gin `CreateTestContext`、`SchedulerOutboxHighWatermark` + `ListSchedulerOutboxEventsAfter`）。
4. **Execution Protocol**: 严格遵循用户指定的模型（编写 `local/qwen`、审查 `glm-coding/glm-5.3-flash` / `deepseek-official/deepseek-flash`）、并发上限（$\le 5$ 全局，各角色上限 1）、提示词首行强制声明、单任务原则和禁止轮询规则；所有 Go 测试命令带 `GOCACHE=/tmp/go-build GOPATH=/tmp/go` 前缀。
