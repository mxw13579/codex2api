# Compose 平滑发版

这套流程把固定 HTTP 入口交给 HAProxy，在两个应用容器之间切换新请求。PostgreSQL、Redis 继续由原 Compose 项目管理，应用的环境变量、持久挂载和共享网络从现有容器读取。无需 Swarm，也不在运行中的容器里替换二进制。

```text
固定入口 / 已有 TLS 反向代理
             │
         HAProxy
             ├── blue  ─┐
             └── green ─┴── 原 PostgreSQL / Redis
```

**保证范围：**正常发版不重启入口；旧 HTTP/SSE 响应和已升级的 WS 连接继续由旧应用处理。脚本只有在旧后端停止接收新请求、活动连接与排队数均为零、新后端仍健康时才停止旧应用。超时保留旧应用，退出码为 `2`；已有连接不会因为部署脚本的等待期限而被强制关闭。应用自身或上游的超时仍然生效。

**适用范围：**Linux Docker Engine、Docker Compose v2+、Python 3.9+，标准 codex2api 镜像中提供 BusyBox `wget`，PostgreSQL + Redis，主要服务同步 HTTP、SSE 和 Responses WebSocket。Docker socket 权限等同宿主机管理权限。脚本使用本地 Unix socket 操作 HAProxy；建议以 `sudo python3` 运行。

## 一次准备

把本目录放在宿主机，例如 `/opt/codex2api/deploy/rolling/`。状态默认放在 `/var/lib/codex2api-rollout`，权限必须是 `700`。其中 `state.json`、`compose.json` 含环境变量和凭据，文件权限为 `600`；不要提交到 Git、打印 `docker compose config` 的完整输出或把状态目录打包分享。

```bash
sudo python3 /opt/codex2api/deploy/rolling/release.py prepare \
  --source codex2api \
  --bind 127.0.0.1 \
  --port 18910
```

`prepare` 只读取现有应用容器、生成配置并执行 Compose 配置校验，不启动/停止任何容器。原来应用与 PostgreSQL、Redis 在同一个 Compose 文件里不影响这一操作：新生成的 Compose 只定义 `blue`、`green`、`edge` 三个服务，通过 **external 网络和 volume** 复用已有资源。

应用没有宿主机端口，两个版本可使用相同容器内部端口（从 `CODEX_PORT` / `PORT` 读取）。只有 HAProxy 暴露选定的固定端口。两个应用不能被客户端绕过入口直连，否则入口连接数不足以证明应用已排空。

自定义状态目录必须在每条命令的子命令之前指定：

```bash
sudo python3 release.py --state-dir /var/lib/my-pool-rollout prepare --source codex2api
```

准备好本次发布镜像，再启动第一个受管理的版本：

```bash
sudo python3 /opt/codex2api/deploy/rolling/release.py bootstrap \
  muze/codex2api:YOUR_VERSION --pull
sudo python3 /opt/codex2api/deploy/rolling/release.py status
curl --fail http://127.0.0.1:18910/health
```

不使用 `--pull` 时只使用本地已有镜像，适合本机构建的版本。应用镜像在准备发布时解析为不可变的本地 image ID；回滚不会重新解析一个已经改变的标签。不要清理仍在状态文件中引用的镜像。HAProxy 默认 `haproxy:3.2-alpine`，需要更严格的镜像管理时，在 `prepare --proxy-image` 中传入审核过的 digest。日常应用发版不更新 HAProxy。

`bootstrap` 首次执行即保存镜像 ID，若中断后入口已开始接单，重试相同命令会继续使用保存的镜像，并以 `--no-recreate` 保留已有应用和入口。不会因为同名标签后来改变而替换正在服务的容器；完成初始化后用 `deploy` 发布另一版本。

首次 `bootstrap` 会另外启动新版应用。**原 `codex2api` 仍然运行，原 Compose 中的数据库、Redis 均不受操作。** 在第一次启动第二实例前完成下述数据库兼容性、并发和后台任务检查。

## 首次入口迁移

首次接入与以后发版分开处理。假设旧应用占用宿主机 `18909`、新代理准备在 `127.0.0.1:18910`：

- 如果调用方经过已有 Nginx，可把相应 `proxy_pass` 切到 `http://127.0.0.1:18910`，执行配置检查和平滑 reload。旧应用继续运行，等旧入口连接结束后再单独停止旧应用。SSE/WS 要保留 HTTP/1.1、Upgrade/Connection 头、长读超时及关闭响应缓冲。TLS 仍由原 Nginx 负责。
- 如果调用方直接访问 `IP:18909`，它们不会自动走新入口。可使用单独的新入口地址逐步迁移调用方，保留原端口给旧连接；也可安排一次迁移窗口，把原端口释放给代理。**普通 Docker 端口映射不能把旧容器上的活动连接迁移到新容器，不能保证首次强占同一端口完全无感。**
- `--bind 127.0.0.1` 仅供宿主机本地反向代理使用；其他容器内的 `127.0.0.1` 指向它们自身。若需要直接公开新端口，首次 `prepare` 时明确设置 `--bind 0.0.0.0`，并按现有入口配置 TLS、防火墙和鉴权。

只有确认所有调用方已切到新入口、原入口请求排空后，才单独停止原应用服务。不要对原项目执行 `docker compose down`，它仍负责 PostgreSQL、Redis 和共享网络。切换后也不要通过原项目的无参数 `docker compose up -d` 重新启动旧应用；维护数据库时指定服务名。

原容器上的连接不经过新代理，脚本不能替你判断它们是否排空。首次迁移需按旧入口实际连接情况处理；后续发版由脚本自动计数。

## 日常发版与回滚

使用新的、明确版本的镜像：

```bash
sudo python3 /opt/codex2api/deploy/rolling/release.py deploy \
  muze/codex2api:YOUR_NEXT_VERSION --pull
```

步骤是：确认备用槽没有旧连接 → 准备新版镜像 → 只启动备用应用 → 等应用健康和代理健康检查通过 → 切换新请求 → 等旧连接排空 → 只停止旧应用。默认启动等待 `180` 秒、排空等待 `300` 秒，可用 `--ready-timeout` / `--drain-timeout` 调整。

HTTP keep-alive 的后续请求可去新版；正在传输的响应留在旧版。已升级的 WS 作为一条长连接保留在旧版，包括该连接上的后续 turn。长时间不关闭的 WS 可能让旧版长期保留；此时脚本不会覆盖该槽执行下一次发布。

```bash
# 查看当前版本、切换日志状态和每个后端的连接/队列数（不打印凭据）
sudo python3 /opt/codex2api/deploy/rolling/release.py status

# 等稍后旧连接结束，再完成清理
sudo python3 /opt/codex2api/deploy/rolling/release.py drain --drain-timeout 300

# 把新请求切回上一个成功版本，保留当前版本尚未结束的连接
sudo python3 /opt/codex2api/deploy/rolling/release.py rollback
```

`rollback` 在旧版仍运行、健康且实际 image ID 与保存版本一致时直接复用；旧版已停止或候选容器与保存版本不同，则在确认无连接后重新启动保存的镜像。首次初始化时把源应用镜像记作初始回滚版本，后续保存上一个成功切换版本；候选版启动失败不会覆盖它。回滚应用镜像不回滚数据库 schema 或业务数据。

切流前先持久化目标路由和操作日志，HAProxy 重启后按已保存目标恢复。若脚本在切流期间中断，其他修改命令会拒绝继续：

```bash
# 完成被中断的目标切换
sudo python3 /opt/codex2api/deploy/rolling/release.py recover

# 或恢复切换前的路由（旧版必须仍健康）
sudo python3 /opt/codex2api/deploy/rolling/release.py recover --abort
```

两种恢复都保留已有连接；之后运行 `drain`。不要手动删除操作日志或独立操作 HAProxy 的后端状态。运行中的发版命令持有宿主机文件锁，避免并行发版；进程退出后锁自动释放。

退出码：`0` 完成；`2` 已切流但旧连接尚未排空，旧应用保留；`1` 检查或操作失败；`130` 被中断。自动化流水线应把 `2` 识别为“等待排空”，不要据此执行 `down` 或强制删容器。

## 部署边界

- **数据库迁移必须兼容新旧版本同时运行。** 新应用启动可能执行迁移；脚本的 `/health` 检查不证明旧版能读取新 schema。涉及删除/重命名字段、破坏性数据迁移或不兼容共享缓存格式时，应分阶段迁移或安排维护窗口。首次启动第二实例前做好数据库备份；脚本不自动迁移/恢复数据库。
- **账号并发和部分限流计数仍在进程内。** “旧版不接收新 HTTP 连接”不代表它已经没有上游工作，长期 WS 也可能继续发起 turn。新旧版的合计账号并发可能高于单进程设置；本流程没有实现跨实例并发租约。满载号池应预留容量、控制新流量，并在排空后观察 429；不要把“双实例”当作可把每号并发翻倍的许可。
- **续聊状态不等于连接迁移。** 已有 WS 保留原连接；新连接或新的 HTTP 请求会进入新版。Redis 可共享部分响应上下文和会话绑定，但不会迁移旧进程内的上游 WS、连接绑定或全部运行态。仅有 `previous_response_id` 且依赖连接内状态的调用，需要在自己的客户端用例中验证续链恢复。
- **该流程不排空异步后台任务。** 显式启用 `IMAGE_JOB_WORKERS` 时脚本拒绝准备；默认旧式图片任务也可能脱离 HTTP 请求继续执行，且旧版启动会标记遗留任务中断。首次和后续发版前确保没有排队/执行中的图片、质量测试等异步任务，并在重叠期间暂停这类任务提交。需要长期多副本图片队列时应另行实现任务租约、共享输入存储和集群并发控制。
- **共享的是已持久化的配置和挂载。** 源容器 writable layer 内的手改文件不会复制；显式声明却未挂载的镜像数据目录会被拒绝。绑定源目录缺失时不自动创建空目录。环境变量是 `prepare` 时快照；以后修改原 `.env` 不会自动更新这个快照。需要变更环境配置时在私有状态中的 `base.environment` 维护，并通过后续发布生效，不要绕过脚本重新创建在用槽。
- **HAProxy 自身是单入口。** 日常应用切换通过管理 socket 完成，不重启它；入口容器/宿主机故障或代理升级仍会断连接。本方案提供应用平滑发版，不提供跨主机高可用。代理 HTTP/SSE 空闲超时为 1 小时，WS 隧道空闲超时为 24 小时，不是发版排空的硬截止时间。

## 验证

离线安全行为测试不调用 Docker、不访问上游：

```bash
python3 -m unittest discover -s deploy/rolling -p 'test_release.py' -v
```

真实容器测试仅使用合成后端，建立 SSE 和 WS 后切流、回滚，验证旧连接继续收发、新请求进入新版、活跃槽不可覆盖、零连接才停止旧版，以及代理重启后保持已选版本：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/rolling-fixture ./deploy/rolling/fixture
docker pull haproxy:3.2-alpine
docker pull redis:7-alpine
sudo python3 deploy/rolling/integration_test.py --fixture /tmp/rolling-fixture
```

这里 Redis 镜像仅作为包含 BusyBox 的合成后端基础镜像，测试不启动 Redis 服务，也不连接任何真实数据库。测试使用独立随机命名的镜像、网络、容器和回环临时端口，在 `finally` 中清理。测试结果不替代实际账号负载、schema 兼容性和续链场景验证。
