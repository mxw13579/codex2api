# 管理操作来源记录

应用普通访问日志记录 `ip`（按 `CODEX_TRUSTED_PROXIES` 解析的客户端）与 `peer`（直接 TCP 对端）。安全审计的 `ADMIN_ACCESS` 包含方法、路径、客户端 IP、直接对端、浏览器和去掉 query/fragment/userinfo 的 Referer；管理写操作另记 `ADMIN_RESULT` 及结果状态。在线更新成功另记 `SYSTEM_UPDATE_APPLIED`，包含前后版本和是否准备重启。

这些记录可定位网络来源和客户端特征；共享管理密钥没有个人身份，不能单凭 IP 认定某个自然人。不要把任意 `CF-Connecting-IP` 或 `X-Forwarded-For` 直接当可信身份。

Cloudflare → Nginx → HAProxy → 应用的部署方式：

1. 在 Nginx **http** 范围加载 `codex-admin-audit.conf` 的日志格式。
2. 将 `cloudflare-real-ip.conf` 放到 snippets 目录，仅在相关站点的 **server** 中 include。它只信任 Cloudflare 官方 IP 网段；官方地址更新后需同步该清单。
3. 保留站点原有 access_log，增加 `access_log /var/log/nginx/codex-admin-audit.log codex_audit if=$codex_admin_audit;`，仅为管理接口额外记录解析后地址和原连接地址。原有日志中的 `$remote_addr` 也会成为真实 IP。
4. 代理 location 设置 `X-Real-IP $remote_addr` 与 `X-Forwarded-For $remote_addr`，覆盖外部传入的链；HAProxy 可以再添加自身收到的源地址。
5. 应用的可信代理清单只包含实际受控代理；应用端口不得允许外部绕过代理链访问。默认私网范围适用于隔离的 Docker / 内网，多个不互信租户共享网络时应收紧。
6. 备份原配置，运行 `nginx -t` 后平滑 reload；用经 Cloudflare 的请求和直接访问时伪造转发头的请求分别验证。

日志不记录请求体、管理密钥或请求 query。安全审计沿用 `logs/security/audit.log` 的轮转；Nginx access log 沿用部署侧轮转策略。
