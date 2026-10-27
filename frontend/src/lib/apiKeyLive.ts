// 自动渠道仍可将 Live 请求路由到 Codex 账号。
export function supportsChatGPTLive(channel: string): boolean {
  return channel === "auto" || channel === "codex";
}
