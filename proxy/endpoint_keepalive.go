package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
)

// Error responses are inspected as a bounded prefix. Success JSON has a
// separate, larger budget so media-bearing responses are not capped at 1 MiB.
const maxUpstreamResponseBodyBytes = 64 << 20

var errUpstreamErrorBodyTooLarge = errors.New("upstream error body exceeds 1 MiB")

func readUpstreamErrorBody(ctx context.Context, reader io.Reader) ([]byte, error) {
	// 多读一个字节区分“恰好达到上限”和“被截断”；业务最多仍得到 1 MiB。
	body, err := readAllWithContinuousRetryKeepalive(ctx, io.LimitReader(reader, maxUpstreamErrorBodyBytes+1))
	if len(body) > maxUpstreamErrorBodyBytes {
		return body[:maxUpstreamErrorBodyBytes], errors.Join(err, errUpstreamErrorBodyTooLarge)
	}
	return body, err
}

// upstreamResponseReadContext 保留普通请求取消后的有界 usage 补读窗口。
// 持续重试的私有尝试则随下游立即结束，不能因补读而延长重试生命周期。
func upstreamResponseReadContext(downstreamCtx, upstreamCtx context.Context, policy database.ContinuousRetryPolicy) context.Context {
	if continuousRetryBuffersAttempts(policy) || upstreamCtx == nil {
		return downstreamCtx
	}
	return upstreamCtx
}

// readAllLimitedWithContinuousRetryKeepalive 将读取上限与请求级保活组合，
// 用于非流式成功响应及原生协议 JSON 聚合，避免读体阶段长时间没有下游活动。
func readAllLimitedWithContinuousRetryKeepalive(ctx context.Context, reader io.Reader, limit int64) ([]byte, error) {
	if limit < 0 {
		return nil, fmt.Errorf("response body limit must be non-negative")
	}
	body, err := readAllWithContinuousRetryKeepalive(ctx, io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return body, nil
}

// activateAnthropicMessagesKeepalive 在选号后应用 Messages 的 Claude 开关，
// relay 和 Codex 路径始终启用保活。
func (h *Handler) activateAnthropicMessagesKeepalive(ctx context.Context, account *auth.Account, stream bool) {
	var store *auth.Store
	if h != nil {
		store = h.store
	}
	if stream && (account == nil || account.IsClaudeOAuth()) {
		setContinuousRetryKeepaliveActive(ctx, store != nil && store.ClaudeStreamKeepaliveEnabled())
		return
	}
	setContinuousRetryKeepaliveActive(ctx, true)
}

// setContinuousRetryKeepaliveActive 支持重试换号时在 relay 与 Claude OAuth
// 的不同保活策略之间切换。
func setContinuousRetryKeepaliveActive(ctx context.Context, active bool) {
	keepalive := continuousRetryKeepaliveForContext(ctx)
	if keepalive == nil {
		return
	}
	if controller, ok := keepalive.(interface{ SetEnabled(bool) }); ok {
		controller.SetEnabled(active)
	}
	if active {
		activateContinuousRetryKeepalive(ctx)
	}
}
