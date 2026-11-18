package proxy

import "context"

type downstreamWebsocketSessionKey struct{}

// WithDownstreamWebsocketSession 标记 ctx 属于一个下游 WebSocket 会话（/v1/responses
// WS 入站、realtime 等）。ctx 必须是会话级上下文：下游连接断开时它被取消。
// 上游连接池据此识别"仍被存活下游会话续链依赖"的空闲连接（排空模式下保留）。
// context.WithoutCancel 等派生上下文仍能读到该标记。
func WithDownstreamWebsocketSession(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, downstreamWebsocketSessionKey{}, ctx.Done())
}

// DownstreamWebsocketSessionDone 返回下游 WS 会话的结束信号；不属于下游 WS 会话时返回 nil。
func DownstreamWebsocketSessionDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	done, _ := ctx.Value(downstreamWebsocketSessionKey{}).(<-chan struct{})
	return done
}
