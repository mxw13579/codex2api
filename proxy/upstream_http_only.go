package proxy

import "context"

type upstreamHTTPOnlyKey struct{}

// WithUpstreamHTTPOnly requires HTTP (including SSE) for this request's upstream
// transport, even when global settings, account settings or the caller select
// WebSocket. It does not change the account, egress route or detector behavior.
func WithUpstreamHTTPOnly(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, upstreamHTTPOnlyKey{}, true)
}

func isUpstreamHTTPOnly(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	marked, _ := ctx.Value(upstreamHTTPOnlyKey{}).(bool)
	return marked
}
