package proxy

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

// Exercise the merge boundary with safe preflight enabled and an otherwise
// eligible real TLS/CONNECT route. External scheduling must still bypass local
// cooldowns and leave all retry decisions to the caller.
func TestPinnedResponsesBypassesSafePreflight(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cooldown   bool
		respond    func(http.ResponseWriter)
		wantStatus int
		wantBody   string
	}{
		{
			name: "success despite cooldown", cooldown: true,
			respond: func(w http.ResponseWriter) {
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_pinned_preflight"))
			},
			wantStatus: http.StatusOK, wantBody: "synthetic-success",
		},
		{
			name: "HTTP rate limit without retry",
			respond: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "7")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":{"message":"Too many requests","type":"rate_limit_error"}}`)
			},
			wantStatus: http.StatusTooManyRequests, wantBody: "Too many requests",
		},
		{
			name: "SSE failure without retry",
			respond: func(w http.ResponseWriter) {
				spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spAcceptanceFailed))
			},
			// No content was committed, so the legacy path returns an HTTP error.
			wantStatus: http.StatusServiceUnavailable, wantBody: "synthetic-private-overload",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := spAcceptanceNewHarness(t, 2, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
				tc.respond(w)
			})
			if tc.cooldown {
				h.store.MarkCooldown(h.account, time.Hour, "rate_limited")
				h.store.MarkModelCooldown(h.account, "gpt-5.5", time.Hour, "rate_limited")
			}
			spSharedEdgesAdmission(t, h, func(c *gin.Context) {
				c.Set(contextAPIKeyID, int64(7))
				c.Set(contextAPIKeyRow, &database.APIKeyRow{
					ID: 7, Enabled: true, Limits: database.APIKeyLimits{ExternalScheduler: true},
				})
				c.Set(contextPinnedAccountID, h.account.ID())
			})

			resp, reader, _ := h.request(t)
			body := spAcceptanceReadRest(t, reader)
			h.wait(t)
			if resp.StatusCode != tc.wantStatus || !strings.Contains(body, tc.wantBody) {
				t.Fatalf("status = %d body = %s, want status %d containing %q", resp.StatusCode, body, tc.wantStatus, tc.wantBody)
			}
			if got := resp.Header.Get(PinnedAccountHeader); got != strconv.FormatInt(h.account.ID(), 10) {
				t.Errorf("pinned account header = %q, want the selected account", got)
			}
			if tc.wantStatus == http.StatusTooManyRequests &&
				(resp.Header.Get("Retry-After") != "7" || resp.Header.Get(pinnedUpstreamStatusHeader) != "429") {
				t.Error("pinned response lost the upstream rate limit headers")
			}
			if h.calls.Load() != 1 || h.connects.Load() == 0 || h.badRoute.Load() {
				t.Errorf("upstream calls = %d, want one attempt through the loopback CONNECT proxy", h.calls.Load())
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			if len(h.sends) == 0 {
				t.Fatal("missing outbound request observation")
			}
			for _, send := range h.sends {
				if send.state != nil {
					t.Error("external scheduler request entered local safe-preflight retry")
				}
			}
		})
	}
}
