package proxy

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/codex2api/database"
	"github.com/gin-gonic/gin"
)

func TestSafePreflightFinalizationProxyRejectionDoesNotChargePhantomAttempt(t *testing.T) {
	path, db, quota := spSharedEdgesQuota(t)
	db.SetUsageLogConfig("full", 100, 60)
	gate := spAcceptanceNewGate()
	t.Cleanup(gate.open)
	f := spHTTP2NewFixture(t, 1, db, func(w http.ResponseWriter, r *http.Request, _ int) {
		spAcceptanceFlush(w, spAcceptanceRate)
		if gate.wait(r.Context()) {
			spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
		}
	})
	limits := database.APIKeyLimits{ModelRequestLimits: quota.rules, ScopeLimits: []database.APIKeyScopeLimit{{ScopeType: database.APIKeyScopeTypeAccount, ScopeID: f.h.account.ID(), QuotaRequests: 100}}}
	if err := db.UpdateAPIKeyLimits(context.Background(), quota.keyID, limits); err != nil {
		t.Fatal("synthetic scope quota setup failed")
	}
	spSharedEdgesAdmission(t, f.h, func(c *gin.Context) {
		c.Set(contextAPIKeyID, quota.keyID)
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), apiKeyModelRequestContextKey{}, quota))
	})
	_, reader, _ := f.h.request(t)
	first := spAcceptanceReadEnvelope(t, reader)
	if first != spAcceptanceRate {
		t.Fatal("missing real rate barrier")
	}
	state := f.h.provePath(t, 1)
	connects := f.h.connects.Load()
	release := spSharedEdgesHoldSQLiteWriter(t, path)
	gate.open()
	spSharedEdgesWaitQuota(t, f.h, state, quota, db)
	// nil means direct routing, so the production guard must reject before any
	// dial. No alternative endpoint or fake RoundTripper can hide a stray packet.
	f.transport.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	release()
	wire := first + spAcceptanceReadRest(t, reader)
	result := f.h.wait(t)
	f.h.provePath(t, 1)
	spAcceptanceTerminal(t, wire, "response.failed", "")
	if result.logicalStatus != 502 || f.h.connects.Load() != connects || state.wasSent() {
		t.Error("pre-dial rejection was recorded as a sent attempt")
	}
	db.FlushUsageLogs()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	usage, err := db.GetAPIKeyWindowUsage(ctx, quota.keyID, time.Hour)
	if err != nil || usage.Requests != 1 {
		t.Error("pre-dial local rejection added a phantom request to usage aggregation")
	}
	scoped, err := db.ListAPIKeyScopeCounters(ctx, quota.keyID)
	if err != nil {
		t.Fatal("could not read isolated scope counters")
	}
	counter, exists := scoped[database.APIKeyScopeCounterKey{ScopeType: database.APIKeyScopeTypeAccount, ScopeID: f.h.account.ID()}]
	if !exists || counter.UsedRequests != 1 {
		t.Error("pre-dial local rejection consumed an extra scope request allowance")
	}
	modelUsage, err := db.GetAPIKeyModelRequestUsage(ctx, quota.keyID, quota.rules, time.Now())
	if err != nil || len(modelUsage) != 1 || modelUsage[0].Used != 1 {
		t.Error("logical model-request admission count changed")
	}
}

func TestSafePreflightCancellationDuringRetryWaitKeeps499(t *testing.T) {
	h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
		spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(spAcceptanceFailed))
	})
	h.store.SetRetryIntervalMS(1000)
	resp, reader, cancel := h.request(t)
	if spAcceptanceReadEnvelope(t, reader) != spAcceptanceRate {
		t.Fatal("missing genuine downstream rate barrier")
	}
	deadline := time.Now().Add(750 * time.Millisecond)
	for h.account.GetActiveRequests() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.account.GetActiveRequests() != 0 || h.calls.Load() != 1 {
		t.Fatal("could not isolate the released-lease retry wait")
	}
	cancel()
	resp.Body.Close()
	result := h.wait(t)
	h.provePath(t, 1)
	if result.logicalStatus != logStatusClientClosed {
		t.Errorf("cancelled retry-wait logical status=%d, want 499", result.logicalStatus)
	}
}

func TestSafePreflightFinalizationFailureKeepsOriginalH2Pool(t *testing.T) {
	for _, terminal := range []string{"eof", "failed"} {
		for _, oldState := range []string{"idle", "active"} {
			t.Run(terminal+"_"+oldState, func(t *testing.T) {
				f := spHTTP2NewFixture(t, 0, nil, func(w http.ResponseWriter, _ *http.Request, _ int) {
					frames := spAcceptanceRate
					if terminal == "failed" {
						frames += spAcceptanceFrame(spAcceptanceFailed)
					}
					spAcceptanceFlush(w, frames)
				})
				original := f.warm(t)
				var active *http.Response
				if oldState == "active" {
					var remote string
					active, remote = f.originalRequest(t, "/synthetic-old-active")
					if remote != original {
						t.Fatal("old active stream did not use prewarmed pool")
					}
				}
				_, reader, _ := f.h.request(t)
				wire := spAcceptanceReadRest(t, reader)
				f.h.wait(t)
				f.h.provePath(t, 1)
				spAcceptanceTerminal(t, wire, "response.failed", "")
				for _, prefix := range []string{"", "account-bound|"} {
					entry, ok := clientPool.Load(prefix + clientPoolKey(f.h.account, f.h.proxyURL, codexTransportModeStandard))
					if !ok || entry.(*poolEntry).client != f.client {
						t.Error("failed safe attempt evicted the shared original pool")
					}
				}
				f.mu.Lock()
				own := append([]string(nil), f.remotes...)
				f.mu.Unlock()
				if len(own) != 1 || own[0] == original {
					t.Fatal("failed attempt did not own its H2 connection")
				}
				f.waitClosed(t, own[0])
				if active != nil {
					f.oldStream.open()
					if spAcceptanceReadRest(t, active.Body) != "synthetic-active\nsynthetic-survived\n" {
						t.Error("failed safe cleanup damaged original active stream")
					}
					active.Body.Close()
				}
				check, remote := f.originalRequest(t, "/synthetic-old-check")
				spAcceptanceReadRest(t, check.Body)
				check.Body.Close()
				if remote != original || f.h.connects.Load() != 2 {
					t.Error("failed safe cleanup closed original idle connection or changed its ownership")
				}
			})
		}
	}
}
