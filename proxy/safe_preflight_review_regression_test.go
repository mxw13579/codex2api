package proxy

// Regressions derived from an independent review, not relaxed acceptance cases.
// Every Handler scenario uses the real TLS/CONNECT loopback fixture and requires
// its outbound safe-preflight context marker. All data and credentials are synthetic.
import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/codex2api/database"
)

func TestSafePreflightReviewStandalonePolicyErrorIsSingleTerminal(t *testing.T) {
	for _, tail := range []string{"eof", "failed", "completed"} {
		t.Run(tail, func(t *testing.T) {
			db, err := database.New("sqlite", filepath.Join(t.TempDir(), "synthetic-policy.db"))
			if err != nil {
				t.Fatal("isolated database unavailable")
			}
			t.Cleanup(func() { _ = db.Close() })
			const standalone = `{"type":"error","error":{"code":"cyber_policy","type":"invalid_request_error","status_code":400,"message":"synthetic policy refusal"}}`
			extra := ""
			if tail == "failed" {
				extra = spAcceptanceFrame(spAcceptanceFailed)
			}
			if tail == "completed" {
				extra = spAcceptanceCompleted("resp_synthetic_after_policy")
			}
			h := spAcceptanceNewHarness(t, 2, db, func(w http.ResponseWriter, _ *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(standalone)+extra)
			})
			before := spAcceptanceHealthOf(h.account)
			resp, reader, _ := h.request(t)
			wire := spAcceptanceReadRest(t, reader)
			result := h.wait(t)
			h.provePath(t, 1)
			spAcceptanceTerminal(t, wire, "response.failed", "")
			if resp.StatusCode != 200 || result.logicalStatus != 400 {
				t.Errorf("policy outcome wire=%d logical=%d; want 200/400", resp.StatusCode, result.logicalStatus)
			}
			if strings.Contains(wire, "upstream_stream_break") || strings.Contains(wire, "resp_synthetic_after_policy") {
				t.Error("authoritative policy error was replaced or followed by another outcome")
			}
			if spAcceptanceHealthOf(h.account) != before {
				t.Error("policy error became an account transport failure")
			}
			if len(getResponseCache(responseCacheOwner(0), "resp_synthetic_after_policy")) != 0 {
				t.Error("response after authoritative error was cached as success")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if !db.WaitPromptFilterAuditIdle(ctx) {
				t.Fatal("policy audit queue did not drain")
			}
			incidents, total, err := db.ListPromptPolicyIncidentsPage(ctx, database.PromptPolicyIncidentQuery{Page: 1, PageSize: 10, Endpoint: "/v1/responses"})
			if err != nil || total != 1 || len(incidents) != 1 {
				t.Fatal("standalone policy error did not retain exactly one governance incident")
			}
			i := incidents[0]
			if i.UpstreamErrorCode != "cyber_policy" || i.Transport != "sse" || i.StatusCode != 400 || i.AccountID != h.account.ID() || i.AttemptIndex != 1 {
				t.Error("policy incident provenance differs from actual failed attempt")
			}
		})
	}
}

func TestSafePreflightReviewCommittedSSEKeepsLegacyCompatibility(t *testing.T) {
	for _, kind := range []string{"id_and_retry", "repeated_event", "eof_tail"} {
		t.Run(kind, func(t *testing.T) {
			gate := spAcceptanceNewGate()
			t.Cleanup(gate.open)
			terminal := spAcceptanceCompleted("resp_synthetic_legacy_tail")
			switch kind {
			case "id_and_retry":
				terminal = "id: synthetic-terminal\nretry: 1000\n" + terminal
			case "repeated_event":
				terminal = "event: response.completed\nevent: response.completed\n" + terminal
			case "eof_tail":
				terminal = strings.TrimRight(terminal, "\n")
			}
			h := spAcceptanceNewHarness(t, 2, nil, func(w http.ResponseWriter, r *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceRate+spAcceptanceFrame(`{"type":"response.output_text.delta","delta":"synthetic-commit"}`))
				if gate.wait(r.Context()) {
					spAcceptanceFlush(w, terminal)
				}
			})
			_, reader, _ := h.request(t)
			var prefix strings.Builder
			for !strings.Contains(prefix.String(), "synthetic-commit") {
				prefix.WriteString(spAcceptanceReadEnvelope(t, reader))
			}
			state := h.provePath(t, 1)
			if state == nil || !state.committed() {
				t.Fatal("actual downstream barrier did not follow protocol commit")
			}
			gate.open()
			wire := prefix.String() + spAcceptanceReadRest(t, reader)
			h.wait(t)
			h.provePath(t, 1)
			spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_legacy_tail")
		})
	}
}

func TestSafePreflightReviewSparkUsesSelectedDispatchPolicy(t *testing.T) {
	for _, stage := range []string{"before_first_send", "after_rate_pin"} {
		t.Run(stage, func(t *testing.T) {
			gate := spAcceptanceNewGate()
			t.Cleanup(gate.open)
			h := spAcceptanceNewHarness(t, 1, nil, func(w http.ResponseWriter, r *http.Request, n int) {
				if stage == "after_rate_pin" && n == 1 {
					spAcceptanceFlush(w, spAcceptanceRate)
					if gate.wait(r.Context()) {
						spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
					}
					return
				}
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_spark"))
			})
			exhaustStandard := func() {
				h.handler.store.SetIgnoreUsageLimitStatus(false)
				h.handler.store.MarkPremium5hRateLimited(h.account, time.Now().Add(3*time.Hour))
				h.account.SetUsageSnapshot5h(100, time.Now().Add(3*time.Hour))
				if h.account.IsAvailable() || !h.account.SparkDispatchEligible() {
					t.Fatal("fixture did not separate standard quota from Spark admission")
				}
			}
			if stage == "before_first_send" {
				exhaustStandard()
			}
			_, reader, _ := spSharedEdgesRequest(t, h, `{"model":"gpt-5.3-codex-spark","input":"synthetic-input","stream":true}`, 10*time.Second)
			prefix := ""
			wantCalls := 1
			if stage == "after_rate_pin" {
				prefix = spAcceptanceReadEnvelope(t, reader)
				if prefix != spAcceptanceRate {
					t.Fatal("no real rate barrier")
				}
				exhaustStandard()
				gate.open()
				wantCalls = 2
			}
			wire := prefix + spAcceptanceReadRest(t, reader)
			h.wait(t)
			h.provePath(t, wantCalls)
			spAcceptanceTerminal(t, wire, "response.completed", "resp_synthetic_spark")
		})
	}
}
