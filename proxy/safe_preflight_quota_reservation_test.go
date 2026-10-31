package proxy

// Handler regressions against real quota storage, not a receipt mock. All
// requests enter the real Handler and executor through the existing verified
// TLS + explicit loopback CONNECT fixture. Quota and usage use separate fresh
// SQLite databases: a usage query cannot masquerade as the quota DB wait.
//
// In particular, the first-attempt cases do NOT use spSharedEdgesWaitQuota:
// that helper requires an already-sent first attempt. A held SQLite writer,
// the real consumer's mutex, its DB connection, and zero outbound observations
// establish our first-attempt barrier. Polling only observes these conditions;
// elapsed time alone is never evidence that quota admission was reached.
// No fake executor, fake RoundTripper, or production data is used. The phase-2
// storage-failure cases additionally check the request's poisoned quota state.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/database"
)

const spQuotaReservationLogicalRequestID = "synthetic-reservation-logical-request"

func spQuotaReservationDatabase(t *testing.T, name, usageMode string) (string, *database.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".db")
	db, err := database.New("sqlite", path)
	if err != nil {
		t.Fatal("could not create isolated synthetic reservation database")
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetUsageLogConfig(usageMode, 100, 60)
	return path, db
}

func spQuotaReservationQuota(t *testing.T) (string, *database.DB, *apiKeyModelRequestQuota) {
	t.Helper()
	path, db := spQuotaReservationDatabase(t, "synthetic-reservation-quota", "off")
	// Keep the fixture away from a real wall-clock weekly boundary. Cross-week
	// release/idempotence belongs in the separately approved database suite.
	weekday := int(time.Now().UTC().AddDate(0, 0, 3).Weekday())
	if weekday == 0 {
		weekday = 7
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := db.InsertAPIKeyWithOptions(ctx, database.APIKeyInput{
		Name: "synthetic-reservation", Key: "synthetic-reservation-not-an-authentication-key",
		Limits: database.APIKeyLimits{ModelRequestLimits: []database.APIKeyModelRequestLimit{{
			ID: "synthetic-reservation-rule", Model: "gpt-5.5", MaxRequests: 10,
			Timezone: "UTC", ResetWeekday: weekday,
		}}},
	})
	if err != nil {
		t.Fatal("could not create synthetic reservation quota row")
	}
	row, err := db.GetAPIKeyByID(ctx, id)
	if err != nil || row == nil || len(row.Limits.ModelRequestLimits) != 1 {
		t.Fatal("could not read synthetic reservation quota rule")
	}
	return path, db, &apiKeyModelRequestQuota{
		db: db, keyID: id, requestID: spQuotaReservationLogicalRequestID, rules: row.Limits.ModelRequestLimits,
	}
}

type spQuotaReservationProxyProbe struct {
	calls  atomic.Int32
	reject atomic.Bool
}

// This is the actual standard Transport.Proxy callback, installed before any
// request starts. It preserves the acceptance fixture's outbound context
// observation even when resolution is deliberately rejected. The production
// safe-preflight wrapper must turn that rejection into a no-dial local error;
// neither the executor nor the HTTP transport is replaced.
func spQuotaReservationObserveProxy(t *testing.T, h *spAcceptanceHarness) *spQuotaReservationProxyProbe {
	t.Helper()
	entry, ok := clientPool.Load("account-bound|" + clientPoolKey(h.account, h.proxyURL, codexTransportModeStandard))
	if !ok {
		t.Fatal("missing synthetic reservation transport")
	}
	transport, ok := entry.(*poolEntry).client.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil {
		t.Fatal("reservation fixture requires the real standard proxy transport")
	}
	original := transport.Proxy
	probe := &spQuotaReservationProxyProbe{}
	transport.Proxy = func(r *http.Request) (*url.URL, error) {
		probe.calls.Add(1)
		resolved, err := original(r)
		if probe.reject.Load() {
			return nil, errors.New("synthetic reservation proxy resolution rejected")
		}
		return resolved, err
	}
	t.Cleanup(func() { transport.Proxy = original })
	return probe
}

type spQuotaReservationHTTPResult struct {
	status int
	wire   string
	err    error
}

// First quota admission deliberately has not committed HTTP headers, so the
// ordinary synchronous h.request would deadlock the test's writer-release
// barrier. Only the loopback client runs asynchronously; assertions stay on
// the test goroutine. Cleanup cancels and joins it before restoring transports.
func spQuotaReservationStartRequest(t *testing.T, h *spAcceptanceHarness) (<-chan spQuotaReservationHTTPResult, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.downstream.URL+"/v1/responses", strings.NewReader(spAcceptanceBody))
	if err != nil {
		cancel()
		t.Fatal("could not construct synthetic reservation request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(downstreamAffinityHeader, h.affinitySignal)
	result := make(chan spQuotaReservationHTTPResult, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		resp, err := h.client.Do(req)
		if err != nil {
			result <- spQuotaReservationHTTPResult{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
		if len(body) > 1<<20 {
			err = errors.New("synthetic reservation response exceeded bound")
		}
		result <- spQuotaReservationHTTPResult{status: resp.StatusCode, wire: string(body), err: err}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(12 * time.Second):
			t.Error("synthetic reservation client did not stop during cleanup")
		}
	})
	return result, cancel
}

func spQuotaReservationFinishRequest(t *testing.T, result <-chan spQuotaReservationHTTPResult) spQuotaReservationHTTPResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(8 * time.Second):
		t.Fatal("synthetic reservation request did not finish")
		return spQuotaReservationHTTPResult{}
	}
}

func spQuotaReservationWaitFirst(t *testing.T, h *spAcceptanceHarness, probe *spQuotaReservationProxyProbe, quota *apiKeyModelRequestQuota, db *database.DB) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		if h.calls.Load() != 0 || h.connects.Load() != 0 || probe.calls.Load() != 0 {
			t.Fatal("first-attempt quota barrier already had outbound activity")
		}
		locked := quota.mu.TryLock()
		if locked {
			quota.mu.Unlock()
		} else if db.Stats().InUse > 0 {
			// This DB is used only by quota, and the test still holds its
			// independent BEGIN IMMEDIATE writer. No charge can commit yet.
			return
		}
		select {
		case <-tick.C:
		case <-h.done:
			t.Fatal("Handler ended before first-attempt quota DB wait")
		case <-deadline.C:
			t.Fatal("could not prove first attempt blocked inside real quota storage")
		}
	}
}

type spQuotaReservationLedgerStamp struct {
	windowStart int64
	createdAt   int64
}

// Read the durable logical ledger and counters independently, including ALL
// windows. A zero usage-log count, admitted flag, or current-week-only read is
// not evidence that a model-request charge was released. Zero-valued counter
// rows may remain; negative counters and orphan logical charges may not.
func spQuotaReservationAccounting(t *testing.T, path string, db *database.DB, quota *apiKeyModelRequestQuota, want int64) spQuotaReservationLedgerStamp {
	t.Helper()
	if quota.requestID != spQuotaReservationLogicalRequestID {
		t.Error("reservation changed the incoming request's logical charge identity")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	usage, err := db.GetAPIKeyModelRequestUsage(ctx, quota.keyID, quota.rules, time.Now())
	if err != nil || len(usage) != 1 {
		t.Fatal("could not read authoritative synthetic model-request usage")
	}
	if usage[0].Used != want {
		t.Errorf("logical model-request Used=%d, want %d", usage[0].Used, want)
	}
	reader, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro&_pragma=busy_timeout%283000%29")
	if err != nil {
		t.Fatal("could not open isolated read-only quota inspector")
	}
	defer reader.Close()
	var total, negative, allLedger, ownLedger int64
	var stamp spQuotaReservationLedgerStamp
	if err := reader.QueryRowContext(ctx, `SELECT COALESCE(SUM(used_requests),0), COALESCE(SUM(CASE WHEN used_requests<0 THEN 1 ELSE 0 END),0) FROM api_key_model_request_counters WHERE api_key_id=$1`, quota.keyID).Scan(&total, &negative); err != nil {
		t.Fatal("could not inspect synthetic model-request counters")
	}
	if err := reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_key_model_request_ledger WHERE api_key_id=$1`, quota.keyID).Scan(&allLedger); err != nil {
		t.Fatal("could not inspect synthetic model-request ledger")
	}
	if err := reader.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(window_start),0), COALESCE(MIN(created_at),0) FROM api_key_model_request_ledger WHERE api_key_id=$1 AND rule_id=$2 AND request_id=$3`, quota.keyID, quota.rules[0].ID, spQuotaReservationLogicalRequestID).Scan(&ownLedger, &stamp.windowStart, &stamp.createdAt); err != nil {
		t.Fatal("could not inspect original synthetic logical charge")
	}
	if total != want || negative != 0 || allLedger != want || ownLedger != want {
		t.Errorf("durable quota: counter total=%d negative=%d ledger=%d original logical ledger=%d, want charge=%d", total, negative, allLedger, ownLedger, want)
	}
	return stamp
}

func spQuotaReservationUsage(t *testing.T, db *database.DB, h *spAcceptanceHarness, want, status int, retry bool) {
	t.Helper()
	db.FlushUsageLogs()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The filter always applies both time bounds, even when they are zero.
	// Include 499 explicitly so cancellation cannot conceal a phantom attempt.
	rows, err := db.ListUsageLogsByFilter(ctx, database.UsageLogFilter{
		Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour), IncludeCanceled: true,
	})
	if err != nil {
		t.Fatal("could not read isolated reservation usage rows")
	}
	if len(rows) != want {
		t.Errorf("actual usage rows=%d, want %d", len(rows), want)
		return
	}
	for _, row := range rows {
		if row.AccountID != h.account.ID() || row.AttemptIndex != 1 || row.IsRetryAttempt != retry ||
			row.StatusCode != status || row.Endpoint != "/v1/responses" || !row.Stream || row.ViaWebsocket {
			t.Error("usage did not describe exactly the real first HTTP attempt")
		}
	}
}

func spQuotaReservationLocalTerminal(t *testing.T, wire string) {
	t.Helper()
	spAcceptanceTerminal(t, wire, "response.failed", "")
	if !strings.Contains(wire, `"message":"`+errSafePreflightLocal.Error()+`"`) {
		t.Error("expected the real safe-preflight local guard rejection, not a quota timeout or unrelated failure")
	}
}

func TestSafePreflightQuotaReservationFirstAttemptRejectedAfterDBWait(t *testing.T) {
	for _, kind := range []string{"account_disabled", "downstream_cancel", "proxy_callback_rejected"} {
		t.Run(kind, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			path, quotaDB, quota := spQuotaReservationQuota(t)
			_, usageDB := spQuotaReservationDatabase(t, "synthetic-reservation-usage", "full")
			h := spAcceptanceNewHarness(t, 1, usageDB, func(w http.ResponseWriter, _ *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_reservation_unexpected"))
			})
			probe := spQuotaReservationObserveProxy(t, h)
			spSharedEdgesAttachQuota(t, h, quota)
			before := spAcceptanceHealthOf(h.account)
			spQuotaReservationAccounting(t, path, quotaDB, quota, 0)
			release := spSharedEdgesHoldSQLiteWriter(t, path)
			defer release() // also precedes client/server cleanup on Fatal
			pending, cancel := spQuotaReservationStartRequest(t, h)
			spQuotaReservationWaitFirst(t, h, probe, quota, quotaDB)

			switch kind {
			case "account_disabled":
				atomic.StoreInt32(&h.account.Disabled, 1)
			case "downstream_cancel":
				cancel()
				select {
				case <-h.canceled:
				case <-time.After(time.Second):
					t.Fatal("server did not observe downstream cancellation while quota was blocked")
				}
			case "proxy_callback_rejected":
				probe.reject.Store(true)
			}
			release()
			clientResult := spQuotaReservationFinishRequest(t, pending)
			result := h.wait(t)
			if kind == "downstream_cancel" {
				if !errors.Is(clientResult.err, context.Canceled) || result.logicalStatus != logStatusClientClosed {
					t.Error("first-attempt cancellation did not reach the real Handler during quota wait")
				}
			} else {
				if clientResult.err != nil || clientResult.status != http.StatusBadGateway || result.logicalStatus != http.StatusBadGateway {
					t.Error("first-attempt final admission did not return its dedicated local 502")
				}
				spQuotaReservationLocalTerminal(t, clientResult.wire)
			}
			if h.calls.Load() != 0 || h.connects.Load() != 0 || h.badRoute.Load() {
				t.Error("proven first-attempt local rejection must have zero upstream requests and zero CONNECTs")
			}
			wantProxy := int32(0)
			if kind == "proxy_callback_rejected" {
				wantProxy = 1
			}
			if probe.calls.Load() != wantProxy {
				t.Errorf("actual Proxy callback calls=%d, want %d", probe.calls.Load(), wantProxy)
			}
			// provePath(0) would wrongly require a CONNECT. Only the proxy
			// rejection reaches a final outbound context; the other no-send
			// cases prove the path via the real DB wait + local Handler result.
			if kind == "proxy_callback_rejected" {
				h.mu.Lock()
				sends := append([]spAcceptanceSend(nil), h.sends...)
				h.mu.Unlock()
				if len(sends) != 1 || sends[0].state == nil || sends[0].attempt != 1 {
					t.Fatal("Proxy rejection did not originate in the first real safe-preflight attempt")
				}
				if sends[0].state.wasSent() {
					t.Error("known pre-dial Proxy rejection retained a sent marker")
				}
			}
			if spAcceptanceHealthOf(h.account) != before {
				t.Error("local reservation rejection penalized upstream account health")
			}
			spQuotaReservationUsage(t, usageDB, h, 0, 0, false)
			spQuotaReservationAccounting(t, path, quotaDB, quota, 0)
		})
	}
}

func TestSafePreflightQuotaReservationFirstAttemptDBWaitThenSendChargesOnce(t *testing.T) {
	spAcceptanceIsolateCache(t)
	path, quotaDB, quota := spQuotaReservationQuota(t)
	_, usageDB := spQuotaReservationDatabase(t, "synthetic-reservation-positive-usage", "full")
	h := spAcceptanceNewHarness(t, 1, usageDB, func(w http.ResponseWriter, _ *http.Request, _ int) {
		spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_reservation_positive"))
	})
	probe := spQuotaReservationObserveProxy(t, h)
	spSharedEdgesAttachQuota(t, h, quota)
	release := spSharedEdgesHoldSQLiteWriter(t, path)
	defer release()
	pending, _ := spQuotaReservationStartRequest(t, h)
	spQuotaReservationWaitFirst(t, h, probe, quota, quotaDB)
	release()
	clientResult := spQuotaReservationFinishRequest(t, pending)
	result := h.wait(t)
	state := h.provePath(t, 1)
	if state == nil {
		t.Fatal("positive quota control did not enter the real safe-preflight path")
	}
	if clientResult.err != nil || clientResult.status != http.StatusOK ||
		result.logicalStatus != 0 && result.logicalStatus != http.StatusOK || !state.wasSent() {
		t.Error("unchanged first-attempt quota wait did not proceed to a real successful send")
	}
	spAcceptanceTerminal(t, clientResult.wire, "response.completed", "resp_synthetic_reservation_positive")
	spQuotaReservationUsage(t, usageDB, h, 1, http.StatusOK, false)
	spQuotaReservationAccounting(t, path, quotaDB, quota, 1)
}

func TestSafePreflightQuotaReservationSentFirstAttemptThenRejectedSecondKeepsCharge(t *testing.T) {
	spAcceptanceIsolateCache(t)
	path, quotaDB, quota := spQuotaReservationQuota(t)
	_, usageDB := spQuotaReservationDatabase(t, "synthetic-reservation-retry-usage", "full")
	gate := spAcceptanceNewGate()
	h := spAcceptanceNewHarness(t, 1, usageDB, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			spAcceptanceFlush(w, spAcceptanceRate)
			if gate.wait(r.Context()) {
				spAcceptanceFlush(w, spAcceptanceFrame(spAcceptanceFailed))
			}
			return
		}
		spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_reservation_forbidden_second"))
	})
	t.Cleanup(gate.open)
	spSharedEdgesAttachQuota(t, h, quota)
	resp, reader, _ := h.request(t)
	first := spAcceptanceReadEnvelope(t, reader)
	if first != spAcceptanceRate || resp.StatusCode != http.StatusOK {
		t.Fatal("retry protection lacks a genuine first upstream send and downstream rate barrier")
	}
	state := h.provePath(t, 1)
	if state == nil {
		t.Fatal("sent-first-attempt protection did not enter the real safe-preflight path")
	}
	original := spQuotaReservationAccounting(t, path, quotaDB, quota, 1)
	connects := h.connects.Load()
	before := spAcceptanceHealthOf(h.account)
	release := spSharedEdgesHoldSQLiteWriter(t, path)
	defer release()
	gate.open()
	// Here, and ONLY here, the pre-existing attempt-2 wait helper is valid.
	spSharedEdgesWaitQuota(t, h, state, quota, quotaDB)
	atomic.StoreInt32(&h.account.Disabled, 1)
	release()
	wire := first + spAcceptanceReadRest(t, reader)
	result := h.wait(t)
	h.provePath(t, 1)
	state.mu.Lock()
	attempts, sent := state.attempts, state.sent
	state.mu.Unlock()
	if attempts != 2 || sent || h.connects.Load() != connects || result.logicalStatus != http.StatusBadGateway {
		t.Error("second attempt was not rejected locally after its real quota wait")
	}
	spQuotaReservationLocalTerminal(t, wire)
	if spAcceptanceHealthOf(h.account) != before {
		t.Error("unsent second attempt penalized upstream account health")
	}
	spQuotaReservationUsage(t, usageDB, h, 1, http.StatusServiceUnavailable, true)
	if retained := spQuotaReservationAccounting(t, path, quotaDB, quota, 1); retained != original {
		t.Error("unsent retry changed the original durable logical charge window or creation time")
	}
}

func TestSafePreflightQuotaReservationDispatchedTransportFailureKeepsCharge(t *testing.T) {
	path, quotaDB, quota := spQuotaReservationQuota(t)
	_, usageDB := spQuotaReservationDatabase(t, "synthetic-reservation-dispatch-usage", "full")
	var closed atomic.Bool
	h := spAcceptanceNewHarness(t, 0, usageDB, func(w http.ResponseWriter, _ *http.Request, _ int) {
		// The TLS server has actually read the POST before dropping the
		// connection without response headers. This is NOT proven local
		// no-send, even though the executor ultimately returns an error.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			http.Error(w, "synthetic hijack unavailable", http.StatusInternalServerError)
			return
		}
		closed.Store(true)
		_ = conn.Close()
	})
	spSharedEdgesAttachQuota(t, h, quota)
	resp, reader, _ := h.request(t)
	wire := spAcceptanceReadRest(t, reader)
	result := h.wait(t)
	state := h.provePath(t, 1)
	if state == nil {
		t.Fatal("transport failure control did not enter the real safe-preflight path")
	}
	if !closed.Load() || !state.wasSent() || resp.StatusCode != http.StatusBadGateway || result.logicalStatus != logStatusUpstreamStreamBreak {
		t.Error("dispatch control did not exercise a real sent request with transport failure")
	}
	spAcceptanceTerminal(t, wire, "response.failed", "")
	spQuotaReservationUsage(t, usageDB, h, 1, logStatusUpstreamStreamBreak, false)
	spQuotaReservationAccounting(t, path, quotaDB, quota, 1)
}

func spQuotaReservationStorageSQL(t *testing.T, path, query string) {
	t.Helper()
	conn, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout%283000%29")
	if err != nil {
		t.Fatal("could not open isolated quota fault connection")
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, query); err != nil {
		t.Fatal("could not install or remove synthetic quota storage fault")
	}
}

func TestSafePreflightQuotaReservationStorageFailurePoisonsRequest(t *testing.T) {
	for _, kind := range []string{"charge_fault", "release_fault", "cancel_release_fault"} {
		t.Run(kind, func(t *testing.T) {
			spAcceptanceIsolateCache(t)
			path, quotaDB, quota := spQuotaReservationQuota(t)
			_, usageDB := spQuotaReservationDatabase(t, "synthetic-storage-failure-usage", "full")
			h := spAcceptanceNewHarness(t, 1, usageDB, func(w http.ResponseWriter, _ *http.Request, _ int) {
				spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_storage_failure_forbidden"))
			})
			probe := spQuotaReservationObserveProxy(t, h)
			spSharedEdgesAttachQuota(t, h, quota)
			before := spAcceptanceHealthOf(h.account)
			if kind == "charge_fault" {
				spQuotaReservationStorageSQL(t, path, `CREATE TRIGGER synthetic_quota_fault BEFORE INSERT ON api_key_model_request_ledger BEGIN SELECT RAISE(ABORT,'synthetic-storage-detail-do-not-expose'); END`)
			} else {
				spQuotaReservationStorageSQL(t, path, `CREATE TRIGGER synthetic_quota_fault BEFORE DELETE ON api_key_model_request_ledger BEGIN SELECT RAISE(ABORT,'synthetic-storage-detail-do-not-expose'); END`)
			}
			var captured bytes.Buffer
			previousLog := log.Writer()
			log.SetOutput(&captured)
			t.Cleanup(func() { log.SetOutput(previousLog) })
			release := spSharedEdgesHoldSQLiteWriter(t, path)
			defer release()
			pending, cancel := spQuotaReservationStartRequest(t, h)
			spQuotaReservationWaitFirst(t, h, probe, quota, quotaDB)
			if kind == "cancel_release_fault" {
				cancel()
				select {
				case <-h.canceled:
				case <-time.After(time.Second):
					t.Fatal("Handler did not observe cancellation during quota wait")
				}
			} else if kind == "release_fault" {
				atomic.StoreInt32(&h.account.Disabled, 1)
			}
			release()
			clientResult := spQuotaReservationFinishRequest(t, pending)
			result := h.wait(t)
			log.SetOutput(previousLog)
			if kind == "cancel_release_fault" {
				if !errors.Is(clientResult.err, context.Canceled) || result.logicalStatus != logStatusClientClosed {
					t.Error("storage failure overrode Handler's existing cancellation priority")
				}
			} else if clientResult.err != nil || clientResult.status != http.StatusServiceUnavailable || result.logicalStatus != http.StatusServiceUnavailable ||
				!strings.Contains(clientResult.wire, "Model request quota storage is unavailable; please retry later") {
				t.Error("quota storage failure did not produce its fixed dedicated 503")
			}
			if !strings.Contains(captured.String(), "[safe-preflight] model request quota storage failure; dispatch stopped") {
				t.Error("quota storage failure was not observable in fixed diagnostics")
			}
			for _, forbidden := range []string{"synthetic-storage-detail-do-not-expose", "api_key_model_request_ledger", spQuotaReservationLogicalRequestID} {
				if strings.Contains(captured.String(), forbidden) || strings.Contains(clientResult.wire, forbidden) {
					t.Error("storage diagnostics exposed underlying storage or receipt fields")
				}
			}
			if h.calls.Load() != 0 || h.connects.Load() != 0 || probe.calls.Load() != 0 || h.badRoute.Load() || spAcceptanceHealthOf(h.account) != before {
				t.Error("local quota storage failure sent upstream or changed account health")
			}
			spQuotaReservationUsage(t, usageDB, h, 0, 0, false)
			want := int64(1)
			if kind == "charge_fault" {
				want = 0
			}
			spQuotaReservationAccounting(t, path, quotaDB, quota, want)
			quota.mu.Lock()
			poisoned, settled := quota.poisoned, quota.active == nil
			quota.mu.Unlock()
			if !poisoned || !settled {
				t.Error("executor returned before settling and poisoning its quota lifecycle")
			}
			// Repair storage, then prove neither a new safe reservation nor the
			// old exported consumer can revive this poisoned incoming request.
			spQuotaReservationStorageSQL(t, path, `DROP TRIGGER synthetic_quota_fault`)
			safe := &safePreflightState{started: time.Now(), downstream: context.Background()}
			ctx := context.WithValue(context.Background(), apiKeyModelRequestContextKey{}, quota)
			ctx = context.WithValue(ctx, safePreflightContextKey{}, safe)
			r, err := reserveAPIKeyModelRequestQuota(ctx, "gpt-5.5")
			quotaErr := apiKeyModelRequestError(err)
			if r != nil || quotaErr == nil || quotaErr.status != http.StatusServiceUnavailable || safe.canStart(ctx) {
				t.Error("poisoned quota allowed a subsequent dispatch reservation")
			}
			if err := ConsumeAPIKeyModelRequestQuota(ctx, "gpt-5.5"); apiKeyModelRequestError(err) == nil {
				t.Error("legacy consume entry revived poisoned safe request state")
			}
			spQuotaReservationAccounting(t, path, quotaDB, quota, want)
		})
	}
}

func TestSafePreflightQuotaReservationNetworkWaitDoesNotHoldQuotaMutex(t *testing.T) {
	path, quotaDB, quota := spQuotaReservationQuota(t)
	_, usageDB := spQuotaReservationDatabase(t, "synthetic-network-wait-usage", "full")
	entered := make(chan struct{}, 1)
	gate := spAcceptanceNewGate()
	h := spAcceptanceNewHarness(t, 0, usageDB, func(w http.ResponseWriter, r *http.Request, _ int) {
		entered <- struct{}{}
		if gate.wait(r.Context()) {
			spAcceptanceFlush(w, spAcceptanceCompleted("resp_synthetic_cancel_after_send"))
		}
	})
	t.Cleanup(gate.open)
	spSharedEdgesAttachQuota(t, h, quota)
	pending, cancel := spQuotaReservationStartRequest(t, h)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("synthetic upstream did not receive the actual dispatch")
	}
	if !quota.mu.TryLock() {
		t.Fatal("quota mutex was held across the real network wait")
	}
	active, admitted := quota.active != nil, quota.admitted
	quota.mu.Unlock()
	if !active || !admitted {
		t.Error("network wait lost the attempt-scoped reservation or admission")
	}
	ctx := context.WithValue(context.Background(), apiKeyModelRequestContextKey{}, quota)
	if apiKeyModelRequestAdmissionPending(ctx) {
		t.Error("in-flight dispatch blocked quota keepalive admission")
	}
	// A separate real writer can acquire and release the same quota DB while
	// upstream response headers are still blocked: no transaction spans IO.
	release := spSharedEdgesHoldSQLiteWriter(t, path)
	release()
	spQuotaReservationAccounting(t, path, quotaDB, quota, 1)
	cancel()
	select {
	case <-h.canceled:
	case <-time.After(time.Second):
		t.Fatal("Handler did not observe cancellation after actual dispatch")
	}
	gate.open()
	clientResult := spQuotaReservationFinishRequest(t, pending)
	result := h.wait(t)
	state := h.provePath(t, 1)
	if !errors.Is(clientResult.err, context.Canceled) || result.logicalStatus != logStatusClientClosed || state == nil || !state.wasSent() {
		t.Error("actual sent cancellation lost its conservative dispatch evidence")
	}
	quota.mu.Lock()
	settled, poisoned := quota.active == nil, quota.poisoned
	quota.mu.Unlock()
	if !settled || poisoned {
		t.Error("sent cancellation did not retain and settle the reservation")
	}
	spQuotaReservationAccounting(t, path, quotaDB, quota, 1)
}
