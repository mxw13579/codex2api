package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/codex2api/security"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

// The transport is real loopback WebSocket ingress. Every upstream execution is
// synthetic; these tests never use an account credential or an external server.
func newResponsesWSMultiplexClient(t *testing.T, execute func(context.Context, []byte) (*http.Response, error)) *websocket.Conn {
	t.Helper()
	gin.SetMode(gin.TestMode)
	oldRuntime, oldExecute := CurrentRuntimeSettings(), WebsocketExecuteFunc
	settings := oldRuntime
	settings.CodexWSSilentRetry = false
	settings.ContinuousRetryPolicy = database.ContinuousRetryPolicy{}
	ApplyRuntimeSettings(settings)
	resetResponseCacheForTest()
	t.Cleanup(func() {
		ApplyRuntimeSettings(oldRuntime)
		WebsocketExecuteFunc = oldExecute
		resetResponseCacheForTest()
	})
	WebsocketExecuteFunc = func(ctx context.Context, _ *auth.Account, body []byte, _, _, _ string, _ *DeviceProfileConfig, _ http.Header, _ string) (*http.Response, error) {
		return execute(ctx, body)
	}
	store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 64})
	t.Cleanup(store.Stop)
	store.AddAccount(&auth.Account{DBID: 1, AccessToken: "synthetic-multiplex-token", AccountID: "synthetic-multiplex", PlanType: "plus"})
	handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
	router := gin.New()
	handled := make(chan struct{}, 8)
	var activeHandlers atomic.Int32
	router.Use(func(c *gin.Context) {
		activeHandlers.Add(1)
		defer func() { activeHandlers.Add(-1); handled <- struct{}{} }()
		c.Next()
	})
	handler.RegisterRoutes(router)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Session-Id": {"multiplex-test"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		timer := time.NewTimer(upstreamDrainTimeout + 3*time.Second)
		defer timer.Stop()
		for activeHandlers.Load() != 0 {
			select {
			case <-handled:
			case <-timer.C:
				t.Error("multiplex handler did not release its workers after client close")
				return
			}
		}
	})
	return client
}

func responsesWSMultiplexResponse(id string) *http.Response {
	var events strings.Builder
	fmt.Fprintf(&events, "data: {\"type\":\"response.created\",\"response\":{\"id\":%q,\"status\":\"in_progress\"}}\n\n", id)
	fmt.Fprintf(&events, "data: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n", id)
	events.WriteString(wsContextTestSSE(id))
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(events.String()))}
}

func responsesWSMultiplexSend(t *testing.T, client *websocket.Conn, lane, label string) {
	t.Helper()
	payload := map[string]any{"type": "response.create", "model": "gpt-5.5", "input": label}
	if lane != "" {
		payload["stream_id"] = lane
	}
	if err := client.WriteJSON(payload); err != nil {
		t.Fatal(err)
	}
}

func responsesWSMultiplexRead(t *testing.T, client *websocket.Conn) []byte {
	t.Helper()
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, frame, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("read multiplex event: %v", err)
	}
	return frame
}

func responsesWSMultiplexReadTerminal(t *testing.T, client *websocket.Conn, lane, id string) {
	t.Helper()
	for range 16 {
		frame := responsesWSMultiplexRead(t, client)
		if got := gjson.GetBytes(frame, "stream_id").String(); got != lane {
			t.Fatalf("event stream_id = %q, want %q: %s", got, lane, frame)
		}
		if gjson.GetBytes(frame, "type").String() == "response.completed" {
			if got := gjson.GetBytes(frame, "response.id").String(); got != id {
				t.Fatalf("response.id = %q, want %q", got, id)
			}
			return
		}
		if gjson.GetBytes(frame, "type").String() == "error" {
			t.Fatalf("unexpected lane error: %s", frame)
		}
	}
	t.Fatal("missing lane completion")
}

func responsesWSMultiplexWaitStart(t *testing.T, started <-chan string, label string) {
	t.Helper()
	select {
	case got := <-started:
		if got != label {
			t.Fatalf("started = %q, want %q", got, label)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("upstream did not start %q", label)
	}
}

func TestResponsesWSMultiplexParallelLanesAndSameLaneFIFO(t *testing.T) {
	started := make(chan string, 3)
	release := make(chan struct{})
	client := newResponsesWSMultiplexClient(t, func(ctx context.Context, body []byte) (*http.Response, error) {
		label := "B1"
		for _, candidate := range []string{"A1", "A2"} {
			if strings.Contains(string(body), candidate) {
				label = candidate
			}
		}
		started <- label
		if label == "A1" {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return responsesWSMultiplexResponse("resp_" + label), nil
	})
	responsesWSMultiplexSend(t, client, "planner", "A1")
	responsesWSMultiplexWaitStart(t, started, "A1")
	responsesWSMultiplexSend(t, client, "planner", "A2")
	responsesWSMultiplexSend(t, client, "research", "B1")
	responsesWSMultiplexWaitStart(t, started, "B1")
	responsesWSMultiplexReadTerminal(t, client, "research", "resp_B1")
	close(release)
	responsesWSMultiplexReadTerminal(t, client, "planner", "resp_A1")
	responsesWSMultiplexWaitStart(t, started, "A2")
	responsesWSMultiplexReadTerminal(t, client, "planner", "resp_A2")
}

func TestResponsesWSMultiplexRequestErrorPreservesOtherLanes(t *testing.T) {
	started := make(chan string, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	client := newResponsesWSMultiplexClient(t, func(ctx context.Context, _ []byte) (*http.Response, error) {
		calls.Add(1)
		started <- "healthy"
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return responsesWSMultiplexResponse("resp_healthy"), nil
	})
	responsesWSMultiplexSend(t, client, "healthy", "healthy")
	responsesWSMultiplexWaitStart(t, started, "healthy")
	if err := client.WriteJSON(map[string]any{"type": "response.create", "stream_id": "broken", "input": "missing model"}); err != nil {
		t.Fatal(err)
	}
	frame := responsesWSMultiplexRead(t, client)
	if gjson.GetBytes(frame, "type").String() != "error" || gjson.GetBytes(frame, "stream_id").String() != "broken" || gjson.GetBytes(frame, "error.code").String() != "missing_field" {
		t.Fatalf("request error did not retain lane identity: %s", frame)
	}
	close(release)
	responsesWSMultiplexReadTerminal(t, client, "healthy", "resp_healthy")
	if calls.Load() != 1 {
		t.Fatalf("invalid turn reached upstream: calls=%d", calls.Load())
	}
}

func TestResponsesWSMultiplexAuditStateIsTurnLocal(t *testing.T) {
	type observation struct {
		trace *upstreamTraceAudit
		ua    *userAgentAudit
		ws    *wsAcquireAudit
		id    string
	}
	observed := make(chan observation, 2)
	release := make(chan struct{})
	client := newResponsesWSMultiplexClient(t, func(ctx context.Context, body []byte) (*http.Response, error) {
		observed <- observation{upstreamTraceFromContext(ctx), userAgentAuditFromContext(ctx), wsAcquireAuditFromContext(ctx), snapshotUpstreamTrace(ctx, false).RequestID}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return responsesWSMultiplexResponse("resp_audit"), nil
	})
	responsesWSMultiplexSend(t, client, "first", "first")
	responsesWSMultiplexSend(t, client, "second", "second")
	var observations [2]observation
	for index := range observations {
		select {
		case observations[index] = <-observed:
		case <-time.After(5 * time.Second):
			t.Fatal("parallel audit fixture did not start both lanes")
		}
	}
	first, second := observations[0], observations[1]
	if first.trace == nil || first.ua == nil || first.ws == nil || first.id == "" || second.id == "" {
		t.Fatal("per-turn audit context was not attached")
	}
	if first.trace == second.trace || first.ua == second.ua || first.ws == second.ws || first.id == second.id {
		t.Fatal("parallel lanes share mutable upstream trace, User-Agent or acquisition audit state")
	}
	close(release)
	completed := map[string]bool{}
	for len(completed) != 2 {
		frame := responsesWSMultiplexRead(t, client)
		lane := gjson.GetBytes(frame, "stream_id").String()
		if lane != "first" && lane != "second" {
			t.Fatalf("unexpected audit lane: %s", frame)
		}
		if gjson.GetBytes(frame, "type").String() == "response.completed" {
			completed[lane] = true
		}
	}
}

func TestResponsesWSMultiplexValidatesStreamIDsWithoutClosingConnection(t *testing.T) {
	var calls atomic.Int32
	client := newResponsesWSMultiplexClient(t, func(context.Context, []byte) (*http.Response, error) {
		calls.Add(1)
		return responsesWSMultiplexResponse("resp_valid"), nil
	})
	for _, invalid := range []any{"", "with space", "slash/lane", "中文", strings.Repeat("x", 257), 7, nil, true, []string{"lane"}} {
		if err := client.WriteJSON(map[string]any{"type": "response.create", "model": "gpt-5.5", "input": "invalid", "stream_id": invalid}); err != nil {
			t.Fatal(err)
		}
		frame := responsesWSMultiplexRead(t, client)
		if gjson.GetBytes(frame, "type").String() != "error" || gjson.GetBytes(frame, "error.code").String() != "invalid_stream_id" || gjson.GetBytes(frame, "status").Int() != http.StatusBadRequest {
			t.Fatalf("invalid stream_id %#v was not rejected: %s", invalid, frame)
		}
	}
	responsesWSMultiplexSend(t, client, "valid-lane_1.2", "valid")
	responsesWSMultiplexReadTerminal(t, client, "valid-lane_1.2", "resp_valid")
	if calls.Load() != 1 {
		t.Fatalf("invalid stream ID reached upstream: calls=%d", calls.Load())
	}
}

func TestResponsesWSMultiplexDistinctStreamLimitAllowsReuseAndDefault(t *testing.T) {
	var calls atomic.Int32
	client := newResponsesWSMultiplexClient(t, func(context.Context, []byte) (*http.Response, error) {
		calls.Add(1)
		return responsesWSMultiplexResponse("resp_limit"), nil
	})
	for index := range 32 {
		lane := fmt.Sprintf("lane_%02d", index)
		responsesWSMultiplexSend(t, client, lane, "valid")
		responsesWSMultiplexReadTerminal(t, client, lane, "resp_limit")
	}
	responsesWSMultiplexSend(t, client, "lane_33", "too many lanes")
	frame := responsesWSMultiplexRead(t, client)
	if gjson.GetBytes(frame, "type").String() != "error" || gjson.GetBytes(frame, "stream_id").String() != "lane_33" || gjson.GetBytes(frame, "error.code").String() != "websocket_stream_limit_reached" || gjson.GetBytes(frame, "status").Int() != http.StatusBadRequest {
		t.Fatalf("33rd named stream did not return a scoped limit: %s", frame)
	}
	responsesWSMultiplexSend(t, client, "lane_00", "reuse")
	responsesWSMultiplexReadTerminal(t, client, "lane_00", "resp_limit")
	responsesWSMultiplexSend(t, client, "", "default")
	responsesWSMultiplexReadTerminal(t, client, "", "resp_limit")
	if calls.Load() != 34 {
		t.Fatalf("upstream calls=%d, want 34", calls.Load())
	}
}

func TestResponsesWSMultiplexLimitsActiveResponsesAndCancelsAllOnDisconnect(t *testing.T) {
	baseline := security.GetRequestMemorySnapshot().UsedBytes
	started := make(chan string, 17)
	releases := make([]chan struct{}, 17)
	for index := range releases {
		releases[index] = make(chan struct{})
	}
	var active, maximum atomic.Int32
	client := newResponsesWSMultiplexClient(t, func(ctx context.Context, body []byte) (*http.Response, error) {
		index := -1
		for candidate := range releases {
			if strings.Contains(string(body), fmt.Sprintf("task_%02d", candidate)) {
				index = candidate
				break
			}
		}
		if index < 0 {
			return nil, fmt.Errorf("unknown synthetic request: %s", body)
		}
		current := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); current > previous; previous = maximum.Load() {
			if maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- fmt.Sprintf("task_%02d", index)
		select {
		case <-releases[index]:
			return responsesWSMultiplexResponse(fmt.Sprintf("resp_task_%02d", index)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	for index := range 16 {
		label := fmt.Sprintf("task_%02d", index)
		responsesWSMultiplexSend(t, client, label, label)
		responsesWSMultiplexWaitStart(t, started, label)
	}
	responsesWSMultiplexSend(t, client, "task_16", "task_16")
	// A malformed frame is a dispatcher barrier: it must be answered while all
	// sixteen workers are blocked, without admitting the queued seventeenth.
	if err := client.WriteJSON(map[string]any{"type": "response.create", "stream_id": "bad lane", "model": "gpt-5.5", "input": "barrier"}); err != nil {
		t.Fatal(err)
	}
	frame := responsesWSMultiplexRead(t, client)
	if gjson.GetBytes(frame, "error.code").String() != "invalid_stream_id" || active.Load() != 16 || maximum.Load() != 16 {
		t.Fatalf("active response admission failed: active=%d max=%d frame=%s", active.Load(), maximum.Load(), frame)
	}
	close(releases[0])
	responsesWSMultiplexReadTerminal(t, client, "task_00", "resp_task_00")
	responsesWSMultiplexWaitStart(t, started, "task_16")
	_ = client.Close()
	// Upstream cancellation deliberately permits the existing bounded usage
	// drain window. Give that window time to expire before checking release.
	deadline := time.Now().Add(upstreamDrainTimeout + 2*time.Second)
	for time.Now().Before(deadline) {
		if active.Load() == 0 && security.GetRequestMemorySnapshot().UsedBytes == baseline {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 0 || security.GetRequestMemorySnapshot().UsedBytes != baseline || maximum.Load() != 16 {
		t.Fatalf("disconnect did not release all work: active=%d memory_delta=%d max=%d", active.Load(), security.GetRequestMemorySnapshot().UsedBytes-baseline, maximum.Load())
	}
}

func TestResponsesWSMultiplexCrossLaneForkUsesCommittedParentAndIsConnectionScoped(t *testing.T) {
	var recoveries atomic.Int32
	client := newResponsesWSMultiplexClient(t, func(_ context.Context, body []byte) (*http.Response, error) {
		if gjson.GetBytes(body, "previous_response_id").String() != "" {
			return nil, &ResponsesContinuationLostError{Reason: "original_connection_unavailable"}
		}
		input := string(body)
		if strings.Contains(input, "grandchild-question") {
			for _, ancestor := range []string{"parent-question", "parent-answer", "child-question", "child-answer"} {
				if !strings.Contains(input, ancestor) {
					t.Errorf("fork child was not committed in its own lane: missing %q in %s", ancestor, body)
				}
			}
			recoveries.Add(1)
			return nativeWSResponse("resp_grandchild"), nil
		}
		if strings.Contains(input, "child-question") {
			if !strings.Contains(input, "parent-question") || !strings.Contains(input, "parent-answer") {
				t.Errorf("cross-lane recovery lost the committed ancestor: %s", body)
			}
			recoveries.Add(1)
			return nativeWSResponse("resp_child", `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"child-answer"}]}`), nil
		}
		return nativeWSResponse("resp_parent", `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"parent-answer"}]}`), nil
	})
	send := func(conn *websocket.Conn, lane, previous, label string) {
		t.Helper()
		payload := map[string]any{"type": "response.create", "model": "gpt-5.5", "store": false, "stream_id": lane, "input": label}
		if previous != "" {
			payload["previous_response_id"] = previous
		}
		if err := conn.WriteJSON(payload); err != nil {
			t.Fatal(err)
		}
	}
	send(client, "main", "", "parent-question")
	responsesWSMultiplexReadTerminal(t, client, "main", "resp_parent")
	send(client, "fork", "resp_parent", "child-question")
	responsesWSMultiplexReadTerminal(t, client, "fork", "resp_child")
	if recoveries.Load() != 1 {
		t.Fatalf("cross-lane recoveries=%d, want 1", recoveries.Load())
	}
	send(client, "fork", "resp_child", "grandchild-question")
	responsesWSMultiplexReadTerminal(t, client, "fork", "resp_grandchild")
	if recoveries.Load() != 2 {
		t.Fatalf("fork-lane continuation recoveries=%d, want 2", recoveries.Load())
	}
	// Use the same Authorization and session headers on another socket. Only
	// the socket that observed the committed parent owns the cross-lane proof.
	other, _, err := websocket.DefaultDialer.Dial("ws://"+client.RemoteAddr().String()+"/v1/responses", http.Header{"Session-Id": {"multiplex-test"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	send(other, "unobserved", "resp_parent", "foreign-question")
	frame := responsesWSMultiplexRead(t, other)
	if gjson.GetBytes(frame, "type").String() != "error" || gjson.GetBytes(frame, "stream_id").String() != "unobserved" || gjson.GetBytes(frame, "error.code").String() != "previous_response_not_found" {
		t.Fatalf("another socket recovered an unobserved cross-lane parent: %s", frame)
	}
	if recoveries.Load() != 2 {
		t.Fatal("unobserved cross-lane lookup reached recovery upstream")
	}
}

func TestResponsesWSLineageScopeAndBoundedCommitIndex(t *testing.T) {
	lineage := newResponsesWSLineage()
	owner := nativeWSCachePrefix + "committed"
	for index := 0; index <= responsesWSLineageMaxEntries; index++ {
		lineage.remember(fmt.Sprintf("resp_%03d", index), owner, "authorized")
	}
	if got, err := lineage.lookup(context.Background(), "resp_000", "authorized"); got != "" || err != nil {
		t.Fatalf("evicted lineage remained available: %q %v", got, err)
	}
	if got, err := lineage.lookup(context.Background(), "resp_001", "other-scope"); got != "" || err != nil {
		t.Fatalf("lineage escaped API-key/group scope: %q %v", got, err)
	}
	if got, err := newResponsesWSLineage().lookup(context.Background(), "resp_001", "authorized"); got != "" || err != nil {
		t.Fatalf("lineage escaped its connection: %q %v", got, err)
	}
	if lineage.order.Len() != responsesWSLineageMaxEntries || len(lineage.entries) != responsesWSLineageMaxEntries {
		t.Fatal("lineage index exceeded its bounded capacity")
	}
	finish := lineage.beginCommit("resp_pending", "authorized")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lineage.lookup(ctx, "resp_pending", "authorized"); err != context.Canceled {
		t.Fatalf("uncommitted parent did not honor cancellation: %v", err)
	}
	lineage.remember("resp_pending", owner, "authorized")
	finish()
	if got, err := lineage.lookup(context.Background(), "resp_pending", "authorized"); got != owner || err != nil {
		t.Fatalf("committed parent was not published: %q %v", got, err)
	}
	finish() // Cleanup must be safe across explicit and deferred commit release.
	if len(lineage.pending) != 0 {
		t.Fatal("completed lineage left a pending commit barrier")
	}
	for _, changed := range []string{"api-key", "groups", "database"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
		c.Request.Header.Set("Authorization", "Bearer synthetic-key")
		row := &database.APIKeyRow{ID: 1, AllowedGroupIDs: []int64{10}}
		c.Set(contextAPIKeyRow, row)
		identity := requestSessionIdentity{}
		original := responsesWSLineageScope(c, identity, "database-one")
		dbScope := "database-one"
		switch changed {
		case "api-key":
			c.Request.Header.Set("Authorization", "Bearer another-synthetic-key")
		case "groups":
			copyRow := *row
			copyRow.AllowedGroupIDs = []int64{20}
			c.Set(contextAPIKeyRow, &copyRow)
		case "database":
			dbScope = "database-two"
		}
		if got := responsesWSLineageScope(c, identity, dbScope); got == original {
			t.Fatalf("lineage scope did not isolate changed %s", changed)
		}
	}
	// Reconstructed IDs may not turn the bounded index into a payload cache.
	oversizedID := strings.Repeat("x", responsesWSLineageMaxIDBytes+1)
	lineage.remember(oversizedID, owner, "authorized")
	if got, err := lineage.lookup(context.Background(), oversizedID, "authorized"); got != "" || err != nil {
		t.Fatal("oversized response ID was retained by the lineage index")
	}
}
