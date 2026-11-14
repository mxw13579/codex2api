package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// In-memory model/search callbacks only: no accounts, transport or authentication.
func TestCodexSearchHTTPFailureTerminal(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		readFailure  bool
		withoutUsage bool
	}{
		{name: "rate limited", status: 429, body: `{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error","message":"Synthetic quota rejection","param":null,"retry_after_seconds":17}}`},
		{name: "unauthorized", status: 401, body: `{"error":{"code":"invalid_token","type":"authentication_error","message":"Synthetic authentication rejection","param":"fixture"}}`},
		{name: "bad request", status: 400, body: `{"error":{"code":"invalid_request_error","type":"invalid_request_error","message":"Synthetic invalid request","param":"input","status_code":503,"fixture_number":9007199254740993}}`},
		{name: "plain text", status: 429, body: "synthetic opaque body must not be echoed"},
		{name: "HTML", status: 401, body: "<html>synthetic opaque body must not be echoed</html>"},
		{name: "invalid JSON", status: 400, body: `{"error":{"message":"synthetic partial body`},
		{name: "unreadable body", status: 429, readFailure: true},
		{name: "no reported usage", status: 400, body: `{"error":{"code":"invalid_request_error","message":"Synthetic rejection without usage"}}`, withoutUsage: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridge := &anthropicCodexWebSearch{name: "private_search", model: "fixture-model", remaining: 2}
			rounds := make([]*nativeSearchFixture, 2)
			for i := range rounds {
				round := &nativeSearchFixture{id: fmt.Sprintf("resp_http_fixture_%d", i), createdAt: int64(1700000000 + i)}
				round.lifecycle("response.created")
				message := nativeSearchMessage(fmt.Sprintf("msg_%d", i), fmt.Sprintf("Synthetic text %d", i))
				round.item("response.output_item.added", 0, nativeSearchMessage(fmt.Sprintf("msg_%d", i)))
				round.event("response.output_text.delta", map[string]any{"item_id": message["id"], "output_index": 0, "content_index": 0, "delta": fmt.Sprintf("Synthetic text %d", i)})
				round.item("response.output_item.done", 0, message)
				call := codexSearchTestCall(bridge.name, fmt.Sprintf("Synthetic query %d", i))
				call["id"], call["call_id"] = fmt.Sprintf("fc_%d", i), fmt.Sprintf("call_%d", i)
				if tc.withoutUsage {
					round.event("response.completed", map[string]any{"response": map[string]any{
						"id": round.id, "status": "completed", "output": []any{message, call},
					}})
				} else {
					round.lifecycle("response.completed", message, call)
				}
				rounds[i] = round
			}
			var rejected io.Reader = strings.NewReader(tc.body)
			if tc.readFailure {
				rejected = safePreflightReadFailure{}
			}
			body := &codexSearchHTTPFailureBody{Reader: rejected}
			response := rounds[0].response()
			attachCodexSearchContinuation(response, bridge)
			stored := response.Body.(*codexSearchContextBody)
			opened, searched := 0, 0
			var stream strings.Builder
			err := bridge.run(context.Background(), response, []byte(`{"input":[]}`), func([]byte) (*http.Response, error) {
				opened++
				if opened == 1 {
					return rounds[1].response(), nil
				}
				if opened != 2 {
					t.Fatal("retried a rejected continuation")
				}
				return &http.Response{StatusCode: tc.status, Body: body}, nil
			}, func(raw []byte) (*CodexAlphaSearchResponse, error) {
				searched++
				return nativeSearchSuccess(raw)
			}, &stream)
			if err != nil {
				t.Fatalf("HTTP rejection must terminate via response.failed, not a pipe error: %v", err)
			}
			if opened != 2 || searched != 2 || !body.closed || stored.completed || len(stored.replay) != 0 {
				t.Fatalf("replayed or cached a failed search: opened=%d searched=%d closed=%t cached=%t", opened, searched, body.closed, stored.completed)
			}
			events, terminal := assertNativeSearchContract(t, stream.String(), bridge.responseID, rounds[0].createdAt)
			if terminal.Get("type").String() != "response.failed" || terminal.Get("response.status").String() != "failed" || terminal.Get("response.status_code").Int() != int64(tc.status) {
				t.Fatal("HTTP status or failed terminal semantics lost")
			}
			outcome := classifyResponseFailedOutcome([]byte(terminal.Raw))
			if outcome.logStatusCode != tc.status || outcome.failureKind == "transport" || outcome.logStatusCode == logStatusUpstreamStreamBreak {
				t.Fatalf("HTTP failure classified as status=%d kind=%s", outcome.logStatusCode, outcome.failureKind)
			}
			if want := gjson.Get(tc.body, "error"); gjson.Valid(tc.body) && want.IsObject() {
				if terminal.Get("response.error").Raw != want.Raw {
					t.Fatal("structured upstream error fields were rewritten or discarded")
				}
			} else if terminal.Get("response.error.message").String() != fmt.Sprintf("HTTP %d", tc.status) {
				t.Fatal("opaque error body or reader details escaped the safe status diagnostic")
			}
			usage := terminal.Get("response.usage")
			if tc.withoutUsage {
				if usage.Exists() {
					t.Fatal("invented usage without a real upstream terminal snapshot")
				}
			} else {
				for path, want := range map[string]int64{"input_tokens": 20, "output_tokens": 10, "total_tokens": 30, "input_tokens_details.cached_tokens": 4, "output_tokens_details.reasoning_tokens": 2} {
					if usage.Get(path).Int() != want {
						t.Errorf("usage %s=%s want=%d (must count each model round once)", path, usage.Get(path).Raw, want)
					}
				}
				if terminal.Get("response.model").String() != "fixture-model" || terminal.Get("response.service_tier").String() != "default" {
					t.Fatal("usage model or service tier lost")
				}
			}
			deltas := 0
			for _, event := range events {
				if event.Get("type").String() == "response.output_text.delta" {
					deltas++
				}
				if event.Get("type").String() == "response.completed" {
					t.Fatal("hidden successful round escaped as a public completion")
				}
			}
			if deltas != 2 || terminal.Get("response.output.#").Int() != 4 || strings.Contains(stream.String(), bridge.name) {
				t.Fatal("already streamed content was repeated, dropped or exposed as private replay")
			}
		})
	}
}

type codexSearchHTTPFailureBody struct {
	io.Reader
	closed bool
}

func (b *codexSearchHTTPFailureBody) Close() error {
	b.closed = true
	return nil
}

type codexSearchHTTPFailureWriter struct {
	strings.Builder
	failedWrites int
}

func (w *codexSearchHTTPFailureWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), `"type":"response.failed"`) {
		w.failedWrites++
		return 0, io.ErrClosedPipe
	}
	return w.Builder.Write(p)
}

func TestCodexSearchHTTPFailureTerminalWriteError(t *testing.T) {
	bridge := &anthropicCodexWebSearch{name: "private_search", remaining: 1, onCompleted: func([]json.RawMessage) { t.Fatal("cached a failed round") }}
	first := &nativeSearchFixture{id: "resp_fixture", createdAt: 1700000000}
	first.lifecycle("response.created")
	first.lifecycle("response.completed", codexSearchTestCall(bridge.name, "Synthetic query"))
	body := &codexSearchHTTPFailureBody{Reader: strings.NewReader(`{"error":{"code":"rate_limit_exceeded","message":"Synthetic rejection"}}`)}
	writer := &codexSearchHTTPFailureWriter{}
	opened := 0
	err := bridge.run(context.Background(), first.response(), []byte(`{"input":[]}`), func([]byte) (*http.Response, error) {
		opened++
		return &http.Response{StatusCode: http.StatusTooManyRequests, Body: body}, nil
	}, nativeSearchSuccess, writer)
	if !errors.Is(err, io.ErrClosedPipe) || opened != 1 || writer.failedWrites != 1 || !body.closed {
		t.Fatalf("failed write must propagate without retry: err=%v opened=%d writes=%d closed=%t", err, opened, writer.failedWrites, body.closed)
	}
	if strings.Contains(writer.String(), `"type":"response.completed"`) || strings.Contains(writer.String(), `"type":"response.failed"`) {
		t.Fatal("write failure fabricated a delivered terminal")
	}
}
