package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// A merged search response has no upstream-native response ID: even its last
// upstream response may precede a hidden search result (parallel external tools).
const codexSearchResponseIDPrefix = "resp_c2a_search_"

func isCodexSearchResponseID(id string) bool {
	return strings.HasPrefix(strings.TrimSpace(id), codexSearchResponseIDPrefix)
}

// Reuse the bounded native snapshot backend, including encrypted provenance and
// shared commit barriers. The public owner's write policy is checked BEFORE this
// internal namespace is selected. Native store:false keeps its existing scope.
func codexSearchContextOwner(owner, id string) string {
	if isCodexSearchResponseID(id) && !strings.HasPrefix(owner, nativeWSCachePrefix) {
		if owner == "" {
			owner = "anon"
		}
		return nativeWSCachePrefix + "search:" + owner
	}
	return owner
}

type codexSearchContextBody struct {
	io.ReadCloser
	mu         sync.Mutex
	responseID string
	replay     []json.RawMessage
	completed  bool
}

// Called after wrapped.Body is assigned and before bridge.run starts. The
// callback runs synchronously before the public response.completed is emitted;
// nothing in the private replay is injected into the public response stream.
func attachCodexSearchContinuation(response *http.Response, bridge *anthropicCodexWebSearch) {
	if response == nil || response.Body == nil || bridge == nil {
		return
	}
	body := &codexSearchContextBody{ReadCloser: response.Body, responseID: codexSearchResponseIDPrefix + uuid.NewString()}
	bridge.responseID = body.responseID
	bridge.onCompleted = func(replay []json.RawMessage) {
		body.mu.Lock()
		defer body.mu.Unlock()
		// The bridge owns an immutable copy of each item. Keep our own slice so
		// publication and snapshot reads cannot race with slice reuse.
		body.replay = append([]json.RawMessage(nil), replay...)
		body.completed = true
	}
	response.Body = body
}

// Returns true for every logical search ID, including missing/incomplete private
// state. Such a response must NEVER fall back to caching its public projection.
// Call only after successful downstream commit.
func cacheCodexSearchContinuation(response *http.Response, snapshot nativeWSCompletedContext) bool {
	id := gjson.GetBytes(snapshot.completed, "response.id").String()
	return cacheCodexSearchContinuationWithID(response, snapshot, id)
}

func cacheCodexSearchContinuationWithID(response *http.Response, snapshot nativeWSCompletedContext, id string) bool {
	if !isCodexSearchResponseID(id) {
		return false
	}
	if response == nil || snapshot.input == "" {
		return true
	}
	body, ok := response.Body.(*codexSearchContextBody)
	if !ok {
		return true
	}
	body.mu.Lock()
	ready := body.completed && body.responseID == id
	replay := append([]json.RawMessage(nil), body.replay...)
	body.mu.Unlock()
	if !ready {
		return true
	}
	if !strings.HasPrefix(snapshot.owner, nativeWSCachePrefix) && skipResponseCacheWrite(snapshot.owner) {
		return true
	}
	var input []json.RawMessage
	if json.Unmarshal([]byte(snapshot.input), &input) != nil {
		return true
	}
	merged, err := json.Marshal(mergeResponsesWSContext(input, replay))
	if err != nil {
		return true
	}
	// outputs-only caching filters out function_call_output. Put the complete
	// private tail into input, and suppress the public web_search_call fallback.
	snapshot.completed, err = sjson.SetRawBytes(snapshot.completed, "response.output", []byte("[]"))
	if err != nil {
		return true
	}
	snapshot.owner = codexSearchContextOwner(snapshot.owner, id)
	snapshot.input, snapshot.outputs = string(merged), nil
	cacheCommittedResponsesWSContext(snapshot)
	return true
}

func cacheCommittedCodexHTTPResponse(response *http.Response, owner, input string, completed []byte, outputs []json.RawMessage, account *auth.Account) {
	id := gjson.GetBytes(completed, "response.id").String()
	if isCodexSearchResponseID(id) {
		// Only private search snapshots need account provenance. A logical ID
		// must never fall back to the public output, even if replay is missing.
		snapshot := nativeWSCompletedContext{owner: owner, input: input, completed: completed, outputs: outputs, provenance: nativeWSAccountProvenance(account)}
		cacheCodexSearchContinuationWithID(response, snapshot, id)
		return
	}
	cacheCompletedResponseWithLazyInput(owner, func() []byte { return []byte(input) }, id, completed, outputs)
}

// Reject unavailable logical ancestry before scheduling, including when the
// ordinary continuation fail-open escape hatch or a native relay is enabled.
// Do not materialize Input until the selected account has passed provenance.
func codexSearchReplayLookupError(source *responsesWSReplaySource) *api.APIError {
	if source == nil || !isCodexSearchResponseID(gjson.GetBytes(source.body, "previous_response_id").String()) {
		return nil
	}
	if source.previous != nil && source.previous.Kind == responseCacheLookupHit {
		return nil
	}
	lookup := responseCacheLookupResult{Kind: responseCacheLookupMiss}
	if source.previous != nil {
		lookup = *source.previous
	}
	status, reason, unavailable := responseCachePreparationFailure(responsesBodyPreparation{
		PreviousResponseID: gjson.GetBytes(source.body, "previous_response_id").String(), CacheLookup: lookup, RequiresLocalContext: true,
	})
	if !unavailable {
		status, reason = http.StatusConflict, "missing_required_call_context"
	}
	return responsesWSContextUnavailable(status, reason)
}

func prepareCodexSearchHTTPContinuation(ctx context.Context, body []byte, owner string) (*responsesWSReplaySource, *api.APIError) {
	id := gjson.GetBytes(body, "previous_response_id").String()
	if !isCodexSearchResponseID(id) {
		return nil, nil
	}
	if err := waitNativeWSCommit(ctx, owner, id); err != nil {
		return nil, responsesWSContextUnavailable(http.StatusServiceUnavailable, "context_commit_pending")
	}
	normalized, _ := prepareResponsesWebSocketTurnBody(body)
	source := newResponsesWSReplaySource(normalized, owner)
	if source.previous != nil {
		recordResponseCacheLookup(owner, *source.previous)
	}
	return source, codexSearchReplayLookupError(source)
}
