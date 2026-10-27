package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/codex2api/auth"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Codex's Responses backend rejects hosted search. Messages and native
// Responses requests execute search through the same official standalone
// endpoint used by Codex clients.
type anthropicCodexWebSearch struct {
	name, model string
	domains     []string
	remaining   int
	hostedTool  json.RawMessage
	responseID  string
	onCompleted func([]json.RawMessage)
	interrupt   *codexSearchInterrupt
}

const (
	anthropicCodexMaxSearchUses     = 8
	anthropicSearchMaxPendingBytes  = 1 << 20
	anthropicSearchMaxPendingEvents = 1024
	anthropicSearchMaxPendingItems  = 128
)

func prepareAnthropicCodexWebSearch(body, messagesBody []byte) ([]byte, *anthropicCodexWebSearch, error) {
	var bridge *anthropicCodexWebSearch
	tools := gjson.GetBytes(body, "tools").Array()
	for i, tool := range tools {
		if !strings.HasPrefix(tool.Get("type").String(), "web_search") {
			continue
		}
		if bridge != nil {
			return nil, nil, fmt.Errorf("only one hosted web_search declaration is supported")
		}
		if len(tool.Get("user_location").Map()) > 0 || len(tool.Get("filters.blocked_domains").Array()) > 0 {
			return nil, nil, fmt.Errorf("Codex standalone web search does not support user_location or blocked_domains")
		}
		bridge = &anthropicCodexWebSearch{name: "codex2api_web_search", model: gjson.GetBytes(body, "model").String(), remaining: anthropicCodexMaxSearchUses, hostedTool: json.RawMessage(tool.Raw)}
		for _, other := range tools {
			if other.Get("name").String() == bridge.name {
				bridge.name += "_" + uuid.NewString()[:8]
			}
		}
		for _, domain := range tool.Get("filters.allowed_domains").Array() {
			bridge.domains = append(bridge.domains, domain.String())
		}
		for _, original := range gjson.GetBytes(messagesBody, "tools").Array() {
			if original.Get("name").String() == "web_search" {
				if limit := original.Get("max_uses"); limit.Exists() {
					if limit.Type != gjson.Number || limit.Float() != float64(limit.Int()) || limit.Int() < 0 {
						return nil, nil, fmt.Errorf("web_search max_uses must be a non-negative integer")
					}
					bridge.remaining = min(bridge.remaining, int(limit.Int()))
				}
			}
		}
		replacement := map[string]any{
			"type": "function", "name": bridge.name, "strict": false,
			"description": "Search the web using the official Codex search service. Use the returned sources to answer with links.",
			"parameters":  map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}, "required": []string{"query"}, "additionalProperties": false},
		}
		body, _ = sjson.SetBytes(body, fmt.Sprintf("tools.%d", i), replacement)
	}
	if bridge == nil {
		return body, nil, nil
	}
	if strings.HasPrefix(gjson.GetBytes(body, "tool_choice.type").String(), "web_search") {
		body, _ = sjson.SetBytes(body, "tool_choice", map[string]any{"type": "function", "name": bridge.name})
	}
	for i, tool := range gjson.GetBytes(body, "tool_choice.tools").Array() {
		if strings.HasPrefix(tool.Get("type").String(), "web_search") {
			body, _ = sjson.SetBytes(body, fmt.Sprintf("tool_choice.tools.%d", i), map[string]any{"type": "function", "name": bridge.name})
		}
	}
	return body, bridge, nil
}

type anthropicSearchStreamBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *anthropicSearchStreamBody) Close() error {
	b.cancel()
	return b.ReadCloser.Close()
}

// codexResponsesBodyHasHostedWebSearch reports a Responses hosted search tool.
// A custom function named web_search is not a hosted tool and stays untouched.
func codexResponsesBodyHasHostedWebSearch(body []byte) bool {
	found := false
	gjson.GetBytes(body, "tools").ForEach(func(_, tool gjson.Result) bool {
		if strings.HasPrefix(strings.TrimSpace(tool.Get("type").String()), "web_search") {
			found = true
			return false
		}
		return true
	})
	return found
}

// executeCodexResponsesRequest sends a Codex Responses request. Hosted web_search
// is executed through /alpha/search so the Codex backend never sees the metered
// hosted tool it rejects. Bodies without that tool use the normal transport.
func executeCodexResponsesRequest(ctx context.Context, account *auth.Account, body []byte, sessionID, proxyURL, apiKey string, deviceCfg *DeviceProfileConfig, headers http.Header, websocket bool) (*http.Response, error) {
	if !codexResponsesBodyHasHostedWebSearch(body) {
		return ExecuteRequest(ctx, account, body, sessionID, proxyURL, apiKey, deviceCfg, headers, websocket)
	}
	return executeAnthropicCodexRequest(ctx, account, body, nil, sessionID, proxyURL, apiKey, deviceCfg, headers, websocket)
}

func executeAnthropicCodexRequest(ctx context.Context, account *auth.Account, body, messagesBody []byte, sessionID, proxyURL, apiKey string, deviceCfg *DeviceProfileConfig, headers http.Header, websocket bool) (*http.Response, error) {
	body, bridge, err := prepareAnthropicCodexWebSearch(body, messagesBody)
	if err != nil {
		return nil, ErrBadRequest(err.Error())
	}
	if bridge == nil {
		return ExecuteRequest(ctx, account, body, sessionID, proxyURL, apiKey, deviceCfg, headers, websocket)
	}
	// The outer Messages reader owns downstream keepalive writes.
	ctx, cancel := context.WithCancel(context.WithValue(ctx, continuousRetryKeepaliveContextKey{}, struct{}{}))
	ctx, unregisterInterrupt := withCodexSearchInterrupt(ctx, bridge)
	open := func(next []byte) (*http.Response, error) {
		if err := bridge.interrupt.beginRound(); err != nil {
			return nil, err
		}
		return ExecuteRequest(ctx, account, next, sessionID, proxyURL, apiKey, deviceCfg, headers, websocket)
	}
	resp, err := open(body)
	if err != nil {
		unregisterInterrupt()
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		unregisterInterrupt()
		resp.Body = &anthropicSearchStreamBody{ReadCloser: resp.Body, cancel: cancel}
		return resp, nil
	}
	search := func(raw []byte) (*CodexAlphaSearchResponse, error) {
		return ForwardCodexAlphaSearch(ctx, account, proxyURL, raw, headers, deviceCfg, apiKey)
	}
	reader, writer := io.Pipe()
	wrapped := *resp
	wrapped.Body = &anthropicSearchStreamBody{ReadCloser: reader, cancel: cancel}
	attachCodexSearchContinuation(&wrapped, bridge)
	bridge.interrupt.setLogicalID(bridge.responseID)
	go func() {
		defer cancel()
		defer unregisterInterrupt()
		err := bridge.run(ctx, resp, body, open, search, writer)
		_ = writer.CloseWithError(err)
	}()
	return &wrapped, nil
}

// The wire response is independent of the per-round upstream response. Allocate
// a slot only when an item becomes visible (hidden calls get no slot), and use
// that same slot for its entire lifetime. Interrupted slots stay reserved but
// are omitted from the final response.output, without reordering retained items.
// In particular, terminal array order must not reorder already streamed items.
type anthropicSearchOutput struct {
	bridge    *anthropicCodexWebSearch
	writer    io.Writer
	seq       int64
	id        string
	createdAt json.RawMessage
	items     []json.RawMessage
	itemIDs   map[string]bool
	err       error
}

func (s *anthropicSearchOutput) rememberResponse(data []byte) {
	response := gjson.GetBytes(data, "response")
	if s.id == "" {
		s.id = response.Get("id").String()
	}
	if s.createdAt == nil {
		if value := response.Get("created_at"); value.Exists() {
			s.createdAt = json.RawMessage(value.Raw)
		}
	}
}

func (s *anthropicSearchOutput) emit(data []byte) bool {
	if s.err != nil {
		return false
	}
	s.rememberResponse(data)
	if gjson.GetBytes(data, "response").IsObject() {
		if s.id != "" {
			data, _ = sjson.SetBytes(data, "response.id", s.id)
		}
		if s.createdAt != nil {
			data, _ = sjson.SetRawBytes(data, "response.created_at", s.createdAt)
		}
		// Responses may echo the prepared tools/choice. Never expose the private
		// function declaration, even in response.created or response.failed.
		hosted := s.bridge.hostedTool
		if len(hosted) == 0 {
			hosted = json.RawMessage(`{"type":"web_search"}`)
		}
		for i, tool := range gjson.GetBytes(data, "response.tools").Array() {
			if tool.Get("type").String() == "function" && tool.Get("name").String() == s.bridge.name {
				data, _ = sjson.SetRawBytes(data, fmt.Sprintf("response.tools.%d", i), hosted)
			}
		}
		choice := map[string]any{"type": gjson.GetBytes(hosted, "type").String()}
		if gjson.GetBytes(data, "response.tool_choice.name").String() == s.bridge.name {
			data, _ = sjson.SetBytes(data, "response.tool_choice", choice)
		}
		for i, tool := range gjson.GetBytes(data, "response.tool_choice.tools").Array() {
			if tool.Get("type").String() == "function" && tool.Get("name").String() == s.bridge.name {
				data, _ = sjson.SetBytes(data, fmt.Sprintf("response.tool_choice.tools.%d", i), choice)
			}
		}
		if gjson.GetBytes(data, "response.output").IsArray() {
			visible := make([]json.RawMessage, 0)
			for _, item := range gjson.GetBytes(data, "response.output").Array() {
				if item.Get("type").String() == "function_call" && (item.Get("name").String() == "" || item.Get("name").String() == s.bridge.name) {
					continue
				}
				visible = append(visible, json.RawMessage(item.Raw))
			}
			data, _ = sjson.SetBytes(data, "response.output", visible)
		}
	}
	if s.id != "" && gjson.GetBytes(data, "response_id").Exists() {
		data, _ = sjson.SetBytes(data, "response_id", s.id)
	}
	data, _ = sjson.SetBytes(data, "sequence_number", s.seq)
	s.seq++
	_, s.err = fmt.Fprintf(s.writer, "data: %s\n\n", data)
	return s.err == nil
}

type anthropicSearchPendingEvent struct {
	order int
	data  []byte
}

type anthropicSearchPart struct {
	text                  string
	added, textDone, done bool
	spec                  anthropicSearchTextSpec
	index                 int64
}

type anthropicSearchItem struct {
	item       json.RawMessage // Upstream snapshot, for internal continuation only.
	id         string          // Downstream identity (also supplied for id-less items).
	upstreamID string
	callID     string
	kind       string // Also inferred from pre-declaration events; never sufficient to publish a call.
	assigned   bool
	index      int
	hidden     bool
	added      bool
	done       bool
	arguments  string
	input      string
	pending    []anthropicSearchPendingEvent
	parts      map[string]*anthropicSearchPart
	nextPart   int64 // First part whose text has not yet been published in full.
	deferred   [][]byte

	interrupted, interruptSent bool // Tombstones survive every alias join and later snapshot.
}

func (entry *anthropicSearchItem) classified() bool {
	kind := gjson.GetBytes(entry.item, "type").String()
	return kind != "" && (kind != "function_call" || gjson.GetBytes(entry.item, "name").String() != "")
}

type anthropicSearchRound struct {
	output                                  *anthropicSearchOutput
	byID                                    map[string]*anthropicSearchItem
	byCall                                  map[string]*anthropicSearchItem
	byIndex                                 map[int64]*anthropicSearchItem
	items                                   []*anthropicSearchItem
	current                                 *anthropicSearchItem
	pendingBytes, pendingEvents, eventOrder int
}

func (r *anthropicSearchRound) fail(message string) bool {
	if r.output.err == nil {
		r.output.err = ErrUpstreamStreamBreak(message)
	}
	return false
}

func anthropicSearchCompatible(entry *anthropicSearchItem, id, callID, kind string) bool {
	return (id == "" || entry.upstreamID == "" || id == entry.upstreamID) &&
		(callID == "" || entry.callID == "" || callID == entry.callID) &&
		(kind == "" || entry.kind == "" || kind == entry.kind)
}

// A declaration can join an id-only and an index-only placeholder. Redirect
// every alias and preserve arrival order, rather than abandoning one queue.
func (r *anthropicSearchRound) merge(entry, other *anthropicSearchItem) *anthropicSearchItem {
	if entry == other {
		return entry
	}
	if !anthropicSearchCompatible(entry, other.upstreamID, other.callID, other.kind) || (entry.added && other.added) {
		r.fail("Codex search stream has conflicting output item aliases")
		return nil
	}
	if other.added {
		entry, other = other, entry
	}
	if entry.upstreamID == "" {
		entry.upstreamID = other.upstreamID
	}
	if entry.id == "" {
		entry.id = other.id
	}
	if entry.callID == "" {
		entry.callID = other.callID
	}
	if entry.kind == "" {
		entry.kind = other.kind
	}
	entry.interrupted = entry.interrupted || other.interrupted
	entry.interruptSent = entry.interruptSent || other.interruptSent
	entry.pending = append(entry.pending, other.pending...)
	other.pending = nil
	sort.Slice(entry.pending, func(i, j int) bool { return entry.pending[i].order < entry.pending[j].order })
	for _, pending := range entry.pending {
		data := gjson.ParseBytes(pending.data)
		switch data.Get("type").String() {
		case "response.function_call_arguments.done":
			entry.arguments = data.Get("arguments").String()
		case "response.custom_tool_call_input.done":
			entry.input = data.Get("input").String()
		}
	}
	if other.item != nil && !r.update(entry, gjson.ParseBytes(other.item)) {
		return nil
	}
	for key, value := range r.byID {
		if value == other {
			r.byID[key] = entry
		}
	}
	for key, value := range r.byCall {
		if value == other {
			r.byCall[key] = entry
		}
	}
	for key, value := range r.byIndex {
		if value == other {
			r.byIndex[key] = entry
		}
	}
	for i, value := range r.items {
		if value == other {
			copy(r.items[i:], r.items[i+1:])
			r.items[len(r.items)-1] = nil
			r.items = r.items[:len(r.items)-1]
			break
		}
	}
	if r.current == other {
		r.current = entry
	}
	if entry.interrupted && !r.interruptItem(entry, nil) {
		return nil
	}
	return entry
}

func (r *anthropicSearchRound) lookup(id, callID, kind string, index gjson.Result, terminal bool) *anthropicSearchItem {
	if index.Exists() && (index.Type != gjson.Number || index.Int() < 0 || index.Float() != float64(index.Int())) {
		r.fail("Codex search stream has an invalid output item index")
		return nil
	}
	var entry *anthropicSearchItem
	for _, candidate := range []*anthropicSearchItem{r.byID[id], r.byCall[callID]} {
		if candidate == nil {
			continue
		}
		if !anthropicSearchCompatible(candidate, id, callID, kind) {
			r.fail("Codex search stream has conflicting output item identities or types")
			return nil
		}
		if entry == nil {
			entry = candidate
		} else if entry = r.merge(entry, candidate); entry == nil {
			return nil
		}
	}
	if index.Exists() {
		candidate := r.byIndex[index.Int()]
		// Terminal array positions are only hints for otherwise unidentified
		// items, never aliases. In particular a compacted hidden call must not
		// overwrite a streamed reasoning/message item at the same position.
		if candidate != nil && (!terminal || entry == nil) {
			compatible := anthropicSearchCompatible(candidate, id, callID, kind)
			if terminal && (id != "" || callID != "") && (candidate.upstreamID != "" || candidate.callID != "") {
				compatible = false // A known identity would already have matched above.
			}
			if !compatible && !terminal {
				r.fail("Codex search stream has conflicting output item indices or types")
				return nil
			}
			if compatible {
				if entry == nil {
					entry = candidate
				} else if entry = r.merge(entry, candidate); entry == nil {
					return nil
				}
			}
		}
	}
	if entry == nil {
		entry = &anthropicSearchItem{index: -1}
		r.items = append(r.items, entry)
	}
	if id != "" {
		r.byID[id], entry.upstreamID = entry, id
		if entry.id == "" {
			entry.id = id
		}
	}
	if callID != "" {
		r.byCall[callID], entry.callID = entry, callID
	}
	if kind != "" {
		entry.kind = kind
	}
	if index.Exists() && !terminal {
		r.byIndex[index.Int()] = entry
	}
	return entry
}

func (r *anthropicSearchRound) checkPending(extraBytes, extraEvents int) bool {
	bytes, events, items := r.pendingBytes+extraBytes, r.pendingEvents+extraEvents, 0
	for _, entry := range r.items {
		if !entry.classified() {
			// Unknown interrupted identities still consume a tombstone slot;
			// clearing their payload must not allow unbounded alias maps.
			if !entry.added {
				items++
			}
			bytes += len(entry.item)
			if entry.item != nil {
				events++
			}
		}
	}
	switch {
	case bytes > anthropicSearchMaxPendingBytes:
		return r.fail("Codex search stream exceeded pending output item bytes limit")
	case events > anthropicSearchMaxPendingEvents:
		return r.fail("Codex search stream exceeded pending output item events limit")
	case items > anthropicSearchMaxPendingItems:
		return r.fail("Codex search stream exceeded pending output item count limit")
	}
	return true
}

func (r *anthropicSearchRound) takePending(entry *anthropicSearchItem) []anthropicSearchPendingEvent {
	pending := entry.pending
	for _, event := range pending {
		r.pendingBytes -= len(event.data)
		r.pendingEvents--
	}
	entry.pending = nil
	return pending
}

// An explicit interruption is authoritative even if a later declaration joins
// this tombstone to an already published item. Keep its aliases/slot, not its
// buffered inputs or snapshots. Never publish an undeclared/private payload.
func (r *anthropicSearchRound) interruptItem(entry *anthropicSearchItem, data []byte) bool {
	entry.interrupted = true
	r.takePending(entry)
	for _, data := range entry.deferred {
		r.pendingBytes -= len(data)
		r.pendingEvents--
	}
	entry.deferred, entry.parts, entry.item = nil, nil, nil
	entry.arguments, entry.input = "", ""
	if entry.index >= 0 {
		r.output.items[entry.index] = nil // Reserve this slot; later items must not reuse it.
	}
	if !r.checkPending(0, 0) {
		return false
	}
	if !entry.added || entry.hidden || entry.interruptSent {
		return true
	}
	entry.interruptSent = true
	if len(data) == 0 {
		// A previously unknown tombstone can join an already published alias.
		data = []byte(`{"type":"response.output_item.interrupted"}`)
	}
	// Do not resurrect a discarded item snapshot, but retain other metadata
	// on directly observed interruption events (including exact JSON numbers).
	data, _ = sjson.DeleteBytes(data, "item")
	data, _ = sjson.SetBytes(data, "item_id", entry.id)
	data, _ = sjson.SetBytes(data, "output_index", entry.index)
	return r.output.emit(data)
}

func (r *anthropicSearchRound) update(entry *anthropicSearchItem, item gjson.Result) bool {
	if entry.interrupted {
		return true
	}
	if name, previous := item.Get("name").String(), gjson.GetBytes(entry.item, "name").String(); name != "" && previous != "" && name != previous {
		return r.fail("Codex search stream has conflicting output function names")
	}
	if entry.item == nil {
		entry.item = json.RawMessage(`{}`)
	}
	item.ForEach(func(key, value gjson.Result) bool {
		// Sparse snapshots must not erase known identity or completed inputs.
		// Keep initial empty fields, though: added function calls require an
		// arguments string even before the first argument delta arrives.
		switch key.String() {
		case "id", "type", "name", "call_id", "arguments", "input":
			if value.String() == "" && gjson.GetBytes(entry.item, key.String()).String() != "" {
				return true
			}
		}
		entry.item, _ = sjson.SetRawBytes(entry.item, key.String(), []byte(value.Raw))
		return true
	})
	if !gjson.GetBytes(entry.item, "type").Exists() && entry.kind != "" {
		entry.item, _ = sjson.SetBytes(entry.item, "type", entry.kind)
	}
	entry.hidden = entry.hidden || (gjson.GetBytes(entry.item, "type").String() == "function_call" && gjson.GetBytes(entry.item, "name").String() == r.output.bridge.name)
	if gjson.GetBytes(entry.item, "arguments").String() == "" && entry.arguments != "" {
		entry.item, _ = sjson.SetBytes(entry.item, "arguments", entry.arguments)
	}
	if gjson.GetBytes(entry.item, "input").String() == "" && entry.input != "" {
		entry.item, _ = sjson.SetBytes(entry.item, "input", entry.input)
	}
	return true
}

func (r *anthropicSearchRound) assignID(entry *anthropicSearchItem, fallback string) {
	if entry.assigned {
		return
	}
	if r.output.itemIDs == nil {
		r.output.itemIDs = map[string]bool{}
	}
	if entry.id == "" {
		entry.id = fallback
	}
	if entry.id == "" || r.output.itemIDs[entry.id] {
		entry.id = "item_" + uuid.NewString()
	}
	r.output.itemIDs[entry.id] = true
	entry.assigned = true
}

func (r *anthropicSearchRound) publicItem(entry *anthropicSearchItem) json.RawMessage {
	r.assignID(entry, "")
	item, _ := sjson.SetBytes(entry.item, "id", entry.id)
	return item
}

type anthropicSearchTextSpec struct {
	prefix, field, indexKey, collection, partType, partPrefix string
}

func anthropicSearchTextEvent(kind string) (anthropicSearchTextSpec, bool) {
	for _, spec := range []anthropicSearchTextSpec{
		{"response.output_text.", "text", "content_index", "content", "output_text", "response.content_part."},
		{"response.refusal.", "refusal", "content_index", "content", "refusal", "response.content_part."},
		{"response.reasoning_summary_text.", "text", "summary_index", "summary", "summary_text", "response.reasoning_summary_part."},
		{"response.function_call_arguments.", "arguments", "", "", "", ""},
		{"response.custom_tool_call_input.", "input", "", "", "", ""},
	} {
		if kind == spec.prefix+"delta" || kind == spec.prefix+"done" {
			return spec, true
		}
	}
	return anthropicSearchTextSpec{}, false
}

func anthropicSearchPartIndex(data gjson.Result) (gjson.Result, bool) {
	kind, key := data.Get("type").String(), ""
	switch {
	case strings.HasPrefix(kind, "response.output_text."), strings.HasPrefix(kind, "response.content_part."), strings.HasPrefix(kind, "response.refusal."):
		key = "content_index"
	case strings.HasPrefix(kind, "response.reasoning_summary_"):
		key = "summary_index"
	default:
		return gjson.Result{}, false
	}
	index := data.Get(key)
	if index.Raw == "" {
		index = gjson.Parse("0") // Compatibility with older single-part streams.
	}
	return index, true
}

func (r *anthropicSearchRound) validPartIndex(entry *anthropicSearchItem, index gjson.Result) bool {
	// sjson pads arrays up to the index. Bound sparse padding as well as the
	// deferred event queue, without limiting the number of contiguous parts.
	if index.Type != gjson.Number || index.Int() < 0 || index.Float() != float64(index.Int()) || index.Int() > int64(len(entry.parts)+anthropicSearchMaxPendingEvents) {
		return r.fail("Codex search stream has an invalid or excessive output part index")
	}
	return true
}

func (entry *anthropicSearchItem) part(spec anthropicSearchTextSpec, index int64) *anthropicSearchPart {
	if entry.parts == nil {
		entry.parts = map[string]*anthropicSearchPart{}
	}
	key := fmt.Sprintf("%s%d", spec.prefix, index)
	if entry.parts[key] == nil {
		entry.parts[key] = &anthropicSearchPart{spec: spec, index: index}
	}
	return entry.parts[key]
}

func (entry *anthropicSearchItem) rememberText(part *anthropicSearchPart) {
	spec := part.spec
	path := spec.field
	if spec.collection != "" {
		partPath := fmt.Sprintf("%s.%d", spec.collection, part.index)
		path = partPath + "." + spec.field
		entry.item, _ = sjson.SetBytes(entry.item, partPath+".type", spec.partType)
	}
	// Fill sparse snapshots, but do not truncate richer final text.
	if strings.HasPrefix(part.text, gjson.GetBytes(entry.item, path).String()) {
		entry.item, _ = sjson.SetBytes(entry.item, path, part.text)
	}
}

func anthropicSearchPartEvent(spec anthropicSearchTextSpec, index int64, kind string, fields map[string]any) []byte {
	fields["type"] = kind
	if spec.indexKey != "" {
		fields[spec.indexKey] = index
	}
	raw, _ := json.Marshal(fields)
	return raw
}

// Reconcile a full text snapshot against only the deltas already accepted for
// this part. Done-only and partial-delta streams need the missing suffix, not
// another copy of the entire final text.
func (r *anthropicSearchRound) completeText(entry *anthropicSearchItem, spec anthropicSearchTextSpec, index int64, text string) bool {
	if spec.indexKey != "" && !r.validPartIndex(entry, gjson.Parse(fmt.Sprint(index))) {
		return false
	}
	part := entry.part(spec, index)
	if !strings.HasPrefix(text, part.text) {
		return r.fail("Codex search stream has inconsistent final output text")
	}
	if len(text) == len(part.text) {
		return true
	}
	if spec.indexKey != "" && part.textDone {
		return r.fail("Codex search stream extended completed output text")
	}
	return r.emitItem(entry, anthropicSearchPartEvent(spec, index, spec.prefix+"delta", map[string]any{"delta": text[len(part.text):]}))
}

func (r *anthropicSearchRound) emitItem(entry *anthropicSearchItem, data []byte) bool {
	if entry.interrupted {
		return true
	}
	parsed := gjson.ParseBytes(data)
	kind := parsed.Get("type").String()
	if index, ok := anthropicSearchPartIndex(parsed); ok {
		if !r.validPartIndex(entry, index) || (index.Int() > entry.nextPart && !r.checkPending(len(data), 1)) {
			return false
		}
	}
	if spec, ok := anthropicSearchTextEvent(kind); ok {
		index := parsed.Get(spec.indexKey).Int()
		part := entry.part(spec, index)
		if spec.partPrefix != "" && !part.added {
			if !r.emitItem(entry, anthropicSearchPartEvent(spec, index, spec.partPrefix+"added", map[string]any{"part": map[string]any{"type": spec.partType, spec.field: ""}})) {
				return false
			}
		}
		if kind == spec.prefix+"done" {
			text := part.text
			if value := parsed.Get(spec.field); value.Type == gjson.String {
				text = value.String()
			}
			if !r.completeText(entry, spec, index, text) {
				return false
			}
			if part.textDone {
				return true
			}
			part.textDone = true
		} else {
			if spec.indexKey != "" && part.textDone && parsed.Get("delta").String() != "" {
				return r.fail("Codex search stream extended completed output text")
			}
			part.text += parsed.Get("delta").String()
		}
		entry.rememberText(part)
	} else if strings.HasPrefix(kind, "response.content_part.") || strings.HasPrefix(kind, "response.reasoning_summary_part.") {
		prefix := "response.output_text."
		if strings.HasPrefix(kind, "response.reasoning_summary_part.") {
			prefix = "response.reasoning_summary_text."
		} else if parsed.Get("part.type").String() == "refusal" {
			prefix = "response.refusal."
		} else if parsed.Get("part.type").String() != "output_text" {
			return r.writeItem(entry, data)
		}
		spec, _ := anthropicSearchTextEvent(prefix + "done")
		index := parsed.Get(spec.indexKey).Int()
		part := entry.part(spec, index)
		partPath := fmt.Sprintf("%s.%d", spec.collection, index)
		parsed.Get("part").ForEach(func(key, value gjson.Result) bool {
			path := partPath + "." + key.String()
			if key.String() != spec.field || strings.HasPrefix(value.String(), gjson.GetBytes(entry.item, path).String()) {
				entry.item, _ = sjson.SetRawBytes(entry.item, path, []byte(value.Raw))
			}
			return true
		})
		if kind == spec.partPrefix+"done" {
			text := parsed.Get("part." + spec.field).String()
			if !r.emitItem(entry, anthropicSearchPartEvent(spec, index, spec.prefix+"done", map[string]any{spec.field: text})) {
				return false
			}
			if part.done {
				return true
			}
			part.done = true
		} else if kind == spec.partPrefix+"added" {
			if part.added {
				return true
			}
			part.added = true
			data, _ = sjson.SetBytes(data, "part."+spec.field, "")
			if !r.writeItem(entry, data) {
				return false
			}
			return r.completeText(entry, spec, index, parsed.Get("part."+spec.field).String())
		}
	}
	return r.writeItem(entry, data)
}

// Native parts retain their indices, but Messages concatenates their deltas
// in wire order. Only the current part may stream immediately; later parts
// wait for its text.done (or part.done), sharing the unclassified-item budget.
func (r *anthropicSearchRound) writeItem(entry *anthropicSearchItem, data []byte) bool {
	if index, ok := anthropicSearchPartIndex(gjson.ParseBytes(data)); ok && index.Int() > entry.nextPart {
		if !r.checkPending(len(data), 1) {
			return false
		}
		entry.deferred = append(entry.deferred, append([]byte(nil), data...))
		r.pendingBytes += len(data)
		r.pendingEvents++
		return true
	}
	previous := entry.nextPart
	if !r.publishItem(entry, data) {
		return false
	}
	return entry.nextPart == previous || r.flushParts(entry)
}

func (r *anthropicSearchRound) flushParts(entry *anthropicSearchItem) bool {
	for {
		ready := -1
		for i, data := range entry.deferred {
			if index, _ := anthropicSearchPartIndex(gjson.ParseBytes(data)); index.Int() <= entry.nextPart {
				ready = i
				break
			}
		}
		if ready < 0 {
			return true
		}
		data := entry.deferred[ready]
		copy(entry.deferred[ready:], entry.deferred[ready+1:])
		entry.deferred[len(entry.deferred)-1] = nil
		entry.deferred = entry.deferred[:len(entry.deferred)-1]
		r.pendingBytes -= len(data)
		r.pendingEvents--
		if !r.publishItem(entry, data) {
			return false
		}
	}
}

func (r *anthropicSearchRound) publishItem(entry *anthropicSearchItem, data []byte) bool {
	if entry.index < 0 {
		entry.index = len(r.output.items)
		r.output.items = append(r.output.items, nil)
	}
	data, _ = sjson.SetBytes(data, "output_index", entry.index)
	if item := gjson.GetBytes(data, "item"); item.IsObject() {
		r.assignID(entry, item.Get("id").String())
		raw, _ := sjson.SetBytes([]byte(item.Raw), "id", entry.id)
		data, _ = sjson.SetRawBytes(data, "item", raw)
		r.output.items[entry.index] = raw
	} else {
		data, _ = sjson.SetBytes(data, "item_id", entry.id)
		kind := gjson.GetBytes(data, "type").String()
		if strings.HasPrefix(kind, "response.output_text.") || strings.HasPrefix(kind, "response.content_part.") || strings.HasPrefix(kind, "response.refusal.") {
			if !gjson.GetBytes(data, "content_index").Exists() {
				data, _ = sjson.SetBytes(data, "content_index", 0)
			}
		}
		if strings.HasPrefix(kind, "response.reasoning_summary_") && !gjson.GetBytes(data, "summary_index").Exists() {
			data, _ = sjson.SetBytes(data, "summary_index", 0)
		}
	}
	if !r.output.emit(data) {
		return false
	}
	parsed := gjson.ParseBytes(data)
	if index, ok := anthropicSearchPartIndex(parsed); ok && index.Int() == entry.nextPart && strings.HasSuffix(parsed.Get("type").String(), ".done") {
		entry.nextPart++
	}
	return true
}

func (r *anthropicSearchRound) finish(entry *anthropicSearchItem) bool {
	if entry.interrupted {
		return true
	}
	if entry.hidden {
		r.takePending(entry)
		return true
	}
	if !entry.classified() {
		return r.fail("Codex search stream has an unclassified output item")
	}
	if !entry.added {
		entry.added = true
		if !r.emitItem(entry, mustMarshalSearchEvent("response.output_item.added", anthropicSearchAddedItem(gjson.ParseBytes(r.publicItem(entry))))) {
			return false
		}
	}
	for _, event := range r.takePending(entry) {
		if !r.emitItem(entry, event.data) {
			return false
		}
	}
	for _, part := range entry.parts {
		entry.rememberText(part)
	}
	ok := emitAnthropicSearchCompletedItem(gjson.ParseBytes(r.publicItem(entry)), func(data []byte) bool {
		parsed := gjson.ParseBytes(data)
		kind := parsed.Get("type").String()
		if kind == "response.output_item.added" {
			return true
		}
		// The completed snapshot visits parts in index order and includes the
		// accepted deferred text. Only now is it safe to skip absent indices.
		if index, ok := anthropicSearchPartIndex(parsed); ok && index.Int() > entry.nextPart {
			entry.nextPart = index.Int()
			if !r.flushParts(entry) {
				return false
			}
		}
		if spec, ok := anthropicSearchTextEvent(kind); ok && kind == spec.prefix+"delta" {
			return r.completeText(entry, spec, parsed.Get(spec.indexKey).Int(), parsed.Get("delta").String())
		}
		if kind == "response.output_item.done" {
			if len(entry.deferred) != 0 {
				return r.fail("Codex search stream has unresolved output parts")
			}
			if entry.done {
				return true
			}
			entry.done = true
			data = mustMarshalSearchEvent(kind, r.publicItem(entry))
		}
		return r.emitItem(entry, data)
	})
	if ok {
		// A terminal snapshot can contain richer metadata than output_item.done.
		r.output.items[entry.index] = r.publicItem(entry)
	}
	return ok
}

func (r *anthropicSearchRound) event(data []byte) bool {
	parsed := gjson.ParseBytes(data)
	kind := parsed.Get("type").String()
	r.eventOrder++
	if kind == "response.output_item.interrupted" {
		item := parsed.Get("item")
		id, callID := parsed.Get("item_id").String(), parsed.Get("call_id").String()
		if nested := item.Get("id").String(); nested != "" {
			if id != "" && id != nested {
				return r.fail("Codex search stream has conflicting interrupted output item identities")
			}
			id = nested
		}
		if nested := item.Get("call_id").String(); nested != "" {
			if callID != "" && callID != nested {
				return r.fail("Codex search stream has conflicting interrupted output call identities")
			}
			callID = nested
		}
		if id == "" && callID == "" && !parsed.Get("output_index").Exists() {
			return r.fail("Codex search stream has an interrupted output item without an identity")
		}
		entry := r.lookup(id, callID, item.Get("type").String(), parsed.Get("output_index"), false)
		if entry == nil {
			return false
		}
		r.current = entry
		return r.interruptItem(entry, data)
	}
	if kind == "response.output_item.added" || kind == "response.output_item.done" {
		item := parsed.Get("item")
		entry := r.lookup(item.Get("id").String(), item.Get("call_id").String(), item.Get("type").String(), parsed.Get("output_index"), false)
		if entry == nil {
			return false
		}
		r.current = entry
		if entry.interrupted {
			return true
		}
		if !r.update(entry, item) {
			return false
		}
		if entry.hidden {
			r.takePending(entry)
			return true
		}
		if !entry.classified() {
			return r.checkPending(0, 0) // Wait until the call can be classified as private/public.
		}
		if kind == "response.output_item.done" {
			return r.finish(entry)
		}
		if !entry.added {
			entry.added = true
			data, _ = sjson.SetRawBytes(data, "item", anthropicSearchAddedItem(gjson.ParseBytes(r.publicItem(entry))))
			if !r.emitItem(entry, data) {
				return false
			}
		}
		for _, pending := range r.takePending(entry) {
			if !r.emitItem(entry, pending.data) {
				return false
			}
		}
		return true
	}
	id, index := parsed.Get("item_id").String(), parsed.Get("output_index")
	itemEvent := id != "" || index.Exists() || strings.HasPrefix(kind, "response.function_call_arguments.") || strings.HasPrefix(kind, "response.custom_tool_call_input.") || strings.HasPrefix(kind, "response.output_text.") || strings.HasPrefix(kind, "response.reasoning_") || strings.HasPrefix(kind, "response.content_part.") || strings.HasPrefix(kind, "response.refusal.")
	if !itemEvent {
		return r.output.emit(data)
	}
	itemKind := ""
	switch {
	case strings.HasPrefix(kind, "response.function_call_arguments."):
		itemKind = "function_call"
	case strings.HasPrefix(kind, "response.custom_tool_call_input."):
		itemKind = "custom_tool_call"
	case strings.HasPrefix(kind, "response.reasoning_"):
		itemKind = "reasoning"
	case strings.HasPrefix(kind, "response.output_text."), strings.HasPrefix(kind, "response.content_part."), strings.HasPrefix(kind, "response.refusal."):
		itemKind = "message"
	}
	var entry *anthropicSearchItem
	if id == "" && parsed.Get("call_id").String() == "" && !index.Exists() && r.current != nil && anthropicSearchCompatible(r.current, "", "", itemKind) {
		entry = r.current // Compatibility with older upstreams that omit both keys.
	}
	if entry == nil {
		entry = r.lookup(id, parsed.Get("call_id").String(), itemKind, index, false)
	}
	if entry == nil {
		return false
	}
	if entry.interrupted {
		return true
	}
	if kind == "response.function_call_arguments.done" && parsed.Get("arguments").Type == gjson.String {
		entry.arguments = parsed.Get("arguments").String()
		if entry.item != nil {
			entry.item, _ = sjson.SetBytes(entry.item, "arguments", entry.arguments)
		}
	}
	if kind == "response.custom_tool_call_input.done" && parsed.Get("input").Type == gjson.String {
		entry.input = parsed.Get("input").String()
		if entry.item != nil {
			entry.item, _ = sjson.SetBytes(entry.item, "input", entry.input)
		}
	}
	if entry.hidden {
		return true
	}
	if !entry.added {
		if !r.checkPending(len(data), 1) {
			return false
		}
		entry.pending = append(entry.pending, anthropicSearchPendingEvent{order: r.eventOrder, data: append([]byte(nil), data...)})
		r.pendingBytes += len(data)
		r.pendingEvents++
		return true
	}
	return r.emitItem(entry, data)
}

// Recursively retain usage metadata, summing token counters at every depth.
// Unknown non-counter fields survive too; a later snapshot wins on conflicts.
func mergeAnthropicSearchUsage(total, next json.RawMessage) json.RawMessage {
	merged := map[string]json.RawMessage{}
	_ = json.Unmarshal(total, &merged)
	if merged == nil {
		merged = map[string]json.RawMessage{}
	}
	gjson.ParseBytes(next).ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		previous := gjson.ParseBytes(merged[name])
		switch {
		case value.IsObject():
			merged[name] = mergeAnthropicSearchUsage(merged[name], json.RawMessage(value.Raw))
		case value.Type == gjson.Number && previous.Type == gjson.Number && (name == "tokens" || strings.HasSuffix(name, "_tokens")):
			merged[name], _ = json.Marshal(previous.Int() + value.Int())
		default:
			merged[name] = json.RawMessage(value.Raw)
		}
		return true
	})
	raw, _ := json.Marshal(merged)
	return raw
}

func (b *anthropicCodexWebSearch) run(ctx context.Context, resp *http.Response, base []byte, open func([]byte) (*http.Response, error), search func([]byte) (*CodexAlphaSearchResponse, error), writer io.Writer) error {
	output := &anthropicSearchOutput{bridge: b, writer: writer, id: b.responseID, items: make([]json.RawMessage, 0)}
	var replay []json.RawMessage
	var usage json.RawMessage
	for round := 0; round <= anthropicCodexMaxSearchUses; round++ {
		var terminal, deferredAck []byte
		var interruptErr error
		state := &anthropicSearchRound{output: output, byID: map[string]*anthropicSearchItem{}, byCall: map[string]*anthropicSearchItem{}, byIndex: map[int64]*anthropicSearchItem{}}
		err := readSSEStreamWithContinuousRetryKeepalive(ctx, resp.Body, func(event string, data []byte) bool {
			kind := normalizedUpstreamSSEEventType(event, data)
			data, _ = sjson.SetBytes(data, "type", kind)
			if interruptErr = b.interrupt.observe(data); interruptErr != nil {
				return false
			}
			if len(terminal) > 0 {
				// A discard arriving while the terminal waits for its ack still
				// invalidates that terminal's stale item snapshots.
				if kind == "response.output_item.interrupted" {
					return state.event(data)
				}
				if isResponsesWSInterruptAck(kind) {
					// The public terminal requires output/usage aggregation first.
					// Retain wire order rather than emitting the ack ahead of it.
					deferredAck = append([]byte(nil), data...)
					return false
				}
				return true
			}
			if isResponsesSuccessTerminalEvent(kind) || kind == "response.failed" || kind == "response.cancelled" {
				terminal = append([]byte(nil), data...)
				return b.interrupt.awaitingAck()
			}
			if kind == "response.created" || kind == "response.in_progress" {
				if round > 0 {
					return true
				}
				return output.emit(data)
			}
			return state.event(data)
		})
		_ = resp.Body.Close()
		if output.err != nil {
			return output.err
		}
		if interruptErr != nil {
			return interruptErr
		}
		// A transport may retire a failed interrupt lease before its ack. Keep
		// the authoritative terminal; the outer protocol reports the missing ack.
		if err != nil && len(terminal) == 0 {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if len(terminal) == 0 {
			return io.ErrUnexpectedEOF
		}
		output.rememberResponse(terminal)
		parsed := gjson.ParseBytes(terminal)
		if current := parsed.Get("response.usage"); current.IsObject() {
			raw := json.RawMessage(current.Raw)
			if !current.Get("total_tokens").Exists() && (current.Get("input_tokens").Exists() || current.Get("output_tokens").Exists()) {
				raw, _ = sjson.SetBytes(raw, "total_tokens", current.Get("input_tokens").Int()+current.Get("output_tokens").Int())
			}
			usage = mergeAnthropicSearchUsage(usage, raw)
		}
		var calls []*anthropicSearchItem
		external := false
		for i, item := range parsed.Get("response.output").Array() {
			entry := state.lookup(item.Get("id").String(), item.Get("call_id").String(), item.Get("type").String(), gjson.Parse(fmt.Sprint(i)), true)
			if entry == nil {
				return output.err
			}
			if !state.update(entry, item) {
				return output.err
			}
		}
		// Include streamed items omitted from sparse terminal arrays, after all
		// identity joins have settled. Keep upstream identities in private replay.
		for _, entry := range state.items {
			if entry.interrupted {
				continue // Neither public completion nor private replay/search may revive it.
			}
			if !entry.classified() {
				if parsed.Get("type").String() != "response.completed" {
					state.takePending(entry)
					continue // Never publish unclassified function arguments on failure.
				}
				return ErrUpstreamStreamBreak("Codex search stream has an undeclared or unclassified output item")
			}
			if entry.hidden {
				state.takePending(entry)
				calls = append(calls, entry)
			} else {
				if !state.finish(entry) {
					return output.err
				}
				kind := gjson.GetBytes(entry.item, "type").String()
				external = external || kind == "function_call" || kind == "custom_tool_call"
			}
			replay = append(replay, append(json.RawMessage(nil), entry.item...))
		}
		acceptedInterrupt, missingAck := b.interrupt.acceptedInterrupt(), b.interrupt.awaitingAck()
		if len(calls) > 0 && parsed.Get("type").String() == "response.completed" && (acceptedInterrupt || missingAck) {
			terminal = stopInterruptedSearchTerminal(terminal, acceptedInterrupt)
			parsed = gjson.ParseBytes(terminal)
		}
		finalRound := len(calls) == 0 || external || parsed.Get("type").String() != "response.completed"
		if len(deferredAck) > 0 && !finalRound {
			// A failed interrupt can leave a private model round completed while
			// the aggregate continues. Its hidden terminal must not finish the
			// logical response, but the acknowledgement still belongs to it.
			if !output.emit(deferredAck) {
				return io.ErrClosedPipe
			}
			deferredAck = nil
		}
		if parsed.Get("type").String() == "response.completed" {
			for _, entry := range calls {
				_, result, err := b.executeSearch(ctx, gjson.ParseBytes(entry.item), search, func(data []byte) bool { return state.emitItem(entry, data) })
				if err != nil {
					return err
				}
				replay = append(replay, result)
			}
		}
		if finalRound {
			visible := make([]json.RawMessage, 0, len(output.items))
			for _, item := range output.items {
				if item != nil {
					visible = append(visible, item)
				}
			}
			terminal, _ = sjson.SetBytes(terminal, "response.output", visible)
			if usage != nil {
				terminal, _ = sjson.SetRawBytes(terminal, "response.usage", usage)
			}
			if parsed.Get("type").String() == "response.completed" && b.onCompleted != nil {
				completed := make([]json.RawMessage, len(replay))
				for i, item := range replay {
					completed[i] = append(json.RawMessage(nil), item...)
				}
				b.onCompleted(completed)
			}
			if !output.emit(terminal) {
				return io.ErrClosedPipe
			}
			if len(deferredAck) > 0 && !output.emit(deferredAck) {
				return io.ErrClosedPipe
			}
			return nil
		}
		if b.remaining <= 0 {
			tools := make([]json.RawMessage, 0)
			for _, tool := range gjson.GetBytes(base, "tools").Array() {
				if tool.Get("name").String() != b.name {
					tools = append(tools, json.RawMessage(tool.Raw))
				}
			}
			base, _ = sjson.SetBytes(base, "tools", tools)
		}
		base, _ = sjson.SetBytes(base, "tool_choice", "auto")
		next, err := buildContinuationBody(base, replay)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err = open(next)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			raw, readErr := io.ReadAll(io.LimitReader(resp.Body, codexAlphaSearchBodyLimit))
			_ = resp.Body.Close()
			if readErr != nil {
				return readErr
			}
			return ErrUpstream(resp.StatusCode, usageLogErrorMessage(resp.StatusCode, raw), nil)
		}
	}
	_ = resp.Body.Close()
	return ErrUpstreamStreamBreak("Codex web search exceeded the maximum number of continuation rounds")
}

func (b *anthropicCodexWebSearch) executeSearch(ctx context.Context, call gjson.Result, search func([]byte) (*CodexAlphaSearchResponse, error), emit func([]byte) bool) (json.RawMessage, json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if call.Get("call_id").String() == "" {
		return nil, nil, ErrUpstreamStreamBreak("Codex search function call is missing call_id")
	}
	arguments := call.Get("arguments").String()
	queryValue := gjson.Get(arguments, "query")
	query := strings.TrimSpace(queryValue.String())
	if !gjson.Valid(arguments) || queryValue.Type != gjson.String || len(arguments) > 64<<10 {
		query = ""
	}
	id := call.Get("id").String()
	if id == "" {
		id = call.Get("call_id").String()
	}
	if id == "" {
		return nil, nil, ErrUpstreamStreamBreak("Codex search function call is missing call_id")
	}
	item := map[string]any{"id": id, "type": "web_search_call", "status": "in_progress", "action": map[string]any{"type": "search", "queries": []string{query}}}
	if !emit(mustMarshalSearchEvent("response.output_item.added", item)) {
		return nil, nil, io.ErrClosedPipe
	}
	errorCode := ""
	result := map[string]any{}
	switch {
	case b.remaining <= 0:
		errorCode = "max_uses_exceeded"
	case query == "":
		errorCode = "invalid_tool_input"
	default:
		b.remaining--
		command := map[string]any{"q": query}
		if len(b.domains) > 0 {
			command["domains"] = b.domains
		}
		raw, _ := json.Marshal(map[string]any{"id": "srch_" + uuid.NewString(), "model": b.model, "commands": map[string]any{"search_query": []any{command}, "response_length": "short"}})
		resp, err := search(raw)
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if err != nil {
			errorCode = "unavailable"
		} else if resp.StatusCode != http.StatusOK {
			errorCode = "unavailable"
			if resp.StatusCode == http.StatusTooManyRequests {
				errorCode = "too_many_requests"
			} else if resp.StatusCode == http.StatusBadRequest {
				errorCode = "invalid_tool_input"
			}
		} else if err := json.Unmarshal(resp.Body, &result); err != nil || (gjson.GetBytes(resp.Body, "output").Type != gjson.String && !gjson.GetBytes(resp.Body, "output").IsArray()) || (gjson.GetBytes(resp.Body, "results").Exists() && !gjson.GetBytes(resp.Body, "results").IsArray()) {
			errorCode = "unavailable"
		} else {
			delete(result, "encrypted_output")
			item["results"] = result["results"]
		}
	}
	item["status"] = "completed"
	if errorCode != "" {
		item["status"] = "failed"
		item["error"] = map[string]any{"code": errorCode}
		result = map[string]any{"error": errorCode}
		b.remaining = 0
	}
	if !emit(mustMarshalSearchEvent("response.output_item.done", item)) {
		return nil, nil, io.ErrClosedPipe
	}
	itemJSON, _ := json.Marshal(item)
	resultJSON, _ := json.Marshal(result)
	toolOutput, _ := json.Marshal(map[string]any{"type": "function_call_output", "call_id": call.Get("call_id").String(), "output": string(resultJSON)})
	return itemJSON, toolOutput, nil
}

// Empty added snapshots avoid consumers double-appending final text/arguments.
func anthropicSearchAddedItem(item gjson.Result) json.RawMessage {
	added := []byte(item.Raw)
	switch item.Get("type").String() {
	case "message":
		added, _ = sjson.SetBytes(added, "content", []any{})
	case "reasoning":
		added, _ = sjson.SetBytes(added, "summary", []any{})
	case "function_call":
		added, _ = sjson.SetBytes(added, "arguments", "")
	case "custom_tool_call":
		added, _ = sjson.SetBytes(added, "input", "")
	}
	if item.Get("status").Exists() {
		added, _ = sjson.SetBytes(added, "status", "in_progress")
	}
	return added
}

// Some upstreams only include an output item in the terminal event (or done).
// Emit the same indexed part/text lifecycle as a streamed item.
func emitAnthropicSearchCompletedItem(item gjson.Result, emit func([]byte) bool) bool {
	if !emit(mustMarshalSearchEvent("response.output_item.added", anthropicSearchAddedItem(item))) {
		return false
	}
	partEvent := func(kind, indexKey string, index int, fields map[string]any) bool {
		fields["type"], fields["item_id"], fields[indexKey] = kind, item.Get("id").String(), index
		raw, _ := json.Marshal(fields)
		return emit(raw)
	}
	if item.Get("type").String() == "message" {
		for i, part := range item.Get("content").Array() {
			if !part.IsObject() {
				continue
			}
			field, prefix := "text", "response.output_text."
			if part.Get("type").String() == "refusal" {
				field, prefix = "refusal", "response.refusal."
			}
			empty, _ := sjson.SetBytes([]byte(part.Raw), field, "")
			if !partEvent("response.content_part.added", "content_index", i, map[string]any{"part": json.RawMessage(empty)}) {
				return false
			}
			if part.Get("type").String() == "output_text" || part.Get("type").String() == "refusal" {
				if !partEvent(prefix+"delta", "content_index", i, map[string]any{"delta": part.Get(field).String()}) || !partEvent(prefix+"done", "content_index", i, map[string]any{field: part.Get(field).String()}) {
					return false
				}
			}
			if !partEvent("response.content_part.done", "content_index", i, map[string]any{"part": json.RawMessage(part.Raw)}) {
				return false
			}
		}
	}
	if item.Get("type").String() == "reasoning" {
		for i, part := range item.Get("summary").Array() {
			if !part.IsObject() {
				continue
			}
			empty, _ := sjson.SetBytes([]byte(part.Raw), "text", "")
			if !partEvent("response.reasoning_summary_part.added", "summary_index", i, map[string]any{"part": json.RawMessage(empty)}) ||
				!partEvent("response.reasoning_summary_text.delta", "summary_index", i, map[string]any{"delta": part.Get("text").String()}) ||
				!partEvent("response.reasoning_summary_text.done", "summary_index", i, map[string]any{"text": part.Get("text").String()}) ||
				!partEvent("response.reasoning_summary_part.done", "summary_index", i, map[string]any{"part": json.RawMessage(part.Raw)}) {
				return false
			}
		}
	}
	if kind := item.Get("type").String(); kind == "function_call" || kind == "custom_tool_call" {
		field, prefix := "arguments", "response.function_call_arguments."
		if kind == "custom_tool_call" {
			field, prefix = "input", "response.custom_tool_call_input."
		}
		for _, suffix := range []string{"delta", "done"} {
			key := field
			if suffix == "delta" {
				key = "delta"
			}
			raw, _ := json.Marshal(map[string]any{"type": prefix + suffix, "item_id": item.Get("id").String(), key: item.Get(field).String()})
			if !emit(raw) {
				return false
			}
		}
	}
	return emit(mustMarshalSearchEvent("response.output_item.done", json.RawMessage(item.Raw)))
}

func mustMarshalSearchEvent(kind string, item any) []byte {
	raw, _ := json.Marshal(map[string]any{"type": kind, "item": item})
	return raw
}
