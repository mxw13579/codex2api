package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/codex2api/api"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ErrResponsesWSInterruptFinished 表示目标轮次已经终止，禁止把控制帧送入后续租约。
var ErrResponsesWSInterruptFinished = errors.New("responses websocket interrupt target has finished")

// ResponsesWSInterruptAckTimeout 限制控制帧确认等待时间，避免长期占用上游租约。
const ResponsesWSInterruptAckTimeout = 30 * time.Second

type responsesWSInterruptContextKey struct{}

type responsesWSInterruptState struct {
	mu           sync.Mutex
	streamID     string
	responseID   string
	sequence     int64
	finished     bool
	requested    bool
	acknowledged bool
	binding      uint64
	send         func([]byte) error
}

type responsesWSInterruptRegistry struct {
	active map[*responsesWSInterruptState]struct{}
	recent []*responsesWSInterruptState
}

func newResponsesWSInterruptRegistry() *responsesWSInterruptRegistry {
	return &responsesWSInterruptRegistry{active: make(map[*responsesWSInterruptState]struct{})}
}

func (r *responsesWSInterruptRegistry) begin(ctx context.Context, streamID string) (context.Context, *responsesWSInterruptState) {
	state := &responsesWSInterruptState{streamID: streamID}
	r.active[state] = struct{}{}
	return context.WithValue(ctx, responsesWSInterruptContextKey{}, state), state
}

func (r *responsesWSInterruptRegistry) finish(state *responsesWSInterruptState) {
	delete(r.active, state)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.finished, state.send = true, nil
	if state.responseID == "" {
		return
	}
	r.recent = append(r.recent, state)
	if len(r.recent) > responsesWSLineageMaxEntries {
		r.recent[0] = nil
		r.recent = r.recent[1:]
	}
}

func responsesWSInterruptFromContext(ctx context.Context) *responsesWSInterruptState {
	state, _ := ctx.Value(responsesWSInterruptContextKey{}).(*responsesWSInterruptState)
	return state
}

func isResponsesWSInterruptAck(eventType string) bool {
	return eventType == "response.interrupt.accepted" || eventType == "response.interrupt.failed"
}

func observeResponsesWSInterruptAck(ctx context.Context, payload []byte) {
	state := responsesWSInterruptFromContext(ctx)
	if state == nil || !isResponsesWSInterruptAck(gjson.GetBytes(payload, "type").String()) {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.responseID == gjson.GetBytes(payload, "response_id").String() {
		state.acknowledged = true
	}
}

func awaitingResponsesWSInterruptAck(ctx context.Context) bool {
	state := responsesWSInterruptFromContext(ctx)
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.requested && !state.acknowledged
}

func failPendingResponsesWSInterrupt(ctx context.Context, conn *websocket.Conn) error {
	state := responsesWSInterruptFromContext(ctx)
	if state == nil || ctx.Err() != nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.requested || state.acknowledged {
		return nil
	}
	state.acknowledged = true
	return state.writeAck(conn, "interrupt_acknowledgement_missing")
}

func responsesWSClientEventType(raw []byte) (string, *api.APIError) {
	root := gjson.ParseBytes(raw)
	if !gjson.ValidBytes(raw) || !root.IsObject() {
		return "", api.NewAPIError(api.ErrCodeInvalidRequest, "websocket request payload must be a JSON object", api.ErrorTypeInvalidRequest)
	}
	if err := validateResponsesWSEnvelopeFields(root); err != nil {
		return "", err
	}
	event := root.Get("type")
	if event.Exists() && event.Type != gjson.String {
		return "", api.NewAPIError(api.ErrCodeInvalidRequest, "websocket request type must be a string", api.ErrorTypeInvalidRequest)
	}
	return strings.TrimSpace(event.String()), nil
}

func validateResponsesWSEnvelopeFields(root gjson.Result) *api.APIError {
	seen := make(map[string]bool)
	var apiErr *api.APIError
	root.ForEach(func(key, _ gjson.Result) bool {
		name := key.String()
		canonical := strings.ToLower(name)
		switch canonical {
		case "type", "stream_id", "response_id", "mode", "input", "previous_response_id":
			if seen[canonical] || name != canonical {
				apiErr = api.NewAPIError(api.ErrCodeInvalidRequest, fmt.Sprintf("websocket field %q must appear once with its canonical spelling", canonical), api.ErrorTypeInvalidRequest)
				return false
			}
			seen[canonical] = true
		}
		return true
	})
	return apiErr
}

// RegisterResponsesWSInterrupt 仅绑定当前上游租约，不允许中断触发重连或请求重放。
func RegisterResponsesWSInterrupt(ctx context.Context, send func([]byte) error) func() {
	state := responsesWSInterruptFromContext(ctx)
	if state == nil {
		return func() {}
	}
	state.mu.Lock()
	state.binding++
	binding := state.binding
	state.send = send
	state.mu.Unlock()
	return func() {
		state.mu.Lock()
		if state.binding == binding {
			state.send = nil
		}
		state.mu.Unlock()
	}
}

func (r *responsesWSInterruptRegistry) handle(conn *websocket.Conn, raw []byte, streamID string) error {
	responseID, apiErr := validateResponsesWSInterrupt(raw)
	if apiErr != nil {
		return writeResponsesWSErrorForStream(conn, apiErr, streamID, responsesWSWriteTimeout)
	}
	for state := range r.active {
		if state.matches(responseID, streamID) {
			return state.interrupt(conn, raw)
		}
	}
	for i := len(r.recent) - 1; i >= 0; i-- {
		if r.recent[i].matches(responseID, streamID) {
			return r.recent[i].acknowledgeFinished(conn)
		}
	}
	return writeResponsesWSInterruptAck(conn, responsesWSInterruptAck{streamID: streamID, responseID: responseID, code: "response_not_found"})
}

func validateResponsesWSInterrupt(raw []byte) (string, *api.APIError) {
	root := gjson.ParseBytes(raw)
	id, mode := root.Get("response_id"), root.Get("mode")
	if !gjson.ValidBytes(raw) || !root.IsObject() || id.Type != gjson.String || strings.TrimSpace(id.String()) == "" || len(id.String()) > responsesWSLineageMaxIDBytes {
		return "", api.NewAPIError(api.ErrCodeInvalidRequest, "response.interrupt requires a nonempty string response_id", api.ErrorTypeInvalidRequest)
	}
	if mode.Type != gjson.String || mode.String() != "discard_partial_items" {
		return "", api.NewAPIError(api.ErrCodeInvalidRequest, "response.interrupt requires mode=discard_partial_items", api.ErrorTypeInvalidRequest)
	}
	return id.String(), nil
}

func (state *responsesWSInterruptState) matches(responseID, streamID string) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.responseID == responseID && (streamID == "" || state.streamID == streamID)
}

func (state *responsesWSInterruptState) interrupt(conn *websocket.Conn, raw []byte) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.requested {
		return nil
	}
	if state.finished {
		return state.acknowledgeFinishedLocked(conn)
	}
	if state.send == nil {
		return state.writeAck(conn, "interrupt_not_supported")
	}
	// stream_id 由网关路由，不能泄漏到上游单轮租约。
	payload, err := sjson.DeleteBytes(raw, "stream_id")
	if err == nil {
		payload, err = sjson.SetBytes(payload, "type", "response.interrupt")
	}
	if err == nil {
		err = state.send(payload)
	}
	if errors.Is(err, ErrResponsesWSInterruptFinished) {
		return state.acknowledgeFinishedLocked(conn)
	}
	if err != nil {
		return state.writeAck(conn, "interrupt_failed")
	}
	state.requested = true
	return nil
}

func (state *responsesWSInterruptState) acknowledgeFinished(conn *websocket.Conn) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.acknowledgeFinishedLocked(conn)
}

func (state *responsesWSInterruptState) acknowledgeFinishedLocked(conn *websocket.Conn) error {
	if state.requested {
		return nil
	}
	state.requested = true
	return state.writeAck(conn, "")
}

func (state *responsesWSInterruptState) writeAck(conn *websocket.Conn, code string) error {
	return writeResponsesWSInterruptAck(conn, responsesWSInterruptAck{streamID: state.streamID, responseID: state.responseID, code: code, sequence: state.nextSequence()})
}

func (state *responsesWSInterruptState) nextSequence() int64 {
	sequence := state.sequence
	state.sequence++
	return sequence
}

type responsesWSInterruptAck struct {
	streamID, responseID, code string
	sequence                   int64
}

func writeResponsesWSInterruptAck(conn *websocket.Conn, ack responsesWSInterruptAck) error {
	event := map[string]any{"type": "response.interrupt.accepted", "response_id": ack.responseID, "sequence_number": ack.sequence}
	if ack.code != "" {
		event["type"] = "response.interrupt.failed"
		event["error"] = map[string]string{"code": ack.code, "message": ack.code}
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return writeResponsesWSMessage(conn, withResponsesWSStreamID(payload, ack.streamID))
}

func writeResponsesWSObservedMessage(ctx context.Context, conn *websocket.Conn, payload []byte) error {
	state := responsesWSInterruptFromContext(ctx)
	if state == nil {
		return writeResponsesWSMessage(conn, payload)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := writeResponsesWSMessage(conn, payload); err != nil {
		return err
	}
	root := gjson.ParseBytes(payload)
	if sequence := root.Get("sequence_number"); sequence.Exists() && sequence.Int() >= state.sequence {
		state.sequence = sequence.Int() + 1
	}
	if id := root.Get("response.id"); id.Type == gjson.String && len(id.String()) <= responsesWSLineageMaxIDBytes {
		state.responseID = id.String()
	}
	if isResponsesTerminalEvent(root.Get("type").String()) {
		state.finished = true
	}
	return nil
}
