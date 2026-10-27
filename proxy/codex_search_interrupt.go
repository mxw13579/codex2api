package proxy

import (
	"context"
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// A hosted-search response is one public turn but can own several sequential
// upstream responses. Keep transport callbacks in a private registry state so
// neither an old lease nor the public logical ID can escape into the next round.
// All fields below are guarded by upstream.mu; no lock is held while emitting
// downstream events. The public registry may call send while holding its lock.
type codexSearchInterrupt struct {
	upstream  *responsesWSInterruptState
	logicalID string
	accepted  bool
	closed    bool
}

func withCodexSearchInterrupt(ctx context.Context, bridge *anthropicCodexWebSearch) (context.Context, func()) {
	parent := responsesWSInterruptFromContext(ctx)
	if parent == nil {
		return ctx, func() {}
	}
	parent.mu.Lock()
	parent.projectSequence = true
	parent.mu.Unlock()
	control := &codexSearchInterrupt{upstream: &responsesWSInterruptState{}}
	bridge.interrupt = control
	// Only this adapter accepts logical search IDs. Transport registrations in
	// the child context still use the native-only public registration function.
	unregister := registerResponsesWSInterrupt(ctx, control.send)
	return context.WithValue(ctx, responsesWSInterruptContextKey{}, control.upstream), func() {
		control.upstream.mu.Lock()
		control.closed = true
		control.upstream.send = nil
		control.upstream.binding++
		control.upstream.mu.Unlock()
		unregister()
	}
}

func (control *codexSearchInterrupt) setLogicalID(id string) {
	if control == nil {
		return
	}
	control.upstream.mu.Lock()
	control.logicalID = id
	control.upstream.mu.Unlock()
}

func (control *codexSearchInterrupt) beginRound() error {
	if control == nil {
		return nil
	}
	state := control.upstream
	state.mu.Lock()
	defer state.mu.Unlock()
	if control.closed || control.accepted || state.requested && !state.acknowledged {
		return fmt.Errorf("cannot start a search round after an unresolved or accepted interruption")
	}
	state.binding++ // Late cleanup from the old transport cannot unbind its successor.
	state.send = nil
	state.responseID = ""
	state.finished, state.requested, state.acknowledged = false, false, false
	return nil
}

func (control *codexSearchInterrupt) send(payload []byte) error {
	state := control.upstream
	state.mu.Lock()
	defer state.mu.Unlock()
	if control.closed {
		return ErrResponsesWSInterruptFinished
	}
	if state.send == nil || state.finished || state.responseID == "" ||
		control.logicalID == "" || gjson.GetBytes(payload, "response_id").String() != control.logicalID {
		// Standalone HTTP search, HTTP fallback, and the gaps between model
		// rounds have no interruptible WS lease. Do not acknowledge completion
		// of the public turn merely because one private round has completed.
		return errResponsesWSInterruptNotSupported
	}
	if state.requested {
		return nil
	}
	translated, err := sjson.SetBytes(payload, "response_id", state.responseID)
	if err != nil {
		return err
	}
	if err = state.send(translated); err != nil {
		if errors.Is(err, ErrResponsesWSInterruptFinished) {
			state.finished = true
			return errResponsesWSInterruptNotSupported
		}
		return err
	}
	state.requested = true
	return nil
}

// Observe private IDs before the search projection rewrites them, including
// response.created events hidden from clients after the first model round.
func (control *codexSearchInterrupt) observe(payload []byte) error {
	if control == nil {
		return nil
	}
	state := control.upstream
	state.mu.Lock()
	defer state.mu.Unlock()
	eventType := gjson.GetBytes(payload, "type").String()
	if isResponsesWSInterruptAck(eventType) {
		if !state.requested || state.acknowledged || gjson.GetBytes(payload, "response_id").String() != state.responseID {
			return fmt.Errorf("unexpected search response interruption acknowledgement")
		}
		state.acknowledged = true
		control.accepted = eventType == "response.interrupt.accepted"
		return nil
	}
	if id := gjson.GetBytes(payload, "response.id"); !state.finished && id.Type == gjson.String && id.String() != "" && len(id.String()) <= responsesWSLineageMaxIDBytes {
		if state.responseID == "" {
			state.responseID = id.String()
		} else if state.responseID != id.String() {
			return fmt.Errorf("search response identity changed within an upstream round")
		}
	}
	if isResponsesTerminalEvent(eventType) || eventType == "error" || eventType == "response.cancelled" {
		state.finished = true
	}
	return nil
}

func (control *codexSearchInterrupt) awaitingAck() bool {
	if control == nil {
		return false
	}
	control.upstream.mu.Lock()
	defer control.upstream.mu.Unlock()
	return control.upstream.requested && !control.upstream.acknowledged
}

func (control *codexSearchInterrupt) acceptedInterrupt() bool {
	if control == nil {
		return false
	}
	control.upstream.mu.Lock()
	defer control.upstream.mu.Unlock()
	return control.accepted
}

// If a private round completed concurrently with an interrupt, unfinished
// hosted-search calls must not run or make the aggregate appear completed.
// Preserve usage and already completed public output; never fabricate an ack.
func stopInterruptedSearchTerminal(terminal []byte, accepted bool) []byte {
	if accepted {
		terminal, _ = sjson.SetBytes(terminal, "type", "response.incomplete")
		terminal, _ = sjson.SetBytes(terminal, "response.status", "incomplete")
		terminal, _ = sjson.SetBytes(terminal, "response.incomplete_details", map[string]string{"reason": "interrupted"})
	} else {
		terminal, _ = sjson.SetBytes(terminal, "type", "response.failed")
		terminal, _ = sjson.SetBytes(terminal, "response.status", "failed")
		terminal, _ = sjson.SetBytes(terminal, "response.error", map[string]string{
			"type": "server_error", "code": "interrupt_acknowledgement_missing",
			"message": "Upstream search response ended without an interruption acknowledgement",
		})
	}
	return terminal
}
