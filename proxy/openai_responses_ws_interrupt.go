package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

type openAIResponsesWSInterrupt struct {
	mu           sync.Mutex
	responseID   string
	terminalSeen bool
	finished     bool
	timer        *time.Timer
}

func (control *openAIResponsesWSInterrupt) send(ctx context.Context, conn *websocket.Conn, payload []byte) error {
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.finished {
		return ErrResponsesWSInterruptFinished
	}
	control.responseID = gjson.GetBytes(payload, "response_id").String()
	if err := writeOpenAIResponsesWS(ctx, conn, payload); err != nil {
		control.responseID = ""
		_ = conn.Close()
		return err
	}
	control.timer = time.AfterFunc(ResponsesWSInterruptAckTimeout, func() {
		control.mu.Lock()
		defer control.mu.Unlock()
		if control.responseID != "" && !control.finished {
			_ = conn.Close()
		}
	})
	return nil
}

func (control *openAIResponsesWSInterrupt) stop() {
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.timer != nil {
		control.timer.Stop()
	}
}

func (control *openAIResponsesWSInterrupt) closeIfUnfinished(conn *websocket.Conn) {
	control.mu.Lock()
	defer control.mu.Unlock()
	if !control.finished && conn != nil {
		_ = conn.Close()
	}
}

// 保留原始 sequence_number 顺序，同时把租约保留到终态与确认均已消费。
func (control *openAIResponsesWSInterrupt) accept(frame []byte) ([]byte, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	// Classify terminal aliases before deciding whether to await an ack. A
	// done frame may carry failure/incomplete evidence or an unknown status;
	// it must not be unconditionally promoted to a successful completion.
	frame = NormalizeResponsesTerminalEvent(frame)
	eventType := gjson.GetBytes(frame, "type").String()
	if isResponsesWSInterruptAck(eventType) {
		return control.acceptAckLocked(frame)
	}
	if control.terminalSeen {
		return nil, nil
	}
	if isResponsesSuccessTerminalEvent(eventType) && control.responseID != "" {
		control.terminalSeen = true
		return frame, nil
	}
	if isResponsesTerminalEvent(eventType) || eventType == "error" {
		control.finished = true
	}
	return frame, nil
}

func (control *openAIResponsesWSInterrupt) acceptAckLocked(frame []byte) ([]byte, error) {
	if control.responseID == "" || gjson.GetBytes(frame, "response_id").String() != control.responseID {
		return nil, fmt.Errorf("unexpected response interruption acknowledgement")
	}
	control.responseID = ""
	if control.timer != nil {
		control.timer.Stop()
	}
	control.finished = control.terminalSeen
	return frame, nil
}

func (control *openAIResponsesWSInterrupt) done() bool {
	control.mu.Lock()
	defer control.mu.Unlock()
	return control.finished
}

func writeOpenAIResponsesSSE(writer io.Writer, frame []byte) error {
	if bytes.IndexByte(frame, '\n') >= 0 {
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, frame); err != nil {
			return err
		}
		frame = compacted.Bytes()
	}
	_, err := fmt.Fprintf(writer, "data: %s\n\n", frame)
	return err
}
