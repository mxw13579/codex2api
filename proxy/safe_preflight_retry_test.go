package proxy

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func collectSafePreflightFrames(r io.Reader) ([]safePreflightSSEFrame, int, error) {
	var frames []safePreflightSSEFrame
	unsafeCount := 0
	err := readSafePreflightSSE(r, func() { unsafeCount++ }, func(f safePreflightSSEFrame) bool {
		frames = append(frames, f)
		return true
	})
	return frames, unsafeCount, err
}

func TestSafePreflightRuntimeFlagIsIndependentAndDefaultOff(t *testing.T) {
	s := DefaultRuntimeSettings()
	if s.CodexSafePreflightRetry {
		t.Fatal("experiment must default off")
	}
	s.CodexSafePreflightRetry = true
	s.CodexPreflightSSEPassthrough = false
	n := NormalizeRuntimeSettings(s)
	if !n.CodexSafePreflightRetry || n.CodexPreflightSSEPassthrough {
		t.Fatal("normalization coupled the experiment to the legacy switch")
	}
	s.CodexSafePreflightRetry = false
	s.CodexPreflightSSEPassthrough = true
	n = NormalizeRuntimeSettings(s)
	if n.CodexSafePreflightRetry || !n.CodexPreflightSSEPassthrough {
		t.Fatal("legacy switch unexpectedly enables the experiment")
	}
}

func TestSafePreflightSSEPreservesEnvelopeAndLargeNumbers(t *testing.T) {
	const data = `{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":10,"reset_at":9007199254740993}}}`
	for _, newline := range []string{"\n", "\r\n"} {
		wire := "event: codex.rate_limits" + newline + "data: " + data + newline + newline
		for _, r := range []io.Reader{
			strings.NewReader(wire),
			iotest.OneByteReader(strings.NewReader(wire)),
			iotest.DataErrReader(strings.NewReader(wire)),
		} {
			frames, unsafeCount, err := collectSafePreflightFrames(r)
			if err != nil || unsafeCount != 0 || len(frames) != 1 {
				t.Fatalf("frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
			}
			f := frames[0]
			if f.event != "codex.rate_limits" || string(f.data) != data || string(f.wire) != wire || !f.envelopeChecked {
				t.Fatal("strict observation changed original bytes")
			}
			// Schema acceptance belongs to the schema worker. This verifies
			// exact delegation without assuming a wider schema than it allows.
			if f.isRateLimits() != isSafePreflightRateLimitsFrame(f.event, f.data) {
				t.Fatal("raw envelope adapter disagrees with the schema helper")
			}
		}
	}
}

func TestSafePreflightSSEMultiLineDataAndOwnership(t *testing.T) {
	first := "event: fixture\ndata: {\ndata:  \"n\":9007199254740993}\n\n"
	second := "data: [DONE]\n\n"
	frames, unsafeCount, err := collectSafePreflightFrames(iotest.OneByteReader(strings.NewReader(first + second)))
	if err != nil || unsafeCount != 0 || len(frames) != 2 {
		t.Fatalf("frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
	}
	if string(frames[0].data) != "{\n \"n\":9007199254740993}" || string(frames[0].wire) != first {
		t.Fatal("multi-line data or retained frame changed")
	}
	if string(frames[1].data) != "[DONE]" || string(frames[1].wire) != second {
		t.Fatal("[DONE] must be observed, not converted to a successful EOF")
	}
	if frames[0].isRateLimits() || frames[1].isRateLimits() {
		t.Fatal("unknown or control event treated as rate metadata")
	}
}

func TestSafePreflightSSERejectsUnsafeEnvelopesBeforeCallback(t *testing.T) {
	const data = "data: {\"type\":\"codex.rate_limits\"}\n"
	cases := map[string]string{
		"id before":       "id: synthetic-id\n" + data + "\n",
		"id after":        data + "id: synthetic-id\n\n",
		"retry":           "retry: 1000\n" + data + "\n",
		"unknown field":   "future: control\n" + data + "\n",
		"duplicate event": "event: codex.rate_limits\nevent: codex.rate_limits\n" + data + "\n",
		"bare field":      "data\n\n",
		"event only":      "event: future.control\n\n",
		"bom":             "\xef\xbb\xbf" + data + "\n",
		"invalid utf8":    "data: \xff\n\n",
		"nul":             "event: future\x00control\n" + data + "\n",
		"embedded cr":     "data: a\rb\n\n",
		"extra cr":        data + "\r\r\n",
	}
	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			frames, unsafeCount, err := collectSafePreflightFrames(iotest.OneByteReader(strings.NewReader(wire)))
			if !errors.Is(err, errSafePreflightSSEEnvelope) || unsafeCount != 1 || len(frames) != 0 {
				t.Fatalf("frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
			}
		})
	}
}

func TestSafePreflightSSEEOFIsNotAFrameDelimiter(t *testing.T) {
	for _, wire := range []string{
		"data: {}", "data: {}\n", "data: {}\r\n", ": partial", "event: error\n", "data: x\r",
	} {
		frames, unsafeCount, err := collectSafePreflightFrames(strings.NewReader(wire))
		if !errors.Is(err, io.ErrUnexpectedEOF) || len(frames) != 0 || unsafeCount != 1 {
			t.Fatalf("incomplete envelope: frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
		}
	}
	for _, wire := range []string{"", "\n", ": neutral comment\n\n"} {
		frames, unsafeCount, err := collectSafePreflightFrames(strings.NewReader(wire))
		if err != nil || len(frames) != 0 || unsafeCount != 0 {
			t.Fatalf("neutral envelope: frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
		}
	}
}

func TestSafePreflightSSEBoundsAllEnvelopeBytes(t *testing.T) {
	// Exactly at the limit is a valid envelope (not necessarily safe JSON).
	wire := "data: " + strings.Repeat("x", safePreflightSSEMaxEnvelopeBytes-len("data: \n\n")) + "\n\n"
	frames, unsafeCount, err := collectSafePreflightFrames(strings.NewReader(wire))
	if err != nil || unsafeCount != 0 || len(frames) != 1 {
		t.Fatalf("boundary: frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
	}
	for _, oversized := range []string{
		"data: x" + wire[len("data: "):],
		":" + strings.Repeat("x", safePreflightSSEMaxEnvelopeBytes),
		strings.Repeat("data:\n", safePreflightSSEMaxEnvelopeBytes/6) + "data: x\n\n",
	} {
		frames, unsafeCount, err = collectSafePreflightFrames(strings.NewReader(oversized))
		if !errors.Is(err, errSafePreflightSSETooLarge) || unsafeCount != 1 || len(frames) != 0 {
			t.Fatalf("oversize: frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
		}
	}
}

func TestSafePreflightSSESchemaCannotValidateUnobservedEnvelope(t *testing.T) {
	data := []byte(`{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":10}}}`)
	if (safePreflightSSEFrame{event: "codex.rate_limits", data: data}).isRateLimits() {
		t.Fatal("event/data callback alone must not certify an SSE envelope")
	}
	wire := ": extra metadata\ndata: " + string(data) + "\n\n"
	frames, unsafeCount, err := collectSafePreflightFrames(strings.NewReader(wire))
	if err != nil || unsafeCount != 0 || len(frames) != 1 {
		t.Fatalf("frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
	}
	if frames[0].isRateLimits() {
		t.Fatal("comments bundled with data must not inherit JSON allowlisting")
	}
}

type safePreflightReadFailure struct{}

func (safePreflightReadFailure) Read([]byte) (int, error) {
	return 0, errors.New("synthetic-sensitive-reader-detail")
}

func TestSafePreflightSSEReadFailureAndObservationOrder(t *testing.T) {
	for _, prefix := range []string{"", "data: {}\n\n", "data: partial"} {
		frames, unsafeCount, err := collectSafePreflightFrames(io.MultiReader(strings.NewReader(prefix), safePreflightReadFailure{}))
		if err == nil || strings.Contains(err.Error(), "synthetic-sensitive-reader-detail") {
			t.Fatal("reader detail escaped into a diagnostic")
		}
		if prefix == "data: partial" {
			if unsafeCount != 1 || len(frames) != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal("partial read failure did not close the replay window")
			}
		} else if unsafeCount != 0 || !errors.Is(err, errSafePreflightSSERead) {
			t.Fatal("clean transport failure confused with unsafe framing")
		}
	}
	var order []string
	err := readSafePreflightSSE(strings.NewReader("data: {}\n\nid: blocked\n\n"), func() {
		order = append(order, "unsafe")
	}, func(safePreflightSSEFrame) bool {
		order = append(order, "frame")
		return true
	})
	if !errors.Is(err, errSafePreflightSSEEnvelope) || strings.Join(order, ",") != "frame,unsafe" {
		t.Fatal("unsafe framing was not observed after the preceding complete frame")
	}
}

func TestSafePreflightSSECallbackStopDoesNotObserveNextEnvelope(t *testing.T) {
	count, unsafeCount := 0, 0
	err := readSafePreflightSSE(strings.NewReader("data: {}\n\nid: not-consumed\n\n"), func() {
		unsafeCount++
	}, func(safePreflightSSEFrame) bool {
		count++
		return false
	})
	if err != nil || count != 1 || unsafeCount != 0 {
		t.Fatalf("callback stop: count=%d unsafe=%d err=%v", count, unsafeCount, err)
	}
}

func TestSafePreflightDoesNotChangeLegacySSEParser(t *testing.T) {
	wire := "id: legacy-ignored\nretry: 1\nevent: fixture\ndata: {\"n\":9007199254740993}"
	var got []byte
	var event string
	err := ReadSSEStreamWithEvent(strings.NewReader(wire), func(e string, data []byte) bool {
		event, got = e, bytes.Clone(data)
		return true
	})
	if err != nil || event != "fixture" || string(got) != `{"n":9007199254740993}` {
		t.Fatal("legacy ignored-fields / EOF behavior changed")
	}
}
