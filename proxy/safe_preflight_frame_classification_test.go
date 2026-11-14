package proxy

import (
	"strings"
	"testing"
	"testing/iotest"
)

func TestSafePreflightFrameClassificationAtCompleteEnvelope(t *testing.T) {
	const quota = `{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":10,"window_minutes":300}}}`
	for _, tc := range []struct {
		name, event, data, prefix string
		want                      bool
	}{
		{name: "quota", data: quota, want: true},
		{name: "named quota", event: "codex.rate_limits", data: quota, want: true},
		{name: "credits", data: `{"type":"codex.rate_limits","credits":{"has_credits":true,"unlimited":false,"balance":"10.25"}}`, want: true},
		{name: "multiline", data: "{\ndata: \"type\":\"codex.rate_limits\",\ndata: \"rate_limits\":{\"primary\":{\"used_percent\":10,\"window_minutes\":300}}}", want: true},
		{name: "comment", data: quota, prefix: ": synthetic comment\r\n"},
		{name: "mismatched event", event: "response.created", data: quota},
		{name: "missing window", data: strings.Replace(quota, `,"window_minutes":300`, "", 1)},
		{name: "duplicate key", data: strings.Replace(quota, `"used_percent":10`, `"used_percent":10,"used_percent":20`, 1)},
		{name: "escaped duplicate key", data: strings.Replace(quota, `"used_percent":10`, `"used_percent":10,"\u0075sed_percent":20`, 1)},
		{name: "unknown field", data: strings.Replace(quota, `"used_percent":10`, `"used_percent":10,"future":true`, 1)},
		{name: "conflicting reset aliases", data: strings.Replace(quota, `"used_percent":10`, `"used_percent":10,"reset_at":1,"resets_at":2`, 1)},
		{name: "percent above bound", data: strings.Replace(quota, `"used_percent":10`, `"used_percent":100.00000000000000000001`, 1)},
		{name: "noninteger window", data: strings.Replace(quota, `"window_minutes":300`, `"window_minutes":3e2`, 1)},
		{name: "metered bucket", data: strings.Replace(quota, `"type":"codex.rate_limits"`, `"type":"codex.rate_limits","limit_id":"other"`, 1)},
		{name: "trailing JSON", data: quota + `{}`},
		{name: "ordinary frame", data: `{"type":"response.created","response":{"id":"resp_synthetic"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := tc.prefix
			if tc.event != "" {
				wire += "event: " + tc.event + "\r\n"
			}
			wire += "data: " + strings.ReplaceAll(tc.data, "\n", "\r\n") + "\r\n\r\n"
			frames, unsafeCount, err := collectSafePreflightFrames(iotest.OneByteReader(strings.NewReader(wire)))
			if err != nil || unsafeCount != 0 || len(frames) != 1 {
				t.Fatalf("frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
			}
			f := frames[0]
			if !f.envelopeChecked || f.isRateLimits() != tc.want || string(f.wire) != wire {
				t.Fatalf("classification=%t want=%t; original envelope must survive", f.isRateLimits(), tc.want)
			}
			state := &safePreflightState{}
			state.observe(f, f.event, f.data)
			if tc.want && state.retryUnsafe() {
				t.Fatal("a classified quota frame closed replay during internal observation")
			}
		})
	}
}

func TestSafePreflightFrameClassificationReusedWithoutSchemaAllocations(t *testing.T) {
	const wire = "event: codex.rate_limits\r\ndata: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"primary\":{\"used_percent\":10,\"window_minutes\":300}}}\r\n\r\n"
	frames, unsafeCount, err := collectSafePreflightFrames(strings.NewReader(wire))
	if err != nil || unsafeCount != 0 || len(frames) != 1 || !frames[0].isRateLimits() {
		t.Fatalf("frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
	}
	f := frames[0]
	state := &safePreflightState{}
	var before, after bool
	// The reader, observation and forwarding paths share one immutable result.
	// Re-running the duplicate-aware JSON schema here would allocate each time.
	allocs := testing.AllocsPerRun(20, func() {
		before = f.isRateLimits()
		state.observe(f, f.event, f.data)
		after = f.isRateLimits()
	})
	if allocs != 0 || !before || !after || state.retryUnsafe() || string(f.wire) != wire {
		t.Fatalf("classification reuse: allocations=%v before=%t after=%t", allocs, before, after)
	}
}

func TestSafePreflightFrameClassificationStopsAtLegacyHandoff(t *testing.T) {
	const data = `{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":10,"window_minutes":300}}}`
	wire := "data: " + data + "\n\n"
	committed, unsafeCount := false, 0
	var frames []safePreflightSSEFrame
	err := readSafePreflightSSEWithCommit(strings.NewReader(wire+wire), func() {
		unsafeCount++
	}, func(f safePreflightSSEFrame) bool {
		frames = append(frames, f)
		committed = true
		return true
	}, func() bool { return committed })
	if err != nil || unsafeCount != 0 || len(frames) != 2 {
		t.Fatalf("frames=%d unsafe=%d err=%v", len(frames), unsafeCount, err)
	}
	if !frames[0].isRateLimits() || !frames[0].envelopeChecked || string(frames[0].wire) != wire {
		t.Fatal("precommit envelope was not classified")
	}
	if frames[1].isRateLimits() || frames[1].envelopeChecked || len(frames[1].wire) != 0 || string(frames[1].data) != data {
		t.Fatal("legacy event/data callback must not re-certify an unchecked envelope")
	}
}
