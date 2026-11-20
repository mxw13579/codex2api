package promptfilter

import (
	"strings"
	"testing"

	"github.com/codex2api/internal/jsondepth"
	"github.com/tidwall/gjson"
)

func newDepthTestBuilder() (*envelopeBuilder, *RequestEnvelope) {
	envelope := &RequestEnvelope{}
	return &envelopeBuilder{
		envelope:         envelope,
		maxSegments:      MaxGuardMaxSegments,
		currentUserBytes: DefaultMaxTextLength,
		auxiliaryBytes:   DefaultMaxTextLength,
		scanChunkBytes:   DefaultGuardScanChunkBytes,
		scanOverlapBytes: DefaultGuardScanOverlapBytes,
		priorityScanner:  builtinDecodedSafetyPriorityScanner(),
	}, envelope
}

func envelopeText(envelope *RequestEnvelope) string {
	var parts []string
	for _, segment := range envelope.Segments {
		parts = append(parts, segment.Text)
	}
	return strings.Join(parts, "|")
}

// 两个自写递归在 JSON 嵌套上限内行为不变，超过上限即停止深入（不再无界递归）。
func TestEnvelopeRecursionStopsAtJSONDepthLimit(t *testing.T) {
	restore := jsondepth.SetMaxDepthForTesting(4)
	defer restore()

	shallow := gjson.Parse(`[[["shallow text"]]]`)   // 文本位于相对深度 3
	deep := gjson.Parse(`[[[[[[["deep text"]]]]]]]`) // 文本位于相对深度 7
	attachShallow := gjson.Parse(`{"source":{"url":"https://a.example/x"}}`)
	attachDeep := gjson.Parse(`{"source":{"source":{"source":{"source":{"source":{"url":"https://b.example/y"}}}}}}`)

	b, env := newDepthTestBuilder()
	b.appendResult(OriginCurrentUser, "user", shallow)
	b.appendResult(OriginCurrentUser, "user", deep)
	b.appendAttachmentReferences(attachShallow, "user")
	b.appendAttachmentReferences(attachDeep, "user")
	got := envelopeText(env)
	if !strings.Contains(got, "shallow text") || !strings.Contains(got, "https://a.example/x") {
		t.Fatalf("content within the depth limit must still be extracted: %q", got)
	}
	if strings.Contains(got, "deep text") || strings.Contains(got, "https://b.example/y") {
		t.Fatalf("content beyond the depth limit must not be reached: %q", got)
	}
}
