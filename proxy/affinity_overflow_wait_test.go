package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestAffinityOverflowWaitForRequest(t *testing.T) {
	ctx := func(body string, length int64) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
		c.Request.ContentLength = length
		return c
	}
	big, small := int64(64<<10), int64(1<<10)

	t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MS", "")
	if got := affinityOverflowWaitForRequest(ctx("", big)); got != 0 {
		t.Fatalf("unset must disable waiting, got %s", got)
	}

	t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MS", "1500")
	t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MIN_BYTES", "")
	if got := affinityOverflowWaitForRequest(ctx("", big)); got != 1500*time.Millisecond {
		t.Fatalf("large request wait = %s", got)
	}
	if got := affinityOverflowWaitForRequest(ctx("", small)); got != 0 {
		t.Fatalf("small request must borrow immediately, got %s", got)
	}
	if got := affinityOverflowWaitForRequest(ctx("", -1)); got != 1500*time.Millisecond {
		t.Fatalf("unknown length must be treated as large, got %s", got)
	}

	t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MIN_BYTES", "0")
	if got := affinityOverflowWaitForRequest(ctx("", small)); got != 1500*time.Millisecond {
		t.Fatalf("min bytes 0 must wait for every request, got %s", got)
	}

	t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MS", "999999")
	if got := affinityOverflowWaitForRequest(ctx("", big)); got != maxAffinityOverflowWait {
		t.Fatalf("wait must be capped at %s, got %s", maxAffinityOverflowWait, got)
	}

	t.Setenv("CODEX_AFFINITY_OVERFLOW_WAIT_MS", "-5")
	if got := affinityOverflowWaitForRequest(ctx("", big)); got != 0 {
		t.Fatalf("negative wait must be ignored, got %s", got)
	}
}
