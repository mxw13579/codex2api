package proxy

import (
	"fmt"
	"strings"

	"github.com/codex2api/internal/jsondepth"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"

	"github.com/codex2api/api"
	"github.com/codex2api/proxy/internal/jsonbody"
)

// rejectDuplicateJSONField rejects duplicate top-level array fields before the
// item-count guard. Reject case variants too: gjson is case-sensitive whereas
// encoding/json matches struct fields case-insensitively. Both parsers must
// agree on the single canonical field before it is counted or decoded.
func rejectDuplicateJSONField(c *gin.Context, rawBody []byte, field string) bool {
	if c == nil || len(rawBody) == 0 || !jsondepth.ValidBytes(rawBody) {
		return false
	}
	root := jsonbody.View(rawBody)
	if !root.IsObject() {
		return false
	}
	seen := false
	duplicate := false
	nonCanonical := false
	root.ForEach(func(key, _ gjson.Result) bool {
		name := key.String()
		if !strings.EqualFold(name, field) {
			return true
		}
		if name != field {
			nonCanonical = true
			return false
		}
		if seen {
			duplicate = true
			return false
		}
		seen = true
		return true
	})
	if !duplicate && !nonCanonical {
		return false
	}
	message := fmt.Sprintf("duplicate top-level field %q is not allowed", field)
	if nonCanonical {
		message = fmt.Sprintf("top-level field %q must use its canonical spelling", field)
	}
	api.SendError(c, api.NewAPIError(
		api.ErrCodeInvalidRequest,
		message,
		api.ErrorTypeInvalidRequest,
	))
	return true
}
