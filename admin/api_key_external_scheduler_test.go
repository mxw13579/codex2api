package admin

import (
	"testing"

	"github.com/codex2api/database"
)

// sanitizeAPIKeyLimits whitelists fields; losing external_scheduler here would
// silently put an external scheduler key back on internal scheduling.
func TestSanitizeAPIKeyLimitsKeepsExternalScheduler(t *testing.T) {
	if !sanitizeAPIKeyLimits(database.APIKeyLimits{ExternalScheduler: true}).ExternalScheduler {
		t.Fatal("sanitizeAPIKeyLimits dropped external_scheduler")
	}
	if sanitizeAPIKeyLimits(database.APIKeyLimits{}).ExternalScheduler {
		t.Fatal("external_scheduler must stay off by default")
	}
}
