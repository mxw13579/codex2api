package main

import (
	"testing"
	"time"
)

func TestUsageLogRetentionPolicyFromEnv(t *testing.T) {
	t.Setenv("CODEX_USAGE_LOG_RETENTION_HOURS", "")
	t.Setenv("CODEX_USAGE_LOG_MAX_MB", "")
	p, err := usageLogRetentionPolicyFromEnv()
	if err != nil || p.Enabled() {
		t.Fatalf("unset must be disabled: %+v %v", p, err)
	}

	t.Setenv("CODEX_USAGE_LOG_RETENTION_HOURS", "48")
	t.Setenv("CODEX_USAGE_LOG_MAX_MB", "1024")
	p, err = usageLogRetentionPolicyFromEnv()
	if err != nil || p.MaxAge != 48*time.Hour || p.MaxBytes != 1<<30 {
		t.Fatalf("got %+v %v", p, err)
	}

	for _, bad := range []string{"-1", "2d", "1.5"} {
		t.Setenv("CODEX_USAGE_LOG_MAX_MB", bad)
		if _, err := usageLogRetentionPolicyFromEnv(); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}
