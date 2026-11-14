package proxy

import (
	"reflect"
	"testing"

	"github.com/codex2api/database"
)

func TestSafePreflightRuntimeSettingsPublishedSnapshot(t *testing.T) {
	previous := safePreflightRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previous) })

	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			input := DefaultRuntimeSettings()
			input.CodexSafePreflightRetry = enabled
			input.CodexWSSilentRetries = maxCodexWSSilentRetries + 1
			input.CodexUserAgentConfig = `{"raw_user_agent":"synthetic-preflight/1.0"}`
			input.ContinuousRetryPolicy = database.ContinuousRetryPolicy{
				Enabled: true, StatusCodes: []int{503, 429, 503},
			}
			published := ApplyRuntimeSettings(input)
			snapshot := safePreflightRuntimeSettings()
			if snapshot.CodexSafePreflightRetry != enabled || snapshot.CodexWSSilentRetries != maxCodexWSSilentRetries {
				t.Fatal("snapshot did not use the normalized publication")
			}
			if !reflect.DeepEqual(snapshot, published) {
				t.Fatal("snapshot differs from the published settings")
			}
			if !reflect.DeepEqual(snapshot, CurrentRuntimeSettings()) {
				t.Fatal("snapshot differs from the existing getter for normalized settings")
			}

			// Nonempty UA JSON and retry selectors would allocate if normalized again.
			allocations := testing.AllocsPerRun(100, func() {
				snapshot = safePreflightRuntimeSettings()
			})
			if allocations != 0 {
				t.Fatalf("snapshot allocations = %g, want 0", allocations)
			}

			updated := UpdateRuntimeSettings(func(current RuntimeSettings) RuntimeSettings {
				current.CodexSafePreflightRetry = !enabled
				current.CodexWSSilentRetries = 0
				return current
			})
			if !reflect.DeepEqual(snapshot, published) {
				t.Fatal("publishing new settings changed an existing request snapshot")
			}
			if !reflect.DeepEqual(safePreflightRuntimeSettings(), updated) {
				t.Fatal("snapshot did not observe the latest publication")
			}
		})
	}
}
