package proxy

import (
	"testing"

	"github.com/codex2api/database"
)

func TestCodexWSChatRetentionRuntimeAndReload(t *testing.T) {
	previous := CurrentRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previous) })
	defaults := ApplyRuntimeSettingsFromSystem(nil)
	if defaults.CodexWSDownstreamKeepaliveSlots != 8 || defaults.CodexWSStatelessSlots != 8 {
		t.Fatal("default chat and blank retention must remain 8")
	}
	for _, tt := range []struct{ input, want int }{
		{-1, 8}, {0, 0}, {8, 8}, {32, 32}, {33, 33}, {48, 48},
		{64, 64}, {96, 96}, {128, 128}, {129, 129}, {256, 256},
		{512, 512}, {513, 512}, {1024, 512},
	} {
		settings := DefaultRuntimeSettings()
		settings.CodexWSDownstreamKeepaliveSlots = tt.input
		settings.CodexWSStatelessSlots = 128
		normalized := NormalizeRuntimeSettings(settings)
		loaded := ApplyRuntimeSettingsFromSystem(&database.SystemSettings{
			CodexWSDownstreamKeepaliveSlots: tt.input,
			CodexWSStatelessSlots:           128,
		})
		for _, got := range []RuntimeSettings{normalized, loaded, CurrentRuntimeSettings()} {
			if got.CodexWSDownstreamKeepaliveSlots != tt.want || got.CodexWSStatelessSlots != 32 {
				t.Fatalf("input=%d: chat/blank=%d/%d, want %d/32", tt.input,
					got.CodexWSDownstreamKeepaliveSlots, got.CodexWSStatelessSlots, tt.want)
			}
		}
	}
}

func TestCodexWSBusyOverflowSlotsRuntimeAndReload(t *testing.T) {
	previous := CurrentRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previous) })
	if got := DefaultRuntimeSettings().CodexWSBusyOverflowSlots; got != 4 {
		t.Fatalf("default overflow slots = %d, want 4", got)
	}
	if got := ApplyRuntimeSettingsFromSystem(nil).CodexWSBusyOverflowSlots; got != 4 {
		t.Fatalf("overflow slots without system settings = %d, want 4", got)
	}
	for _, tt := range []struct{ input, want int }{{-1, 4}, {0, 4}, {1, 1}, {6, 6}, {16, 16}, {17, 16}, {100, 16}} {
		settings := DefaultRuntimeSettings()
		settings.CodexWSBusyOverflowSlots = tt.input
		normalized := NormalizeRuntimeSettings(settings)
		loaded := ApplyRuntimeSettingsFromSystem(&database.SystemSettings{CodexWSBusyOverflowSlots: tt.input})
		for _, got := range []RuntimeSettings{normalized, loaded, CurrentRuntimeSettings()} {
			if got.CodexWSBusyOverflowSlots != tt.want {
				t.Fatalf("input=%d: overflow slots=%d, want %d", tt.input, got.CodexWSBusyOverflowSlots, tt.want)
			}
		}
	}
}
