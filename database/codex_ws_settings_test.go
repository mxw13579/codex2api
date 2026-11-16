package database

import "testing"

func TestCodexWSChatRetentionNormalization(t *testing.T) {
	for _, tt := range []struct {
		input int
		want  int
	}{
		{-1, 8}, {0, 0}, {8, 8}, {32, 32}, {33, 33},
		{48, 48}, {64, 64}, {96, 96}, {127, 127}, {128, 128},
		{129, 129}, {256, 256}, {511, 511}, {512, 512},
		{513, 512}, {1024, 512}, {int(^uint(0) >> 1), 512},
	} {
		if got := NormalizeCodexWSDownstreamKeepaliveSlots(tt.input); got != tt.want {
			t.Errorf("NormalizeCodexWSDownstreamKeepaliveSlots(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
	if DefaultCodexWSDownstreamKeepaliveSlots != 8 {
		t.Fatal("raising the maximum must not raise the default")
	}
	for _, tt := range []struct{ input, want int }{{-1, 8}, {0, 0}, {32, 32}, {64, 32}, {128, 32}} {
		if got := NormalizeCodexWSStatelessSlots(tt.input); got != tt.want {
			t.Errorf("blank retention/scan limit changed: input=%d got=%d want=%d", tt.input, got, tt.want)
		}
	}
}

func TestCodexWSBusyOverflowSlotsNormalization(t *testing.T) {
	for _, tt := range []struct{ input, want int }{
		{-5, 4}, {-1, 4}, {0, 4}, {1, 1}, {2, 2}, {4, 4}, {15, 15}, {16, 16}, {17, 16}, {int(^uint(0) >> 1), 16},
	} {
		if got := NormalizeCodexWSBusyOverflowSlots(tt.input); got != tt.want {
			t.Errorf("NormalizeCodexWSBusyOverflowSlots(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
	if DefaultCodexWSBusyOverflowSlots != 4 || MaxCodexWSBusyOverflowSlots != 16 {
		t.Fatalf("default/max = %d/%d, want 4/16", DefaultCodexWSBusyOverflowSlots, MaxCodexWSBusyOverflowSlots)
	}
}
