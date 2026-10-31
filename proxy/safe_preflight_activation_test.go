package proxy

import "testing"

func TestSafePreflightActivationDefaultsAndEnvironment(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", false}, {"0", false}, {"false", false}, {"no", false}, {"yes", false}, {"typo", false},
		{"true", true}, {"TRUE", true}, {"1", true}, {" true ", true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("CODEX_SAFE_PREFLIGHT_RETRY", tc.value)
			settings := DefaultRuntimeSettings()
			if settings.CodexSafePreflightRetry != tc.want {
				t.Errorf("experimental activation=%t, want %t", settings.CodexSafePreflightRetry, tc.want)
			}
			if settings.CodexPreflightSSEPassthrough {
				t.Error("activation changed legacy compatibility switch")
			}
			settings.CodexSafePreflightRetry = false
			if NormalizeRuntimeSettings(settings).CodexSafePreflightRetry {
				t.Error("normalization overrode an explicit runtime disable")
			}
		})
	}
}

func TestSafePreflightActivationSystemBootstrap(t *testing.T) {
	previous := CurrentRuntimeSettings()
	t.Cleanup(func() { ApplyRuntimeSettings(previous) })
	for _, enabled := range []bool{false, true} {
		value := "false"
		if enabled {
			value = "true"
		}
		t.Setenv("CODEX_SAFE_PREFLIGHT_RETRY", value)
		settings := ApplyRuntimeSettingsFromSystem(nil)
		if settings.CodexSafePreflightRetry != enabled || CurrentRuntimeSettings().CodexSafePreflightRetry != enabled {
			t.Error("normal system bootstrap did not apply the explicit experimental default")
		}
		if settings.CodexPreflightSSEPassthrough {
			t.Error("system bootstrap enabled old passthrough implicitly")
		}
	}
}
