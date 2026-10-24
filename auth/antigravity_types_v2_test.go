package auth

import (
	"encoding/json"
	"testing"
	"time"
)

// TestAntigravityQuotaSnapshot_V2SchemaAndCompatibility 覆盖 V2 快照格式升级与对历史单精度快照的反序列化兼容。
func TestAntigravityQuotaSnapshot_V2SchemaAndCompatibility(t *testing.T) {
	legacyJSON := `{"groups":[{"display_name":"Weekly","buckets":[{"window":"weekly","remaining_fraction":0.0}]}]}`
	var snap AntigravityQuotaSnapshot
	if err := json.Unmarshal([]byte(legacyJSON), &snap); err != nil {
		t.Fatalf("failed to unmarshal legacy JSON: %v", err)
	}
	if snap.SchemaVersion != 0 {
		t.Fatalf("expected legacy schema_version 0, got %d", snap.SchemaVersion)
	}
	if len(snap.Groups) != 1 || len(snap.Groups[0].Buckets) != 1 {
		t.Fatalf("expected 1 group with 1 bucket, got %+v", snap.Groups)
	}
	b := snap.Groups[0].Buckets[0]
	if b.FractionPresent {
		t.Fatal("legacy bucket should not have FractionPresent=true")
	}
	if b.RemainingFraction == nil || *b.RemainingFraction != 0.0 {
		t.Fatalf("expected remaining_fraction 0.0, got %v", b.RemainingFraction)
	}

	val := 0.75
	v2 := AntigravityQuotaSnapshot{
		SchemaVersion: 2,
		SourceMetadata: map[string]SourceMeta{
			"groups": {
				Producer:         "summary_loop",
				PresenceVerified: true,
				AppliedSeq:       10,
				ObservedAt:       time.Now().UTC(),
			},
		},
		Groups: []AntigravityQuotaGroup{
			{
				Buckets: []AntigravityQuotaBucket{
					{
						Window:            "weekly",
						RemainingFraction: &val,
						FractionPresent:   true,
						TTLSeconds:        3600,
					},
				},
			},
		},
	}
	raw, err := json.Marshal(v2)
	if err != nil {
		t.Fatalf("marshal V2 failed: %v", err)
	}
	var decoded AntigravityQuotaSnapshot
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal V2 failed: %v", err)
	}
	if decoded.SchemaVersion != 2 {
		t.Fatalf("expected SchemaVersion 2, got %d", decoded.SchemaVersion)
	}
	if !decoded.Groups[0].Buckets[0].FractionPresent || *decoded.Groups[0].Buckets[0].RemainingFraction != 0.75 {
		t.Fatalf("mismatched V2 bucket: %+v", decoded.Groups[0].Buckets[0])
	}
}
