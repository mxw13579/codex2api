package auth

import (
	"encoding/json"
	"strings"

	"github.com/codex2api/database"
)

// AntigravityPlanAPIKey is the plan key of an Antigravity API-key account, the
// same value Grok API-key accounts expose to plan_allow.
const AntigravityPlanAPIKey = "api"

// CanonicalAntigravityPlan maps a Google tier id or display name to the stable
// value accepted by API-key plan_allow. Tier ids are the authoritative input:
// "g1-pro-tier" → "pro", "g1-ultra-tier" → "ultra", "free-tier" → "free",
// "standard-tier" → "standard". Display names are only a fallback for rows
// persisted without tier ids; they shed the brand prefix and the
// "(Restricted)" marker ("Google AI Pro (Restricted)" → "pro"). Unknown values
// are normalized rather than guessed, so a new upstream tier fails closed
// against existing whitelists instead of impersonating a known plan.
func CanonicalAntigravityPlan(value string) string {
	plan := strings.ToLower(strings.TrimSpace(value))
	plan = strings.TrimSpace(strings.TrimSuffix(plan, "(restricted)"))
	if plan == "" {
		return ""
	}
	if trimmed, ok := strings.CutSuffix(plan, "-tier"); ok {
		// Google One AI plans carry a "gN-" product prefix (g1-pro-tier).
		if prefix, rest, found := strings.Cut(trimmed, "-"); found && isGoogleOneTierPrefix(prefix) {
			return rest
		}
		return trimmed
	}
	for _, brand := range []string{"google ai ", "gemini code assist "} {
		if rest, ok := strings.CutPrefix(plan, brand); ok {
			return strings.TrimSpace(rest)
		}
	}
	return plan
}

func isGoogleOneTierPrefix(prefix string) bool {
	if len(prefix) < 2 || prefix[0] != 'g' {
		return false
	}
	for _, r := range prefix[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// effectiveAntigravityTier picks the tier EffectiveTier describes: the paid
// tier first, the current tier unless Google restricted the account, then the
// default allowed tier. fromAllowed reports the last fallback, the only one
// whose label carries the "(Restricted)" marker.
func effectiveAntigravityTier(e *AntigravityEntitlements) (tier *AntigravityTier, fromAllowed bool) {
	if e.PaidTier != nil && firstNonEmpty(e.PaidTier.Name, e.PaidTier.ID) != "" {
		return e.PaidTier, false
	}
	if !e.Restricted && e.CurrentTier != nil && firstNonEmpty(e.CurrentTier.Name, e.CurrentTier.ID) != "" {
		return e.CurrentTier, false
	}
	if len(e.AllowedTiers) == 0 {
		return nil, false
	}
	selected := e.AllowedTiers[0]
	for _, candidate := range e.AllowedTiers {
		if candidate.IsDefault {
			selected = candidate
			break
		}
	}
	if firstNonEmpty(selected.Name, selected.ID) == "" {
		return nil, false
	}
	return &selected, true
}

// PlanKey returns the plan_allow key of the tier EffectiveTier names. It reads
// the tier id, so it stays stable whether or not Google sends a display name;
// EffectiveTier (persisted as plan_type) keeps the display name for the UI.
func (e AntigravityEntitlements) PlanKey() string {
	tier, _ := effectiveAntigravityTier(&e)
	if tier == nil {
		return CanonicalAntigravityPlan(e.EffectiveTier)
	}
	return CanonicalAntigravityPlan(firstNonEmpty(tier.ID, tier.Name))
}

// antigravityPlanKeyFromRow derives the runtime plan_allow key of an
// Antigravity row from its persisted entitlement snapshot. Rows synced before
// the snapshot existed fall back to plan_type.
func antigravityPlanKeyFromRow(row *database.AccountRow) string {
	if row == nil {
		return ""
	}
	if strings.TrimSpace(row.GetCredential("api_key")) != "" {
		return AntigravityPlanAPIKey
	}
	raw := strings.TrimSpace(row.GetCredential("antigravity_permissions"))
	if raw == "" {
		raw = strings.TrimSpace(row.GetCredential("antigravity_entitlements"))
	}
	if raw != "" {
		var entitlements AntigravityEntitlements
		if json.Unmarshal([]byte(raw), &entitlements) == nil {
			if key := entitlements.PlanKey(); key != "" {
				return key
			}
		}
	}
	return CanonicalAntigravityPlan(row.GetCredential("plan_type"))
}
