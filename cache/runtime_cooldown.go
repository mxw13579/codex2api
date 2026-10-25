package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const CooldownKindTransient = "transient"

// Missing Kind denotes a regular cooldown, never an inferred short throttle.
type RuntimeCooldown struct {
	Model        string    `json:"model,omitempty"`
	Kind         string    `json:"kind,omitempty"`
	Reason       string    `json:"reason"`
	ResetAt      time.Time `json:"reset_at"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	BackoffLevel int       `json:"backoff_level,omitempty"`
	// Missing generation is legacy zero. Decimal strings preserve int64
	// revisions through Redis Lua/cjson (whose numbers are IEEE-754 doubles).
	PolicyGeneration int64 `json:"policy_generation,omitempty,string"`
	ExpiresAtMS      int64 `json:"expires_at_ms,omitempty"`
}

// RuntimeCooldownMerger prevents delayed short freezes from replacing quota or
// auth cooldowns. It is optional to preserve compatibility with custom caches.
type RuntimeCooldownMerger interface {
	MergeRuntimeCooldown(context.Context, string, string, RuntimeCooldown) (RuntimeCooldown, error)
}

func (c RuntimeCooldown) Strength() int {
	if c.Reason == "unauthorized" {
		return 3
	}
	if c.Kind == CooldownKindTransient && c.Reason == "responses_rate_limited" {
		return 1
	}
	return 2
}

// Outranks compares strength first. Only two positively identified transient
// freezes compare generations before deadlines; quota/auth never lose to a
// newer transient policy. Backoff may only be merged within one generation.
func (c RuntimeCooldown) Outranks(other RuntimeCooldown) bool {
	if c.Strength() != other.Strength() {
		return c.Strength() > other.Strength()
	}
	if c.Strength() == 1 && c.PolicyGeneration != other.PolicyGeneration {
		return c.PolicyGeneration > other.PolicyGeneration
	}
	return c.ResetAt.After(other.ResetAt)
}

func (tc *MemoryTokenCache) MergeRuntimeCooldown(ctx context.Context, namespace, key string, incoming RuntimeCooldown) (RuntimeCooldown, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeCooldown{}, err
	}
	mapKey := runtimeMapKey(namespace, key)
	if mapKey == "" || !incoming.ResetAt.After(time.Now()) {
		return incoming, nil
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	winner := incoming
	if entry, ok := tc.runtime[mapKey]; ok && entry.expiresAt.After(time.Now()) {
		var current RuntimeCooldown
		if json.Unmarshal(entry.value, &current) == nil && current.ResetAt.After(time.Now()) {
			if current.Outranks(incoming) {
				winner = current
			}
			if current.Strength() == 1 && incoming.Strength() == 1 && current.PolicyGeneration == incoming.PolicyGeneration {
				winner.BackoffLevel = max(current.BackoffLevel, incoming.BackoffLevel)
			}
		}
	}
	winner.ExpiresAtMS = winner.ResetAt.UnixMilli()
	payload, err := json.Marshal(winner)
	if err != nil {
		return RuntimeCooldown{}, err
	}
	tc.runtime[mapKey] = memoryRuntimeEntry{value: payload, expiresAt: winner.ResetAt}
	return winner, nil
}

var mergeRuntimeCooldownScript = redis.NewScript(`
local incoming = cjson.decode(ARGV[1])
local stamp = redis.call('TIME')
local now = tonumber(stamp[1]) * 1000 + math.floor(tonumber(stamp[2]) / 1000)
local winner = incoming
local function strength(record)
    if record.reason == 'unauthorized' then return 3 end
    if record.kind == 'transient' and record.reason == 'responses_rate_limited' then return 1 end
    return 2
end
local payload = redis.call('GET', KEYS[1])
if payload then
    local ok, current = pcall(cjson.decode, payload)
    local ttl = redis.call('PTTL', KEYS[1])
    if ok and type(current) == 'table' and ttl > 0 then
        local expires = tonumber(current.expires_at_ms) or (now + ttl)
        current.expires_at_ms = expires
        if expires > now then
            local oldStrength, newStrength = strength(current), strength(incoming)
            local oldGeneration = current.policy_generation or '0'
            local newGeneration = incoming.policy_generation or '0'
            if oldStrength ~= newStrength then
                if oldStrength > newStrength then winner = current end
            elseif oldStrength == 1 and oldGeneration ~= newGeneration then
                if #oldGeneration > #newGeneration or (#oldGeneration == #newGeneration and oldGeneration > newGeneration) then winner = current end
            elseif expires > incoming.expires_at_ms then
                winner = current
            end
            if oldStrength == 1 and newStrength == 1 and oldGeneration == newGeneration then
                winner.backoff_level = math.max(tonumber(current.backoff_level) or 0, tonumber(incoming.backoff_level) or 0)
            end
        end
    end
end
local result = cjson.encode(winner)
local ttl = math.floor(winner.expires_at_ms - now)
if ttl > 0 then redis.call('SET', KEYS[1], result, 'PX', ttl) end
return result
`)

func (tc *redisTokenCache) MergeRuntimeCooldown(ctx context.Context, namespace, key string, incoming RuntimeCooldown) (RuntimeCooldown, error) {
	if key == "" || !incoming.ResetAt.After(time.Now()) {
		return incoming, nil
	}
	incoming.ExpiresAtMS = incoming.ResetAt.UnixMilli()
	payload, err := json.Marshal(incoming)
	if err != nil {
		return RuntimeCooldown{}, err
	}
	raw, err := mergeRuntimeCooldownScript.Run(ctx, tc.client, []string{runtimeValueKey(namespace, key)}, payload).Text()
	if err != nil {
		return RuntimeCooldown{}, err
	}
	var winner RuntimeCooldown
	if err := json.Unmarshal([]byte(raw), &winner); err != nil {
		return RuntimeCooldown{}, err
	}
	return winner, nil
}
