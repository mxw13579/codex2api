package proxy

import (
	"bytes"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/codex2api/internal/jsondepth"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"

	"github.com/codex2api/database"
)

// Codex 客户端的 "Ultra" 档不是线路上的 reasoning.effort 取值:客户端把它翻成
// effort=xhigh,并在 input 里追加一条 <multi_agent_mode> developer 消息开启主动
// 多代理委派;切回其他档位时再追加一条"no longer applies"的消息关闭(issue #669,
// 抓包实证)。因此 Ultra 只能从请求体里最后一条 multi_agent_mode 消息判定。
//
// 同一回合内的续接帧(带 previous_response_id、input 只有工具输出)不携带历史,
// 看不到这条消息,按 turn_id 继承本回合首帧的判定结果。
const (
	ultraModeMarkerTag  = "<multi_agent_mode>"
	ultraModeActiveText = "Proactive multi-agent delegation is active"

	requestUltraModeContextKey = "codex2api.request.ultra_mode"

	ultraModeTurnTTL        = 6 * time.Hour
	ultraModeTurnMaxEntries = 20000
)

type ultraModeState uint8

const (
	ultraModeUnknown ultraModeState = iota
	ultraModeOff
	ultraModeOn
)

// detectUltraModeFromBody 只看 input 顶层的 developer 消息,以最后一条
// multi_agent_mode 消息为准;没有任何标记返回 unknown。
func detectUltraModeFromBody(body []byte) ultraModeState {
	if len(body) == 0 || !bytes.Contains(body, []byte("multi_agent_mode")) {
		return ultraModeUnknown
	}
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return ultraModeUnknown
	}
	state := ultraModeUnknown
	inspect := func(text string) {
		if !strings.Contains(text, ultraModeMarkerTag) {
			return
		}
		if strings.Contains(text, ultraModeActiveText) {
			state = ultraModeOn
		} else {
			state = ultraModeOff
		}
	}
	input.ForEach(func(_, item gjson.Result) bool {
		if !item.IsObject() || item.Get("role").String() != "developer" {
			return true
		}
		content := item.Get("content")
		switch {
		case content.Type == gjson.String:
			inspect(content.String())
		case content.IsArray():
			content.ForEach(func(_, part gjson.Result) bool {
				if part.IsObject() {
					inspect(part.Get("text").String())
				}
				return true
			})
		}
		return true
	})
	return state
}

// ultraModeTurnKey 取 Codex 回合 ID:WS 帧在 client_metadata.turn_id /
// client_metadata.x-codex-turn-metadata,HTTP 在 X-Codex-Turn-Metadata 头。
func ultraModeTurnKey(headers http.Header, body []byte) string {
	if id := strings.TrimSpace(gjson.GetBytes(body, "client_metadata.turn_id").String()); id != "" {
		return id
	}
	raw := strings.TrimSpace(gjson.GetBytes(body, "client_metadata.x-codex-turn-metadata").String())
	if raw == "" && headers != nil {
		raw = strings.TrimSpace(headers.Get("X-Codex-Turn-Metadata"))
	}
	if raw == "" || !jsondepth.Valid(raw) {
		return ""
	}
	return strings.TrimSpace(gjson.Get(raw, "turn_id").String())
}

type ultraModeTurnEntry struct {
	on   bool
	seen time.Time
}

type ultraModeTurnCache struct {
	mu      sync.Mutex
	entries map[string]ultraModeTurnEntry
}

var ultraModeTurns = &ultraModeTurnCache{entries: make(map[string]ultraModeTurnEntry)}

func (c *ultraModeTurnCache) set(key string, on bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= ultraModeTurnMaxEntries {
		c.pruneLocked(now)
	}
	c.entries[key] = ultraModeTurnEntry{on: on, seen: now}
}

func (c *ultraModeTurnCache) get(key string, now time.Time) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return false, false
	}
	if now.Sub(entry.seen) > ultraModeTurnTTL {
		delete(c.entries, key)
		return false, false
	}
	entry.seen = now
	c.entries[key] = entry
	return entry.on, true
}

// pruneLocked 先清过期项;仍然超限就整体清空——回合缓存丢失只影响续接帧的标记,
// 不影响计费与路由,比在热路径上做 LRU 划算。
func (c *ultraModeTurnCache) pruneLocked(now time.Time) {
	for key, entry := range c.entries {
		if now.Sub(entry.seen) > ultraModeTurnTTL {
			delete(c.entries, key)
		}
	}
	if len(c.entries) >= ultraModeTurnMaxEntries {
		c.entries = make(map[string]ultraModeTurnEntry)
	}
}

// resolveRequestUltraMode 判定本请求是否处于 Codex Ultra 档。
func resolveRequestUltraMode(headers http.Header, body []byte) bool {
	now := time.Now()
	key := ultraModeTurnKey(headers, body)
	switch detectUltraModeFromBody(body) {
	case ultraModeOn:
		if key != "" {
			ultraModeTurns.set(key, true, now)
		}
		return true
	case ultraModeOff:
		if key != "" {
			ultraModeTurns.set(key, false, now)
		}
		return false
	}
	if key == "" {
		return false
	}
	on, _ := ultraModeTurns.get(key, now)
	return on
}

func cacheRequestUltraMode(c *gin.Context, on bool) {
	if c != nil {
		c.Set(requestUltraModeContextKey, on)
	}
}

func cachedRequestUltraMode(c *gin.Context) (bool, bool) {
	if c == nil {
		return false, false
	}
	value, exists := c.Get(requestUltraModeContextKey)
	if !exists {
		return false, false
	}
	on, ok := value.(bool)
	return on, ok
}

// populateUltraUsageMetaFromRequest 把 Ultra 判定写进用量日志。入口已缓存的
// 直接用;没缓存的(如非 /v1/responses 入口)退回原始请求体判定一次。
func populateUltraUsageMetaFromRequest(c *gin.Context, input *database.UsageLogInput) {
	if input == nil || input.Ultra {
		return
	}
	if on, ok := cachedRequestUltraMode(c); ok {
		input.Ultra = on
		return
	}
	body, ok := rawRequestBodyFromContext(c)
	if !ok {
		return
	}
	var headers http.Header
	if c != nil && c.Request != nil {
		headers = c.Request.Header
	}
	input.Ultra = resolveRequestUltraMode(headers, body)
}
