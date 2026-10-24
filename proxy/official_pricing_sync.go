package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/codex2api/database"
)

const (
	OfficialOpenAIPricingURL    = "https://developers.openai.com/api/docs/pricing"
	OfficialXAIPricingURL       = "https://docs.x.ai/developers/pricing"
	OfficialXAIModelsURL        = "https://docs.x.ai/developers/models"
	OfficialAnthropicPricingURL = "https://platform.claude.com/docs/en/about-claude/pricing"
	officialPricingTimeout      = 20 * time.Second
	officialPricingBodyLimit    = 4 << 20
)

// OfficialPricingSyncOptions 指定本轮同步的模型和官方来源。
type OfficialPricingSyncOptions struct {
	Models        []string
	IncludeOpenAI bool
	IncludeGrok   bool
	IncludeClaude bool
}

// OfficialPricingSyncResult 包含有效价格数、覆盖更新数、缺价模型及参考来源。
type OfficialPricingSyncResult struct {
	Fetched   int               `json:"fetched"`
	Applied   int               `json:"applied"`
	Skipped   int               `json:"skipped"`
	Missing   []string          `json:"missing,omitempty"`
	Sources   []string          `json:"sources"`
	Notices   []string          `json:"notices,omitempty"`
	Warnings  []string          `json:"warnings,omitempty"`
	SyncedAt  time.Time         `json:"synced_at"`
	Fallbacks map[string]string `json:"fallbacks,omitempty"`
}

type officialPricingSource struct {
	name    string
	url     string
	enabled bool
	parse   func([]byte) (map[string]database.ModelPricingOverride, error)
}

type officialPricingSync struct {
	options OfficialPricingSyncOptions
	allowed map[string]struct{}
	pricing map[string]database.ModelPricingOverride
	result  *OfficialPricingSyncResult
}

// SyncOfficialModelPricing 在事务外抓取价格，再原子更新覆盖；缺价时恢复默认，自定义价优先。
func SyncOfficialModelPricing(ctx context.Context, db *database.DB, proxyURL string, options OfficialPricingSyncOptions) (*OfficialPricingSyncResult, error) {
	if db == nil {
		return nil, fmt.Errorf("数据库不可用，无法同步官方模型价格")
	}
	sync := &officialPricingSync{
		options: options,
		allowed: make(map[string]struct{}),
		pricing: make(map[string]database.ModelPricingOverride),
		result:  &OfficialPricingSyncResult{SyncedAt: time.Now().UTC(), Sources: []string{}},
	}
	for _, model := range options.Models {
		if key := database.PricingManagementModelKey(model); key != "" {
			sync.allowed[key] = struct{}{}
		}
	}
	// 与版本同步共用浏览器传输：HTTPS 使用 uTLS，兼容环境代理并显式释放连接。
	client, closeClient := newVersionSyncClient(OfficialOpenAIPricingURL, proxyURL, officialPricingTimeout)
	defer closeClient()
	sync.fetch(ctx, client)
	sync.fetchReferences(ctx, proxyURL)
	if err := ctx.Err(); err != nil {
		return sync.result, err
	}
	sync.findMissing()
	if len(sync.pricing) == 0 && len(sync.result.Missing) == 0 {
		return sync.result, fmt.Errorf("官方页面未解析到当前模型的价格")
	}
	_, err := db.MutateModelPricingSettings(ctx, nil, sync.apply)
	if err == nil {
		sync.referenceNotices()
	}
	return sync.result, err
}

// fetch 独立处理每个官方来源的失败，使其他来源及默认价回退继续生效。
func (sync *officialPricingSync) fetch(ctx context.Context, client *http.Client) {
	sources := []officialPricingSource{
		{"OpenAI", OfficialOpenAIPricingURL, sync.options.IncludeOpenAI, ParseOpenAIOfficialPricingHTML},
		{"xAI", OfficialXAIPricingURL, sync.options.IncludeGrok && sync.hasGrokModels(), ParseXAIOfficialPricingHTML},
		{"Anthropic", OfficialAnthropicPricingURL, sync.options.IncludeClaude, ParseAnthropicOfficialPricingHTML},
	}
	for _, source := range sources {
		if !source.enabled {
			continue
		}
		body, err := fetchOfficialPricingHTML(ctx, client, source.url)
		if err != nil {
			sync.result.Warnings = append(sync.result.Warnings, fmt.Sprintf("读取 %s 官方价格失败: %v", source.name, err))
			continue
		}
		parsed, err := source.parse(body)
		if err != nil {
			sync.result.Warnings = append(sync.result.Warnings, fmt.Sprintf("解析 %s 官方价格失败: %v", source.name, err))
			continue
		}
		if source.url == OfficialOpenAIPricingURL {
			if err := openAIPriceData(parsed).supplementContext(ctx, client, body); err != nil {
				sync.result.Warnings = append(sync.result.Warnings, fmt.Sprintf("读取 OpenAI 长上下文价格失败: %v", err))
			}
		}
		if source.url == OfficialAnthropicPricingURL {
			parsed = projectClaudeOfficialPricing(sync.allowed, parsed)
		}
		if sync.importPricing(parsed) > 0 {
			sync.result.Sources = append(sync.result.Sources, source.url)
		}
	}
	sync.result.Fetched = len(sync.pricing)
}

// importPricing 仅导入当前模型列表中出现的价格。
func (sync *officialPricingSync) importPricing(parsed map[string]database.ModelPricingOverride) int {
	imported := 0
	projectImageResolutionPricing(sync.allowed, parsed)
	projectDaybreakPricing(sync.allowed, parsed)
	for model, override := range parsed {
		if _, ok := sync.allowed[model]; len(sync.allowed) > 0 && !ok {
			continue
		}
		if _, exists := sync.pricing[model]; exists || !officialPricingModelEnabled(model, sync.options) {
			continue
		}
		if err := validateSyncedPricing(model, override, true); err != nil {
			sync.result.Warnings = append(sync.result.Warnings, fmt.Sprintf("%s 价格校验失败: %v", model, err))
			continue
		}
		sync.pricing[model] = override
		imported++
	}
	return imported
}

// hasGrokModels 避免没有 Grok 模型时请求 xAI 价目。
func (sync *officialPricingSync) hasGrokModels() bool {
	for model := range sync.allowed {
		if strings.HasPrefix(model, "grok-") {
			return true
		}
	}
	return false
}

// findMissing 只对本轮启用来源的模型收集缺价信息。
func (sync *officialPricingSync) findMissing() {
	for model := range sync.allowed {
		if _, ok := sync.pricing[model]; ok || !officialPricingModelEnabled(model, sync.options) {
			continue
		}
		sync.result.Missing = append(sync.result.Missing, model)
	}
	sort.Strings(sync.result.Missing)
}

// officialPricingModelEnabled 判断模型是否属于本轮启用的官方来源。
func officialPricingModelEnabled(model string, options OfficialPricingSyncOptions) bool {
	switch {
	case strings.HasPrefix(model, "grok-"):
		return options.IncludeGrok
	case isClaudeBillingModel(model):
		return options.IncludeClaude
	default:
		return options.IncludeOpenAI
	}
}

// apply 清除缺价模型的旧同步覆盖，让内置规则和别名继承规则重新生效。
func (sync *officialPricingSync) apply(current map[string]database.ModelPricingOverride) error {
	for model, override := range sync.pricing {
		if current[model].Source == database.ModelPricingSourceCustom {
			delete(sync.result.Fallbacks, model)
			sync.result.Skipped++
			continue
		}
		override.Source = database.ModelPricingSourceSynced
		current[model] = override
		sync.result.Applied++
	}
	for _, model := range sync.result.Missing {
		if current[model].Source == database.ModelPricingSourceCustom {
			sync.result.Skipped++
			continue
		}
		delete(current, model)
	}
	return nil
}

// fetchOfficialPricingHTML 只接受 HTML 响应，格式变化时由参考 JSON 接管。
func fetchOfficialPricingHTML(ctx context.Context, client *http.Client, sourceURL string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, officialPricingTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", subscriptionsBrowserUserAgent)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("官方价格页面返回 %d", resp.StatusCode)
	}
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
		return nil, fmt.Errorf("官方价格页面 Content-Type 不是 HTML")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, officialPricingBodyLimit+1))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 || len(body) > officialPricingBodyLimit {
		return nil, fmt.Errorf("官方价格页面为空或超过大小限制")
	}
	return body, nil
}
