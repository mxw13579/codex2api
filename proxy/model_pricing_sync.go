package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/codex2api/database"
)

// DefaultModelPricingSyncURL 是项目维护的定价 JSON（raw GitHub）。部署方可在设置页改成
// 自己的镜像/私有列表。格式：{ "<model>": {input,cached_input,output,...}, ... }，
// 见 database.ModelPricingOverride。
const DefaultModelPricingSyncURL = "https://raw.githubusercontent.com/james-6-23/codex2api/main/pricing.json"

// ModelsDevPricingSyncURL 是 models.dev 公开定价 API，格式为 provider→models→cost
// （USD / 1M tokens，含 272K 长上下文分档），同步时自动识别并转换。
const ModelsDevPricingSyncURL = "https://models.dev/api.json"

// modelPricingSyncURLForTest 允许测试替换默认 URL。生产代码不要赋值。
var modelPricingSyncURLForTest = ""

// ModelPricingSyncResult 是一次定价同步的结果投影。
type ModelPricingSyncResult struct {
	SourceURL string `json:"source_url"`
	Fetched   int    `json:"fetched"` // 从 URL 解析到的模型数
	Applied   int    `json:"applied"` // 实际写入的 synced 条目数
	Skipped   int    `json:"skipped"` // 因已有 custom 覆盖而跳过的模型数
}

// SyncModelPricingFromURL 从 JSON URL 拉取定价并写入 synced 覆盖。
// custom 覆盖优先级更高，同步不覆盖已有 custom 条目（只跳过、不清除）。
// 成功后持久化到系统设置并刷新运行时覆盖表。
func SyncModelPricingFromURL(ctx context.Context, db *database.DB, syncURL, proxyURL string) (*ModelPricingSyncResult, error) {
	if db == nil {
		return nil, fmt.Errorf("数据库不可用，无法同步模型定价")
	}
	// 字段即准：传入的 syncURL 就是设置页里那个 URL 字段的当前值。
	// 非空 → 用它并存为来源；空 → 用内置默认并清空存储的来源（回退默认）。
	fieldURL := strings.TrimSpace(syncURL)
	fetchURL := fieldURL
	if modelPricingSyncURLForTest != "" {
		fetchURL = modelPricingSyncURLForTest
	} else if fetchURL == "" {
		fetchURL = DefaultModelPricingSyncURL
	}
	result := &ModelPricingSyncResult{SourceURL: fetchURL}

	fetched, err := fetchModelPricingJSON(ctx, fetchURL, proxyURL)
	if err != nil {
		return result, err
	}
	result.Fetched = len(fetched)
	if len(fetched) == 0 {
		return result, fmt.Errorf("定价源未解析到任何模型")
	}

	_, err = db.MutateModelPricingSettings(ctx, &fieldURL, func(current map[string]database.ModelPricingOverride) error {
		for model, ov := range fetched {
			key := strings.ToLower(strings.TrimSpace(model))
			if key == "" || ov.IsEmpty() {
				continue
			}
			// 已有 custom 覆盖优先，跳过不动。
			if existing, ok := current[key]; ok && existing.Source == database.ModelPricingSourceCustom {
				result.Skipped++
				continue
			}
			ov.Source = database.ModelPricingSourceSynced
			current[key] = ov
			result.Applied++
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

func fetchModelPricingJSON(ctx context.Context, syncURL, proxyURL string) (map[string]database.ModelPricingOverride, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, syncURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build pricing request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", subscriptionsBrowserUserAgent)

	// 目标是 GitHub 域（默认定价 JSON 在 raw.githubusercontent.com）时套用专用代理；
	// URL 可被部署方改成任意地址，故这里绝不附加 github_token。
	client, closeClient := newVersionSyncClient(syncURL, GithubProxyOrDefault(syncURL, proxyURL), officialPricingTimeout)
	defer closeClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pricing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("pricing upstream status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, referencePricingBodyLimit+1))
	if err != nil {
		return nil, fmt.Errorf("read pricing response: %w", err)
	}
	if len(body) > referencePricingBodyLimit {
		return nil, fmt.Errorf("定价 JSON 超过大小限制")
	}
	return parseModelPricingPayload(body)
}
