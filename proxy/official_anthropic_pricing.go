package proxy

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/codex2api/database"
	"golang.org/x/net/html"
)

type anthropicPricingColumns struct {
	model, input, output, read, write5m, write1h int
}

// ParseAnthropicOfficialPricingHTML 兼容单行及分组双行表头，分别读取缓存命中和写入价。
func ParseAnthropicOfficialPricingHTML(body []byte) (map[string]database.ModelPricingOverride, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	out := make(map[string]database.ModelPricingOverride)
	walkPricingHTML(doc, func(node *html.Node) bool {
		if node.Type == html.ElementNode && node.Data == "table" {
			parseAnthropicPricingTable(node, out)
			return false
		}
		return true
	})
	if len(out) == 0 {
		return nil, fmt.Errorf("未找到 Anthropic model pricing 表")
	}
	return out, nil
}

// parseAnthropicPricingTable 从包含完整语义列名的表头之后读取价格，跳过分组表头。
func parseAnthropicPricingTable(table *html.Node, out map[string]database.ModelPricingOverride) {
	var columns *anthropicPricingColumns
	for _, row := range pricingHTMLTableRows(table) {
		if candidate, ok := anthropicPricingHeader(row); ok {
			columns = &candidate
			continue
		}
		if columns == nil {
			continue
		}
		model, override := parseAnthropicPricingRow(row, *columns)
		if model != "" && override.Input > 0 && override.Output > 0 {
			out[model] = override
		}
	}
}

// anthropicPricingHeader 同时识别旧版 Cache Read 和新版 Hits and refreshes 等列名。
func anthropicPricingHeader(row []*html.Node) (anthropicPricingColumns, bool) {
	header := make([]string, len(row))
	for i, cell := range row {
		header[i] = htmlNodeText(cell)
	}
	header = normalizeOfficialPricingHeader(header)
	columns := anthropicPricingColumns{
		model:   findOfficialPricingColumn(header, "model"),
		input:   findOfficialPricingColumn(header, "input"),
		output:  findOfficialPricingColumn(header, "output"),
		read:    -1,
		write5m: findOfficialPricingColumn(header, "5m", "write"),
		write1h: findOfficialPricingColumn(header, "1h", "write"),
	}
	if columns.model < 0 {
		columns.model = findOfficialPricingColumn(header, "name")
	}
	for _, label := range []string{"hit", "refresh", "read"} {
		if columns.read = findOfficialPricingColumn(header, label); columns.read >= 0 {
			break
		}
	}
	return columns, min(columns.model, columns.input, columns.output, columns.read) >= 0
}

// findOfficialPricingColumn 查找同时包含所有关键词的列。
func findOfficialPricingColumn(header []string, needles ...string) int {
	for i, cell := range header {
		matched := true
		for _, needle := range needles {
			matched = matched && strings.Contains(cell, needle)
		}
		if matched {
			return i
		}
	}
	return -1
}

// parseAnthropicPricingRow 校验列数后读取单个模型；合并单元格的续行不会覆盖基础价。
func parseAnthropicPricingRow(row []*html.Node, columns anthropicPricingColumns) (string, database.ModelPricingOverride) {
	if len(row) <= max(columns.model, columns.input, columns.output, columns.read) {
		return "", database.ModelPricingOverride{}
	}
	model := normalizeAnthropicPricingModel(pricingHTMLModelText(row[columns.model]))
	if !isClaudeBillingModel(model) {
		return "", database.ModelPricingOverride{}
	}
	return model, database.ModelPricingOverride{
		Input:        anthropicPricingCellPrice(row, columns.input),
		CachedInput:  anthropicPricingCellPrice(row, columns.read),
		Output:       anthropicPricingCellPrice(row, columns.output),
		CacheWrite5m: anthropicPricingCellPrice(row, columns.write5m),
		CacheWrite1h: anthropicPricingCellPrice(row, columns.write1h),
	}
}

// anthropicPricingCellPrice 安全读取可选的价格列。
func anthropicPricingCellPrice(row []*html.Node, index int) float64 {
	if index < 0 || index >= len(row) {
		return 0
	}
	return parseOfficialPrice(htmlNodeText(row[index]))
}

// normalizeAnthropicPricingModel 将官方模型名称转换为项目的规范计费键。
func normalizeAnthropicPricingModel(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if idx := strings.Index(value, " ("); idx >= 0 {
		value = value[:idx]
	}
	value = strings.NewReplacer(" ", "-", ".", "-").Replace(value)
	return database.CanonicalBillingModelKey(value)
}

// projectClaudeOfficialPricing 将带日期等后缀的模型映射到最长匹配的官方型号。
func projectClaudeOfficialPricing(allowed map[string]struct{}, parsed map[string]database.ModelPricingOverride) map[string]database.ModelPricingOverride {
	out := make(map[string]database.ModelPricingOverride)
	if len(allowed) == 0 {
		for model, override := range parsed {
			out[model] = override
		}
		return out
	}
	for model := range allowed {
		if !isClaudeBillingModel(model) {
			continue
		}
		candidate := strings.ReplaceAll(model, ".", "-")
		if override, ok := parsed[candidate]; ok {
			out[model] = override
			continue
		}
		bestKey := ""
		for officialModel := range parsed {
			if strings.HasPrefix(candidate, officialModel+"-") && len(officialModel) > len(bestKey) {
				bestKey = officialModel
			}
		}
		if bestKey != "" {
			out[model] = parsed[bestKey]
		}
	}
	return out
}
