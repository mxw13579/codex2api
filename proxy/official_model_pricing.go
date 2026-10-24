package proxy

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/codex2api/database"
)

const xAIPricingLongContextThreshold = 200000

var officialDollarPrice = regexp.MustCompile(`^\$(-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)(?:\s|/|$)`)

var officialPriceUnit = regexp.MustCompile(`(?i)^/\s*(?:mtok|1m\s+tokens|million\s+tokens|image|img|sec|second)(?:\s|$)`)

// isClaudeBillingModel 判断某规范计费键是否为 Claude 模型。
func isClaudeBillingModel(model string) bool {
	return strings.Contains(model, "claude") || strings.Contains(model, "opus") ||
		strings.Contains(model, "sonnet") || strings.Contains(model, "haiku")
}

// ParseXAIOfficialPricingHTML 读取 xAI 网页的命名列，兼容分组的短、长上下文表头。
func ParseXAIOfficialPricingHTML(body []byte) (map[string]database.ModelPricingOverride, error) {
	tables, err := parseOfficialPricingHTML(body)
	if err != nil {
		return nil, err
	}
	out := make(map[string]database.ModelPricingOverride)
	for _, table := range tables {
		if table.tier != "standard" {
			continue
		}
		for _, row := range table.rows {
			applyXAIPricingHTMLRow(out, row)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("未找到格式有效的 xAI HTML 价格表")
	}
	return out, nil
}

// applyXAIPricingHTMLRow 区分 token 与媒体单价，校验字段存在后才取价。
func applyXAIPricingHTMLRow(out map[string]database.ModelPricingOverride, row officialPricingRow) {
	model := normalizeOfficialPricingModel(row.cell("model"))
	if !strings.HasPrefix(model, "grok-") {
		return
	}
	if row.cell("cost") != "" {
		applyXAIMediaPricingHTMLRow(out, row)
		return
	}
	if row.cell("short context input", "input / 1m tokens", "input") == "" {
		return
	}
	price := out[model]
	price.Input = row.price("short context input", "input / 1m tokens", "input")
	price.CachedInput = row.price("short context cached", "cached input / 1m tokens", "cached input")
	price.Output = row.price("short context output", "output / 1m tokens", "output")
	price.InputLong, price.CachedInputLong, price.OutputLong = row.price("long context input"), row.price("long context cached"), row.price("long context output")
	if strings.Contains(row.cell("model"), "≥") || strings.Contains(row.cell("model"), ">=") {
		previous := out[model]
		previous.InputLong, previous.CachedInputLong, previous.OutputLong = price.Input, price.CachedInput, price.Output
		price = previous
	}
	if price.InputLong > 0 {
		price.LongContextThresholdTokens = xAIPricingLongContextThreshold
	}
	out[model] = price
}

// applyXAIMediaPricingHTMLRow 仅接受模型计费单位对应的美元输出成本。
func applyXAIMediaPricingHTMLRow(out map[string]database.ModelPricingOverride, row officialPricingRow) {
	model := database.GrokMediaPricingKey(row.cell("model"))
	unit := database.MediaBillingUnit(model)
	cost := strings.ToLower(row.cell("cost"))
	if unit == database.MediaUnitImage && strings.Contains(cost, "/ image") {
		out[model] = database.ModelPricingOverride{MediaUnitCost: parseOfficialPrice(cost)}
	}
	if unit == database.MediaUnitSecond && strings.Contains(cost, "/ sec") {
		out[model] = database.ModelPricingOverride{MediaUnitCost: parseOfficialPrice(cost)}
	}
}

// normalizeOfficialPricingModel 去除上下文注释并保留模型的独立价格键。
func normalizeOfficialPricingModel(value string) string {
	value = strings.ToLower(strings.Trim(strings.TrimSpace(value), "`"))
	if idx := strings.Index(value, " ("); idx >= 0 {
		value = value[:idx]
	}
	return database.PricingManagementModelKey(value)
}

// parseOfficialPrice 严格读取美元数值，未知格式返回 NaN，禁止静默当成零价。
func parseOfficialPrice(value string) float64 {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" || value == "—" {
		return 0
	}
	match := officialDollarPrice.FindStringSubmatch(value)
	if len(match) < 2 {
		return math.NaN()
	}
	unit := strings.TrimSpace(value[len(match[1])+1:])
	if !validOfficialPriceUnit(unit) {
		return math.NaN()
	}
	price, err := strconv.ParseFloat(strings.ReplaceAll(match[1], ",", ""), 64)
	if err != nil {
		return math.NaN()
	}
	return price
}

// validOfficialPriceUnit 拒绝按千 token、分钟或字符计费的价格混入 token/媒体单价。
func validOfficialPriceUnit(unit string) bool {
	if unit == "" || strings.EqualFold(strings.Join(strings.Fields(unit), ""), "/1m") {
		return true
	}
	return officialPriceUnit.MatchString(unit)
}

// uniqueSortedStrings 返回去空白、去重且按字典序排列的字符串。
func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
