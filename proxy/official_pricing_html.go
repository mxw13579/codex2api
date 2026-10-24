package proxy

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

const maxPricingTableSpan = 32

type officialPricingRow map[string]string

type officialHTMLPricingTable struct {
	rows []officialPricingRow
	tier string
}

type pricingSpan struct {
	node *html.Node
	left int
}

// parseOfficialPricingHTML 读取网页中的表格并验证合并单元格结构，不接受 Markdown。
func parseOfficialPricingHTML(body []byte) ([]officialHTMLPricingTable, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	var tables []officialHTMLPricingTable
	var parseErr error
	walkPricingHTML(doc, func(node *html.Node) bool {
		if node.Type != html.ElementNode || node.Data != "table" {
			return true
		}
		grid, err := pricingHTMLGrid(node)
		if err != nil {
			parseErr = err
			return false
		}
		tables = append(tables, officialHTMLPricingTable{rows: pricingHTMLRows(grid), tier: pricingHTMLTier(node)})
		return false
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("官方页面格式不符：未找到 HTML 价格表")
	}
	return tables, nil
}

// pricingHTMLGrid 展开 rowspan/colspan，拒绝重叠、超限或缺列的数据。
func pricingHTMLGrid(table *html.Node) ([][]*html.Node, error) {
	var grid [][]*html.Node
	var spans [maxPricingTableSpan]pricingSpan
	for _, cells := range pricingHTMLTableRows(table) {
		row := make([]*html.Node, maxPricingTableSpan)
		for i := range spans {
			if spans[i].left > 0 {
				row[i] = spans[i].node
				spans[i].left--
			}
		}
		if err := placePricingHTMLCells(row, spans[:], cells); err != nil {
			return nil, err
		}
		for len(row) > 0 && row[len(row)-1] == nil {
			row = row[:len(row)-1]
		}
		grid = append(grid, row)
	}
	return grid, nil
}

// placePricingHTMLCells 在已有跨行单元格之外放置本行数据。
func placePricingHTMLCells(row []*html.Node, spans []pricingSpan, cells []*html.Node) error {
	column := 0
	for _, cell := range cells {
		for column < len(row) && row[column] != nil {
			column++
		}
		width, height := pricingHTMLSpan(cell, "colspan"), pricingHTMLSpan(cell, "rowspan")
		if width <= 0 || height <= 0 || column+width > len(row) {
			return fmt.Errorf("官方价格表合并单元格格式不符")
		}
		for i := column; i < column+width; i++ {
			if row[i] != nil {
				return fmt.Errorf("官方价格表单元格重叠")
			}
			row[i] = cell
			spans[i] = pricingSpan{node: cell, left: height - 1}
		}
		column += width
	}
	return nil
}

// pricingHTMLSpan 只接受有界的正整数跨度。
func pricingHTMLSpan(node *html.Node, key string) int {
	value := pricingHTMLAttribute(node, key)
	if value == "" {
		return 1
	}
	span, err := strconv.Atoi(value)
	if err != nil || span > maxPricingTableSpan {
		return 0
	}
	return span
}

// pricingHTMLRows 将多行表头合成为列名，保留每列的语义。
func pricingHTMLRows(grid [][]*html.Node) []officialPricingRow {
	var header []string
	var rows []officialPricingRow
	for _, cells := range grid {
		if pricingHTMLHeaderRow(cells) {
			header = mergePricingHTMLHeader(header, cells)
			continue
		}
		if len(header) == 0 || len(cells) != len(header) {
			continue
		}
		row := make(officialPricingRow, len(header))
		for i, cell := range cells {
			text := htmlNodeText(cell)
			if header[i] == "model" || header[i] == "name" {
				text = pricingHTMLModelText(cell)
			}
			row[header[i]] = strings.TrimSpace(text)
		}
		rows = append(rows, row)
	}
	return rows
}

// pricingHTMLHeaderRow 判断该行是否完全由表头单元格组成。
func pricingHTMLHeaderRow(cells []*html.Node) bool {
	for _, cell := range cells {
		if cell == nil || cell.Data != "th" {
			return false
		}
	}
	return len(cells) > 0
}

// mergePricingHTMLHeader 合并分组名称，去掉不影响价格语义的分组前缀。
func mergePricingHTMLHeader(header []string, cells []*html.Node) []string {
	if len(header) < len(cells) {
		header = append(header, make([]string, len(cells)-len(header))...)
	}
	for i, cell := range cells {
		label := strings.ToLower(strings.Join(strings.Fields(htmlNodeText(cell)), " "))
		if header[i] != "" && label != "" && label != header[i] {
			label = header[i] + " " + label
		}
		label = strings.TrimPrefix(label, "base tokens ")
		label = strings.TrimPrefix(label, "prompt caching ")
		if label == "model name" {
			label = "model"
		}
		header[i] = label
	}
	return header
}

// pricingHTMLTier 读取最近的档位标记，避免 Batch/Ultrafast 价格覆盖标准价。
func pricingHTMLTier(node *html.Node) string {
	for current := node; current != nil; current = current.Parent {
		for _, key := range []string{"data-pricing-tier", "data-value"} {
			value := pricingHTMLAttribute(current, key)
			switch value {
			case "standard", "fast", "priority", "batch", "flex", "ultrafast":
				return value
			}
		}
	}
	return "standard"
}

// pricingHTMLAttribute 返回 HTML 属性值。
func pricingHTMLAttribute(node *html.Node, key string) string {
	for _, attribute := range node.Attr {
		if attribute.Key == key {
			return attribute.Val
		}
	}
	return ""
}

// cell 只按明确列名取值，不通过列位置推测价格。
func (row officialPricingRow) cell(columns ...string) string {
	for _, column := range columns {
		if value, ok := row[column]; ok {
			return value
		}
	}
	return ""
}

// price 读取命名列中的美元单价；非法内容返回 NaN，由价格校验拒绝。
func (row officialPricingRow) price(columns ...string) float64 {
	return parseOfficialPrice(row.cell(columns...))
}

// normalizeOfficialPricingHeader 统一旧式单行表头的大小写及空白。
func normalizeOfficialPricingHeader(cells []string) []string {
	header := make([]string, len(cells))
	for i, cell := range cells {
		header[i] = strings.ToLower(strings.Join(strings.Fields(cell), " "))
	}
	return header
}

// walkPricingHTML 遍历节点；回调返回 false 时不再访问该节点的子树。
func walkPricingHTML(node *html.Node, visit func(*html.Node) bool) {
	if !visit(node) {
		return
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		walkPricingHTML(child, visit)
	}
}

// pricingHTMLTableRows 收集 HTML 表格的单元格，保留节点以区分模型链接和说明文字。
func pricingHTMLTableRows(table *html.Node) [][]*html.Node {
	var rows [][]*html.Node
	walkPricingHTML(table, func(node *html.Node) bool {
		if node.Type != html.ElementNode || node.Data != "tr" {
			return true
		}
		var cells []*html.Node
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			if child.Type == html.ElementNode && (child.Data == "th" || child.Data == "td") {
				cells = append(cells, child)
			}
		}
		rows = append(rows, cells)
		return false
	})
	return rows
}

// pricingHTMLModelText 优先读取模型链接，避免把描述及状态图标拼进模型 ID。
func pricingHTMLModelText(cell *html.Node) string {
	model := ""
	walkPricingHTML(cell, func(node *html.Node) bool {
		if model == "" && node.Type == html.ElementNode && node.Data == "a" {
			label := strings.TrimSpace(htmlNodeText(node))
			if isOfficialPricingModelLabel(label) {
				model = label
			}
		}
		return model == ""
	})
	if model != "" {
		return model
	}
	return htmlNodeText(cell)
}

// isOfficialPricingModelLabel 只把模型链接当作名称，排除帮助链接和说明。
func isOfficialPricingModelLabel(label string) bool {
	label = strings.ToLower(label)
	return strings.HasPrefix(label, "claude ") || strings.HasPrefix(label, "gpt-") || strings.HasPrefix(label, "grok-")
}

// htmlNodeText 提取单元格的可见文本。
func htmlNodeText(node *html.Node) string {
	if node == nil {
		return ""
	}
	if node.Type == html.TextNode {
		return node.Data
	}
	for _, attribute := range node.Attr {
		if attribute.Key == "aria-hidden" && attribute.Val == "true" {
			return ""
		}
	}
	var text strings.Builder
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		text.WriteString(htmlNodeText(child))
	}
	return text.String()
}
