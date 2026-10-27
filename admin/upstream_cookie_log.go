package admin

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// GetUsageLogUpstreamCookies is registered only under the authenticated admin
// group. Ordinary usage lists and exports never load this diagnostic column.
func (h *Handler) GetUsageLogUpstreamCookies(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(c, http.StatusBadRequest, "无效的用量日志 ID")
		return
	}
	trace, err := h.db.GetUsageLogUpstreamCookies(c.Request.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(c, http.StatusNotFound, "用量日志不存在")
		return
	}
	if err != nil {
		writeError(c, http.StatusInternalServerError, "读取 Cookie 诊断失败")
		return
	}
	response := struct {
		Entries   []json.RawMessage `json:"entries"`
		Truncated bool              `json:"truncated"`
	}{Entries: []json.RawMessage{}}
	if trace != "" {
		if err := json.Unmarshal([]byte(trace), &response); err != nil {
			writeError(c, http.StatusInternalServerError, "读取 Cookie 诊断失败")
			return
		}
	}
	c.JSON(http.StatusOK, response)
}
