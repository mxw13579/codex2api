package main

import (
	"github.com/codex2api/api"
	"github.com/codex2api/security"
	"github.com/gin-gonic/gin"
)

func configureRequestBodyMiddleware(r *gin.Engine, maxSize int64) {
	// CORS headers must also be present on early body-size/decompression errors.
	r.Use(api.CORSMiddleware())
	r.Use(security.RequestSizeLimiter(maxSize))
	r.Use(security.RequestBodyDecompressor(maxSize))
	r.Use(api.BodyCacheMiddleware())
	// 请求体此时已解压并缓存；在任何 JSON 解析（含 gjson 的递归校验）之前拦截超深嵌套。
	r.Use(api.JSONDepthGuardMiddleware())
}
