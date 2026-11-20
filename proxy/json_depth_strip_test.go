package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"

	"github.com/codex2api/api"
	"github.com/codex2api/auth"
	"github.com/codex2api/config"
	"github.com/codex2api/database"
	"github.com/codex2api/security"
)

// 回归：工具数组里的字符串元素含大量 '[' 时，第二道防线的预检曾把它误判为过深，
// 使 strip 策略放弃整组改写，image_generation 被转发给上游。字符串的 Value 不递归，
// 只有对象/数组才需要预检；发往上游的字节须与 main 一致。
//
// 设置 ZZ_STRIP_DUMP=<目录> 时把每个用例发往上游的请求体写到该目录，便于与 main 对比。
func TestJSONDepthStripPolicyWithBracketHeavyStringTools(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var mu sync.Mutex
	var bodies [][]byte
	var wsCalls atomic.Int32
	record := func(b []byte) {
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), b...))
		mu.Unlock()
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.Header.Get("Content-Encoding") == "zstd" {
			d, _ := zstd.NewReader(nil)
			b, _ = d.DecodeAll(b, nil)
			d.Close()
		}
		record(b)
		writePinnedSuccessSSE(w)
	}))
	t.Cleanup(upstream.Close)
	previousResin := resinCfg.Load()
	t.Cleanup(func() { resinCfg.Store(previousResin) })
	SetResinConfig(&ResinConfig{BaseURL: upstream.URL, PlatformName: "test"})
	previousExec := WebsocketExecuteFunc
	t.Cleanup(func() { WebsocketExecuteFunc = previousExec })
	WebsocketExecuteFunc = func(ctx context.Context, account *auth.Account, requestBody []byte, sessionID string, proxyOverride string, apiKey string, deviceCfg *DeviceProfileConfig, headers http.Header, poolRouteKey string) (*http.Response, error) {
		wsCalls.Add(1)
		record(requestBody)
		sse := `data: {"type":"response.completed","response":{"id":"resp_1","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(sse))}, nil
	}

	pad := `"` + strings.Repeat("[", 10001) + `"`
	cases := map[string]string{
		"control":           `{"model":"gpt-5.5","input":"draw a cat","stream":true,"tools":[{"type":"image_generation"},{"type":"function","name":"f","parameters":{"type":"object"}}]}`,
		"string-after":      `{"model":"gpt-5.5","input":"draw a cat","stream":true,"tools":[{"type":"image_generation"},` + pad + `]}`,
		"string-before":     `{"model":"gpt-5.5","input":"draw a cat","stream":true,"tools":[` + pad + `,{"type":"image_generation"}]}`,
		"additional-tools":  `{"model":"gpt-5.5","stream":true,"input":[{"type":"additional_tools","tools":[{"type":"image_generation"},` + pad + `]},{"role":"user","content":"draw a cat"}]}`,
		"string-only":       `{"model":"gpt-5.5","input":"draw a cat","stream":true,"tools":[` + pad + `]}`,
		"string-only-plain": `{"model":"gpt-5.5","input":"hello","stream":true,"tools":[` + pad + `]}`,
	}
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	dump := os.Getenv("ZZ_STRIP_DUMP")

	for _, transport := range []string{"http-upstream", "ws-upstream"} {
		store := auth.NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 2, TestConcurrency: 1, TestModel: "gpt-5.5"})
		store.AddAccount(&auth.Account{DBID: 1, AccessToken: "at", PlanType: "pro", AccountID: "acct-strip"})
		store.SetCodexForceWebsocket(transport == "ws-upstream")
		handler := NewHandler(store, nil, &config.Config{AllowAnonymousV1: true}, nil)
		router := gin.New()
		router.Use(api.RecoveryMiddleware())
		router.Use(security.RequestSizeLimiter(int64(security.DefaultMaxRequestBodySize)))
		router.Use(api.BodyCacheMiddleware())
		router.Use(api.JSONDepthGuardMiddleware())
		router.Use(func(c *gin.Context) {
			c.Set(contextAPIKeyRow, &database.APIKeyRow{ID: 7, Name: "strip", Limits: database.APIKeyLimits{ImageGenerationPolicy: database.ImageGenerationPolicyStrip}})
			c.Next()
		})
		handler.RegisterRoutes(router)
		srv := httptest.NewServer(router)

		for _, name := range names {
			mu.Lock()
			bodies = nil
			mu.Unlock()
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(cases[name]))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			mu.Lock()
			got := append([][]byte(nil), bodies...)
			mu.Unlock()
			if resp.StatusCode != http.StatusOK || len(got) == 0 {
				t.Fatalf("%s %s: status=%d upstream=%d", transport, name, resp.StatusCode, len(got))
			}
			for i, b := range got {
				if bytes.Contains(b, []byte(`"image_generation"`)) {
					t.Errorf("%s %s: image_generation forwarded upstream under strip policy: tools=%.120s input0=%.120s", transport, name, gjson.GetBytes(b, "tools").Raw, gjson.GetBytes(b, "input.0").Raw)
				}
				if strings.Contains(cases[name], pad) && !bytes.Contains(b, []byte(pad)) {
					t.Errorf("%s %s: the plain string tool must be kept verbatim", transport, name)
				}
				if dump != "" {
					sum := sha256.Sum256(b)
					_ = os.WriteFile(filepath.Join(dump, transport+"_"+name+"_"+string(rune('0'+i))+".json"), b, 0o644)
					t.Logf("%s %s #%d sha256=%s", transport, name, i, hex.EncodeToString(sum[:8]))
				}
			}
		}
		t.Logf("%s: upstream websocket calls=%d", transport, wsCalls.Load())
		if transport == "ws-upstream" && wsCalls.Load() == 0 {
			t.Fatalf("ws-upstream cases never reached the websocket upstream")
		}
		wsCalls.Store(0)
		srv.Close()
		store.Stop()
	}
}
