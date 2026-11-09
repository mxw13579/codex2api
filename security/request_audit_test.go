package security

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequestAuditDetailsTrustBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, remote, forwarded, wantIP string
	}{
		{"nginx and haproxy", "172.18.0.2:443", "198.51.100.7, 10.20.30.2", "198.51.100.7"},
		{"untrusted direct peer", "203.0.113.10:443", "127.0.0.1", "203.0.113.10"},
		{"spoof before real client", "172.18.0.2:443", "127.0.0.1, 198.51.100.7, 10.20.30.2", "198.51.100.7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			if err := r.SetTrustedProxies([]string{"172.18.0.0/16", "10.20.30.2"}); err != nil {
				t.Fatal(err)
			}
			var details string
			r.POST("/api/admin/system/update", func(c *gin.Context) {
				details = RequestAuditDetails(c)
				c.Status(http.StatusOK)
			})
			req := httptest.NewRequest(http.MethodPost, "/api/admin/system/update?token=query-secret", strings.NewReader("body-secret"))
			req.RemoteAddr = tc.remote
			req.Header.Set("X-Forwarded-For", tc.forwarded)
			req.Header.Set("CF-Connecting-IP", "127.0.0.2")
			req.Header.Set("Authorization", "Bearer bearer-secret")
			req.Header.Set("X-Admin-Key", "admin-secret")
			req.Header.Set("User-Agent", "Chrome/155\r\nforged-line")
			req.Header.Set("Referer", "https://user:password@example.com/admin/images/studio?token=referer-secret#fragment-secret")
			r.ServeHTTP(httptest.NewRecorder(), req)
			for _, want := range []string{"method=POST", `path="/api/admin/system/update"`, "ip=" + tc.wantIP + " ", `referer="https://example.com/admin/images/studio"`, "Chrome/155"} {
				if !strings.Contains(details, want) {
					t.Errorf("audit %q missing %q", details, want)
				}
			}
			for _, forbidden := range []string{"\r", "\n", "query-secret", "body-secret", "bearer-secret", "admin-secret", "password", "referer-secret", "fragment-secret", "127.0.0.2"} {
				if strings.Contains(details, forbidden) {
					t.Errorf("audit leaked %q: %q", forbidden, details)
				}
			}
		})
	}
}
