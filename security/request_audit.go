package security

import (
	"fmt"
	"net/url"

	"github.com/gin-gonic/gin"
)

// RequestAuditDetails records the client resolved by Gin's trusted-proxy policy.
// Raw forwarding headers are not identities. Never include credentials, query
// strings or request bodies in the audit trail.
func RequestAuditDetails(c *gin.Context) string {
	referer := ""
	if u, err := url.Parse(c.Request.Referer()); err == nil && (u.Scheme == "http" || u.Scheme == "https") {
		u.User = nil
		u.RawQuery = ""
		u.ForceQuery = false
		u.Fragment = ""
		u.RawFragment = ""
		referer = u.String()
	}
	return fmt.Sprintf("method=%s path=%q ip=%s peer=%s user_agent=%q referer=%q",
		c.Request.Method, c.Request.URL.Path, c.ClientIP(), c.RemoteIP(),
		SafeTruncate(SanitizeLog(c.Request.UserAgent()), 256),
		SafeTruncate(SanitizeLog(referer), 256))
}
