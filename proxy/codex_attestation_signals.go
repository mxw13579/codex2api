package proxy

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/codex2api/auth"
	"github.com/codex2api/proxy/attestation"
	"golang.org/x/text/language"
)

const (
	attestationDefaultLocale   = "en-US"
	attestationDefaultTimezone = "Etc/UTC"
	attestationDisplaySlots    = 10
)

type codexAttestationDisplay struct {
	width  int
	height int
	scale  float64
}

var codexAttestationDisplays = [...]codexAttestationDisplay{
	{1920, 1080, 1}, {1920, 1080, 1.25}, {1920, 1200, 1.25},
	{2560, 1440, 1.25}, {2560, 1440, 1.5}, {2560, 1600, 1.5},
	{2880, 1800, 2}, {3840, 2160, 1.5}, {3840, 2160, 2},
}

func codexAttestationSignals(account *auth.Account, now time.Time) attestation.Signals {
	timezone := account.EffectiveCodexTimezone()
	if timezone == "" {
		timezone = attestationDefaultTimezone
	}
	accountID := account.ID()
	display := codexAttestationDisplayForAccount(accountID)
	width := math.Round(float64(display.width) / display.scale)
	height := math.Round(float64(display.height) / display.scale)
	locale := codexAttestationLocale(timezone)
	return attestation.Signals{
		Languages: []string{locale}, Locale: locale, Timezone: timezone,
		ScreenSizeSum: uint64(math.Max(0, math.Round(width+height))),
		ScreenScale:   display.scale,
		SessionID:     codexAttestationDailySessionID(accountID, timezone, now),
	}
}

func codexAttestationLocale(timezone string) string {
	country := codexAttestationCountryByZone[timezone]
	if country == "" {
		return attestationDefaultLocale
	}
	base, _ := language.Make("und-" + country).Base()
	if base.String() == "und" {
		return attestationDefaultLocale
	}
	return base.String() + "-" + country
}

func codexAttestationDisplayForAccount(accountID int64) codexAttestationDisplay {
	// 保留已有种子和档位映射，升级不会改变 Windows 账号画像。
	seed := fmt.Sprintf("codex2api:windows-desktop-display:v1:%d", accountID)
	digest := sha256.Sum256([]byte(seed))
	slot := binary.BigEndian.Uint64(digest[:8]) % attestationDisplaySlots
	if slot == 0 {
		return codexAttestationDisplays[0]
	}
	return codexAttestationDisplays[slot-1]
}

func codexAttestationDailySessionID(accountID int64, timezone string, now time.Time) string {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.UTC
	}
	date := now.In(location).Format(time.DateOnly)
	seed := fmt.Sprintf("codex2api:windows-desktop-session:v1:%d:%s", accountID, date)
	return deriveStableCodexUUID(seed)
}
