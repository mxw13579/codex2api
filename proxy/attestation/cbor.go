package attestation

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
)

const (
	cborUnsigned = 0
	cborBytes    = 2
	cborText     = 3
	cborArray    = 4
	cborMap      = 5
	cborShift    = 5
	cborDirect   = 24
	cborUint8    = 24
	cborUint16   = 25
	cborUint32   = 26
	cborUint64   = 27
	cborFloat64  = 0xfb
	tokenFields  = 3
)

const (
	signalSchema = iota
	signalLanguages
	signalLocale
	signalTimezone
	signalScreenSize
	signalScreenScale
	signalSessionID
	signalFields
)

func encodeEnvelope(result tokenResult, signals Signals) string {
	fields := uint64(tokenFields)
	latency := result.latencyMS
	if latency != nil && (*latency < 0 || math.IsNaN(*latency) || math.IsInf(*latency, 0)) {
		latency = nil
	}
	if latency != nil {
		fields++
	}
	data := appendHeader(nil, cborMap, fields)
	if result.token != nil {
		data = appendText(appendText(data, "token"), *result.token)
	} else {
		data = appendHeader(appendText(data, "error_code"), cborUnsigned, result.errorCode)
	}
	data = appendText(appendText(data, "bundle_id"), BundleID)
	fingerprint := encodeSignals(signals)
	data = appendHeader(appendText(data, "f"), cborBytes, uint64(len(fingerprint)))
	data = append(data, fingerprint...)
	if latency != nil {
		data = appendFloat(appendText(data, "t"), *latency)
	}
	envelope, _ := json.Marshal(struct {
		Version int    `json:"v"`
		Status  int    `json:"s"`
		Token   string `json:"t"`
	}{Version: Version, Token: "v1." + base64.RawURLEncoding.EncodeToString(data)})
	return string(envelope)
}

func encodeSignals(signals Signals) []byte {
	data := appendHeader(nil, cborMap, signalFields)
	data = appendHeader(appendHeader(data, cborUnsigned, signalSchema), cborUnsigned, Version)
	data = appendHeader(appendHeader(data, cborUnsigned, signalLanguages), cborArray, uint64(len(signals.Languages)))
	for _, value := range signals.Languages {
		data = appendText(data, value)
	}
	data = appendText(appendHeader(data, cborUnsigned, signalLocale), signals.Locale)
	data = appendText(appendHeader(data, cborUnsigned, signalTimezone), signals.Timezone)
	data = appendHeader(appendHeader(data, cborUnsigned, signalScreenSize), cborUnsigned, signals.ScreenSizeSum)
	data = appendNumber(appendHeader(data, cborUnsigned, signalScreenScale), signals.ScreenScale)
	return appendText(appendHeader(data, cborUnsigned, signalSessionID), signals.SessionID)
}

func appendText(data []byte, value string) []byte {
	return append(appendHeader(data, cborText, uint64(len(value))), value...)
}

func appendFloat(data []byte, value float64) []byte {
	return binary.BigEndian.AppendUint64(append(data, cborFloat64), math.Float64bits(value))
}

func appendNumber(data []byte, value float64) []byte {
	if value >= 0 && math.Trunc(value) == value {
		return appendHeader(data, cborUnsigned, uint64(value))
	}
	return appendFloat(data, value)
}

func appendHeader(data []byte, major byte, value uint64) []byte {
	prefix := major << cborShift
	switch {
	case value < cborDirect:
		return append(data, prefix|byte(value))
	case value <= math.MaxUint8:
		return append(data, prefix|cborUint8, byte(value))
	case value <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(data, prefix|cborUint16), uint16(value))
	case value <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(data, prefix|cborUint32), uint32(value))
	default:
		return binary.BigEndian.AppendUint64(append(data, prefix|cborUint64), value)
	}
}
