//go:build !darwin

package attestation

import (
	"context"
	"errors"
)

func generateNative(context.Context) (NativeResult, error) {
	return NativeResult{}, errors.New("native DeviceCheck requires macOS")
}
