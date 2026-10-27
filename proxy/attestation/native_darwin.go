//go:build darwin

package attestation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	nativeTimeout      = 5 * time.Second
	nativeOutputLimit  = 16 << 10
	chatGPTAppName     = "ChatGPT.app"
	legacyCodexAppName = "Codex.app"
)

type nativeRuntime struct {
	node   string
	module string
}

func generateNative(ctx context.Context) (NativeResult, error) {
	runCtx, cancel := context.WithTimeout(ctx, nativeTimeout)
	defer cancel()
	installation, err := resolveNativeRuntime(runCtx)
	if err != nil {
		return NativeResult{}, err
	}
	command := exec.CommandContext(runCtx, installation.node, "-e", nativeScript)
	command.Env = []string{"PATH=/usr/bin:/bin", "CODEX2API_DEVICECHECK_MODULE=" + installation.module}
	var output bytes.Buffer
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return NativeResult{}, errors.New("native DeviceCheck process failed")
	}
	if output.Len() > nativeOutputLimit {
		return NativeResult{}, errors.New("native DeviceCheck result is oversized")
	}
	var result *NativeResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result == nil {
		return NativeResult{}, errors.New("invalid native DeviceCheck result")
	}
	return *result, nil
}

func resolveNativeRuntime(ctx context.Context) (nativeRuntime, error) {
	if runtime.GOARCH != "arm64" {
		return nativeRuntime{}, errors.New("native DeviceCheck requires Apple Silicon")
	}
	directories := []string{"/Applications"}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		directories = append(directories, filepath.Join(home, "Applications"))
	}
	for _, directory := range directories {
		for _, name := range []string{chatGPTAppName, legacyCodexAppName} {
			installation, err := nativeRuntimeAt(ctx, filepath.Join(directory, name))
			if err == nil {
				return installation, nil
			}
		}
	}
	return nativeRuntime{}, errors.New("official ChatGPT DeviceCheck runtime is unavailable")
}

func nativeRuntimeAt(ctx context.Context, appPath string) (nativeRuntime, error) {
	resources := filepath.Join(appPath, "Contents", "Resources")
	installation := nativeRuntime{
		node:   filepath.Join(resources, "cua_node", "bin", "node"),
		module: filepath.Join(resources, "native", "devicecheck.node"),
	}
	for _, path := range []string{installation.node, installation.module} {
		if info, err := os.Stat(path); err != nil || info.IsDir() {
			return nativeRuntime{}, errors.New("DeviceCheck runtime component is missing")
		}
	}
	output, err := exec.CommandContext(ctx, "/usr/bin/plutil", "-extract", "CFBundleIdentifier", "raw", filepath.Join(appPath, "Contents", "Info.plist")).Output()
	if err != nil || strings.TrimSpace(string(output)) != BundleID {
		return nativeRuntime{}, errors.New("unexpected DeviceCheck application identity")
	}
	return installation, nil
}

// 子进程隔离原生框架异常；画像和 CBOR 封装均由 Go 统一处理。
const nativeScript = `
(async () => {
  const addon = require(process.env.CODEX2API_DEVICECHECK_MODULE);
  const result = await addon.generateToken();
  if (!result || typeof result.supported !== "boolean") throw new Error("Invalid DeviceCheck result");
  process.stdout.write(JSON.stringify({
    supported: result.supported,
    tokenBase64: result.tokenBase64,
    latencyMs: result.latencyMs
  }));
})().catch(() => { process.exitCode = 1; });`
