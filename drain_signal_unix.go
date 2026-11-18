//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

// drainModeSignalsSupported 表示本平台已注册 SIGUSR1/SIGUSR2 处理函数，
// 发布脚本读取 /health 的 drain_mode_supported 后才会发信号。
const drainModeSignalsSupported = true

func isDrainEnterSignal(sig os.Signal) bool { return sig == syscall.SIGUSR1 }
func isDrainExitSignal(sig os.Signal) bool  { return sig == syscall.SIGUSR2 }

// notifyDrainSignals 注册排空信号；返回的函数取消注册。
func notifyDrainSignals(ch chan<- os.Signal) func() {
	signal.Notify(ch, syscall.SIGUSR1, syscall.SIGUSR2)
	return func() { signal.Stop(ch) }
}
