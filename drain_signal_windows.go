//go:build windows

package main

import "os"

// Windows 没有 SIGUSR1/SIGUSR2：排空模式不可用，/health 报告 drain_mode_supported=false。
const drainModeSignalsSupported = false

func isDrainEnterSignal(os.Signal) bool { return false }
func isDrainExitSignal(os.Signal) bool  { return false }

func notifyDrainSignals(chan<- os.Signal) func() { return func() {} }
