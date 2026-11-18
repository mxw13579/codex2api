//go:build !windows

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/codex2api/proxy/wsrelay"
	"github.com/gin-gonic/gin"
)

type fakeDrainHooks struct {
	mu         sync.Mutex
	enters     int
	exits      int
	httpCloses int
	keepAlives []bool
	logs       []string
}

func (f *fakeDrainHooks) hooks() drainModeHooks {
	return drainModeHooks{
		enterUpstreamWS: func() (wsrelay.DrainSweepResult, bool) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.enters++
			return wsrelay.DrainSweepResult{Closed: 5, KeptBusy: 2, KeptDownstream: 1}, true
		},
		exitUpstreamWS: func() bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.exits++
			return true
		},
		closeIdleHTTP: func() int {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.httpCloses++
			return 3
		},
		logf: func(format string, args ...any) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.logs = append(f.logs, fmt.Sprintf(format, args...))
		},
	}
}

func (f *fakeDrainHooks) setKeepAlives(enabled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keepAlives = append(f.keepAlives, enabled)
}

func TestDrainControllerEnterExitIdempotent(t *testing.T) {
	fake := &fakeDrainHooks{}
	drain := newDrainController(fake.hooks())
	drain.SetHTTPKeepAlives(fake.setKeepAlives)

	if drain.Exit() {
		t.Fatal("exit while not draining must be a no-op")
	}
	if !drain.Enter() || !drain.Draining() {
		t.Fatal("enter must switch into drain mode")
	}
	if drain.Enter() {
		t.Fatal("repeated enter must be a no-op")
	}
	if fake.enters != 1 || fake.httpCloses != 1 {
		t.Fatalf("enters=%d httpCloses=%d, want one sweep", fake.enters, fake.httpCloses)
	}
	if !drain.Exit() || drain.Draining() || drain.Exit() {
		t.Fatal("exit must change state exactly once")
	}
	if fake.exits != 1 {
		t.Fatalf("exits=%d, want 1", fake.exits)
	}
	if fmt.Sprint(fake.keepAlives) != "[false true]" {
		t.Fatalf("keep-alive toggles=%v, want [false true]", fake.keepAlives)
	}
	joined := strings.Join(fake.logs, "\n")
	if !strings.Contains(joined, "[Drain] entered closed_idle_upstream_ws=5 kept_busy_upstream_ws=2 ") || !strings.Contains(joined, "[Drain] exited") {
		t.Fatalf("missing fixed-format drain log lines:\n%s", joined)
	}
}

func TestDrainControllerKeepAliveHookRegisteredLateFollowsState(t *testing.T) {
	fake := &fakeDrainHooks{}
	drain := newDrainController(fake.hooks())
	drain.Enter()
	drain.SetHTTPKeepAlives(fake.setKeepAlives)
	if fmt.Sprint(fake.keepAlives) != "[false]" {
		t.Fatalf("late registration while draining must disable keep-alives, got %v", fake.keepAlives)
	}
}

func TestDrainControllerSignalDispatch(t *testing.T) {
	fake := &fakeDrainHooks{}
	drain := newDrainController(fake.hooks())
	signals := make(chan os.Signal, 8)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		drain.run(signals, stop)
	}()
	// 注入信号值，不向测试进程发送真实信号。
	for _, sig := range []os.Signal{syscall.SIGUSR1, syscall.SIGUSR1, syscall.SIGHUP, syscall.SIGUSR2, syscall.SIGUSR2, syscall.SIGUSR1} {
		signals <- sig
	}
	close(signals)
	<-done
	if fake.enters != 2 || fake.exits != 1 || !drain.Draining() {
		t.Fatalf("enters=%d exits=%d draining=%v, want 2/1/true", fake.enters, fake.exits, drain.Draining())
	}
}

func TestDrainControllerConcurrentToggles(t *testing.T) {
	fake := &fakeDrainHooks{}
	drain := newDrainController(fake.hooks())
	drain.SetHTTPKeepAlives(fake.setKeepAlives)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if (i+j)%2 == 0 {
					drain.Enter()
				} else {
					drain.Exit()
				}
				_ = drain.Draining()
			}
		}(i)
	}
	wg.Wait()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if diff := fake.enters - fake.exits; diff < 0 || diff > 1 || (diff == 1) != drain.Draining() {
		t.Fatalf("enters=%d exits=%d draining=%v are inconsistent", fake.enters, fake.exits, drain.Draining())
	}
}

func TestHealthReportsDrainModeFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fake := &fakeDrainHooks{}
	drain := newDrainController(fake.hooks())
	router := gin.New()
	router.GET("/health", newHealthHandler(func() (int, int, bool) { return 3, 4, true }, &healthLockProbe{}, drain))

	read := func() map[string]any {
		t.Helper()
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d", recorder.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	body := read()
	for _, key := range []string{"status", "build_version", "available", "total", "counts_complete"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("existing field %q missing", key)
		}
	}
	if body["status"] != "ok" || body["available"] != float64(3) || body["total"] != float64(4) || body["counts_complete"] != true {
		t.Fatalf("existing fields changed: %v", body)
	}
	if body["drain_mode_supported"] != true || body["draining"] != false {
		t.Fatalf("drain fields=%v/%v, want true/false", body["drain_mode_supported"], body["draining"])
	}
	drain.Enter()
	if body := read(); body["draining"] != true {
		t.Fatalf("draining=%v after enter, want true", body["draining"])
	}
}
