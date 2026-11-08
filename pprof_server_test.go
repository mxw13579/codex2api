package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidatePprofAddrOnlyAllowsLoopback(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:6060", "localhost:6060", "[::1]:6060"} {
		if err := validatePprofAddr(addr); err != nil {
			t.Errorf("%s should be allowed: %v", addr, err)
		}
	}
	for _, addr := range []string{"0.0.0.0:6060", ":6060", "10.20.30.1:6060", "[::]:6060", "127.0.0.1:0", "127.0.0.1", "example.com:6060"} {
		if err := validatePprofAddr(addr); err == nil {
			t.Errorf("%s should be rejected", addr)
		}
	}
}

func TestStartPprofServerDisabledWhenEmpty(t *testing.T) {
	if err := startPprofServer(""); err != nil {
		t.Fatalf("empty addr must be a no-op: %v", err)
	}
}

func TestStartPprofServerRejectsPublicBind(t *testing.T) {
	if err := startPprofServer("0.0.0.0:6060"); err == nil {
		t.Fatal("public bind must be rejected")
	}
}

func TestPprofHandlerServesIndex(t *testing.T) {
	rec := httptest.NewRecorder()
	newPprofHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("index status = %d", rec.Code)
	}
}
