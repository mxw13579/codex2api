package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"
)

// pprofAddrFromEnv 读取 CODEX_PPROF_ADDR。未设置时返回空串，表示不启动性能分析端口。
func pprofAddrFromEnv() string {
	return strings.TrimSpace(os.Getenv("CODEX_PPROF_ADDR"))
}

// validatePprofAddr 只允许回环地址：pprof 会暴露堆栈、命令行与内存内容，
// 不能借由端口映射或 0.0.0.0 绑定被容器外访问。
func validatePprofAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("CODEX_PPROF_ADDR must be host:port: %w", err)
	}
	if port == "" || port == "0" {
		return fmt.Errorf("CODEX_PPROF_ADDR must use a fixed port")
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("CODEX_PPROF_ADDR must bind a loopback address, got %q", host)
	}
	return nil
}

func newPprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// startPprofServer 在独立的回环端口上提供 net/http/pprof，不挂到业务路由，默认关闭。
// 用法：容器内 wget -O cpu.pprof 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
func startPprofServer(addr string) error {
	if addr == "" {
		return nil
	}
	if err := validatePprofAddr(addr); err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: newPprofHandler(), ReadHeaderTimeout: 10 * time.Second}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("pprof listen %s: %w", addr, err)
	}
	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("pprof 服务退出: %v", err)
		}
	}()
	log.Printf("pprof 已启用: http://%s/debug/pprof/（仅回环）", addr)
	return nil
}
