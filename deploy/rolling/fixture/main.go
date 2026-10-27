// Synthetic HTTP/SSE/WebSocket backend for the opt-in Docker release test.
// It has no database, credentials or upstream calls.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	nameBytes, err := os.ReadFile("/fixture-name")
	if err != nil {
		log.Fatal(err)
	}
	name := strings.TrimSpace(string(nameBytes))
	var unhealthy atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if unhealthy.Load() {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, "OK")
	})
	mux.HandleFunc("/unhealthy", func(w http.ResponseWriter, r *http.Request) {
		unhealthy.Store(true)
		fmt.Fprint(w, "OK")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, name)
	})
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		f := w.(http.Flusher)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for n := 0; ; n++ {
			if _, err := fmt.Fprintf(w, "data: %s-%d\n\n", name, n); err != nil {
				return
			}
			f.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			kind, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(kind, append([]byte(name+":"), data...)); err != nil {
				return
			}
		}
	})
	log.Fatal(http.ListenAndServe(":8080", mux))
}
