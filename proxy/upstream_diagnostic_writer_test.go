package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func diagnosticWriterFixture(n int) upstreamHTTPDiagnostic {
	return upstreamHTTPDiagnostic{
		Time:       "2000-01-01T00:00:00Z",
		RequestID:  fmt.Sprintf("fixture-%02d", n),
		Status:     500,
		Transport:  "http",
		BodyFormat: "empty",
	}
}

func diagnosticWriterTestDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "logs")
	t.Setenv("LOG_DIR", dir)
	t.Setenv("LOG_DISABLED", "false")
	return dir
}

func diagnosticWriterRead(t *testing.T, path string) []upstreamHTTPDiagnostic {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		t.Fatal("expected nonempty, newline-terminated JSONL")
	}
	var records []upstreamHTTPDiagnostic
	for _, line := range bytes.Split(data[:len(data)-1], []byte{'\n'}) {
		var d upstreamHTTPDiagnostic
		if err := json.Unmarshal(line, &d); err != nil {
			t.Fatal(err)
		}
		records = append(records, d)
	}
	return records
}

func diagnosticWriterRecordBytes(t *testing.T) int64 {
	t.Helper()
	data, err := json.Marshal(diagnosticWriterFixture(0))
	if err != nil {
		t.Fatal(err)
	}
	return int64(len(data) + 1)
}

func diagnosticWriterWaitDropped(t *testing.T, w *upstreamDiagnosticWriter, want uint64) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if got := w.dropped.Load(); got == want {
			return
		} else if got > want {
			t.Fatalf("dropped = %d, want %d", got, want)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for dropped = %d; got %d", want, w.dropped.Load())
		}
	}
}

func TestUpstreamDiagnosticWriterAppendAndPermissions(t *testing.T) {
	dir := diagnosticWriterTestDir(t)
	path := filepath.Join(dir, upstreamDiagnosticFileName)
	for i := 0; i < 2; i++ {
		w := &upstreamDiagnosticWriter{}
		d := diagnosticWriterFixture(i)
		d.DroppedTotal = 999 // The writer owns this value, not the caller.
		w.enqueue(d)
		if !w.close(2 * time.Second) {
			t.Fatal("writer did not flush")
		}
		if !w.close(0) || w.dropped.Load() != 0 {
			t.Fatal("completed close must be idempotent, with no drops")
		}
		if i == 0 {
			// Simulate a file left by the old logger; reopening must tighten it.
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	records := diagnosticWriterRead(t, path)
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	for i, d := range records {
		if d.RequestID != diagnosticWriterFixture(i).RequestID || d.DroppedTotal != 0 {
			t.Fatalf("unexpected fixture record %d", i)
		}
	}
	if runtime.GOOS != "windows" {
		for path, want := range map[string]os.FileMode{dir: 0o750, path: 0o600} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != want {
				t.Fatalf("permissions = %o, want %o", info.Mode().Perm(), want)
			}
		}
	}
}

func TestUpstreamDiagnosticWriterDisabledAndUnused(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			dir := diagnosticWriterTestDir(t)
			w := &upstreamDiagnosticWriter{}
			if disabled {
				t.Setenv("LOG_DISABLED", "true")
				w.enqueue(diagnosticWriterFixture(0))
			}
			if !w.close(0) || !w.close(-time.Second) {
				t.Fatal("unused writer should close immediately")
			}
			w.enqueue(diagnosticWriterFixture(1))
			if w.queue != nil || w.done != nil {
				t.Fatal("unused/disabled/closed writer started a worker")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("unused writer created a log directory")
			}
		})
	}
}

func TestUpstreamDiagnosticWriterQueueFullDoesNotBlock(t *testing.T) {
	dir := diagnosticWriterTestDir(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var records [][]byte // Only the sole worker writes; read after close succeeds.
	w := &upstreamDiagnosticWriter{write: func(data []byte) (int, error) {
		if len(records) == 0 {
			close(entered)
			<-release
		}
		records = append(records, append([]byte(nil), data...))
		return len(data), nil
	}}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		w.close(2 * time.Second)
	})
	w.enqueue(diagnosticWriterFixture(0))
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not enter slow sink")
	}
	if cap(w.queue) != 256 {
		t.Fatalf("default capacity = %d, want 256", cap(w.queue))
	}
	finished := make(chan struct{})
	const overflow = 1024
	go func() {
		defer close(finished)
		for i := 0; i < upstreamDiagnosticQueueLimit+overflow; i++ {
			w.enqueue(diagnosticWriterFixture(1))
		}
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue waited for the blocked sink")
	}
	if got := w.dropped.Load(); got != overflow {
		t.Fatalf("dropped = %d, want %d", got, overflow)
	}
	releaseOnce.Do(func() { close(release) })
	if !w.close(2 * time.Second) {
		t.Fatal("writer did not drain")
	}
	if len(records) != 1+upstreamDiagnosticQueueLimit {
		t.Fatalf("written = %d, want %d", len(records), 1+upstreamDiagnosticQueueLimit)
	}
	var last upstreamHTTPDiagnostic
	if err := json.Unmarshal(records[len(records)-1], &last); err != nil {
		t.Fatal(err)
	}
	if last.DroppedTotal != overflow {
		t.Fatalf("reported drops = %d, want %d", last.DroppedTotal, overflow)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("enqueue performed filesystem work despite the injected sink")
	}
}

func TestUpstreamDiagnosticWriterRotationBound(t *testing.T) {
	dir := diagnosticWriterTestDir(t)
	limit := 2 * diagnosticWriterRecordBytes(t)
	w := &upstreamDiagnosticWriter{maxBytes: limit}
	for i := 0; i < 7; i++ {
		w.enqueue(diagnosticWriterFixture(i))
	}
	if !w.close(2*time.Second) || w.dropped.Load() != 0 {
		t.Fatal("rotation did not complete without drops")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("files = %d, want active + one backup", len(entries))
	}
	var total int64
	for _, entry := range entries {
		if entry.Name() != upstreamDiagnosticFileName && entry.Name() != upstreamDiagnosticFileName+".1" {
			t.Fatal("unexpected additional generation")
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > limit {
			t.Fatalf("generation exceeds %d bytes", limit)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Fatal("rotation did not preserve private file permissions")
		}
		total += info.Size()
	}
	if total > 2*limit {
		t.Fatal("combined retention limit exceeded")
	}
	active := diagnosticWriterRead(t, filepath.Join(dir, upstreamDiagnosticFileName))
	backup := diagnosticWriterRead(t, filepath.Join(dir, upstreamDiagnosticFileName+".1"))
	if len(active) != 1 || active[0].RequestID != "fixture-06" || len(backup) != 2 || backup[0].RequestID != "fixture-04" || backup[1].RequestID != "fixture-05" {
		t.Fatal("rotation did not retain the expected latest records")
	}
}

func TestUpstreamDiagnosticWriterOversizedRecord(t *testing.T) {
	dir := diagnosticWriterTestDir(t)
	w := &upstreamDiagnosticWriter{maxBytes: diagnosticWriterRecordBytes(t) + 32}
	t.Cleanup(func() { w.close(2 * time.Second) })
	d := diagnosticWriterFixture(0)
	d.RequestID = strings.Repeat("x", 2048)
	w.enqueue(d)
	diagnosticWriterWaitDropped(t, w, 1)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("oversized record created a file")
	}
	w.enqueue(diagnosticWriterFixture(1))
	if !w.close(2 * time.Second) {
		t.Fatal("writer did not flush after oversized record")
	}
	records := diagnosticWriterRead(t, filepath.Join(dir, upstreamDiagnosticFileName))
	if len(records) != 1 || records[0].DroppedTotal != 1 || w.dropped.Load() != 1 {
		t.Fatal("oversized record loss was not reported")
	}
}

func TestUpstreamDiagnosticWriterLegacyOversizedFiles(t *testing.T) {
	dir := diagnosticWriterTestDir(t)
	limit := 2 * diagnosticWriterRecordBytes(t)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, upstreamDiagnosticFileName)
	for _, name := range []string{path, path + ".1"} {
		if err := os.WriteFile(name, bytes.Repeat([]byte{'x'}, int(limit)+1), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	w := &upstreamDiagnosticWriter{maxBytes: limit}
	w.enqueue(diagnosticWriterFixture(0))
	if !w.close(2*time.Second) || w.dropped.Load() != 0 {
		t.Fatal("writer did not recover from oversized legacy generations")
	}
	if records := diagnosticWriterRead(t, path); len(records) != 1 {
		t.Fatal("oversized legacy active file was retained")
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatal("oversized legacy backup was retained")
	}
}

func TestUpstreamDiagnosticWriterFailureCounts(t *testing.T) {
	t.Run("open-and-recover", func(t *testing.T) {
		dir := diagnosticWriterTestDir(t)
		// A regular file cannot serve as the log directory, on Windows or Unix.
		if err := os.WriteFile(dir, []byte("synthetic blocker"), 0o600); err != nil {
			t.Fatal(err)
		}
		w := &upstreamDiagnosticWriter{}
		t.Cleanup(func() { w.close(2 * time.Second) })
		w.enqueue(diagnosticWriterFixture(0))
		diagnosticWriterWaitDropped(t, w, 1)
		if err := os.Remove(dir); err != nil {
			t.Fatal(err)
		}
		w.enqueue(diagnosticWriterFixture(1))
		if !w.close(2 * time.Second) {
			t.Fatal("writer did not recover after open failure")
		}
		records := diagnosticWriterRead(t, filepath.Join(dir, upstreamDiagnosticFileName))
		if len(records) != 1 || records[0].DroppedTotal != 1 || w.dropped.Load() != 1 {
			t.Fatal("open failure was not counted exactly once")
		}
	})

	t.Run("write-and-short-write", func(t *testing.T) {
		diagnosticWriterTestDir(t)
		calls := 0
		var last []byte
		w := &upstreamDiagnosticWriter{write: func(data []byte) (int, error) {
			calls++
			switch calls {
			case 1:
				return 0, errors.New("synthetic write failure")
			case 2:
				return len(data) - 1, nil
			default:
				last = append([]byte(nil), data...)
				return len(data), nil
			}
		}}
		for i := 0; i < 3; i++ {
			w.enqueue(diagnosticWriterFixture(i))
		}
		if !w.close(2 * time.Second) {
			t.Fatal("writer did not drain after write failures")
		}
		var d upstreamHTTPDiagnostic
		if err := json.Unmarshal(last, &d); err != nil {
			t.Fatal(err)
		}
		if calls != 3 || w.dropped.Load() != 2 || d.DroppedTotal != 2 {
			t.Fatal("failed and short writes were not counted exactly once")
		}
	})

	t.Run("rotation-and-recover", func(t *testing.T) {
		dir := diagnosticWriterTestDir(t)
		backup := filepath.Join(dir, upstreamDiagnosticFileName+".1")
		if err := os.MkdirAll(backup, 0o750); err != nil {
			t.Fatal(err)
		}
		blocker := filepath.Join(backup, "synthetic-blocker")
		if err := os.WriteFile(blocker, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
		w := &upstreamDiagnosticWriter{maxBytes: diagnosticWriterRecordBytes(t)}
		t.Cleanup(func() { w.close(2 * time.Second) })
		w.enqueue(diagnosticWriterFixture(0))
		w.enqueue(diagnosticWriterFixture(1))
		diagnosticWriterWaitDropped(t, w, 1)
		if err := os.Remove(blocker); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(backup); err != nil {
			t.Fatal(err)
		}
		w.enqueue(diagnosticWriterFixture(2))
		if !w.close(2 * time.Second) {
			t.Fatal("writer did not recover after rotation failure")
		}
		active := diagnosticWriterRead(t, filepath.Join(dir, upstreamDiagnosticFileName))
		previous := diagnosticWriterRead(t, backup)
		if len(active) != 1 || active[0].RequestID != "fixture-02" || active[0].DroppedTotal != 1 || len(previous) != 1 || previous[0].RequestID != "fixture-00" || w.dropped.Load() != 1 {
			t.Fatal("rotation failure lost, duplicated, or failed to count a record")
		}
	})
}

func TestUpstreamDiagnosticWriterShutdownTimeout(t *testing.T) {
	diagnosticWriterTestDir(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var written atomic.Uint64
	w := &upstreamDiagnosticWriter{queueSize: 2, write: func(data []byte) (int, error) {
		if written.Load() == 0 {
			close(entered)
			<-release
		}
		written.Add(1)
		return len(data), nil
	}}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		w.close(2 * time.Second)
	})
	w.enqueue(diagnosticWriterFixture(0))
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not start")
	}
	w.enqueue(diagnosticWriterFixture(1))
	w.enqueue(diagnosticWriterFixture(2))
	start := time.Now()
	if w.close(20 * time.Millisecond) {
		t.Fatal("close reported completion while sink was blocked")
	}
	if time.Since(start) > time.Second {
		t.Fatal("close exceeded its bounded wait")
	}
	if w.close(0) || w.close(-time.Second) {
		t.Fatal("blocked close must remain incomplete")
	}
	w.enqueue(diagnosticWriterFixture(3))
	if w.dropped.Load() != 1 {
		t.Fatal("closed writer accepted another record")
	}
	releaseOnce.Do(func() { close(release) })
	if !w.close(2*time.Second) || !w.close(0) || written.Load() != 3 {
		t.Fatal("timed-out shutdown did not subsequently drain exactly once")
	}
}

func TestUpstreamDiagnosticWriterConcurrentLifecycle(t *testing.T) {
	for _, concurrentClose := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrent-close-%t", concurrentClose), func(t *testing.T) {
			diagnosticWriterTestDir(t)
			var written atomic.Uint64
			var active atomic.Int32
			var overlap atomic.Bool
			w := &upstreamDiagnosticWriter{write: func(data []byte) (int, error) {
				if active.Add(1) != 1 {
					overlap.Store(true)
				}
				runtime.Gosched()
				written.Add(1)
				active.Add(-1)
				return len(data), nil
			}}
			const producers, perProducer = 16, 64
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < producers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for j := 0; j < perProducer; j++ {
						w.enqueue(diagnosticWriterFixture(0))
					}
				}()
			}
			if concurrentClose {
				for i := 0; i < 8; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						for j := 0; j < 32; j++ {
							w.close(0)
						}
					}()
				}
			}
			close(start)
			wg.Wait()
			if !w.close(2*time.Second) || !w.close(0) {
				t.Fatal("concurrent lifecycle did not complete")
			}
			if overlap.Load() {
				t.Fatal("multiple workers called the sink concurrently")
			}
			if got := written.Load() + w.dropped.Load(); got != producers*perProducer {
				t.Fatalf("written + dropped = %d, want %d", got, producers*perProducer)
			}
			if !concurrentClose && written.Load() == 0 {
				t.Fatal("concurrent lazy start did not start a worker")
			}
			before := written.Load()
			w.enqueue(diagnosticWriterFixture(1))
			if !w.close(0) || written.Load() != before {
				t.Fatal("closed writer restarted")
			}
		})
	}
}
