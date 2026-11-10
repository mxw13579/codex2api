package proxy

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codex2api/security"
)

const (
	upstreamDiagnosticQueueLimit = 256
	upstreamDiagnosticFileLimit  = 16 << 20
	upstreamDiagnosticFileName   = "upstream_errors.jsonl"
)

var upstreamDiagnosticLogger = &upstreamDiagnosticWriter{}

// upstreamDiagnosticWriter keeps request goroutines independent of disk I/O.
// mu protects only admission and shutdown; the worker never acquires it.
// The zero value is ready to use. A closed writer cannot be restarted.
type upstreamDiagnosticWriter struct {
	mu      sync.Mutex
	queue   chan upstreamHTTPDiagnostic
	done    chan struct{}
	closed  bool
	dropped atomic.Uint64

	// Test hooks, immutable after the first enqueue. Limits can only be reduced.
	queueSize int
	maxBytes  int64
	write     func([]byte) (int, error)
}

func (w *upstreamDiagnosticWriter) enqueue(d upstreamHTTPDiagnostic) {
	if security.FileLogsDisabled() {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		w.dropped.Add(1)
		return
	}
	if w.queue == nil {
		size := w.queueSize
		if size <= 0 || size > upstreamDiagnosticQueueLimit {
			size = upstreamDiagnosticQueueLimit
		}
		maxBytes := w.maxBytes
		if maxBytes <= 0 || maxBytes > upstreamDiagnosticFileLimit {
			maxBytes = upstreamDiagnosticFileLimit
		}
		w.queue = make(chan upstreamHTTPDiagnostic, size)
		w.done = make(chan struct{})
		go w.run(w.queue, w.done, maxBytes, w.write)
	}
	// Never wait for capacity, serialization, or the worker's filesystem calls.
	select {
	case w.queue <- d:
	default:
		w.dropped.Add(1)
	}
}

// close stops admission exactly once, then waits at most timeout for the worker
// to drain and close its file. A timeout leaves the sole worker draining; later
// close calls can wait again. Closing an unused writer does not start a worker.
func (w *upstreamDiagnosticWriter) close(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		if w.queue != nil {
			close(w.queue)
		}
	}
	done := w.done
	w.mu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (w *upstreamDiagnosticWriter) run(queue <-chan upstreamHTTPDiagnostic, done chan<- struct{}, maxBytes int64, write func([]byte) (int, error)) {
	defer close(done)
	sink := upstreamDiagnosticFile{path: filepath.Join(errorLogDir(), upstreamDiagnosticFileName), maxBytes: maxBytes}
	defer sink.close()
	for d := range queue {
		d.DroppedTotal = w.dropped.Load()
		data, err := json.Marshal(d)
		// Include the newline in the bound; an oversized record is never written.
		if err != nil || int64(len(data))+1 > maxBytes {
			w.dropped.Add(1)
			continue
		}
		data = append(data, '\n')
		if write != nil {
			var n int
			n, err = write(data)
			if err == nil && n != len(data) {
				err = io.ErrShortWrite
			}
		} else {
			err = sink.append(data)
		}
		if err != nil {
			// Never echo filesystem paths or error text into request logs.
			w.dropped.Add(1)
		}
	}
}

// upstreamDiagnosticFile is owned exclusively by the worker. Only the active
// file and its single .1 backup are used; there are no timestamped generations.
type upstreamDiagnosticFile struct {
	path     string
	maxBytes int64
	file     *os.File
	size     int64
}

func (s *upstreamDiagnosticFile) open() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return err
	}
	// Do not retain an oversized backup left by an earlier unbounded logger.
	backup := s.path + ".1"
	if info, err := os.Lstat(backup); err == nil {
		if info.Mode().IsRegular() {
			if info.Size() > s.maxBytes {
				if err := os.Remove(backup); err != nil {
					return err
				}
			} else if err := os.Chmod(backup, 0o600); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	// Windows 的 append-only 句柄不能 Truncate；在打开追加句柄前处理历史超限文件。
	if info, err := os.Lstat(s.path); err == nil {
		if !info.Mode().IsRegular() {
			return os.ErrInvalid
		}
		if info.Size() > s.maxBytes {
			if err := os.Truncate(s.path, 0); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	keepOpen := false
	defer func() {
		if !keepOpen {
			_ = f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return os.ErrInvalid
	}
	// Also tighten permissions on files created by the old synchronous logger.
	if err := f.Chmod(0o600); err != nil {
		return err
	}
	size := info.Size()
	if size > s.maxBytes {
		// 打开期间若另一个写入者已扩张文件，不在未知写入期间截断它。
		return os.ErrInvalid
	}
	s.file, s.size = f, size
	keepOpen = true
	return nil
}

func (s *upstreamDiagnosticFile) append(data []byte) error {
	if s.file == nil {
		if err := s.open(); err != nil {
			return err
		}
	}
	if s.size > s.maxBytes-int64(len(data)) {
		if err := s.close(); err != nil {
			return err
		}
		// Remove first for Windows too, where Rename cannot replace the backup.
		if err := os.Remove(s.path + ".1"); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Rename(s.path, s.path+".1"); err != nil {
			return err
		}
		if err := s.open(); err != nil {
			return err
		}
	}
	n, err := s.file.Write(data)
	if err == nil && n == len(data) {
		s.size += int64(n)
		return nil
	}
	// Best effort: avoid leaving a partial JSON line after a failed append.
	// Reopen/stat on the next record even if rollback or close also fails.
	_ = s.close()
	_ = os.Truncate(s.path, s.size)
	if err != nil {
		return err
	}
	return io.ErrShortWrite
}

func (s *upstreamDiagnosticFile) close() error {
	if s.file == nil {
		return nil
	}
	f := s.file
	s.file = nil
	return f.Close()
}
