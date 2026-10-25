package database

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// These tests use only synthetic entries and an in-memory DB queue: no connection
// or background flusher is started.
func makeUsageLogQueueEntries(count int) []usageLogEntry {
	entries := make([]usageLogEntry, count)
	for i := range entries {
		mismatch := i%2 == 0
		entries[i] = usageLogEntry{
			RequestID:             fmt.Sprintf("queue-%d", i),
			UpstreamRequestID:     "synthetic-upstream-request",
			UpstreamModelMismatch: &mismatch,
			StoreUsageLog:         i%2 == 0,
			AccountID:             int64(i + 1),
			CredentialGeneration:  7,
			Model:                 "synthetic-queue-model",
			StatusCode:            200,
			APIKeyID:              int64(i + 1),
			AccountBilled:         0.25,
			UserBilled:            0.5,
			UserBilling: UserBilling{
				UserBillingMode:  UserBillingModePerImage,
				ImageUnitPrice:   0.5,
				BilledImageCount: 1,
			},
		}
	}
	return entries
}

func usageLogQueueInput(index int) *UsageLogInput {
	return &UsageLogInput{
		RequestID:  fmt.Sprintf("queue-%d", index),
		AccountID:  int64(index + 1),
		Model:      "synthetic-queue-model",
		Endpoint:   "/synthetic-queue-test",
		StatusCode: 200,
	}
}

func TestUsageLogQueueDequeueFIFOAndClear(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured int
		batchSize  int
	}{
		{name: "default", configured: 0, batchSize: defaultUsageLogBatchSize},
		{name: "single", configured: 1, batchSize: 1},
		{name: "small", configured: 3, batchSize: 3},
		{name: "normal", configured: 200, batchSize: 200},
		{name: "clamped_max", configured: maxUsageLogBatchSize + 1, batchSize: maxUsageLogBatchSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := makeUsageLogQueueEntries(2*tc.batchSize + 1)
			backing := append([]usageLogEntry(nil), want...)
			db := &DB{logBuf: backing}
			db.SetUsageLogConfig(UsageLogModeFull, tc.configured, maxUsageLogFlushIntervalSeconds)

			consumed := 0
			for consumed < len(want) {
				batch, remaining := db.dequeueUsageLogBatch()
				take := min(tc.batchSize, len(want)-consumed)
				if len(batch) != take || remaining != len(want)-consumed-take {
					t.Fatalf("dequeue at %d: batch=%d remaining=%d, want %d/%d",
						consumed, len(batch), remaining, take, len(want)-consumed-take)
				}
				for i := range batch {
					// Compare the whole entry, including quota-only events and pointers.
					if batch[i] != want[consumed+i] {
						t.Fatalf("entry %d changed or was dequeued out of order", consumed+i)
					}
				}
				consumed += take
				if len(db.logBuf) != remaining {
					t.Fatalf("queue length = %d, want %d", len(db.logBuf), remaining)
				}
				for i := 0; i < consumed; i++ {
					if backing[i] != (usageLogEntry{}) {
						t.Fatalf("consumed slot %d still retains entry data", i)
					}
				}
				if remaining > 0 && &db.logBuf[0] != &backing[consumed] {
					t.Fatal("partial dequeue copied the remaining queue instead of advancing the slice")
				}
			}
			if cap(db.logBuf) != tc.batchSize {
				t.Fatalf("empty queue capacity = %d, want %d", cap(db.logBuf), tc.batchSize)
			}
			if db.usageLogDropped.Load() != 0 {
				t.Fatal("dequeue unexpectedly counted entries as dropped")
			}
		})
	}
}

func TestUsageLogQueueEmptyDequeuePreservesCapacity(t *testing.T) {
	for _, capacity := range []int{0, 7, defaultUsageLogBatchSize} {
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			db := &DB{}
			if capacity > 0 {
				db.logBuf = make([]usageLogEntry, 0, capacity)
			}
			before := db.logBuf
			for i := 0; i < 3; i++ {
				batch, remaining := db.dequeueUsageLogBatch()
				if batch != nil || remaining != 0 || len(db.logBuf) != 0 || cap(db.logBuf) != capacity {
					t.Fatalf("empty dequeue changed state: batch=%d remaining=%d len=%d cap=%d",
						len(batch), remaining, len(db.logBuf), cap(db.logBuf))
				}
				if capacity > 0 && &before[:capacity][0] != &db.logBuf[:capacity][0] {
					t.Fatal("empty dequeue replaced the reusable buffer")
				}
			}
		})
	}
}

func TestUsageLogQueueBatchOwnsStorage(t *testing.T) {
	for _, initial := range []int{2, 5} {
		t.Run(fmt.Sprintf("initial_%d", initial), func(t *testing.T) {
			const batchSize = 2
			want := makeUsageLogQueueEntries(initial)
			backing := make([]usageLogEntry, initial, initial+4)
			copy(backing, want)
			db := &DB{logBuf: backing}
			db.SetUsageLogConfig(UsageLogModeFull, batchSize, maxUsageLogFlushIntervalSeconds)

			batch, _ := db.dequeueUsageLogBatch()
			if len(batch) != batchSize {
				t.Fatalf("batch length = %d, want %d", len(batch), batchSize)
			}
			// Reuse consumed source slots and exercise actual enqueue/append paths,
			// both with a remaining tail and after resetting an empty queue.
			for i := 0; i < batchSize; i++ {
				backing[i] = usageLogEntry{RequestID: "reused-source-slot"}
			}
			for i := initial; i < initial+4; i++ {
				if err := db.InsertUsageLog(context.Background(), usageLogQueueInput(i)); err != nil {
					t.Fatalf("InsertUsageLog: %v", err)
				}
			}
			// Appending to an owned batch must not overwrite the pending queue.
			batch = append(batch, usageLogEntry{RequestID: "batch-only-entry"})
			for next := batchSize; ; {
				pending, _ := db.dequeueUsageLogBatch()
				if len(pending) == 0 {
					if next != initial+4 {
						t.Fatalf("drained %d entries, want %d", next, initial+4)
					}
					break
				}
				for _, entry := range pending {
					if entry.RequestID != fmt.Sprintf("queue-%d", next) {
						t.Fatalf("pending entry %d overwritten or out of order", next)
					}
					next++
				}
			}
			for i := 0; i < batchSize; i++ {
				if batch[i] != want[i] {
					t.Fatalf("owned batch entry %d changed after source reuse/enqueue/dequeue", i)
				}
			}
		})
	}
}

func TestUsageLogQueueConcurrentInsertAfterPartialDequeue(t *testing.T) {
	const initial = 11
	const total = 311
	want := makeUsageLogQueueEntries(initial)
	backing := make([]usageLogEntry, initial, initial+3)
	copy(backing, want)
	db := &DB{logBuf: backing}
	db.SetUsageLogConfig(UsageLogModeFull, 4, maxUsageLogFlushIntervalSeconds)

	first, remaining := db.dequeueUsageLogBatch()
	if len(first) != 4 || remaining != initial-4 {
		t.Fatalf("initial dequeue = %d/%d, want 4/%d", len(first), remaining, initial-4)
	}
	got := append([]usageLogEntry(nil), first...)
	inserted := make(chan error, 1)
	go func() {
		for i := initial; i < total; i++ {
			if err := db.InsertUsageLog(context.Background(), usageLogQueueInput(i)); err != nil {
				inserted <- err
				return
			}
			runtime.Gosched()
		}
		inserted <- nil
	}()

collect:
	for {
		batch, _ := db.dequeueUsageLogBatch()
		got = append(got, batch...)
		select {
		case err := <-inserted:
			if err != nil {
				t.Fatalf("concurrent InsertUsageLog: %v", err)
			}
			break collect
		default:
			runtime.Gosched()
		}
	}
	for {
		batch, _ := db.dequeueUsageLogBatch()
		if len(batch) == 0 {
			break
		}
		got = append(got, batch...)
	}
	if len(got) != total {
		t.Fatalf("dequeued %d entries, want %d", len(got), total)
	}
	for i, entry := range got {
		if entry.RequestID != fmt.Sprintf("queue-%d", i) || entry.AccountID != int64(i+1) {
			t.Fatalf("entry %d missing, duplicated, or out of FIFO order", i)
		}
	}
	for i := range first {
		if first[i] != want[i] {
			t.Fatalf("in-flight batch entry %d changed during concurrent enqueue/dequeue", i)
		}
	}
}

func TestUsageLogQueueFailedBatchRequeueFIFO(t *testing.T) {
	for _, tc := range []struct {
		name     string
		initial  int
		arrivals int
	}{
		{name: "empty", initial: 2},
		{name: "empty_then_arrivals", initial: 2, arrivals: 3},
		{name: "pending_then_arrivals", initial: 7, arrivals: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := makeUsageLogQueueEntries(tc.initial)
			db := &DB{logBuf: append([]usageLogEntry(nil), want...)}
			db.SetUsageLogConfig(UsageLogModeFull, 2, maxUsageLogFlushIntervalSeconds)

			// Simulate two transient failures through the production requeue path.
			// Entries arriving while a batch is in flight must stay behind it.
			for attempt := 0; attempt < 2; attempt++ {
				failed, _ := db.dequeueUsageLogBatch()
				if len(failed) != 2 || failed[0] != want[0] || failed[1] != want[1] {
					t.Fatalf("retry %d did not receive the original failed batch", attempt)
				}
				if attempt == 0 {
					for i := tc.initial; i < tc.initial+tc.arrivals; i++ {
						if err := db.InsertUsageLog(context.Background(), usageLogQueueInput(i)); err != nil {
							t.Fatalf("InsertUsageLog: %v", err)
						}
					}
				}
				db.requeueUsageLogBatch(failed)
				// The retry queue must own its copy as well.
				failed[0] = usageLogEntry{RequestID: "released-failed-batch"}
			}
			next := 0
			for {
				batch, _ := db.dequeueUsageLogBatch()
				if len(batch) == 0 {
					break
				}
				for _, entry := range batch {
					if entry.RequestID != fmt.Sprintf("queue-%d", next) {
						t.Fatalf("requeued entry %d missing, duplicated, or out of order", next)
					}
					if next < len(want) && entry != want[next] {
						t.Fatalf("requeue changed original entry %d", next)
					}
					next++
				}
			}
			if next != tc.initial+tc.arrivals || db.usageLogDropped.Load() != 0 {
				t.Fatalf("requeue drained=%d dropped=%d, want %d/0",
					next, db.usageLogDropped.Load(), tc.initial+tc.arrivals)
			}
		})
	}
}
