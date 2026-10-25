package database

import (
	"fmt"
	"testing"
	"unsafe"
)

var usageLogQueueBenchmarkSink int64

// One operation drains an entire backlog in batches of 200 using the same helper
// as flushLogBatch, without SQL or a background flusher. Fixture refill is outside
// the timer; B/op and allocs/op include all owned batches and the empty-queue reset.
// Compare 200/2000/20000 backlogs to detect quadratic remaining-tail copies.
func BenchmarkUsageLogQueueDrain(b *testing.B) {
	for _, backlog := range []int{200, 2000, 20000} {
		b.Run(fmt.Sprintf("backlog_%d", backlog), func(b *testing.B) {
			const batchSize = 200
			db := &DB{}
			db.SetUsageLogConfig(UsageLogModeFull, batchSize, maxUsageLogFlushIntervalSeconds)
			backing := make([]usageLogEntry, backlog)
			mismatch := true
			fixture := usageLogEntry{
				RequestID:             "synthetic-benchmark-request",
				Model:                 "synthetic-benchmark-model",
				UpstreamModelMismatch: &mismatch,
				StoreUsageLog:         true,
				StatusCode:            200,
				AccountBilled:         0.25,
				UserBilled:            0.5,
			}
			b.ReportAllocs()
			b.SetBytes(int64(backlog) * int64(unsafe.Sizeof(usageLogEntry{})))
			b.ResetTimer()
			var checksum int64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := range backing {
					backing[j] = fixture
					backing[j].AccountID = int64(j + 1)
				}
				db.logBuf = backing
				b.StartTimer()

				for pending := backlog; pending > 0; {
					batch, remaining := db.dequeueUsageLogBatch()
					if len(batch) != min(batchSize, pending) || remaining != pending-len(batch) {
						b.Fatalf("dequeue: pending=%d batch=%d remaining=%d", pending, len(batch), remaining)
					}
					checksum += batch[0].AccountID + batch[len(batch)-1].AccountID
					pending = remaining
				}
			}
			b.StopTimer()
			usageLogQueueBenchmarkSink = checksum
			b.ReportMetric(float64(backlog), "entries/op")
			b.ReportMetric(float64(backlog/batchSize), "batches/op")
		})
	}
}
