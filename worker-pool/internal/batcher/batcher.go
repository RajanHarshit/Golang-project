package batcher

import (
	"time"
	"worker-pool/internal"
)

// Strategy 1 - Size-Based
// Flush to worker when the batch accumulates exactly `batchSize` items.
// Ideal for: DB bulk inserts, API calls with a known optimal payload size.
// Risk:      Tail latency — the last partial batch waits until traffic fills it.

type Ctx interface {
	Done() <-chan struct{}
}

func SizeBased(ctx Ctx, in <-chan internal.Requests, jobs chan<- []internal.Requests, batchSize int) {
	batch := make([]internal.Requests, 0, batchSize)

	flush := func() {
		if len(batch) > 0 {
			out := make([]internal.Requests, 0, len(batch))
			copy(out, batch)
			jobs <- out
			batch = batch[:0]
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush() // drain whatever remain in batch slice to job channel
			return
		case req, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, req)
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
}

// ── Strategy 2: Time-based ────────────────────────────────────────────────────
//
// Flush to workers on a fixed ticker, regardless of how many items accumulated.
// Ideal for: low-to-medium traffic where latency matters more than batch size.
// Risk:      Unpredictable batch sizes — tiny batches during quiet periods.

func TimeBased(ctx interface{ Done() <-chan struct{} }, in <-chan internal.Requests, jobs chan<- []internal.Requests, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var batch []internal.Requests
	flush := func() {
		if len(batch) > 0 {
			out := make([]internal.Requests, len(batch))
			copy(out, batch)
			jobs <- out
			batch = batch[:0]
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case req, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, req)

		case <-ticker.C:
			flush()
		}
	}
}

// ── Strategy 3: Hybrid (recommended for production) ───────────────────────────
//
// Flush when size OR timer fires — whichever comes first.
// During high traffic: size trigger fires → large efficient batches.
// During low traffic:  timer trigger fires → small batches, bounded latency.
// This is the strategy you want for a 1M-request system.

func Hybrid(ctx interface{ Done() <-chan struct{} }, in <-chan internal.Requests, jobs chan<- []internal.Requests, batchSize int, maxWait time.Duration) {
	ticker := time.NewTicker(maxWait)
	defer ticker.Stop()

	batch := make([]internal.Requests, 0, batchSize)
	flush := func() {
		if len(batch) > 0 {
			out := make([]internal.Requests, len(batch))
			copy(out, batch)
			jobs <- out
			batch = batch[:0]
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case req, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, req)
			if len(batch) >= batchSize {
				flush()

				// Reset the timer - we just flushed, so the next
				// time-based flush should start counting from now.
				// not from when last tick happend.
				ticker.Reset(maxWait)
			}
		// Time trigger: flush partial batch to guarantee latency bound.
		case <-ticker.C:
			flush()
		}
	}
}
