package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"worker-pool/internal"
	"worker-pool/internal/batcher"
	"worker-pool/internal/metrics"
	"worker-pool/internal/worker"
)

// ── Benchmark helpers ─────────────────────────────────────────────────────────
//
// Go's built-in testing/benchmark framework is perfect for comparing strategies.
// Run with:
//   go test -bench=. -benchtime=5s -v ./cmd/server/
//
// Each BenchmarkXxx function is called repeatedly with increasing b.N until
// the framework gets a stable measurement. b.N is the number of requests
// to process in that run.

const (
	benchWorkers   = 50
	benchBatchSize = 200
	benchMaxWait   = 10 * time.Millisecond
)

func BenchmarkSizeBased(b *testing.B) {
	runBenchmark(b, func(ctx context.Context, in <-chan internal.Requests, jobs chan<- []internal.Requests) {
		batcher.SizeBased(ctx, in, jobs, benchBatchSize)
	})
}

func BenchmarkTimeBased(b *testing.B) {
	runBenchmark(b, func(ctx context.Context, in <-chan internal.Requests, jobs chan<- []internal.Requests) {
		batcher.TimeBased(ctx, in, jobs, benchMaxWait)
	})
}

func BenchmarkHybrid(b *testing.B) {
	runBenchmark(b, func(ctx context.Context, in <-chan internal.Requests, jobs chan<- []internal.Requests) {
		batcher.Hybrid(ctx, in, jobs, benchBatchSize, benchMaxWait)
	})
}

// runBenchmark is the shared harness used by all three benchmark functions.
// b.N is the number of requests Go's test framework decides to send.
func runBenchmark(
	b *testing.B,
	batchFn func(context.Context, <-chan internal.Requests, chan<- []internal.Requests),
) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	in := make(chan internal.Requests, 20_000)
	jobs := make(chan []internal.Requests, 500)

	m := metrics.New()
	pool := worker.New(benchWorkers, jobs, m)
	pool.Start(ctx)

	go func() {
		batchFn(ctx, in, jobs)
		close(jobs)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range pool.Results() {
		}
	}()

	b.ResetTimer() // don't count setup time in the benchmark

	for i := 0; i < b.N; i++ {
		in <- internal.Requests{ID: i, Payload: "bench", ArrivedAt: time.Now()}
	}
	close(in)

	<-done
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "req/s")
}

// ── Load tests ────────────────────────────────────────────────────────────────
//
// These are regular *testing.T tests that simulate a sustained load and
// assert on throughput. They're more readable than benchmarks for tuning
// because you can print detailed stats and assert SLOs.
//
// Run with:   go test -run=TestLoad -v ./cmd/server/

func TestLoadSizeBased(t *testing.T) {
	runLoadTest(t, "Size-based", 200_000, func(
		ctx context.Context,
		in <-chan internal.Requests,
		jobs chan<- []internal.Requests,
	) {
		batcher.SizeBased(ctx, in, jobs, benchBatchSize)
	})
}

func TestLoadTimeBased(t *testing.T) {
	runLoadTest(t, "Time-based", 200_000, func(
		ctx context.Context,
		in <-chan internal.Requests,
		jobs chan<- []internal.Requests,
	) {
		batcher.TimeBased(ctx, in, jobs, benchMaxWait)
	})
}

func TestLoadHybrid(t *testing.T) {
	runLoadTest(t, "Hybrid", 200_000, func(
		ctx context.Context,
		in <-chan internal.Requests,
		jobs chan<- []internal.Requests,
	) {
		batcher.Hybrid(ctx, in, jobs, benchBatchSize, benchMaxWait)
	})
}

func runLoadTest(
	t *testing.T,
	name string,
	numRequests int,
	batchFn func(context.Context, <-chan internal.Requests, chan<- []internal.Requests),
) {
	t.Helper()
	t.Logf("── %s: sending %d requests ──", name, numRequests)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	in := make(chan internal.Requests, 20_000)
	jobs := make(chan []internal.Requests, 500)

	m := metrics.New()
	pool := worker.New(benchWorkers, jobs, m)
	pool.Start(ctx)

	go func() {
		batchFn(ctx, in, jobs)
		close(jobs)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range pool.Results() {
		}
	}()

	start := time.Now()
	for i := 0; i < numRequests; i++ {
		in <- internal.Requests{ID: i, Payload: "load-test", ArrivedAt: time.Now()}
	}
	close(in)
	<-done

	elapsed := time.Since(start)
	rps := float64(numRequests) / elapsed.Seconds()

	t.Logf("  Elapsed   : %s", elapsed.Round(time.Millisecond))
	t.Logf("  Throughput: %.0f req/s", rps)
	m.Report()

	// Assert a minimum throughput SLO — fail the test if we're too slow.
	// Adjust this threshold to match your production requirements.
	minRPS := 10_000.0
	if rps < minRPS {
		t.Errorf("throughput %.0f req/s is below minimum SLO of %.0f req/s", rps, minRPS)
	}
}

// ── Concurrent producer stress test ──────────────────────────────────────────
//
// Simulates N goroutines all pushing requests simultaneously — closer to
// how a real HTTP server behaves when handling many connections at once.
//
// Run with:   go test -run=TestConcurrentProducers -v ./cmd/server/

func TestConcurrentProducers(t *testing.T) {
	const (
		numProducers        = 100   // concurrent producers (like 100 HTTP handlers)
		requestsPerProducer = 1_000 // each producer sends this many requests
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	in := make(chan internal.Requests, 50_000)
	jobs := make(chan []internal.Requests, 1_000)

	m := metrics.New()
	pool := worker.New(100, jobs, m)
	pool.Start(ctx)

	go func() {
		batcher.Hybrid(ctx, in, jobs, 500, 10*time.Millisecond)
		close(jobs)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range pool.Results() {
		}
	}()

	// Launch all producers concurrently
	var produced atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < requestsPerProducer; i++ {
				in <- internal.Requests{
					ID:        producerID*requestsPerProducer + i,
					Payload:   fmt.Sprintf("producer-%d", producerID),
					ArrivedAt: time.Now(),
				}
				produced.Add(1)
			}
		}(p)
	}

	wg.Wait() // wait for all producers to finish sending
	close(in)
	<-done

	total := produced.Load()
	elapsed := time.Since(start)
	t.Logf("  Producers   : %d", numProducers)
	t.Logf("  Total sent  : %d", total)
	t.Logf("  Elapsed     : %s", elapsed.Round(time.Millisecond))
	t.Logf("  Throughput  : %.0f req/s", float64(total)/elapsed.Seconds())
	m.Report()
}
