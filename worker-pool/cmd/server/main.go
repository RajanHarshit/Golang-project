package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
	"worker-pool/internal"
	"worker-pool/internal/batcher"
	"worker-pool/internal/metrics"
	"worker-pool/internal/worker"
)

// Config holds all tunable paramerter in one place.
// You can override these via environment variables when load testing
type Config struct {
	NumRequests int // total requests to generate
	NumWorkers int // goroutines in the worker pool
	InBuffer int // buffer size of incoming request channel
	JobBuffer int // buffer size of the batched job channel
	BatchSize int  // max items per batch (size + hybrid strategies)
	MaxWait time.Duration // max time before a partial flush (time + hybrid)

}

func defaultConfig() Config {
	return Config{
		NumRequests: 100_000,
		NumWorkers: 50,
		InBuffer: 20_000,
		JobBuffer: 500,
		BatchSize: 200,
		MaxWait: 10 * time.Millisecond,
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d,err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func loadConfig() Config {
	c := defaultConfig()
	c.NumRequests = envInt("NUM_REQUESTS", c.NumRequests)
	c.NumWorkers = envInt("NUM_WORKERS", c.NumWorkers)
	c.InBuffer = envInt("IN_BUFFER", c.InBuffer)
	c.JobBuffer = envInt("JOBS_BUFFER", c.JobBuffer)
	c.BatchSize = envInt("BATCH_SIZE", c.BatchSize)
	c.MaxWait = envDuration("MAX_WAIT", c.MaxWait)
	return c

}

func main() {
	cfg := loadConfig()

	fmt.Println("=========================================")
	fmt.Println(" Go Worker Pool - All 3 batch strategies ")
	fmt.Println("=========================================")

	fmt.Printf(" Requests     : %d\n", cfg.NumRequests)
	fmt.Printf(" Workers      : %d\n", cfg.NumWorkers)
	fmt.Printf("  Batch size : %d\n", cfg.BatchSize)
	fmt.Printf("  Max wait   : %s\n", cfg.MaxWait)

	fmt.Println()

	runStrategy(" Strategy : 1 - Size Based ", cfg, func(
		ctx context.Context,
		in <-chan internal.Requests,
		jobs chan <-[]internal.Requests,
	) {
		batcher.SizeBased(ctx, in, jobs, cfg.BatchSize)
	})

	runStrategy(" Strategy : 2 - Time Based ", cfg, func(
		ctx context.Context,
		in <-chan internal.Requests,
		jobs chan <-[]internal.Requests,
	) {
		batcher.TimeBased(ctx, in, jobs, cfg.MaxWait)
	})

	runStrategy(" Strategy : 3 - Hybrid Based ", cfg, func(
		ctx context.Context,
		in <-chan internal.Requests,
		jobs chan <-[]internal.Requests,
	) {
		batcher.Hybrid(ctx, in, jobs, cfg.BatchSize, cfg.MaxWait)
	})
	


}

// runStrategy is the generic harness that wires up:
// producer -> in chan -> batcher -> jobs chan -> worker pool -> results -> aggregator
// The batchFn parameter is the only thing that differs between strategies —
// everything else (channels, workers, metrics) is identical.

func runStrategy(
	name string, 
	cfg Config, 
	batchFn func(context.Context, <-chan internal.Requests, chan <-[]internal.Requests),
	) {
		fmt.Printf("\n %s\n", name)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		
		// Two channels form the pipeline backbone
		in := make(chan internal.Requests, cfg.InBuffer) // raw requests
		jobs := make(chan []internal.Requests, cfg.JobBuffer) // batched slices

		m := metrics.New()
		pool := worker.New(cfg.NumWorkers, jobs, m)
		pool.Start(ctx)

		// - Batcher goroutines -----------------------
		// Sits between the raw requests stream and worker pool.
		// Reads from `in`, group requests, writes []Request slice to `jobs`.

		go func() {
			batchFn(ctx, in, jobs)
			close(jobs) // signal worker : no more batches coming
		}()

		// Aggregator goroutine
		// Drains the results channel so workers never block trying to send.
		// In a real system you'd write results to a DB, send HTTP responses, etc.
		done := make(chan struct{})
		go func(){
			defer close(done)
			for range pool.Results() {

			}
		}()

		// - Producer -------------------------
		// Generates all requests and feeds them into the pipeline.
		// This runs in the main goroutine of this strategy call.
		produceRequests(in, cfg.NumRequests)
		close(in) // signal batcher : no more requests

		// Wait for the full pipeline to drain before printing the report.
		<- done
		m.Report()
	}

	// produceRequests simulates a stream of incoming work.
	// In a real server this would be your http.Handler pushing into the channel.
	func produceRequests(in chan<- internal.Requests, count int) {
		for i:=0; i<count; i++ {
			in <- internal.Requests{
				ID: i,
				Payload: "request-payload",
				ArrivedAt: time.Now(),
			}
		}
	}
