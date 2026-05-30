package worker

import (
	"context"
	"fmt"
	"sync"
	"time"
	"worker-pool/internal"
	"worker-pool/internal/metrics"
)

// Pool manages fixed number of worker go routines
// Each worker receives a []Request batch, processes it,
// and records the result via the metrics collector.

type Pool struct {
	numWorkers int
	jobs       <-chan []internal.Requests
	results    chan internal.Result
	metrics    *metrics.Collector
	wg         sync.WaitGroup
}

func New(numWorkers int, jobs <-chan []internal.Requests, m *metrics.Collector) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		jobs:       jobs,
		results:    make(chan internal.Result, numWorkers*2),
		metrics:    m,
	}
}

// Start launches all worker goroutines and return immediatly.
// Call Wait() to block until all workers have finished.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}

	// when all workers goroutines are done, close the result channel so
	// any consumer ranging over it will exit cleanly
	go func() {
		p.wg.Wait()
		close(p.results)
	}()
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()

	// Recover from panic, so one bad batch doesn't kill en entire worker
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[worker %d] recovered from panic: %v\n", id, r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return // graceful shutdown on context cancellation
		case batch, ok := <-p.jobs:
			if !ok {
				return // jobs channel closed - no more works
			}
			p.processBatch(id, batch)
		}
	}
}

func (p *Pool) processBatch(id int, batch []internal.Requests) {
	start := time.Now()

	// Simulate I/O work: a real system might do:
	//   db.BulkInsert(batch)
	//   httpClient.Post("/bulk", batch)
	//   kafkaProducer.SendBatch(batch)
	//
	// Base cost (network round-trip / DB overhead) = 1ms
	// Per-item cost (serialization, row processing) = tiny
	// This makes batching dramatically more efficient than 1 call per item.

	simulateDuration := time.Duration(1+len(batch)/100) * time.Millisecond
	time.Sleep(simulateDuration)

	duration := time.Since(start)
	p.metrics.RecordBatch(len(batch), duration, nil)

	p.results <- internal.Result{
		BatchSize: len(batch),
		ProcessedAt: time.Now(),
		Duration: duration,
	}

}

// Results returned the channel where processed results are published
// Range over this channel in your aggregator goroutine.

func(p *Pool) Results() <-chan internal.Result {
	return p.results
}

// Wait blocks until all workers have finished and the results channel is closed.
func (p *Pool) Wait() {
	p.wg.Wait()
}