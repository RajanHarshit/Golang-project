package metrics

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Collector tracks latency and TPS stats atomically
// Using atomic interger means workers can update counters from
// multiple goroutines without any mutex contentions

type Collector struct {
	totalRequests atomic.Int64
	totalBatches  atomic.Int64
	totalErrors   atomic.Int64
	totalLatency  atomic.Int64
	startTime     time.Time
}

func New() *Collector {
	return &Collector{
		startTime: time.Now(),
	}
}

func (c *Collector) RecordBatch(size int, duration time.Duration, err error) {
	c.totalRequests.Add(int64(size))
	c.totalBatches.Add(1)
	c.totalLatency.Add(duration.Milliseconds())
	if err != nil {
		c.totalErrors.Add(1)
	}
}

func (c *Collector) Report() {
	elapsed := time.Since(c.startTime).Seconds()
	reqs := c.totalRequests.Load()
	batches := c.totalBatches.Load()
	errs := c.totalErrors.Load()
	latMs := c.totalLatency.Load()

	rps := float64(reqs) / elapsed
	var avgBatch float64
	var avgLatMs float64

	if batches > 0 {
		avgBatch = float64(reqs) / float64(batches)
		avgLatMs = float64(latMs) / float64(batches)
	}

	fmt.Println("──────────────────────────────────────────")
	fmt.Printf("  Total requests   : %d\n", reqs)
	fmt.Printf("  Total batches    : %d\n", batches)
	fmt.Printf("  Avg batch size   : %.1f\n", avgBatch)
	fmt.Printf("  Avg batch latency: %.2f ms\n", avgLatMs)
	fmt.Printf("  Errors           : %d\n", errs)
	fmt.Printf("  Elapsed          : %.2f s\n", elapsed)
	fmt.Printf("  Throughput       : %.0f req/s\n", rps)
	fmt.Println("──────────────────────────────────────────")
}
