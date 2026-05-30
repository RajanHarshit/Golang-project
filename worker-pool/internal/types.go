package internal

import "time"

// Requests represent a single incoming unit of works.
// In a real system this might be an HTTP request body, queue message, etc.
type Requests struct {
	ID int
	Payload string
	ArrivedAt time.Time
}

// Result is what a worker produces after processing a batch
type Result struct {
	BatchSize int
	ProcessedAt time.Time
	Duration time.Duration
	Err error
}