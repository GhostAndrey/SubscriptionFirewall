// Package worker runs the claim-process-acknowledge loop shared by the
// detection pipeline and the lifecycle syncer. One implementation keeps the
// retry, backoff and shutdown rules from drifting apart between consumers.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const (
	claimBatchSize  = 16
	claimRetryDelay = time.Second
)

// Runner owns the goroutines that drain one queue of T.
type Runner[T any] struct {
	claim    func(ctx context.Context, limit int) ([]T, error)
	complete func(ctx context.Context, ids []int64) error
	process  func(ctx context.Context, item T)
	maintain func(ctx context.Context)
	name     string
	logger   *slog.Logger

	interval time.Duration
	workers  sync.WaitGroup
	cancel   context.CancelFunc
	mu       sync.Mutex
	started  bool
	stopped  bool
}

// Options configures a Runner over a queue of T.
type Options[T any] struct {
	// Claim reserves a batch of items for this worker.
	Claim func(ctx context.Context, limit int) ([]T, error)
	// Complete acknowledges the given item ids.
	Complete func(ctx context.Context, ids []int64) error
	// Process handles one item. It must not block indefinitely, because the
	// batch it belongs to is acknowledged only after it returns.
	Process func(ctx context.Context, item T)
	// Maintenance requeues stale entries and refreshes gauges. Nil disables
	// the maintenance loop.
	Maintenance func(ctx context.Context)
	// MaintenanceInterval is the maintenance tick; zero defaults to one minute.
	MaintenanceInterval time.Duration
	// Name labels the workers in log records.
	Name   string
	Logger *slog.Logger
}

// New builds a runner from the given options.
func New[T any](options Options[T]) *Runner[T] {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.MaintenanceInterval <= 0 {
		options.MaintenanceInterval = time.Minute
	}
	return &Runner[T]{
		claim:    options.Claim,
		complete: options.Complete,
		process:  options.Process,
		maintain: options.Maintenance,
		name:     options.Name,
		logger:   options.Logger,
		interval: options.MaintenanceInterval,
	}
}

// Start launches workerCount processing goroutines plus the maintenance loop.
// Starting twice is a no-op, so a partially failed start cannot double the
// number of consumers.
func (r *Runner[T]) Start(ctx context.Context, workerCount int) {
	workerContext, cancel := context.WithCancel(ctx)

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		cancel()
		return
	}
	r.started = true
	r.cancel = cancel
	r.mu.Unlock()

	for range workerCount {
		r.workers.Add(1)
		go r.processLoop(workerContext)
	}
	if r.maintain != nil {
		r.workers.Add(1)
		go r.maintenanceLoop(workerContext)
	}

	r.logger.Info(r.name+" started", "workers", workerCount)
}

// Stop cancels the loops and waits for the in-flight batches to drain.
func (r *Runner[T]) Stop() {
	r.mu.Lock()
	if !r.stopped {
		r.stopped = true
	}
	cancel := r.cancel
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	r.workers.Wait()
	r.logger.Info(r.name + " stopped")
}

func (r *Runner[T]) processLoop(ctx context.Context) {
	defer r.workers.Done()
	for {
		batch, err := r.claim(ctx, claimBatchSize)
		for _, item := range batch {
			r.process(ctx, item)
		}
		switch {
		case errors.Is(err, context.Canceled):
			return
		case err != nil:
			r.logger.Error("claim "+r.name+" failed", "error", err)
			if !Sleep(ctx, claimRetryDelay) {
				return
			}
			continue
		}
		if len(batch) == 0 && !Sleep(ctx, claimRetryDelay) {
			return
		}
	}
}

func (r *Runner[T]) maintenanceLoop(ctx context.Context) {
	defer r.workers.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r.maintain(ctx)
	}
}

// Sleep waits for the delay and reports whether it elapsed. A cancelled context
// returns false, which callers use to leave their loop.
func Sleep(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
