package worker

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

const (
	claimBatchSize  = 16
	claimRetryDelay = time.Second
)

type Runner[T any] struct {
	claim    func(ctx context.Context, limit int) ([]T, error)
	process  func(ctx context.Context, item T)
	onPanic  func(ctx context.Context, item T, recovered any)
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

type Options[T any] struct {
	Claim               func(ctx context.Context, limit int) ([]T, error)
	Process             func(ctx context.Context, item T)
	OnPanic             func(ctx context.Context, item T, recovered any)
	Maintenance         func(ctx context.Context)
	MaintenanceInterval time.Duration
	Name                string
	Logger              *slog.Logger
}

func New[T any](options Options[T]) *Runner[T] {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.MaintenanceInterval <= 0 {
		options.MaintenanceInterval = time.Minute
	}
	return &Runner[T]{
		claim:    options.Claim,
		process:  options.Process,
		onPanic:  options.OnPanic,
		maintain: options.Maintenance,
		name:     options.Name,
		logger:   options.Logger,
		interval: options.MaintenanceInterval,
	}
}

func (r *Runner[T]) Start(ctx context.Context, workerCount int) {
	claimContext, cancel := context.WithCancel(ctx)

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		cancel()
		return
	}
	r.started = true
	r.cancel = cancel
	r.mu.Unlock()

	processContext := context.WithoutCancel(ctx)
	for range workerCount {
		r.workers.Add(1)
		go r.processLoop(claimContext, processContext)
	}
	if r.maintain != nil {
		r.workers.Add(1)
		go r.maintenanceLoop(claimContext)
	}

	r.logger.Info(r.name+" started", "workers", workerCount)
}

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

func (r *Runner[T]) processLoop(claimContext, processContext context.Context) {
	defer r.workers.Done()
	for {
		batch, err := r.claim(claimContext, claimBatchSize)
		for _, item := range batch {
			r.processSafely(processContext, item)
		}
		switch {
		case errors.Is(err, context.Canceled):
			return
		case err != nil:
			r.logger.Error("claim "+r.name+" failed", "error", err)
			if !Sleep(claimContext, claimRetryDelay) {
				return
			}
			continue
		}
		if len(batch) == 0 && !Sleep(claimContext, claimRetryDelay) {
			return
		}
	}
}

func (r *Runner[T]) processSafely(ctx context.Context, item T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		r.logger.Error("panic in "+r.name+" worker",
			"panic", recovered, "stack", string(debug.Stack()))
		if r.onPanic != nil {
			r.onPanic(ctx, item, recovered)
		}
	}()
	r.process(ctx, item)
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
		r.maintainSafely(ctx)
	}
}

func (r *Runner[T]) maintainSafely(ctx context.Context) {
	defer func() {
		if recovered := recover(); recovered != nil {
			r.logger.Error("panic in "+r.name+" maintenance",
				"panic", recovered, "stack", string(debug.Stack()))
		}
	}()
	r.maintain(ctx)
}

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
