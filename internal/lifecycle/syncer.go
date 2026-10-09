package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"subscriptionfirewall/internal/ports"
)

const (
	claimBatchSize      = 16
	claimRetryDelay     = time.Second
	maintenanceInterval = time.Minute
	staleTaskAfter      = 5 * time.Minute
)

// Syncer replays lifecycle actions that were deferred because the counterpart
// entity could not be updated inline.
type Syncer struct {
	service *Service
	queue   ports.LifecycleSyncQueue
	metrics lifecycleMetrics
	logger  *slog.Logger

	workers sync.WaitGroup
	cancel  context.CancelFunc
	stopped bool
	mu      sync.Mutex
}

type lifecycleMetrics interface {
	SetLifecycleQueueDepth(depth int)
}

func NewSyncer(service *Service, queue ports.LifecycleSyncQueue, metrics lifecycleMetrics, logger *slog.Logger) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Syncer{service: service, queue: queue, metrics: metrics, logger: logger}
}

func (s *Syncer) Start(ctx context.Context, workerCount int) {
	workerContext, cancel := context.WithCancel(ctx)

	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		cancel()
		return
	}
	s.cancel = cancel
	s.mu.Unlock()

	for range workerCount {
		s.workers.Add(1)
		go s.workerLoop(workerContext)
	}
	s.workers.Add(1)
	go s.maintenanceLoop(workerContext)

	s.logger.Info("lifecycle syncer started", "workers", workerCount)
}

func (s *Syncer) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		s.workers.Wait()
		return
	}
	s.stopped = true
	cancel := s.cancel
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.workers.Wait()
	s.logger.Info("lifecycle syncer stopped")
}

func (s *Syncer) workerLoop(ctx context.Context) {
	defer s.workers.Done()

	for {
		tasks, err := s.queue.ClaimBatch(ctx, claimBatchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.logger.Error("claim lifecycle syncs failed", "error", err)
			if !sleepContext(ctx, claimRetryDelay) {
				return
			}
			continue
		}
		if len(tasks) == 0 {
			if !sleepContext(ctx, claimRetryDelay) {
				return
			}
			continue
		}
		for _, task := range tasks {
			s.processTask(ctx, task)
		}
	}
}

func (s *Syncer) processTask(ctx context.Context, task ports.LifecycleSyncTask) {
	if err := s.service.ReplayTask(ctx, task); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		s.logger.Error("replay lifecycle sync failed",
			"entity_type", task.EntityType, "entity_id", task.EntityID,
			"action", task.Action, "attempt", task.Attempts, "error", err,
		)
		if err := s.queue.Fail(ctx, task); err != nil {
			s.logger.Error("mark lifecycle sync failed", "task_id", task.ID, "error", err)
		}
		return
	}
	if err := s.queue.Complete(ctx, []int64{task.ID}); err != nil {
		s.logger.Error("complete lifecycle sync", "task_id", task.ID, "error", err)
		return
	}
	s.logger.Info("lifecycle sync replayed",
		"entity_type", task.EntityType, "entity_id", task.EntityID, "action", task.Action,
	)
}

func (s *Syncer) maintenanceLoop(ctx context.Context) {
	defer s.workers.Done()

	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		requeued, err := s.queue.RequeueStale(ctx, staleTaskAfter)
		if err != nil {
			s.logger.Error("requeue stale lifecycle syncs failed", "error", err)
		} else if requeued > 0 {
			s.logger.Warn("requeued stale lifecycle syncs", "count", requeued)
		}
		if pending, err := s.queue.PendingCount(ctx); err == nil && s.metrics != nil {
			s.metrics.SetLifecycleQueueDepth(int(pending))
		}
	}
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
