package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/worker"
)

const staleTaskAfter = 5 * time.Minute

// Syncer replays lifecycle actions that were deferred because the counterpart
// entity could not be updated inline.
type Syncer struct {
	service *Service
	queue   ports.LifecycleSyncQueue
	metrics *obs.Metrics
	logger  *slog.Logger
	runner  *worker.Runner[ports.LifecycleSyncTask]
}

func NewSyncer(service *Service, queue ports.LifecycleSyncQueue, metrics *obs.Metrics, logger *slog.Logger) *Syncer {
	syncer := &Syncer{
		service: service,
		queue:   queue,
		metrics: metrics,
		logger:  logger,
	}
	syncer.runner = worker.New(worker.Options[ports.LifecycleSyncTask]{
		Claim:               queue.ClaimBatch,
		Complete:            queue.Complete,
		Process:             syncer.processTask,
		Maintenance:         syncer.maintain,
		MaintenanceInterval: time.Minute,
		Name:                "lifecycle syncer",
		Logger:              logger,
	})
	return syncer
}

func (s *Syncer) Start(ctx context.Context, workerCount int) {
	s.runner.Start(ctx, workerCount)
}

func (s *Syncer) Stop() {
	s.runner.Stop()
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

func (s *Syncer) maintain(ctx context.Context) {
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
