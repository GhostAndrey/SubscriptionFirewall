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

type Syncer struct {
	service           *Service
	queue             ports.LifecycleSyncQueue
	metrics           *obs.Metrics
	logger            *slog.Logger
	finishedRetention time.Duration
	runner            *worker.Runner[ports.LifecycleSyncTask]
}

func NewSyncer(
	service *Service,
	queue ports.LifecycleSyncQueue,
	metrics *obs.Metrics,
	logger *slog.Logger,
	finishedRetention time.Duration,
) *Syncer {
	syncer := &Syncer{
		service:           service,
		queue:             queue,
		metrics:           metrics,
		logger:            logger,
		finishedRetention: finishedRetention,
	}
	syncer.runner = worker.New(worker.Options[ports.LifecycleSyncTask]{
		Claim:               queue.ClaimBatch,
		Process:             syncer.processTask,
		OnPanic:             syncer.onTaskPanic,
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
		if err := s.queue.Fail(context.WithoutCancel(ctx), task); err != nil {
			s.logger.Error("mark lifecycle sync failed", "task_id", task.ID, "error", err)
		}
		return
	}
	if err := s.queue.Complete(context.WithoutCancel(ctx), []int64{task.ID}); err != nil {
		s.logger.Error("complete lifecycle sync", "task_id", task.ID, "error", err)
		return
	}
	s.logger.Info("lifecycle sync replayed",
		"entity_type", task.EntityType, "entity_id", task.EntityID, "action", task.Action,
	)
}

func (s *Syncer) onTaskPanic(ctx context.Context, task ports.LifecycleSyncTask, _ any) {
	if s.metrics != nil {
		s.metrics.CountWorkerPanic("lifecycle")
	}
	if err := s.queue.Fail(context.WithoutCancel(ctx), task); err != nil {
		s.logger.Error("fail lifecycle sync after panic", "task_id", task.ID, "error", err)
	}
}

func (s *Syncer) maintain(ctx context.Context) {
	requeued, err := s.queue.RequeueStale(ctx, staleTaskAfter)
	if err != nil {
		s.logger.Error("requeue stale lifecycle syncs failed", "error", err)
	} else if requeued > 0 {
		s.logger.Warn("requeued stale lifecycle syncs", "count", requeued)
	}
	s.deleteFinished(ctx)
	if pending, err := s.queue.PendingCount(ctx); err == nil && s.metrics != nil {
		s.metrics.SetLifecycleQueueDepth(int(pending))
	}
}

func (s *Syncer) deleteFinished(ctx context.Context) {
	if s.finishedRetention <= 0 {
		return
	}
	deleted, err := s.queue.DeleteFinished(ctx, s.finishedRetention)
	if err != nil {
		s.logger.Error("delete finished lifecycle syncs failed", "error", err)
	} else if deleted > 0 {
		s.logger.Info("deleted finished lifecycle syncs", "count", deleted)
	}
}
