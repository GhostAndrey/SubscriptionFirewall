package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"subscriptionfirewall/internal/detector"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

const (
	claimBatchSize      = 16
	claimRetryDelay     = time.Second
	maintenanceInterval = time.Minute
	staleItemAfter      = 5 * time.Minute
)

var tracer = otel.Tracer("subscriptionfirewall/pipeline")

type Pipeline struct {
	outbox        ports.DetectionOutbox
	detector      *detector.Detector
	subscriptions *subscription.Service
	tokens        *token.Service
	metrics       *obs.Metrics
	logger        *slog.Logger

	workers sync.WaitGroup
	cancel  context.CancelFunc
	stopped atomic.Bool
}

func New(
	outbox ports.DetectionOutbox,
	detectorService *detector.Detector,
	subscriptions *subscription.Service,
	tokens *token.Service,
	metrics *obs.Metrics,
	logger *slog.Logger,
) *Pipeline {
	return &Pipeline{
		outbox:        outbox,
		detector:      detectorService,
		subscriptions: subscriptions,
		tokens:        tokens,
		metrics:       metrics,
		logger:        logger,
	}
}

func (p *Pipeline) Start(ctx context.Context, workerCount int) {
	workerContext, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for range workerCount {
		p.workers.Add(1)
		go p.workerLoop(workerContext)
	}
	p.workers.Add(1)
	go p.maintenanceLoop(workerContext)

	p.logger.Info("detection pipeline started", "workers", workerCount)
}

func (p *Pipeline) Stop() {
	if p.stopped.CompareAndSwap(false, true) {
		if closer, ok := p.outbox.(interface{ Close() }); ok {
			closer.Close()
		}
		if p.cancel != nil {
			p.cancel()
		}
	}
	p.workers.Wait()
	p.logger.Info("detection pipeline stopped")
}

func (p *Pipeline) workerLoop(ctx context.Context) {
	defer p.workers.Done()
	for {
		items, err := p.outbox.ClaimBatch(ctx, claimBatchSize)

		for _, item := range items {
			p.processItem(ctx, item)
		}
		switch {
		case errors.Is(err, ports.ErrOutboxClosed), errors.Is(err, context.Canceled):
			return
		case err != nil:
			p.logger.Error("claim detection jobs failed", "error", err)
			if !sleepContext(ctx, claimRetryDelay) {
				return
			}
			continue
		}
		if len(items) == 0 {

			if !sleepContext(ctx, claimRetryDelay) {
				return
			}
		}
	}
}

func (p *Pipeline) processItem(ctx context.Context, item ports.OutboxItem) {
	p.metrics.CountDetectionRun()

	spanContext, span := tracer.Start(ctx, "pipeline.detect",
		trace.WithAttributes(attribute.String("user_id", string(item.UserID))))
	defer span.End()

	if err := p.runDetection(spanContext, item.UserID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "detection failed")
		p.logger.Error("detection failed, job will be retried",
			"user_id", string(item.UserID),
			"attempt", item.Attempts,
			"error", err,
		)
		if err := p.outbox.Fail(ctx, item); err != nil {
			p.logger.Error("mark detection job failed", "job_id", item.ID, "error", err)
		}
		return
	}
	if err := p.outbox.Complete(ctx, []int64{item.ID}); err != nil {
		p.logger.Error("complete detection job", "job_id", item.ID, "error", err)
	}
}

func (p *Pipeline) runDetection(ctx context.Context, userID domain.UserID) error {
	detections, err := p.detector.Detect(ctx, userID)
	if err != nil {
		return err
	}
	for _, detected := range detections {
		if err := p.applyDetection(ctx, detected); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) applyDetection(ctx context.Context, detected domain.DetectedSubscription) error {
	virtualToken, err := p.tokens.EnsureToken(ctx, detected.UserID, detected.MerchantID)
	if err != nil {
		p.logger.Error("virtual token provisioning failed",
			"user_id", string(detected.UserID),
			"merchant_id", string(detected.MerchantID),
			"error", err,
		)
		return err
	}
	subscription, err := p.subscriptions.ApplyDetection(ctx, detected, string(virtualToken.ID))
	if err != nil {
		p.logger.Error("subscription persistence failed",
			"user_id", string(detected.UserID),
			"merchant_id", string(detected.MerchantID),
			"error", err,
		)
		return err
	}
	p.metrics.CountSubscriptionDetected(string(subscription.State))
	p.logger.Info("subscription detected",
		"user_id", string(detected.UserID),
		"merchant_id", string(detected.MerchantID),
		"subscription_id", string(subscription.ID),
		"state", string(subscription.State),
		"billing_window", string(subscription.BillingWindow),
	)
	return nil
}

func (p *Pipeline) maintenanceLoop(ctx context.Context) {
	defer p.workers.Done()
	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		requeued, err := p.outbox.RequeueStale(ctx, staleItemAfter)
		if err != nil {
			p.logger.Error("requeue stale detection jobs failed", "error", err)
		} else if requeued > 0 {
			p.logger.Warn("requeued stale detection jobs", "count", requeued)
		}
		if pending, err := p.outbox.PendingCount(ctx); err == nil {
			p.metrics.SetQueueDepth(int(pending))
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
