package pipeline

import (
	"context"
	"log/slog"
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
	"subscriptionfirewall/internal/worker"
)

const staleItemAfter = 5 * time.Minute

var tracer = otel.Tracer("subscriptionfirewall/pipeline")

// Pipeline turns ingested transactions into detected subscriptions: it claims
// queued users, runs the detector and provisions the virtual cards behind each
// detection.
type Pipeline struct {
	outbox        ports.DetectionOutbox
	detector      *detector.Detector
	subscriptions *subscription.Service
	tokens        *token.Service
	metrics       *obs.Metrics
	logger        *slog.Logger
	runner        *worker.Runner[ports.OutboxItem]
}

func New(
	outbox ports.DetectionOutbox,
	detectorService *detector.Detector,
	subscriptions *subscription.Service,
	tokens *token.Service,
	metrics *obs.Metrics,
	logger *slog.Logger,
) *Pipeline {
	p := &Pipeline{
		outbox:        outbox,
		detector:      detectorService,
		subscriptions: subscriptions,
		tokens:        tokens,
		metrics:       metrics,
		logger:        logger,
	}
	p.runner = worker.New(worker.Options[ports.OutboxItem]{
		Claim:               outbox.ClaimBatch,
		Complete:            outbox.Complete,
		Process:             p.processItem,
		Maintenance:         p.maintain,
		MaintenanceInterval: time.Minute,
		Name:                "detection pipeline",
		Logger:              logger,
	})
	return p
}

func (p *Pipeline) Start(ctx context.Context, workerCount int) {
	p.runner.Start(ctx, workerCount)
}

// Stop closes the in-memory outbox first, which unblocks workers parked on the
// queue, then drains the runners before returning.
func (p *Pipeline) Stop() {
	if closer, ok := p.outbox.(interface{ Close() }); ok {
		closer.Close()
	}
	p.runner.Stop()
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

func (p *Pipeline) maintain(ctx context.Context) {
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
