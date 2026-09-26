package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"subscriptionfirewall/internal/detector"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

type Pipeline struct {
	detector      *detector.Detector
	subscriptions *subscription.Service
	tokens        *token.Service
	metrics       *obs.Metrics
	logger        *slog.Logger
	queue         chan domain.UserID
	workers       sync.WaitGroup
	queueClosed   sync.Once
}

func New(
	detectorService *detector.Detector,
	subscriptions *subscription.Service,
	tokens *token.Service,
	metrics *obs.Metrics,
	logger *slog.Logger,
	queueCapacity int,
) *Pipeline {
	return &Pipeline{
		detector:      detectorService,
		subscriptions: subscriptions,
		tokens:        tokens,
		metrics:       metrics,
		logger:        logger,
		queue:         make(chan domain.UserID, queueCapacity),
	}
}

func (p *Pipeline) Enqueue(userID domain.UserID) {
	select {
	case p.queue <- userID:
	default:
		p.metrics.CountEnqueueDropped()
		p.logger.Warn("detection queue full, user dropped", "user_id", string(userID))
	}
}

func (p *Pipeline) Start(ctx context.Context, workerCount int) {
	for range workerCount {
		p.workers.Add(1)
		go p.workerLoop(ctx)
	}
	p.logger.Info("detection pipeline started", "workers", workerCount)
}

func (p *Pipeline) Stop() {
	p.queueClosed.Do(func() { close(p.queue) })
	p.workers.Wait()
	p.logger.Info("detection pipeline stopped")
}

func (p *Pipeline) workerLoop(ctx context.Context) {
	defer p.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case userID, ok := <-p.queue:
			if !ok {
				return
			}
			p.processUser(ctx, userID)
		}
	}
}

func (p *Pipeline) processUser(ctx context.Context, userID domain.UserID) {
	p.metrics.CountDetectionRun()

	detections, err := p.detector.Detect(ctx, userID)
	if err != nil {
		p.logger.Error("detection failed", "user_id", string(userID), "error", err)
		return
	}
	for _, detected := range detections {
		p.applyDetection(ctx, detected)
	}
}

func (p *Pipeline) applyDetection(ctx context.Context, detected domain.DetectedSubscription) {
	virtualToken, err := p.tokens.EnsureToken(ctx, detected.UserID, detected.MerchantID)
	if err != nil {
		p.logger.Error("virtual token provisioning failed",
			"user_id", string(detected.UserID),
			"merchant_id", string(detected.MerchantID),
			"error", err,
		)
		return
	}
	subscription, err := p.subscriptions.ApplyDetection(ctx, detected, string(virtualToken.ID))
	if err != nil {
		p.logger.Error("subscription persistence failed",
			"user_id", string(detected.UserID),
			"merchant_id", string(detected.MerchantID),
			"error", err,
		)
		return
	}
	p.metrics.CountSubscriptionDetected(string(subscription.State))
	p.logger.Info("subscription detected",
		"user_id", string(detected.UserID),
		"merchant_id", string(detected.MerchantID),
		"subscription_id", string(subscription.ID),
		"state", string(subscription.State),
		"billing_window", string(subscription.BillingWindow),
	)
}
