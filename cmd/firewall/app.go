package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"

	"subscriptionfirewall/internal/adapter/httpapi"
	"subscriptionfirewall/internal/adapter/issuer"
	"subscriptionfirewall/internal/detector"
	"subscriptionfirewall/internal/lifecycle"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/pipeline"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

const (
	storageAuto   = "auto"
	storageMemory = "memory"
	storageMySQL  = "mysql"
)

const issuerSimulated = "simulated"

type services struct {
	detector      *detector.Detector
	subscriptions *subscription.Service
	tokens        *token.Service
	lifecycle     *lifecycle.Service
	outbox        ports.DetectionOutbox
	metrics       *obs.Metrics
	logger        *slog.Logger
	syncer        *lifecycle.Syncer
	pipeline      *pipeline.Pipeline
	sweeper       *subscriptionSweeper
	readiness     func(ctx context.Context) error
	closeDatabase func()
}

// close releases everything the process started in the reverse order of
// construction: background work first, storage last, so no worker can observe
// a closed connection while draining.
func (s *services) close() {
	if s.sweeper != nil {
		s.sweeper.stop()
	}
	s.pipeline.Stop()
	s.syncer.Stop()
	s.closeDatabase()
}

func build(
	ctx context.Context,
	cfg config,
	logger *slog.Logger,
	metrics *obs.Metrics,
	clock ports.Clock,
) (*services, error) {
	storage, err := openStorage(ctx, cfg, logger, metrics)
	if err != nil {
		return nil, err
	}

	cardIssuer, err := buildCardIssuer(cfg, logger)
	if err != nil {
		storage.close()
		return nil, err
	}

	assembled := &services{
		detector:      detector.New(storage.transactions, clock, detector.DefaultConfig()),
		subscriptions: subscription.NewService(storage.subscriptions, clock),
		tokens: token.NewService(storage.tokens, cardIssuer, clock, token.Options{
			IssuanceTimeout: cfg.issuanceTimeout,
			Logger:          logger,
		}),
		outbox:        storage.outbox,
		metrics:       metrics,
		logger:        logger,
		readiness:     storage.readiness,
		closeDatabase: storage.close,
	}
	assembled.lifecycle = lifecycle.NewService(assembled.subscriptions, assembled.tokens, lifecycle.Options{
		Queue:   storage.lifecycleQueue,
		Audit:   storage.audit,
		Metrics: metrics,
		Logger:  logger,
	})
	assembled.syncer = lifecycle.NewSyncer(assembled.lifecycle, storage.lifecycleQueue, metrics, logger)
	assembled.pipeline = pipeline.New(storage.outbox, assembled.detector, assembled.subscriptions, assembled.tokens, metrics, logger)
	assembled.sweeper = newSubscriptionSweeper(
		storage.database, assembled.subscriptions, assembled.lifecycle, metrics, logger, cfg.sweepInterval)
	return assembled, nil
}

func (s *services) start(ctx context.Context, cfg config) {
	s.syncer.Start(ctx, 1)
	s.pipeline.Start(ctx, cfg.workerCount)
	s.sweeper.start(ctx)
}

// buildCardIssuer resolves the configured card provider and wraps it in the
// resilient decorator, so a misbehaving issuer cannot stall card provisioning.
func buildCardIssuer(cfg config, logger *slog.Logger) (ports.VirtualCardIssuer, error) {
	var inner ports.VirtualCardIssuer
	switch cfg.issuer {
	case issuerSimulated:
		logger.Warn("using the simulated card issuer: cards are not real and must not be used in production",
			"issuer", cfg.issuer,
			"monthly_limit_minor", cfg.issuerMonthlyLimitMinor,
		)
		inner = issuer.NewSimulatedCardIssuer(cfg.issuerMonthlyLimitMinor)
	default:
		return nil, fmt.Errorf("unknown card issuer %q: currently only %q exists", cfg.issuer, issuerSimulated)
	}

	return issuer.NewResilientIssuer(inner, issuer.ResilientConfig{
		Timeout:          cfg.issuerTimeout,
		Retries:          cfg.issuerRetries,
		BreakerThreshold: cfg.issuerBreakerThreshold,
		BreakerCooldown:  cfg.issuerBreakerCooldown,
	}), nil
}

func httpConfig(cfg config, clock ports.Clock, readiness func(ctx context.Context) error) httpapi.Config {
	return httpapi.Config{
		Address:         cfg.address,
		ReadTimeout:     cfg.readTimeout,
		WriteTimeout:    cfg.writeTimeout,
		ShutdownTimeout: cfg.shutdownTimeout,
		IdleTimeout:     cfg.idleTimeout,
		APIKeys:         cfg.apiKeys,
		AllowNoAuth:     cfg.allowNoAuth,
		RateLimit:       cfg.rateLimit,
		RateBurst:       cfg.rateBurst,
		Clock:           clock,
		Readiness:       readiness,
		Tracing:         cfg.otlpEndpoint != "",
		RedisAddr:       cfg.redisAddr,
		RedisPassword:   cfg.redisPassword,
	}
}

func configuredAddress() string {
	if configured := os.Getenv("SUBSCRIPTION_FIREWALL_ADDRESS"); configured != "" {
		return configured
	}
	return defaultAddress
}

// addressPort extracts the listen port for the in-process healthcheck and
// falls back to the default when the address cannot be understood.
func addressPort(address string) string {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return defaultAddress
	}
	if parsed, err := strconv.Atoi(port); err != nil || parsed <= 0 || parsed > 65535 {
		return defaultAddress
	}
	return ":" + port
}
