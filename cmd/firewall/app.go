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

// storage values for SUBSCRIPTION_FIREWALL_STORAGE: auto picks mysql when a
// DSN is set and memory otherwise; memory and mysql pin the choice explicitly.
const (
	storageAuto   = "auto"
	storageMemory = "memory"
	storageMySQL  = "mysql"
)

// Card providers selectable through SUBSCRIPTION_FIREWALL_ISSUER.
const issuerSimulated = "simulated"

// services is the assembled application graph. Every dependency is wired here
// and nowhere else, so the composition stays readable as adapters grow.
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

// close releases everything the process started, in the reverse order of
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

// build assembles the application graph described by cfg.
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

// start launches the background workers bound to ctx.
func (s *services) start(ctx context.Context, cfg config) {
	s.syncer.Start(ctx, 1)
	s.pipeline.Start(ctx, cfg.workerCount)
	s.sweeper.start(ctx)
}

// buildCardIssuer resolves the configured card provider. Every provider is
// wrapped in the resilient decorator, so a misbehaving issuer cannot stall card
// provisioning. Selecting the provider through configuration keeps the seam
// explicit instead of hard-wiring a stub into the composition root.
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

// httpConfig projects the process configuration onto the transport layer.
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

// configuredAddress returns the listen address, falling back to the default
// when the variable is unset or empty.
func configuredAddress() string {
	if configured := os.Getenv("SUBSCRIPTION_FIREWALL_ADDRESS"); configured != "" {
		return configured
	}
	return defaultAddress
}

// addressPort extracts the listen port for the in-process healthcheck. It
// falls back to the default whenever the address cannot be understood, so a
// malformed SUBSCRIPTION_FIREWALL_ADDRESS degrades to a working probe instead
// of an unparsable URL.
func addressPort(address string) string {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return defaultPort
	}
	if parsed, err := strconv.Atoi(port); err != nil || parsed <= 0 || parsed > 65535 {
		return defaultPort
	}
	return ":" + port
}
