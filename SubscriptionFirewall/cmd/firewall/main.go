package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/httpapi"
	"subscriptionfirewall/internal/adapter/issuer"
	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/detector"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/pipeline"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

const (
	defaultAddress          = ":8080"
	ingestQueueCapacity     = 1024
	detectionWorkerCount    = 4
	issuerMonthlyLimitMinor = 100_000
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	applicationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	logger := slog.New(obs.NewSanitizingHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	slog.SetDefault(logger)

	metrics := obs.NewMetrics(prometheus.DefaultRegisterer)
	clock := memory.Clock{}

	transactionRepository := memory.NewTransactionRepository()
	subscriptionRepository := memory.NewSubscriptionRepository()
	tokenRepository := memory.NewVirtualTokenRepository()

	detectorService := detector.New(transactionRepository, clock, detector.DefaultConfig())
	subscriptionService := subscription.NewService(subscriptionRepository, clock)
	tokenService := token.NewService(tokenRepository, issuer.NewSimulatedCardIssuer(issuerMonthlyLimitMinor), clock)
	pipelineService := pipeline.New(detectorService, subscriptionService, tokenService, metrics, logger, ingestQueueCapacity)
	pipelineService.Start(applicationContext, detectionWorkerCount)

	apiServer := httpapi.NewServer(
		httpapi.Config{
			Address:         address(),
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    10 * time.Second,
			ShutdownTimeout: 10 * time.Second,
		},
		transactionRepository,
		subscriptionService,
		tokenService,
		pipelineService,
		metrics,
		logger,
	)

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- apiServer.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		pipelineService.Stop()
		return err
	case <-applicationContext.Done():
		logger.Info("shutdown signal received")
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	if err := apiServer.Shutdown(shutdownContext); err != nil {
		logger.Error("http server shutdown failed", "error", err)
	}
	pipelineService.Stop()
	logger.Info("shutdown complete")
	return nil
}

func address() string {
	if configured := os.Getenv("SUBSCRIPTION_FIREWALL_ADDRESS"); configured != "" {
		return configured
	}
	return defaultAddress
}
