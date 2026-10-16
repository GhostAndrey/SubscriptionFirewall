package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/httpapi"
	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/obs"
)

const defaultAddress = ":8080"
const defaultPort = ":8080"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck())
	}
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if len(cfg.apiKeys) == 0 && !cfg.allowNoAuth {
		return fmt.Errorf("no api keys configured: set SUBSCRIPTION_FIREWALL_API_KEYS, or set SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH=true to start unauthenticated")
	}

	applicationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	logger := slog.New(obs.NewSanitizingHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.logLevel})))
	slog.SetDefault(logger)

	shutdownTracing, err := obs.SetupTracing(applicationContext, cfg.otlpEndpoint, cfg.otlpInsecure)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer shutdownTracingOnExit(shutdownTracing, cfg.shutdownTimeout, logger)
	if shutdownTracing != nil {
		logger.Info("tracing enabled", "otlp_endpoint", cfg.otlpEndpoint)
	}

	metrics := obs.NewMetrics(prometheus.DefaultRegisterer)
	clock := memory.Clock{}

	application, err := build(applicationContext, cfg, logger, metrics, clock)
	if err != nil {
		return err
	}
	application.start(applicationContext, cfg)

	apiServer, err := httpapi.NewServer(
		httpConfig(cfg, clock, readinessOf(application)),
		application.subscriptions,
		application.tokens,
		application.lifecycle,
		application.outbox,
		metrics,
		logger,
	)
	if err != nil {
		application.close()
		return fmt.Errorf("build http server: %w", err)
	}

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- apiServer.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		application.close()
		return err
	case <-applicationContext.Done():
		logger.Info("shutdown signal received")
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancelShutdown()
	if err := apiServer.Shutdown(shutdownContext); err != nil {
		logger.Error("http server shutdown failed", "error", err)
	}

	application.close()
	logger.Info("shutdown complete")
	return nil
}

func shutdownTracingOnExit(shutdownTracing func(context.Context) error, timeout time.Duration, logger *slog.Logger) {
	if shutdownTracing == nil {
		return
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := shutdownTracing(shutdownContext); err != nil {
		logger.Error("tracer provider shutdown failed", "error", err)
	}
}

func readinessOf(application *services) func(ctx context.Context) error {
	return application.readiness
}
