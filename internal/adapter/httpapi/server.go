package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"subscriptionfirewall/internal/lifecycle"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
	"subscriptionfirewall/internal/version"
)

type Server struct {
	subscriptions *subscription.Service
	tokens        *token.Service
	lifecycle     *lifecycle.Service
	outbox        ports.DetectionOutbox
	metrics       *obs.Metrics
	logger        *slog.Logger
	clock         ports.Clock
	readiness     func(ctx context.Context) error
	redis         *redis.Client
	http          *http.Server
}

type Config struct {
	Address         string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	// IdleTimeout bounds how long a keep-alive connection may sit unused
	// between requests; zero falls back to ReadTimeout.
	IdleTimeout time.Duration
	APIKeys     []string
	// AllowNoAuth starts the server without API key verification. It must be
	// set explicitly for every environment where keys are absent, because
	// without either keys or this flag NewServer refuses to start instead of
	// serving an open API.
	AllowNoAuth bool
	RateLimit   float64
	RateBurst   int
	// Clock is the time source used by request validation; nil falls back to
	// the system clock.
	Clock ports.Clock
	// Readiness reports whether the service can serve traffic; nil means always ready.
	Readiness func(ctx context.Context) error
	// Tracing wraps the mux with OpenTelemetry HTTP spans. OTLP export is
	// configured at process start; without a collector spans are no-ops.
	Tracing bool
	// RedisAddr, when set, moves the per-IP rate limit into Redis so it is
	// shared by all replicas; empty keeps the in-process limiter.
	RedisAddr     string
	RedisPassword string
}

func NewServer(
	config Config,
	subscriptions *subscription.Service,
	tokens *token.Service,
	lifecycleService *lifecycle.Service,
	outbox ports.DetectionOutbox,
	metrics *obs.Metrics,
	logger *slog.Logger,
) (*Server, error) {
	authenticator, err := newAPIKeyAuthenticator(config.APIKeys)
	if err != nil {
		return nil, fmt.Errorf("httpapi: %w", err)
	}
	if !authenticator.enabled() && !config.AllowNoAuth {
		return nil, errors.New("httpapi: no api keys configured; set APIKeys or AllowNoAuth")
	}
	if !authenticator.enabled() {
		logger.Warn("api authentication disabled: started with AllowNoAuth and no api keys")
	}

	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	idleTimeout := config.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = config.ReadTimeout
	}
	server := &Server{
		subscriptions: subscriptions,
		tokens:        tokens,
		lifecycle:     lifecycleService,
		outbox:        outbox,
		metrics:       metrics,
		logger:        logger,
		clock:         clock,
		readiness:     config.Readiness,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.handleHealth)
	mux.HandleFunc("GET /readyz", server.handleReady)
	mux.HandleFunc("GET /version", server.handleVersion)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /v1/transactions", server.handleIngestTransaction)
	mux.HandleFunc("GET /v1/users/{user_id}/subscriptions", server.handleListSubscriptions)
	mux.HandleFunc("GET /v1/subscriptions/{id}", server.handleGetSubscription)
	mux.HandleFunc("POST /v1/subscriptions/{id}/freeze", server.handleFreezeSubscription)
	mux.HandleFunc("POST /v1/subscriptions/{id}/reactivate", server.handleReactivateSubscription)
	mux.HandleFunc("POST /v1/subscriptions/{id}/terminate", server.handleTerminateSubscription)
	mux.HandleFunc("GET /v1/users/{user_id}/tokens", server.handleListTokens)
	mux.HandleFunc("GET /v1/tokens/{id}", server.handleGetToken)
	mux.HandleFunc("POST /v1/tokens/{id}/freeze", server.handleFreezeToken)
	mux.HandleFunc("POST /v1/tokens/{id}/reactivate", server.handleReactivateToken)
	mux.HandleFunc("POST /v1/tokens/{id}/terminate", server.handleTerminateToken)
	mux.HandleFunc("POST /v1/tokens/{id}/authorize", server.handleAuthorizeCharge)

	limiterMiddleware := func(next http.Handler) http.Handler { return next }
	if limiter := newIPRateLimiter(config.RateLimit, config.RateBurst); limiter != nil {
		limiterMiddleware = limiter.middleware
	}
	if config.RedisAddr != "" {
		redisClient := redis.NewClient(&redis.Options{
			Addr:     config.RedisAddr,
			Password: config.RedisPassword,
		})
		if redisLimiter := newRedisRateLimiter(redisClient, int(config.RateLimit), config.RateBurst, logger); redisLimiter != nil {
			limiterMiddleware = redisLimiter.middleware
			server.redis = redisClient
		} else {
			_ = redisClient.Close()
		}
	}

	var instrumented http.Handler = mux
	if config.Tracing {
		instrumented = otelhttp.NewHandler(mux, "http",
			otelhttp.WithFilter(func(request *http.Request) bool {
				return !isPublicPath(request.URL.Path)
			}),
		)
	}

	handler := server.requestIDMiddleware(observationMiddleware(logger, metrics,
		recoveryMiddleware(logger,
			limiterMiddleware(
				authenticator.middleware(logger, instrumented)))))
	server.http = &http.Server{
		Addr:              config.Address,
		Handler:           handler,
		ReadHeaderTimeout: config.ReadTimeout,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       idleTimeout,
	}
	return server, nil
}

func (s *Server) ListenAndServe() error {
	s.logger.Info("http server listening", "address", s.http.Addr)
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	// The Redis client backs the rate limiter, so it must outlive the drain:
	// closing it first would make in-flight requests fail the limiter check.
	err := s.http.Shutdown(ctx)
	if s.redis != nil {
		_ = s.redis.Close()
	}
	return err
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.readiness != nil {
		if err := s.readiness(r.Context()); err != nil {
			s.logger.Error("readiness check failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version":    version.Version,
		"commit":     version.Commit,
		"build_date": version.BuildDate,
	})
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
