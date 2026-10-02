package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
	"subscriptionfirewall/internal/version"
)

type Server struct {
	transactions  ports.TransactionRepository
	subscriptions *subscription.Service
	tokens        *token.Service
	outbox        ports.DetectionOutbox
	audit         ports.AuditLog
	metrics       *obs.Metrics
	logger        *slog.Logger
	readiness     func(ctx context.Context) error
	redis         *redis.Client
	http          *http.Server
}

type Config struct {
	Address         string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	APIKeys         []string
	RateLimit       float64
	RateBurst       int
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
	transactions ports.TransactionRepository,
	subscriptions *subscription.Service,
	tokens *token.Service,
	outbox ports.DetectionOutbox,
	audit ports.AuditLog,
	metrics *obs.Metrics,
	logger *slog.Logger,
) *Server {
	server := &Server{
		transactions:  transactions,
		subscriptions: subscriptions,
		tokens:        tokens,
		outbox:        outbox,
		audit:         audit,
		metrics:       metrics,
		logger:        logger,
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

	authenticator := newAPIKeyAuthenticator(config.APIKeys)
	if !authenticator.enabled() {
		logger.Warn("api authentication disabled: no api keys configured")
	}

	limiterMiddleware := func(next http.Handler) http.Handler { return next }
	if limiter := newIPRateLimiter(config.RateLimit, config.RateBurst); limiter != nil {
		limiterMiddleware = limiter.middleware
	}
	if config.RedisAddr != "" {
		redisClient := redis.NewClient(&redis.Options{
			Addr:     config.RedisAddr,
			Password: config.RedisPassword,
		})
		if redisLimiter := newRedisRateLimiter(redisClient, int(config.RateLimit), logger); redisLimiter != nil {
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
				return !publicPaths[strings.TrimSuffix(request.URL.Path, "/")]
			}),
		)
	}

	handler := server.requestIDMiddleware(loggingMiddleware(logger, recoveryMiddleware(logger,
		metricsMiddleware(metrics,
			limiterMiddleware(
				authenticator.middleware(logger, instrumented))))))
	server.http = &http.Server{
		Addr:         config.Address,
		Handler:      handler,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}
	return server
}

func (s *Server) ListenAndServe() error {
	s.logger.Info("http server listening", "address", s.http.Addr)
	if err := s.http.ListenAndServe(); err != nil {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.redis != nil {
		_ = s.redis.Close()
	}
	return s.http.Shutdown(ctx)
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
