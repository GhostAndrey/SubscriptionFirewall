package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/pipeline"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

type Server struct {
	transactions  ports.TransactionRepository
	subscriptions *subscription.Service
	tokens        *token.Service
	pipeline      *pipeline.Pipeline
	metrics       *obs.Metrics
	logger        *slog.Logger
	http          *http.Server
}

type Config struct {
	Address         string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

func NewServer(
	config Config,
	transactions ports.TransactionRepository,
	subscriptions *subscription.Service,
	tokens *token.Service,
	pipelineService *pipeline.Pipeline,
	metrics *obs.Metrics,
	logger *slog.Logger,
) *Server {
	server := &Server{
		transactions:  transactions,
		subscriptions: subscriptions,
		tokens:        tokens,
		pipeline:      pipelineService,
		metrics:       metrics,
		logger:        logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /v1/transactions", server.handleIngestTransaction)
	mux.HandleFunc("GET /v1/users/{user_id}/subscriptions", server.handleListSubscriptions)
	mux.HandleFunc("POST /v1/subscriptions/{id}/freeze", server.handleFreezeSubscription)
	mux.HandleFunc("POST /v1/subscriptions/{id}/reactivate", server.handleReactivateSubscription)
	mux.HandleFunc("POST /v1/subscriptions/{id}/terminate", server.handleTerminateSubscription)
	mux.HandleFunc("GET /v1/users/{user_id}/tokens", server.handleListTokens)
	mux.HandleFunc("POST /v1/tokens/{id}/freeze", server.handleFreezeToken)
	mux.HandleFunc("POST /v1/tokens/{id}/reactivate", server.handleReactivateToken)
	mux.HandleFunc("POST /v1/tokens/{id}/terminate", server.handleTerminateToken)
	mux.HandleFunc("POST /v1/tokens/{id}/authorize", server.handleAuthorizeCharge)

	handler := loggingMiddleware(logger, recoveryMiddleware(logger, mux))
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
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
