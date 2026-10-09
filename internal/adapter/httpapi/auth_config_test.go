package httpapi

import (
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/issuer"
	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/lifecycle"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

type serverFixture struct {
	clock         memory.Clock
	transactions  *memory.TransactionRepository
	subscriptions *memory.SubscriptionRepository
	tokens        *memory.VirtualTokenRepository
	audit         *memory.AuditLog
	metrics       *obs.Metrics
}

func newServerFixture() serverFixture {
	return serverFixture{
		clock:         memory.Clock{},
		transactions:  memory.NewTransactionRepository(),
		subscriptions: memory.NewSubscriptionRepository(),
		tokens:        memory.NewVirtualTokenRepository(),
		audit:         memory.NewAuditLog(),
		metrics:       obs.NewMetrics(prometheus.NewRegistry()),
	}
}

func (f serverFixture) buildServer(t *testing.T, config Config) (*Server, error) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	subscriptionService := subscription.NewService(f.subscriptions, f.clock)
	tokenService := token.NewService(f.tokens, issuer.NewSimulatedCardIssuer(100_000), f.clock, token.Options{})
	lifecycleService := lifecycle.NewService(subscriptionService, tokenService, lifecycle.Options{
		Queue:   memory.NewLifecycleSyncQueue(),
		Audit:   f.audit,
		Metrics: f.metrics,
		Logger:  logger,
	})

	return NewServer(
		config,
		f.transactions,
		subscriptionService,
		tokenService,
		lifecycleService,
		memory.NewDetectionOutbox(f.transactions, f.metrics, logger, 16),
		f.metrics,
		logger,
	)
}

func buildServer(t *testing.T, config Config) (*Server, error) {
	t.Helper()

	return newServerFixture().buildServer(t, config)
}

func TestNewServerRefusesToStartWithoutKeys(t *testing.T) {
	for name, keys := range map[string][]string{
		"nil keys":   nil,
		"empty keys": {},
		"blank keys": {"   "},
	} {
		t.Run(name, func(t *testing.T) {
			server, err := buildServer(t, Config{APIKeys: keys})
			if err == nil {
				t.Fatal("expected NewServer to fail without api keys")
			}
			if server != nil {
				t.Fatal("expected no server to be returned on failure")
			}
		})
	}
}

func TestNewServerStartsWithAllowNoAuth(t *testing.T) {
	server, err := buildServer(t, Config{AllowNoAuth: true})
	if err != nil {
		t.Fatalf("expected AllowNoAuth to permit startup, got %v", err)
	}
	if server == nil {
		t.Fatal("expected a server to be returned")
	}
}

func TestNewServerStartsWithKeys(t *testing.T) {
	server, err := buildServer(t, Config{APIKeys: []string{"key:full"}})
	if err != nil {
		t.Fatalf("expected startup with api keys, got %v", err)
	}
	if server == nil {
		t.Fatal("expected a server to be returned")
	}
}
