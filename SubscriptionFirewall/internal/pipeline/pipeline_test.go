package pipeline

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/detector"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"

	"github.com/prometheus/client_golang/prometheus"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(discardWriter{}, nil))
}

func TestPipelineDetectsAndStopsGracefully(t *testing.T) {
	now := time.Now()
	clock := fixedClock{now: now}
	transactions := memory.NewTransactionRepository()
	userID := domain.UserID("user-1")

	subscriptions := memory.NewSubscriptionRepository()
	tokens := memory.NewVirtualTokenRepository()
	subscriptionService := subscription.NewService(subscriptions, clock)
	tokenService := token.NewService(tokens, stubIssuer{}, clock)
	outbox := memory.NewDetectionOutbox(transactions, obs.NewMetrics(prometheus.NewRegistry()), testLogger(), 16)

	pipelineService := New(
		outbox,
		detector.New(transactions, clock, detector.DefaultConfig()),
		subscriptionService,
		tokenService,
		obs.NewMetrics(prometheus.NewRegistry()),
		testLogger(),
	)

	pipelineService.Start(context.Background(), 2)

	for i := range 4 {
		transaction := domain.Transaction{
			ID: domain.TransactionID(string(rune('a' + i))), UserID: userID,
			MerchantID: "netflix", MerchantName: "Netflix",
			MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD",
			AuthorizedAt: now.AddDate(0, 0, -30*(4-i)),
		}
		if err := outbox.EnqueueWithTransaction(context.Background(), transaction); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	pipelineService.Stop()

	listed, err := subscriptions.ListByUser(context.Background(), userID)
	if err != nil {
		t.Fatalf("list subscriptions: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(listed))
	}
	if listed[0].State != domain.SubscriptionActive {
		t.Errorf("expected Active, got %s", listed[0].State)
	}
	provisioned, err := tokens.FindByUserAndMerchant(context.Background(), userID, "netflix")
	if err != nil {
		t.Fatalf("provisioned token missing: %v", err)
	}
	if provisioned.MerchantID != "netflix" {
		t.Errorf("expected token bound to merchant, got %s", provisioned.MerchantID)
	}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type stubIssuer struct{}

func (stubIssuer) Issue(_ context.Context, userID domain.UserID, merchantID domain.MerchantID) (ports.IssuedCard, error) {
	return ports.IssuedCard{
		TokenID:      domain.VirtualTokenID("vtok-test"),
		MaskedPAN:    "411111******1234",
		MonthlyLimit: 100_000,
		Currency:     "USD",
	}, nil
}

type discardWriter struct{}

func (discardWriter) Write(payload []byte) (int, error) { return len(payload), nil }
