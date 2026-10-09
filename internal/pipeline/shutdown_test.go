package pipeline

import (
	"context"
	"errors"
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

func TestStopDrainsQueuedUsers(t *testing.T) {
	transactions := memory.NewTransactionRepository()
	userID := domain.UserID("user-drain")
	now := time.Now()

	outbox := memory.NewDetectionOutbox(transactions, obs.NewMetrics(prometheus.NewRegistry()), testLogger(), 16)
	for i := range 4 {
		if err := outbox.EnqueueWithTransaction(context.Background(), domain.Transaction{
			ID:           domain.TransactionID(string(rune('a' + i))),
			UserID:       userID,
			MerchantID:   "netflix",
			MerchantName: "Netflix",
			MCC:          domain.MCCRecurringBilling,
			AmountMinor:  1500,
			Currency:     "USD",
			AuthorizedAt: now.AddDate(0, 0, -30*(4-i)),
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	subscriptions := memory.NewSubscriptionRepository()
	pipelineService := New(
		outbox,
		detector.New(transactions, memory.Clock{}, detector.DefaultConfig()),
		subscription.NewService(subscriptions, memory.Clock{}),
		token.NewService(memory.NewVirtualTokenRepository(), stubIssuer{}, memory.Clock{}, token.Options{}),
		obs.NewMetrics(prometheus.NewRegistry()),
		testLogger(),
	)
	pipelineService.Start(context.Background(), 2)
	pipelineService.Stop()

	listed, err := subscriptions.ListByUserPage(context.Background(), userID, ports.Page{})
	if err != nil {
		t.Fatalf("list subscriptions: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected queued users to be processed before Stop returned, got %d subscriptions", len(listed))
	}
}

func TestStopIsIdempotentAndOutboxRejectsAfterStop(t *testing.T) {
	transactions := memory.NewTransactionRepository()
	outbox := memory.NewDetectionOutbox(transactions, obs.NewMetrics(prometheus.NewRegistry()), testLogger(), 16)
	pipelineService := New(
		outbox,
		detector.New(transactions, memory.Clock{}, detector.DefaultConfig()),
		subscription.NewService(memory.NewSubscriptionRepository(), memory.Clock{}),
		token.NewService(memory.NewVirtualTokenRepository(), stubIssuer{}, memory.Clock{}, token.Options{}),
		obs.NewMetrics(prometheus.NewRegistry()),
		testLogger(),
	)

	pipelineService.Start(context.Background(), 1)
	pipelineService.Stop()
	pipelineService.Stop()

	err := outbox.EnqueueWithTransaction(context.Background(), domain.Transaction{
		ID: "tx-late", UserID: "user-late", MerchantID: "netflix", MerchantName: "Netflix",
		MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD",
		AuthorizedAt: time.Now(),
	})
	if !errors.Is(err, ports.ErrOutboxClosed) {
		t.Fatalf("expected ErrOutboxClosed after stop, got %v", err)
	}
}
