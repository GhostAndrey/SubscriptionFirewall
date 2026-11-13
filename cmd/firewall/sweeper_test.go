package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/issuer"
	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/lifecycle"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

func newTestSweeper(t *testing.T, interval time.Duration) *subscriptionSweeper {
	t.Helper()

	subscriptions := memory.NewSubscriptionRepository()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	subs := subscription.NewService(subscriptions, memory.Clock{})
	tokens := token.NewService(
		memory.NewVirtualTokenRepository(),
		issuer.NewSimulatedCardIssuer(10_000),
		memory.Clock{},
		token.Options{Logger: logger},
	)
	return newSubscriptionSweeper(
		nil,
		subs,
		lifecycle.NewService(subs, tokens, lifecycle.Options{Queue: memory.NewLifecycleSyncQueue(), Logger: logger}),
		nil,
		obs.NewMetrics(prometheus.NewRegistry()),
		logger,
		interval,
	)
}

func TestSweeperStopWithoutStartDoesNotBlock(t *testing.T) {
	sweeper := newTestSweeper(t, time.Minute)

	done := make(chan struct{})
	go func() {
		defer close(done)
		sweeper.stop()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop must return for a sweeper that was never started")
	}
}

func TestSweeperStopIsIdempotent(t *testing.T) {
	sweeper := newTestSweeper(t, time.Minute)
	sweeper.start(context.Background())
	sweeper.stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		sweeper.stop()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a second stop must return immediately")
	}
}

func TestSweeperStartIsIdempotent(t *testing.T) {
	sweeper := newTestSweeper(t, time.Hour)
	sweeper.start(context.Background())
	sweeper.start(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		sweeper.stop()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop must return after a duplicated start")
	}
}

func TestSweeperStopCancelsTheRunningLoop(t *testing.T) {
	sweeper := newTestSweeper(t, 10*time.Millisecond)
	sweeper.start(context.Background())

	// Let at least one pass run so the loop is provably alive before stopping.
	time.Sleep(30 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		sweeper.stop()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop must interrupt the ticker-driven loop")
	}
}

func TestSweepPassMarksOverdueSubscriptionsZombie(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repository := memory.NewSubscriptionRepository()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

	overdue, err := domain.NewSubscription(
		"sub-overdue", "user-1", "netflix", "Netflix", domain.SubscriptionActive,
		domain.WindowMonthly, 1500, "USD", now.Add(-90*24*time.Hour), 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.Save(context.Background(), overdue); err != nil {
		t.Fatalf("save: %v", err)
	}

	subs := subscription.NewService(repository, fixedNow{now: now})
	tokens := token.NewService(
		memory.NewVirtualTokenRepository(),
		issuer.NewSimulatedCardIssuer(10_000),
		fixedNow{now: now},
		token.Options{Logger: logger},
	)
	sweeper := newSubscriptionSweeper(
		nil, subs,
		lifecycle.NewService(subs, tokens, lifecycle.Options{Queue: memory.NewLifecycleSyncQueue(), Logger: logger}),
		nil,
		obs.NewMetrics(prometheus.NewRegistry()),
		logger,
		time.Hour,
	)

	sweeper.sweepOnce(context.Background())

	persisted, err := repository.GetByID(context.Background(), "sub-overdue")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if persisted.State != domain.SubscriptionZombie {
		t.Errorf("state = %s, want Zombie", persisted.State)
	}
}

func TestSweeperSweepsImmediatelyOnStart(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repository := memory.NewSubscriptionRepository()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

	overdue, err := domain.NewSubscription(
		"sub-overdue", "user-1", "netflix", "Netflix", domain.SubscriptionActive,
		domain.WindowMonthly, 1500, "USD", now.Add(-90*24*time.Hour), 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.Save(context.Background(), overdue); err != nil {
		t.Fatalf("save: %v", err)
	}

	subs := subscription.NewService(repository, fixedNow{now: now})
	tokens := token.NewService(
		memory.NewVirtualTokenRepository(),
		issuer.NewSimulatedCardIssuer(10_000),
		fixedNow{now: now},
		token.Options{Logger: logger},
	)
	// An hour-long interval means only the immediate pass can run within the test.
	sweeper := newSubscriptionSweeper(
		nil, subs,
		lifecycle.NewService(subs, tokens, lifecycle.Options{Queue: memory.NewLifecycleSyncQueue(), Logger: logger}),
		nil,
		obs.NewMetrics(prometheus.NewRegistry()),
		logger,
		time.Hour,
	)
	sweeper.start(context.Background())
	t.Cleanup(sweeper.stop)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		persisted, err := repository.GetByID(context.Background(), "sub-overdue")
		if err == nil && persisted.State == domain.SubscriptionZombie {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected the first sweep to run immediately on start")
}

// fixedNow is a clock the test controls, so the sweep sees deterministic ages.
type fixedNow struct{ now time.Time }

func (c fixedNow) Now() time.Time { return c.now }

type failingSecondUpdateRepository struct {
	ports.SubscriptionRepository
	calls int
}

func (r *failingSecondUpdateRepository) UpdateVersion(ctx context.Context, subscription *domain.Subscription) error {
	r.calls++
	if r.calls == 2 {
		return errors.New("update failed")
	}
	return r.SubscriptionRepository.UpdateVersion(ctx, subscription)
}

func TestSweepPassFreezesCardOfPersistedZombieDespiteSweepError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()

	subscriptionRepository := memory.NewSubscriptionRepository()
	tokenRepository := memory.NewVirtualTokenRepository()
	subscriptions := make([]*domain.Subscription, 0, 2)
	for _, id := range []string{"sub-1", "sub-2"} {
		subscription, err := domain.NewSubscription(
			domain.SubscriptionID(id), domain.UserID(id), domain.MerchantID(id), id,
			domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now.Add(-90*24*time.Hour), 3)
		if err != nil {
			t.Fatalf("create subscription: %v", err)
		}
		card, err := domain.NewVirtualToken(
			domain.VirtualTokenID("vtok-"+id), domain.UserID(id), domain.MerchantID(id),
			"411111******1111", 10_000, "USD", now)
		if err != nil {
			t.Fatalf("create token: %v", err)
		}
		if err := tokenRepository.Save(ctx, card); err != nil {
			t.Fatalf("save token: %v", err)
		}
		subscription.VirtualTokenID = "vtok-" + id
		if err := subscriptionRepository.Save(ctx, subscription); err != nil {
			t.Fatalf("save subscription: %v", err)
		}
		subscriptions = append(subscriptions, subscription)
	}

	subs := subscription.NewService(
		&failingSecondUpdateRepository{SubscriptionRepository: subscriptionRepository},
		fixedNow{now: now})
	tokens := token.NewService(
		tokenRepository, issuer.NewSimulatedCardIssuer(10_000), fixedNow{now: now}, token.Options{Logger: logger})
	sweeper := newSubscriptionSweeper(
		nil, subs,
		lifecycle.NewService(subs, tokens, lifecycle.Options{Queue: memory.NewLifecycleSyncQueue(), Logger: logger}),
		nil,
		obs.NewMetrics(prometheus.NewRegistry()),
		logger,
		time.Hour,
	)

	sweeper.sweepOnce(ctx)

	zombieCount := 0
	for _, subscription := range subscriptions {
		persisted, err := subscriptionRepository.GetByID(ctx, subscription.ID)
		if err != nil {
			t.Fatalf("load subscription: %v", err)
		}
		card, err := tokenRepository.GetByID(ctx, domain.VirtualTokenID("vtok-"+string(subscription.ID)))
		if err != nil {
			t.Fatalf("load token: %v", err)
		}
		if persisted.State == domain.SubscriptionZombie {
			zombieCount++
			if card.State != domain.VirtualTokenFrozen {
				t.Errorf("subscription %s persisted as zombie but its card is %s, want Frozen",
					subscription.ID, card.State)
			}
		} else if card.State != domain.VirtualTokenActive {
			t.Errorf("subscription %s did not transition but its card is %s, want Active",
				subscription.ID, card.State)
		}
	}
	if zombieCount != 1 {
		t.Fatalf("expected exactly one zombie transition before the failure, got %d", zombieCount)
	}
}

func TestHistoryPrunerDeletesExpiredTransactions(t *testing.T) {
	ctx := context.Background()
	transactions := memory.NewTransactionRepository()

	for _, transaction := range []domain.Transaction{
		{ID: "tx-old", UserID: "user-1", MerchantID: "netflix", MerchantName: "Netflix",
			MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD",
			AuthorizedAt: time.Now().Add(-2 * time.Hour)},
		{ID: "tx-recent", UserID: "user-1", MerchantID: "netflix", MerchantName: "Netflix",
			MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD",
			AuthorizedAt: time.Now().Add(-time.Minute)},
	} {
		if err := transactions.Save(ctx, transaction); err != nil {
			t.Fatalf("save transaction: %v", err)
		}
	}

	pruner := &historyPruner{
		transactions:         transactions,
		transactionRetention: time.Hour,
		logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	pruner.prune(ctx)

	if _, err := transactions.GetByID(ctx, "tx-old"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("expired transaction must be deleted, got %v", err)
	}
	if _, err := transactions.GetByID(ctx, "tx-recent"); err != nil {
		t.Errorf("recent transaction must survive, got %v", err)
	}
}

func TestHistoryPrunerKeepsAuditEntriesWithinRetention(t *testing.T) {
	ctx := context.Background()
	audit := memory.NewAuditLog()
	if err := audit.Record(ctx, ports.AuditEntry{Actor: "key", Action: "freeze", EntityType: "token", EntityID: "vtok-1"}); err != nil {
		t.Fatalf("record: %v", err)
	}

	pruner := &historyPruner{
		audit:          audit,
		auditRetention: time.Hour,
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	pruner.prune(ctx)

	if len(audit.Entries()) != 1 {
		t.Errorf("fresh audit entries must survive retention, got %d", len(audit.Entries()))
	}
}

func TestMemoryAuditLogDeleteOlderThan(t *testing.T) {
	ctx := context.Background()
	audit := memory.NewAuditLog()
	for _, entityID := range []string{"vtok-1", "vtok-2"} {
		if err := audit.Record(ctx, ports.AuditEntry{Actor: "key", Action: "freeze", EntityType: "token", EntityID: entityID}); err != nil {
			t.Fatalf("record: %v", err)
		}
	}

	deleted, err := audit.DeleteOlderThan(ctx, -time.Hour)
	if err != nil {
		t.Fatalf("delete older than: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if len(audit.Entries()) != 0 {
		t.Fatalf("entries = %d, want 0", len(audit.Entries()))
	}
}
