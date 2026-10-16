package main

import (
	"context"
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
