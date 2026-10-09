package subscription

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
)

func TestSweepMarksOverdueSubscriptionZombie(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	lastChargedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	now := lastChargedAt.Add(90 * 24 * time.Hour)
	service := NewService(repository, fixedClock{now: now})

	created, err := domain.NewSubscription("sub-1", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", lastChargedAt, 3)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if err := repository.Save(context.Background(), created); err != nil {
		t.Fatalf("save subscription: %v", err)
	}

	changed, err := service.Sweep(context.Background(), 0)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(changed) != 1 || changed[0].State != domain.SubscriptionZombie {
		t.Fatalf("expected subscription to become Zombie, got %+v", changed)
	}

	persisted, err := repository.GetByID(context.Background(), "sub-1")
	if err != nil {
		t.Fatalf("load persisted subscription: %v", err)
	}
	if persisted.State != domain.SubscriptionZombie {
		t.Errorf("expected Zombie persisted, got %s", persisted.State)
	}
}

func TestSweepSkipsCurrentAndFrozenSubscriptions(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	service := NewService(repository, fixedClock{now: now})

	overdue, err := domain.NewSubscription("sub-overdue", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now.Add(-45*24*time.Hour), 3)
	if err != nil {
		t.Fatalf("create overdue subscription: %v", err)
	}
	frozen, err := domain.NewSubscription("sub-frozen", "user-1", "spotify", "Spotify", domain.SubscriptionActive, domain.WindowMonthly, 900, "USD", now.Add(-90*24*time.Hour), 3)
	if err != nil {
		t.Fatalf("create frozen subscription: %v", err)
	}
	if err := frozen.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	for _, subscription := range []*domain.Subscription{overdue, frozen} {
		if err := repository.Save(context.Background(), subscription); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	changed, err := service.Sweep(context.Background(), 0)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(changed) != 0 {
		t.Fatalf("expected no changes, got %+v", changed)
	}
}

func TestSweepProcessesBacklogInBatches(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	service := NewService(repository, fixedClock{now: now})

	const total = 7
	for i := range total {
		lastChargedAt := now.Add(-time.Duration(90+i) * 24 * time.Hour)
		created, err := domain.NewSubscription(
			domain.SubscriptionID(fmt.Sprintf("sub-%02d", i)), "user-1",
			domain.MerchantID(fmt.Sprintf("merchant-%02d", i)), "Merchant",
			domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", lastChargedAt, 3)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		if err := repository.Save(context.Background(), created); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	changed, err := service.Sweep(context.Background(), 3)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(changed) != total {
		t.Fatalf("expected all %d overdue subscriptions to be swept, got %d", total, len(changed))
	}
	for i := range total {
		persisted, err := repository.GetByID(context.Background(), domain.SubscriptionID(fmt.Sprintf("sub-%02d", i)))
		if err != nil {
			t.Fatalf("load sub-%02d: %v", i, err)
		}
		if persisted.State != domain.SubscriptionZombie {
			t.Errorf("sub-%02d expected Zombie, got %s", i, persisted.State)
		}
	}
}

func TestSweepStopsOnContextCancellation(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	service := NewService(repository, fixedClock{now: now})

	for i := range 10 {
		created, err := domain.NewSubscription(
			domain.SubscriptionID(fmt.Sprintf("sub-%02d", i)), "user-1",
			domain.MerchantID(fmt.Sprintf("merchant-%02d", i)), "Merchant",
			domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD",
			now.Add(-time.Duration(90+i)*24*time.Hour), 3)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		if err := repository.Save(context.Background(), created); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := service.Sweep(ctx, 5); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestListByUserDoesNotMutateState(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	lastChargedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	now := lastChargedAt.Add(90 * 24 * time.Hour)
	service := NewService(repository, fixedClock{now: now})

	created, err := domain.NewSubscription("sub-1", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", lastChargedAt, 3)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if err := repository.Save(context.Background(), created); err != nil {
		t.Fatalf("save subscription: %v", err)
	}

	if _, err := service.ListByUser(context.Background(), "user-1"); err != nil {
		t.Fatalf("list: %v", err)
	}

	persisted, err := repository.GetByID(context.Background(), "sub-1")
	if err != nil {
		t.Fatalf("load persisted subscription: %v", err)
	}
	if persisted.State != domain.SubscriptionActive {
		t.Errorf("read path must not mutate state, expected Active, got %s", persisted.State)
	}
}
