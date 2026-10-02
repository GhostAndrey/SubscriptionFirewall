package subscription

import (
	"context"
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

	changed, err := service.Sweep(context.Background())
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

	changed, err := service.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(changed) != 0 {
		t.Fatalf("expected no changes, got %+v", changed)
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
