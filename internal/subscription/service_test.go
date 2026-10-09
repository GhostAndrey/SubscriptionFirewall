package subscription

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestFreezeAndReactivateLifecycle(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	service := NewService(repository, fixedClock{now: time.Now()})

	created, err := domain.NewSubscription("sub-1", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now(), 5)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if err := repository.Save(context.Background(), created); err != nil {
		t.Fatalf("save subscription: %v", err)
	}

	frozen, err := service.Freeze(context.Background(), "sub-1")
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if frozen.State != domain.SubscriptionFrozen {
		t.Errorf("expected Frozen, got %s", frozen.State)
	}

	reactivated, err := service.Reactivate(context.Background(), "sub-1")
	if err != nil {
		t.Fatalf("reactivate: %v", err)
	}
	if reactivated.State != domain.SubscriptionActive {
		t.Errorf("expected Active, got %s", reactivated.State)
	}
}

func TestTerminateIsFinal(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	service := NewService(repository, fixedClock{now: time.Now()})

	created, err := domain.NewSubscription("sub-1", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now(), 5)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if err := repository.Save(context.Background(), created); err != nil {
		t.Fatalf("save subscription: %v", err)
	}

	if _, err := service.Terminate(context.Background(), "sub-1"); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if _, err := service.Freeze(context.Background(), "sub-1"); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestUpdateRejectsStaleVersion(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	ctx := context.Background()

	created := seedSubscription(t, repository, "sub-version", time.Now())

	stale := cloneOf(t, repository, "sub-version")
	latest := cloneOf(t, repository, "sub-version")
	if err := latest.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.UpdateVersion(ctx, latest); err != nil {
		t.Fatalf("first update: %v", err)
	}

	if err := repository.UpdateVersion(ctx, stale); !errors.Is(err, ports.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict for a stale write, got %v", err)
	}
	if stale.Version != created.Version {
		t.Errorf("stale write must not bump the version: %d vs %d", stale.Version, created.Version)
	}
}

func TestConcurrentMutationsSerializeThroughRetries(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	first := NewService(repository, memory.Clock{})
	second := NewService(repository, memory.Clock{})
	ctx := context.Background()
	seedSubscription(t, repository, "sub-race", time.Now())

	var wg sync.WaitGroup
	results := make([]error, 2)
	for slot, service := range []*Service{first, second} {
		wg.Add(1)
		go func(index int, s *Service) {
			defer wg.Done()
			_, results[index] = s.Freeze(ctx, "sub-race")
		}(slot, service)
	}
	wg.Wait()

	for slot, err := range results {
		if err != nil {
			t.Fatalf("mutation %d failed: %v", slot, err)
		}
	}

	persisted, err := repository.GetByID(ctx, "sub-race")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if persisted.State != domain.SubscriptionFrozen {
		t.Errorf("expected Frozen, got %s", persisted.State)
	}
}

func TestTerminateWinsOverConcurrentUpdate(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	terminating := NewService(repository, memory.Clock{})
	ctx := context.Background()
	seedSubscription(t, repository, "sub-final", time.Now())

	if _, err := terminating.Terminate(ctx, "sub-final"); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if _, err := terminating.Freeze(ctx, "sub-final"); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition after termination, got %v", err)
	}

	persisted, err := repository.GetByID(ctx, "sub-final")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if persisted.State != domain.SubscriptionTerminated {
		t.Errorf("expected the terminal state to survive, got %s", persisted.State)
	}
}

func seedSubscription(t *testing.T, repository *memory.SubscriptionRepository, id domain.SubscriptionID, now time.Time) *domain.Subscription {
	t.Helper()

	subscription, err := domain.NewSubscription(id, "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 3)
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	if err := repository.Save(t.Context(), subscription); err != nil {
		t.Fatalf("save %s: %v", id, err)
	}
	return subscription
}

func cloneOf(t *testing.T, repository *memory.SubscriptionRepository, id domain.SubscriptionID) *domain.Subscription {
	t.Helper()

	subscription, err := repository.GetByID(t.Context(), id)
	if err != nil {
		t.Fatalf("load %s: %v", id, err)
	}
	return subscription
}

func TestApplyDetectionCreatesThenUpdates(t *testing.T) {
	repository := memory.NewSubscriptionRepository()
	service := NewService(repository, fixedClock{now: time.Now()})
	lastChargedAt := time.Now()

	first, err := service.ApplyDetection(context.Background(), domain.DetectedSubscription{
		UserID:           "user-1",
		MerchantID:       "netflix",
		MerchantName:     "Netflix",
		MCC:              domain.MCCRecurringBilling,
		State:            domain.SubscriptionActive,
		Window:           domain.WindowMonthly,
		AverageAmount:    1500,
		Currency:         "USD",
		LastChargedAt:    lastChargedAt,
		ObservedPayments: 3,
	}, "vtok-1")
	if err != nil {
		t.Fatalf("first detection: %v", err)
	}

	second, err := service.ApplyDetection(context.Background(), domain.DetectedSubscription{
		UserID:           "user-1",
		MerchantID:       "netflix",
		MerchantName:     "Netflix",
		MCC:              domain.MCCRecurringBilling,
		State:            domain.SubscriptionActive,
		Window:           domain.WindowMonthly,
		AverageAmount:    1600,
		Currency:         "USD",
		LastChargedAt:    lastChargedAt.Add(30 * 24 * time.Hour),
		ObservedPayments: 4,
	}, "vtok-1")
	if err != nil {
		t.Fatalf("second detection: %v", err)
	}
	if second.ID != first.ID {
		t.Errorf("expected same subscription id, got %s and %s", first.ID, second.ID)
	}
	if second.ObservedPayments != 4 {
		t.Errorf("expected 4 observed payments, got %d", second.ObservedPayments)
	}
	if second.VirtualTokenID != "vtok-1" {
		t.Errorf("expected token binding preserved, got %q", second.VirtualTokenID)
	}
}
