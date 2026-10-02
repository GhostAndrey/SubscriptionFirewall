package subscription

import (
	"context"
	"errors"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
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
