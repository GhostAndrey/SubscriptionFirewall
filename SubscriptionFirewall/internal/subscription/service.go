package subscription

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type Service struct {
	subscriptions ports.SubscriptionRepository
	clock         ports.Clock
	mu            sync.Mutex
}

func NewService(subscriptions ports.SubscriptionRepository, clock ports.Clock) *Service {
	return &Service{subscriptions: subscriptions, clock: clock}
}

func (s *Service) Get(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	subscription, err := s.subscriptions.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (s *Service) ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.Subscription, error) {
	subscriptions, err := s.subscriptions.ListByUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions for user %s: %w", userID, err)
	}
	for _, subscription := range subscriptions {
		if err := subscription.MarkMissed(s.clock.Now()); err != nil {
			return nil, fmt.Errorf("mark subscription %s missed: %w", subscription.ID, err)
		}
	}
	return subscriptions, nil
}

func (s *Service) Freeze(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	subscription, err := s.mutate(ctx, id, (*domain.Subscription).Freeze)
	if err != nil {
		return nil, fmt.Errorf("freeze subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (s *Service) Reactivate(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	subscription, err := s.mutate(ctx, id, (*domain.Subscription).Reactivate)
	if err != nil {
		return nil, fmt.Errorf("reactivate subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (s *Service) Terminate(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	subscription, err := s.mutate(ctx, id, (*domain.Subscription).Terminate)
	if err != nil {
		return nil, fmt.Errorf("terminate subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (s *Service) mutate(ctx context.Context, id domain.SubscriptionID, mutation func(*domain.Subscription) error) (*domain.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	subscription, err := s.subscriptions.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load subscription %s: %w", id, err)
	}
	if err := mutation(subscription); err != nil {
		return nil, err
	}
	if err := s.subscriptions.Save(ctx, subscription); err != nil {
		return nil, fmt.Errorf("persist subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (s *Service) ApplyDetection(ctx context.Context, detected domain.DetectedSubscription, virtualTokenID string) (*domain.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, err := s.subscriptions.FindByUserAndMerchant(ctx, detected.UserID, detected.MerchantID)
	switch {
	case err == nil:
		return s.mergeDetected(ctx, existing, detected, virtualTokenID)
	case errors.Is(err, domain.ErrNotFound):
		return s.createDetected(ctx, detected, virtualTokenID)
	default:
		return nil, fmt.Errorf("find subscription for merchant %s: %w", detected.MerchantID, err)
	}
}

func (s *Service) mergeDetected(ctx context.Context, existing *domain.Subscription, detected domain.DetectedSubscription, virtualTokenID string) (*domain.Subscription, error) {
	if detected.State == domain.SubscriptionZombie {
		if err := existing.MarkMissed(s.clock.Now()); err != nil {
			return nil, fmt.Errorf("mark subscription %s missed: %w", existing.ID, err)
		}
	} else {
		if err := existing.ObserveActivity(detected.LastChargedAt, detected.AverageAmount, detected.ObservedPayments); err != nil {
			return nil, fmt.Errorf("observe activity on subscription %s: %w", existing.ID, err)
		}
	}
	if virtualTokenID != "" {
		existing.VirtualTokenID = virtualTokenID
	}
	if err := s.subscriptions.Save(ctx, existing); err != nil {
		return nil, fmt.Errorf("persist subscription %s: %w", existing.ID, err)
	}
	return existing, nil
}

func (s *Service) createDetected(ctx context.Context, detected domain.DetectedSubscription, virtualTokenID string) (*domain.Subscription, error) {
	created, err := domain.NewSubscription(
		domain.SubscriptionID(newIdentifier("sub")),
		detected.UserID,
		detected.MerchantID,
		detected.MerchantName,
		detected.State,
		detected.Window,
		detected.AverageAmount,
		detected.Currency,
		detected.LastChargedAt,
		detected.ObservedPayments,
	)
	if err != nil {
		return nil, err
	}
	created.VirtualTokenID = virtualTokenID
	if err := s.subscriptions.Save(ctx, created); err != nil {
		return nil, fmt.Errorf("persist subscription %s: %w", created.ID, err)
	}
	return created, nil
}

func newIdentifier(prefix string) string {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(random)
}
