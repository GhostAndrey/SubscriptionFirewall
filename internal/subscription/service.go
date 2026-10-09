package subscription

import (
	"context"
	"errors"
	"fmt"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/identifier"
	"subscriptionfirewall/internal/ports"
)

const (
	subscriptionIDPrefix = "sub"
	updateAttempts       = 3
	sweepBatchSize       = 500
)

type Service struct {
	subscriptions ports.SubscriptionRepository
	clock         ports.Clock
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
	return subscriptions, nil
}

func (s *Service) GetByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.Subscription, error) {
	subscription, err := s.subscriptions.FindByUserAndMerchant(ctx, userID, merchantID)
	if err != nil {
		return nil, fmt.Errorf("find subscription for user %s merchant %s: %w", userID, merchantID, err)
	}
	return subscription, nil
}

// Sweep marks overdue subscriptions as zombies. It reads pending rows in
// bounded batches and keeps going until none are left, so the work scales with
// the backlog rather than with the total subscription count.
func (s *Service) Sweep(ctx context.Context, batchSize int) ([]*domain.Subscription, error) {
	if batchSize <= 0 {
		batchSize = sweepBatchSize
	}
	now := s.clock.Now()
	changed := make([]*domain.Subscription, 0, batchSize)

	for {
		pending, err := s.subscriptions.ListPendingZombieTransition(ctx, now, batchSize)
		if err != nil {
			return nil, fmt.Errorf("list subscriptions pending zombie transition: %w", err)
		}
		if len(pending) == 0 {
			return changed, nil
		}

		for _, subscription := range pending {
			before := subscription.State
			if err := s.markMissed(ctx, subscription, now); err != nil {
				if errors.Is(err, ports.ErrVersionConflict) {
					continue
				}
				return nil, err
			}
			if subscription.State != before {
				changed = append(changed, subscription)
			}
		}

		if err := ctx.Err(); err != nil {
			return changed, err
		}
	}
}

func (s *Service) Freeze(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	subscription, err := s.update(ctx, id, (*domain.Subscription).Freeze)
	if err != nil {
		return nil, fmt.Errorf("freeze subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (s *Service) Reactivate(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	subscription, err := s.update(ctx, id, (*domain.Subscription).Reactivate)
	if err != nil {
		return nil, fmt.Errorf("reactivate subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (s *Service) Terminate(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	subscription, err := s.update(ctx, id, (*domain.Subscription).Terminate)
	if err != nil {
		return nil, fmt.Errorf("terminate subscription %s: %w", id, err)
	}
	return subscription, nil
}

// update loads a subscription, applies the mutation and persists it with an
// optimistic version check, retrying while another replica keeps winning the
// race.
func (s *Service) update(ctx context.Context, id domain.SubscriptionID, mutation func(*domain.Subscription) error) (*domain.Subscription, error) {
	var lastErr error
	for range updateAttempts {
		subscription, err := s.subscriptions.GetByID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load subscription %s: %w", id, err)
		}
		if err := mutation(subscription); err != nil {
			return nil, err
		}
		if err := s.subscriptions.UpdateVersion(ctx, subscription); err != nil {
			if errors.Is(err, ports.ErrVersionConflict) {
				lastErr = err
				continue
			}
			return nil, fmt.Errorf("persist subscription %s: %w", id, err)
		}
		return subscription, nil
	}
	return nil, fmt.Errorf("persist subscription %s after %d attempts: %w", id, updateAttempts, lastErr)
}

func (s *Service) ApplyDetection(ctx context.Context, detected domain.DetectedSubscription, virtualTokenID string) (*domain.Subscription, error) {
	created, err := s.newDetectedSubscription(detected, virtualTokenID)
	if err != nil {
		return nil, err
	}

	stored, err := s.subscriptions.CreateIfAbsent(ctx, created)
	if err != nil {
		return nil, fmt.Errorf("persist detected subscription for merchant %s: %w", detected.MerchantID, err)
	}
	if stored.ID == created.ID {
		return stored, nil
	}
	return s.mergeWithRetry(ctx, detected, virtualTokenID)
}

// mergeWithRetry folds a detection into an existing subscription, retrying
// while another replica keeps changing the same row.
func (s *Service) mergeWithRetry(ctx context.Context, detected domain.DetectedSubscription, virtualTokenID string) (*domain.Subscription, error) {
	var lastErr error
	for range updateAttempts {
		existing, err := s.subscriptions.FindByUserAndMerchant(ctx, detected.UserID, detected.MerchantID)
		if err != nil {
			return nil, fmt.Errorf("find subscription for merchant %s: %w", detected.MerchantID, err)
		}
		subscription, mergeErr := s.mergeDetected(ctx, existing, detected, virtualTokenID)
		if errors.Is(mergeErr, ports.ErrVersionConflict) {
			lastErr = mergeErr
			continue
		}
		return subscription, mergeErr
	}
	return nil, fmt.Errorf("apply detection for merchant %s after %d attempts: %w", detected.MerchantID, updateAttempts, lastErr)
}

func (s *Service) mergeDetected(ctx context.Context, existing *domain.Subscription, detected domain.DetectedSubscription, virtualTokenID string) (*domain.Subscription, error) {
	if detected.State == domain.SubscriptionZombie {
		if err := existing.MarkMissed(s.clock.Now()); err != nil {
			return nil, fmt.Errorf("mark subscription %s missed: %w", existing.ID, err)
		}
	} else if err := existing.ObserveActivity(detected.LastChargedAt, detected.AverageAmount, detected.ObservedPayments); err != nil {
		return nil, fmt.Errorf("observe activity on subscription %s: %w", existing.ID, err)
	}
	if virtualTokenID != "" {
		existing.VirtualTokenID = virtualTokenID
	}
	if err := s.subscriptions.UpdateVersion(ctx, existing); err != nil {
		return nil, err
	}
	return existing, nil
}

func (s *Service) newDetectedSubscription(detected domain.DetectedSubscription, virtualTokenID string) (*domain.Subscription, error) {
	created, err := domain.NewSubscription(
		domain.SubscriptionID(identifier.New(subscriptionIDPrefix)),
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
	return created, nil
}

// markMissed re-reads the subscription and applies the overdue transition under
// an optimistic version check, skipping the row when another replica wins.
func (s *Service) markMissed(ctx context.Context, subscription *domain.Subscription, now time.Time) error {
	for range updateAttempts {
		current, err := s.subscriptions.GetByID(ctx, subscription.ID)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil
			}
			return fmt.Errorf("load subscription %s: %w", subscription.ID, err)
		}
		if err := current.MarkMissed(now); err != nil {
			return fmt.Errorf("mark subscription %s missed: %w", current.ID, err)
		}
		if err := s.subscriptions.UpdateVersion(ctx, current); err != nil {
			if errors.Is(err, ports.ErrVersionConflict) {
				continue
			}
			return fmt.Errorf("persist subscription %s: %w", current.ID, err)
		}
		subscription.State = current.State
		subscription.Version = current.Version
		return nil
	}
	return fmt.Errorf("mark subscription %s missed after %d attempts: %w", subscription.ID, updateAttempts, ports.ErrVersionConflict)
}
