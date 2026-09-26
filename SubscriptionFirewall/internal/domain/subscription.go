package domain

import (
	"fmt"
	"time"
)

type SubscriptionID string

type SubscriptionState string

const (
	SubscriptionActive     SubscriptionState = "Active"
	SubscriptionTrial      SubscriptionState = "Trial"
	SubscriptionZombie     SubscriptionState = "Zombie"
	SubscriptionFrozen     SubscriptionState = "Frozen"
	SubscriptionTerminated SubscriptionState = "Terminated"
)

type BillingWindow string

const (
	WindowWeekly  BillingWindow = "7d"
	WindowMonthly BillingWindow = "30d"
	WindowYearly  BillingWindow = "365d"
)

func (s SubscriptionState) IsValid() bool {
	switch s {
	case SubscriptionActive, SubscriptionTrial, SubscriptionZombie, SubscriptionFrozen, SubscriptionTerminated:
		return true
	default:
		return false
	}
}

func (w BillingWindow) Duration() time.Duration {
	switch w {
	case WindowWeekly:
		return 7 * 24 * time.Hour
	case WindowMonthly:
		return 30 * 24 * time.Hour
	case WindowYearly:
		return 365 * 24 * time.Hour
	default:
		return 0
	}
}

type Subscription struct {
	ID               SubscriptionID
	UserID           UserID
	MerchantID       MerchantID
	MerchantName     string
	VirtualTokenID   string
	State            SubscriptionState
	BillingWindow    BillingWindow
	AverageAmount    int64
	Currency         string
	LastChargedAt    time.Time
	NextExpectedAt   time.Time
	ObservedPayments int
}

func NewSubscription(
	id SubscriptionID,
	userID UserID,
	merchantID MerchantID,
	merchantName string,
	state SubscriptionState,
	window BillingWindow,
	averageAmount int64,
	currency string,
	lastChargedAt time.Time,
	observedPayments int,
) (*Subscription, error) {
	if !state.IsValid() {
		return nil, fmt.Errorf("unknown subscription state %q: %w", state, ErrInvalidTransition)
	}
	if window.Duration() == 0 {
		return nil, fmt.Errorf("unknown billing window %q: %w", window, ErrInvalidTransition)
	}
	if observedPayments <= 0 {
		return nil, fmt.Errorf("subscription must observe at least one payment: %w", ErrInvalidTransition)
	}
	return &Subscription{
		ID:               id,
		UserID:           userID,
		MerchantID:       merchantID,
		MerchantName:     merchantName,
		State:            state,
		BillingWindow:    window,
		AverageAmount:    averageAmount,
		Currency:         currency,
		LastChargedAt:    lastChargedAt,
		NextExpectedAt:   lastChargedAt.Add(window.Duration()),
		ObservedPayments: observedPayments,
	}, nil
}

func (s *Subscription) MarkMissed(now time.Time) error {
	switch s.State {
	case SubscriptionTerminated, SubscriptionFrozen:
		return nil
	}
	if now.Sub(s.NextExpectedAt) > s.BillingWindow.Duration() {
		s.State = SubscriptionZombie
	}
	return nil
}

func (s *Subscription) ObserveActivity(lastChargedAt time.Time, averageAmount int64, observedPayments int) error {
	if s.State == SubscriptionTerminated {
		return fmt.Errorf("subscription %s is terminated: %w", s.ID, ErrInvalidTransition)
	}
	s.State = SubscriptionActive
	s.AverageAmount = averageAmount
	s.LastChargedAt = lastChargedAt
	s.NextExpectedAt = lastChargedAt.Add(s.BillingWindow.Duration())
	s.ObservedPayments = observedPayments
	return nil
}

func (s *Subscription) Freeze() error {
	return s.transitionToFrozen()
}

func (s *Subscription) transitionToFrozen() error {
	switch s.State {
	case SubscriptionActive, SubscriptionTrial, SubscriptionZombie:
		s.State = SubscriptionFrozen
		return nil
	case SubscriptionFrozen:
		return nil
	default:
		return fmt.Errorf("cannot freeze subscription in state %s: %w", s.State, ErrInvalidTransition)
	}
}

func (s *Subscription) Reactivate() error {
	switch s.State {
	case SubscriptionFrozen:
		s.State = SubscriptionActive
		return nil
	case SubscriptionActive, SubscriptionTrial, SubscriptionZombie:
		return nil
	default:
		return fmt.Errorf("cannot reactivate subscription in state %s: %w", s.State, ErrInvalidTransition)
	}
}

func (s *Subscription) Terminate() error {
	if s.State == SubscriptionTerminated {
		return nil
	}
	s.State = SubscriptionTerminated
	return nil
}
