package domain

import (
	"fmt"
	"sync"
	"time"
)

type VirtualTokenID string

type VirtualTokenState string

const (
	VirtualTokenActive     VirtualTokenState = "Active"
	VirtualTokenFrozen     VirtualTokenState = "Frozen"
	VirtualTokenTerminated VirtualTokenState = "Terminated"
)

type VirtualToken struct {
	ID            VirtualTokenID
	UserID        UserID
	MerchantID    MerchantID
	MaskedPAN     string
	MonthlyLimit  int64
	SpentInPeriod int64
	Currency      string
	State         VirtualTokenState
	PeriodStarted time.Time

	mu sync.Mutex
}

func NewVirtualToken(id VirtualTokenID, userID UserID, merchantID MerchantID, maskedPAN string, monthlyLimit int64, currency string, now time.Time) (*VirtualToken, error) {
	if monthlyLimit <= 0 {
		return nil, fmt.Errorf("monthly limit must be positive: %w", ErrInvalidTransition)
	}
	if maskedPAN == "" {
		return nil, fmt.Errorf("masked PAN is required: %w", ErrInvalidTransition)
	}
	return &VirtualToken{
		ID:            id,
		UserID:        userID,
		MerchantID:    merchantID,
		MaskedPAN:     maskedPAN,
		MonthlyLimit:  monthlyLimit,
		Currency:      currency,
		State:         VirtualTokenActive,
		PeriodStarted: now,
	}, nil
}

func (t *VirtualToken) AuthorizeCharge(amountMinor int64, currency string, now time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.State != VirtualTokenActive {
		return fmt.Errorf("token %s is %s: %w", t.ID, t.State, ErrTokenNotActive)
	}
	if currency != t.Currency {
		return fmt.Errorf("charge currency %s != token currency %s: %w", currency, t.Currency, ErrUnknownCurrency)
	}
	t.rollPeriodIfElapsed(now)
	if t.SpentInPeriod+amountMinor > t.MonthlyLimit {
		return fmt.Errorf("spent %d + charge %d exceeds limit %d: %w", t.SpentInPeriod, amountMinor, t.MonthlyLimit, ErrSpendLimitExceeded)
	}
	t.SpentInPeriod += amountMinor
	return nil
}

func (t *VirtualToken) Freeze() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch t.State {
	case VirtualTokenActive:
		t.State = VirtualTokenFrozen
		return nil
	case VirtualTokenFrozen:
		return nil
	default:
		return fmt.Errorf("cannot freeze token in state %s: %w", t.State, ErrInvalidTransition)
	}
}

func (t *VirtualToken) Reactivate() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch t.State {
	case VirtualTokenFrozen:
		t.State = VirtualTokenActive
		return nil
	case VirtualTokenActive:
		return nil
	default:
		return fmt.Errorf("cannot reactivate token in state %s: %w", t.State, ErrInvalidTransition)
	}
}

func (t *VirtualToken) Terminate() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.State = VirtualTokenTerminated
	return nil
}

func (t *VirtualToken) rollPeriodIfElapsed(now time.Time) {
	if now.Sub(t.PeriodStarted) >= 30*24*time.Hour {
		t.PeriodStarted = now
		t.SpentInPeriod = 0
	}
}

func (t *VirtualToken) Snapshot() VirtualTokenSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return VirtualTokenSnapshot{
		ID:            t.ID,
		UserID:        t.UserID,
		MerchantID:    t.MerchantID,
		MaskedPAN:     t.MaskedPAN,
		MonthlyLimit:  t.MonthlyLimit,
		SpentInPeriod: t.SpentInPeriod,
		Currency:      t.Currency,
		State:         t.State,
		PeriodStarted: t.PeriodStarted,
	}
}

type VirtualTokenSnapshot struct {
	ID            VirtualTokenID
	UserID        UserID
	MerchantID    MerchantID
	MaskedPAN     string
	MonthlyLimit  int64
	SpentInPeriod int64
	Currency      string
	State         VirtualTokenState
	PeriodStarted time.Time
}
