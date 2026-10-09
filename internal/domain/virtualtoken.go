package domain

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type VirtualTokenID string

type VirtualTokenState string

const (
	VirtualTokenActive     VirtualTokenState = "Active"
	VirtualTokenFrozen     VirtualTokenState = "Frozen"
	VirtualTokenTerminated VirtualTokenState = "Terminated"
	// VirtualTokenIssuing marks a reserved but not yet provisioned card. It
	// blocks charging and makes concurrent issuance attempts for the same
	// user and merchant collapse onto a single provider call.
	VirtualTokenIssuing VirtualTokenState = "Issuing"
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
	if strings.TrimSpace(maskedPAN) == "" {
		return nil, fmt.Errorf("masked PAN is required: %w", ErrInvalidTransition)
	}
	if !IsISOCurrency(currency) {
		return nil, fmt.Errorf("currency %q must be a 3-letter ISO code: %w", currency, ErrInvalidTransition)
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

// NewIssuingVirtualToken creates a reservation for a card that has not been
// provisioned yet. The reservation is persisted before the issuer is called so
// that concurrent attempts for the same user and merchant cannot both charge
// the card provider.
func NewIssuingVirtualToken(id VirtualTokenID, userID UserID, merchantID MerchantID, now time.Time) *VirtualToken {
	return &VirtualToken{
		ID:            id,
		UserID:        userID,
		MerchantID:    merchantID,
		State:         VirtualTokenIssuing,
		PeriodStarted: now,
	}
}

// CompleteIssuance promotes a reservation to an active card once the provider
// returns it.
func (t *VirtualToken) CompleteIssuance(maskedPAN string, monthlyLimit int64, currency string, now time.Time) error {
	if t.State != VirtualTokenIssuing {
		return fmt.Errorf("cannot complete issuance of token %s in state %s: %w", t.ID, t.State, ErrInvalidTransition)
	}
	if strings.TrimSpace(maskedPAN) == "" {
		return fmt.Errorf("masked PAN is required: %w", ErrInvalidTransition)
	}
	if monthlyLimit <= 0 {
		return fmt.Errorf("monthly limit must be positive: %w", ErrInvalidTransition)
	}
	if !IsISOCurrency(currency) {
		return fmt.Errorf("currency %q must be a 3-letter ISO code: %w", currency, ErrInvalidTransition)
	}
	t.MaskedPAN = maskedPAN
	t.MonthlyLimit = monthlyLimit
	t.Currency = currency
	t.State = VirtualTokenActive
	t.PeriodStarted = now
	return nil
}

// AuthorizeCharge locks the token and delegates to ApplyCharge. Callers that
// already own exclusive access, such as a repository holding a row lock,
// should call ApplyCharge directly.
func (t *VirtualToken) AuthorizeCharge(amountMinor int64, currency string, now time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.ApplyCharge(amountMinor, currency, now)
}

// ApplyCharge debits amountMinor from the current period and fails when the
// token is not active, the currency differs, or the monthly limit would be
// exceeded. The caller must guarantee that no other goroutine observes the
// token while it runs.
func (t *VirtualToken) ApplyCharge(amountMinor int64, currency string, now time.Time) error {
	if amountMinor <= 0 {
		return fmt.Errorf("charge amount %d: %w", amountMinor, ErrInvalidAmount)
	}
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
