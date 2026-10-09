package domain

import (
	"fmt"
	"time"
)

type TransactionID string

type UserID string

type MerchantID string

type MCC uint16

const (
	MCCRecurringBilling      MCC = 5968
	MCCDigitalGoodsSubscript MCC = 5817
	MCCInsuranceSubscription MCC = 6300
	MCCStreamingMedia        MCC = 5815
)

func (m MCC) IsSubscriptionProne() bool {
	switch m {
	case MCCRecurringBilling, MCCDigitalGoodsSubscript, MCCInsuranceSubscription, MCCStreamingMedia:
		return true
	default:
		return false
	}
}

// Storage limits mirroring the transactions table schema. Validating them at
// the domain edge keeps oversized input out of the adapter instead of turning
// into a database error surfaced as a 500.
const (
	MaxTransactionIDLength = 64
	MaxUserIDLength        = 64
	MaxMerchantIDLength    = 64
	MaxMerchantNameLength  = 255
)

const (
	// MaxAuthorizationFutureSkew tolerates clock drift between the issuer of a
	// transaction and this service.
	MaxAuthorizationFutureSkew = 24 * time.Hour
	// MaxTransactionAge bounds how far back the detector has to reason about.
	MaxTransactionAge = 2 * 365 * 24 * time.Hour
)

type Transaction struct {
	ID           TransactionID
	UserID       UserID
	MerchantID   MerchantID
	MerchantName string
	MCC          MCC
	AmountMinor  int64
	Currency     string
	AuthorizedAt time.Time
}

// Validate reports whether the transaction satisfies every invariant the
// persistence layer and the detector rely on.
func (t Transaction) Validate(now time.Time) error {
	if err := validateText(
		textRule{field: "id", value: string(t.ID), maxBytes: MaxTransactionIDLength, required: true},
		textRule{field: "user_id", value: string(t.UserID), maxBytes: MaxUserIDLength, required: true},
		textRule{field: "merchant_id", value: string(t.MerchantID), maxBytes: MaxMerchantIDLength, required: true},
		textRule{field: "merchant_name", value: t.MerchantName, maxBytes: MaxMerchantNameLength},
	); err != nil {
		return err
	}
	if t.AmountMinor <= 0 {
		return fmt.Errorf("amount_minor must be positive, got %d: %w", t.AmountMinor, ErrInvalidArgument)
	}
	if !IsISOCurrency(t.Currency) {
		return fmt.Errorf("currency %q must be a 3-letter ISO code: %w", t.Currency, ErrInvalidArgument)
	}
	return t.validateAuthorizationTime(now)
}

func (t Transaction) validateAuthorizationTime(now time.Time) error {
	if t.AuthorizedAt.IsZero() {
		return fmt.Errorf("authorized_at is required: %w", ErrInvalidArgument)
	}
	if latest := now.Add(MaxAuthorizationFutureSkew); t.AuthorizedAt.After(latest) {
		return fmt.Errorf("authorized_at %s is beyond allowed skew from %s: %w", t.AuthorizedAt, latest, ErrInvalidArgument)
	}
	if earliest := now.Add(-MaxTransactionAge); t.AuthorizedAt.Before(earliest) {
		return fmt.Errorf("authorized_at %s is older than allowed horizon %s: %w", t.AuthorizedAt, earliest, ErrInvalidArgument)
	}
	return nil
}
