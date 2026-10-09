package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTransactionValidateAcceptsWellFormed(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	transaction := Transaction{
		ID:           "tx-1",
		UserID:       "user-1",
		MerchantID:   "netflix",
		MerchantName: "Netflix",
		MCC:          MCCRecurringBilling,
		AmountMinor:  1500,
		Currency:     "USD",
		AuthorizedAt: now.Add(-24 * time.Hour),
	}

	if err := transaction.Validate(now); err != nil {
		t.Fatalf("expected valid transaction, got %v", err)
	}
}

func TestTransactionValidateRejectsInvalidFields(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	valid := Transaction{
		ID:           "tx-1",
		UserID:       "user-1",
		MerchantID:   "netflix",
		MerchantName: "Netflix",
		MCC:          MCCRecurringBilling,
		AmountMinor:  1500,
		Currency:     "USD",
		AuthorizedAt: now.Add(-24 * time.Hour),
	}

	tests := map[string]func(*Transaction){
		"blank id":              func(tx *Transaction) { tx.ID = "   " },
		"blank user id":         func(tx *Transaction) { tx.UserID = "" },
		"blank merchant id":     func(tx *Transaction) { tx.MerchantID = " " },
		"oversized id":          func(tx *Transaction) { tx.ID = TransactionID(strings.Repeat("i", MaxTransactionIDLength+1)) },
		"oversized user id":     func(tx *Transaction) { tx.UserID = UserID(strings.Repeat("u", MaxUserIDLength+1)) },
		"oversized merchant id": func(tx *Transaction) { tx.MerchantID = MerchantID(strings.Repeat("m", MaxMerchantIDLength+1)) },
		"oversized merchant":    func(tx *Transaction) { tx.MerchantName = strings.Repeat("n", MaxMerchantNameLength+1) },
		"zero amount":           func(tx *Transaction) { tx.AmountMinor = 0 },
		"negative amount":       func(tx *Transaction) { tx.AmountMinor = -1 },
		"lowercase currency":    func(tx *Transaction) { tx.Currency = "usd" },
		"long currency":         func(tx *Transaction) { tx.Currency = "USDX" },
		"zero time":             func(tx *Transaction) { tx.AuthorizedAt = time.Time{} },
		"beyond skew":           func(tx *Transaction) { tx.AuthorizedAt = now.Add(MaxAuthorizationFutureSkew + time.Minute) },
		"beyond horizon":        func(tx *Transaction) { tx.AuthorizedAt = now.Add(-MaxTransactionAge - time.Minute) },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			transaction := valid
			mutate(&transaction)
			if err := transaction.Validate(now); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("expected ErrInvalidArgument, got %v", err)
			}
		})
	}
}

func TestTransactionValidateAcceptsTimeBoundaries(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	boundaries := map[string]time.Time{
		"future skew limit": now.Add(MaxAuthorizationFutureSkew),
		"age horizon":       now.Add(-MaxTransactionAge),
	}

	for name, authorizedAt := range boundaries {
		t.Run(name, func(t *testing.T) {
			transaction := Transaction{
				ID: "tx-1", UserID: "user-1", MerchantID: "netflix",
				AmountMinor: 1500, Currency: "USD", AuthorizedAt: authorizedAt,
			}
			if err := transaction.Validate(now); err != nil {
				t.Fatalf("expected %s to be accepted, got %v", name, err)
			}
		})
	}
}

func TestTransactionValidateAllowsEmptyMerchantName(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	transaction := Transaction{
		ID: "tx-1", UserID: "user-1", MerchantID: "netflix",
		AmountMinor: 1500, Currency: "USD", AuthorizedAt: now.Add(-time.Hour),
	}

	if err := transaction.Validate(now); err != nil {
		t.Fatalf("expected empty merchant name to be accepted, got %v", err)
	}
}

func TestIsISOCurrency(t *testing.T) {
	tests := map[string]bool{
		"USD": true, "EUR": true, "RUB": true,
		"usd": false, "US": false, "USDT": false, "": false, "US1": false,
	}

	for currency, want := range tests {
		t.Run(currency, func(t *testing.T) {
			if got := IsISOCurrency(currency); got != want {
				t.Fatalf("IsISOCurrency(%q) = %v, want %v", currency, got, want)
			}
		})
	}
}
