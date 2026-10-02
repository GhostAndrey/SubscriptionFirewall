package memory

import (
	"context"
	"testing"
	"time"

	"subscriptionfirewall/internal/domain"
)

func TestTransactionRepositoryGetByID(t *testing.T) {
	repository := NewTransactionRepository()
	transaction := domain.Transaction{
		ID:           "tx-1",
		UserID:       "user-1",
		MerchantID:   "netflix",
		AmountMinor:  1500,
		Currency:     "USD",
		AuthorizedAt: time.Now(),
	}
	if err := repository.Save(context.Background(), transaction); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := repository.GetByID(context.Background(), "tx-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if loaded.ID != transaction.ID {
		t.Errorf("expected %s, got %s", transaction.ID, loaded.ID)
	}

	if _, err := repository.GetByID(context.Background(), "tx-missing"); err == nil {
		t.Error("expected ErrNotFound for missing transaction")
	}
}

func TestSubscriptionRepositoryListAll(t *testing.T) {
	repository := NewSubscriptionRepository()
	for _, id := range []domain.SubscriptionID{"sub-2", "sub-1"} {
		subscription, err := domain.NewSubscription(id, "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now(), 1)
		if err != nil {
			t.Fatalf("create subscription: %v", err)
		}
		if err := repository.Save(context.Background(), subscription); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	listed, err := repository.ListAll(context.Background())
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 subscriptions, got %d", len(listed))
	}
	if listed[0].ID != "sub-1" || listed[1].ID != "sub-2" {
		t.Errorf("expected deterministic order sub-1, sub-2; got %s, %s", listed[0].ID, listed[1].ID)
	}
}

func TestVirtualTokenRepositoryReturnsIsolatedCopies(t *testing.T) {
	repository := NewVirtualTokenRepository()
	token, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", time.Now())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := repository.Save(context.Background(), token); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := repository.GetByID(context.Background(), "vtok-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := loaded.AuthorizeCharge(5_000, "USD", time.Now()); err != nil {
		t.Fatalf("authorize on returned copy: %v", err)
	}

	persisted, err := repository.GetByID(context.Background(), "vtok-1")
	if err != nil {
		t.Fatalf("get persisted: %v", err)
	}
	if persisted.SpentInPeriod != 0 {
		t.Errorf("expected repository copy to be untouched until Save, got spent %d", persisted.SpentInPeriod)
	}
}
