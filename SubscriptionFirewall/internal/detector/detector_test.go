package detector

import (
	"context"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
)

type frozenClock struct{ now time.Time }

func (c frozenClock) Now() time.Time { return c.now }

func monthlyTransactions(userID domain.UserID, merchantID domain.MerchantID, count int, end time.Time) []domain.Transaction {
	transactions := make([]domain.Transaction, 0, count)
	for i := range count {
		transactions = append(transactions, domain.Transaction{
			ID:           domain.TransactionID(string(merchantID) + "-" + string(rune('a'+i))),
			UserID:       userID,
			MerchantID:   merchantID,
			MerchantName: "Test Merchant",
			MCC:          domain.MCCRecurringBilling,
			AmountMinor:  1500,
			Currency:     "USD",
			AuthorizedAt: end.AddDate(0, 0, -30*(count-1-i)),
		})
	}
	return transactions
}

func TestDetectFindsMonthlySubscription(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	repository := memory.NewTransactionRepository()
	userID := domain.UserID("user-1")
	merchantID := domain.MerchantID("netflix")
	for _, transaction := range monthlyTransactions(userID, merchantID, 5, now) {
		if err := repository.Save(context.Background(), transaction); err != nil {
			t.Fatalf("save transaction: %v", err)
		}
	}

	service := New(repository, frozenClock{now: now}, DefaultConfig())
	detections, err := service.Detect(context.Background(), userID)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(detections) != 1 {
		t.Fatalf("expected 1 detection, got %d", len(detections))
	}

	detection := detections[0]
	if detection.State != domain.SubscriptionActive {
		t.Errorf("expected Active state, got %s", detection.State)
	}
	if detection.Window != domain.WindowMonthly {
		t.Errorf("expected monthly window, got %s", detection.Window)
	}
	if detection.AverageAmount != 1500 {
		t.Errorf("expected average 1500, got %d", detection.AverageAmount)
	}
}

func TestDetectIgnoresIrregularAmounts(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	repository := memory.NewTransactionRepository()
	userID := domain.UserID("user-1")
	merchantID := domain.MerchantID("grocery")
	transactions := monthlyTransactions(userID, merchantID, 4, now)
	transactions[2].AmountMinor = 9900
	for _, transaction := range transactions {
		if err := repository.Save(context.Background(), transaction); err != nil {
			t.Fatalf("save transaction: %v", err)
		}
	}

	service := New(repository, frozenClock{now: now}, DefaultConfig())
	detections, err := service.Detect(context.Background(), userID)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(detections) != 0 {
		t.Fatalf("expected no detections, got %d", len(detections))
	}
}

func TestDetectClassifiesZombieSubscription(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	repository := memory.NewTransactionRepository()
	userID := domain.UserID("user-1")
	merchantID := domain.MerchantID("abandoned-gym")
	transactions := monthlyTransactions(userID, merchantID, 4, now.AddDate(0, -4, 0))
	for _, transaction := range transactions {
		if err := repository.Save(context.Background(), transaction); err != nil {
			t.Fatalf("save transaction: %v", err)
		}
	}

	service := New(repository, frozenClock{now: now}, DefaultConfig())
	detections, err := service.Detect(context.Background(), userID)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(detections) != 1 {
		t.Fatalf("expected 1 detection, got %d", len(detections))
	}
	if detections[0].State != domain.SubscriptionZombie {
		t.Errorf("expected Zombie state, got %s", detections[0].State)
	}
}
