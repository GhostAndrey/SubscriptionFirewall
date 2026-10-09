package detector

import (
	"context"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type frozenClock struct{ now time.Time }

func (c frozenClock) Now() time.Time { return c.now }

func monthlyTransactions(userID domain.UserID, merchantID domain.MerchantID, count int, end time.Time) []domain.Transaction {
	transactions := make([]domain.Transaction, 0, count)
	for i := range count {
		transactions = append(transactions, domain.Transaction{
			ID:           domain.TransactionID(string(userID) + "-" + string(merchantID) + "-" + string(rune('a'+i))),
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

// countingRepository records which merchants the detector asked to load.
type countingRepository struct {
	*memory.TransactionRepository
	loadedMerchants [][]domain.MerchantID
}

func (r *countingRepository) ListRecurringMerchantCandidates(
	ctx context.Context,
	userID domain.UserID,
	since time.Time,
	minOccurrences int,
) ([]ports.MerchantChargeCount, error) {
	return r.TransactionRepository.ListRecurringMerchantCandidates(ctx, userID, since, minOccurrences)
}

func (r *countingRepository) ListByUserAndMerchants(
	ctx context.Context,
	userID domain.UserID,
	merchants []domain.MerchantID,
	since time.Time,
) ([]domain.Transaction, error) {
	r.loadedMerchants = append(r.loadedMerchants, append([]domain.MerchantID(nil), merchants...))
	return r.TransactionRepository.ListByUserAndMerchants(ctx, userID, merchants, since)
}

func TestDetectIsolatesUsers(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	repository := memory.NewTransactionRepository()
	ctx := context.Background()

	for _, transaction := range monthlyTransactions("user-with-subscription", "netflix", 4, now) {
		if err := repository.Save(ctx, transaction); err != nil {
			t.Fatalf("save recurring: %v", err)
		}
	}
	for _, transaction := range monthlyTransactions("other-user", "netflix", 1, now) {
		if err := repository.Save(ctx, transaction); err != nil {
			t.Fatalf("save single charge: %v", err)
		}
	}

	service := New(repository, frozenClock{now: now}, DefaultConfig())

	withSubscription, err := service.Detect(ctx, "user-with-subscription")
	if err != nil {
		t.Fatalf("detect first user: %v", err)
	}
	if len(withSubscription) != 1 {
		t.Fatalf("expected 1 detection for the subscribing user, got %d", len(withSubscription))
	}

	other, err := service.Detect(ctx, "other-user")
	if err != nil {
		t.Fatalf("detect second user: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("another user's charges must not form a subscription, got %+v", other)
	}
}

func TestDetectLoadsOnlyCandidateMerchants(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	repository := &countingRepository{TransactionRepository: memory.NewTransactionRepository()}
	userID := domain.UserID("user-1")
	ctx := context.Background()

	// One recurring merchant plus many one-off merchants.
	for _, transaction := range monthlyTransactions(userID, "netflix", 5, now) {
		if err := repository.Save(ctx, transaction); err != nil {
			t.Fatalf("save recurring: %v", err)
		}
	}
	for i := range 50 {
		transaction := domain.Transaction{
			ID:           domain.TransactionID("one-off-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10))),
			MerchantID:   domain.MerchantID("shop-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26))),
			MerchantName: "Shop",
			MCC:          5411,
			AmountMinor:  500,
			Currency:     "USD",
			AuthorizedAt: now.AddDate(0, 0, -i),
		}
		if err := repository.Save(ctx, transaction); err != nil {
			t.Fatalf("save one-off: %v", err)
		}
	}

	detections, err := New(repository, frozenClock{now: now}, DefaultConfig()).Detect(ctx, userID)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(detections) != 1 || detections[0].MerchantID != "netflix" {
		t.Fatalf("expected only the recurring merchant, got %+v", detections)
	}
	if len(repository.loadedMerchants) != 1 {
		t.Fatalf("expected a single history load, got %d", len(repository.loadedMerchants))
	}
	if loaded := repository.loadedMerchants[0]; len(loaded) != 1 || loaded[0] != "netflix" {
		t.Fatalf("expected only netflix to be loaded, got %v", loaded)
	}
}

func TestDetectWithoutCandidatesSkipsHistoryLoad(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	repository := &countingRepository{TransactionRepository: memory.NewTransactionRepository()}
	ctx := context.Background()

	transaction := domain.Transaction{
		ID: "tx-solo", UserID: "user-1", MerchantID: "coffee", MerchantName: "Coffee",
		MCC: 5814, AmountMinor: 400, Currency: "USD", AuthorizedAt: now,
	}
	if err := repository.Save(ctx, transaction); err != nil {
		t.Fatalf("save: %v", err)
	}

	detections, err := New(repository, frozenClock{now: now}, DefaultConfig()).Detect(ctx, "user-1")
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(detections) != 0 {
		t.Fatalf("expected no detections, got %+v", detections)
	}
	if len(repository.loadedMerchants) != 0 {
		t.Fatalf("expected no history load for a user without candidates, got %v", repository.loadedMerchants)
	}
}
