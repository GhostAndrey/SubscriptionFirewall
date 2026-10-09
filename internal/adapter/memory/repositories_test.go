package memory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
)

func tx(t *testing.T, id string, userID, merchantID string, mcc domain.MCC, amount int64, at time.Time) domain.Transaction {
	t.Helper()

	return domain.Transaction{
		ID:           domain.TransactionID(id),
		UserID:       domain.UserID(userID),
		MerchantID:   domain.MerchantID(merchantID),
		MerchantName: merchantID,
		MCC:          mcc,
		AmountMinor:  amount,
		Currency:     "USD",
		AuthorizedAt: at,
	}
}

func TestTransactionListings(t *testing.T) {
	ctx := context.Background()
	repository := NewTransactionRepository()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	since := now.AddDate(0, 0, -30)

	saved := []domain.Transaction{
		tx(t, "tx-1", "user-1", "netflix", domain.MCCRecurringBilling, 1500, now.AddDate(0, 0, -1)),
		tx(t, "tx-2", "user-1", "netflix", domain.MCCRecurringBilling, 1500, now.AddDate(0, 0, -2)),
		tx(t, "tx-3", "user-1", "coffee", 5814, 400, now.AddDate(0, 0, -3)),
		tx(t, "tx-4", "user-2", "netflix", domain.MCCRecurringBilling, 1500, now.AddDate(0, 0, -4)),
		tx(t, "tx-5", "user-1", "netflix", domain.MCCRecurringBilling, 1500, now.AddDate(0, -2, 0)),
	}
	for _, transaction := range saved {
		if err := repository.Save(ctx, transaction); err != nil {
			t.Fatalf("save %s: %v", transaction.ID, err)
		}
	}

	t.Run("ListByUserSince filters by user and time", func(t *testing.T) {
		found, err := repository.ListByUserSince(ctx, "user-1", since)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(found) != 3 {
			t.Fatalf("expected 3 transactions, got %d", len(found))
		}
		for i := 1; i < len(found); i++ {
			if found[i].AuthorizedAt.Before(found[i-1].AuthorizedAt) {
				t.Fatal("transactions must be ordered by time")
			}
		}
	})

	t.Run("ListByUserSince excludes other users", func(t *testing.T) {
		found, err := repository.ListByUserSince(ctx, "user-3", since)
		if err != nil || len(found) != 0 {
			t.Fatalf("expected nothing, got %d (%v)", len(found), err)
		}
	})

	t.Run("ListRecurringMerchantCandidates applies the threshold", func(t *testing.T) {
		candidates, err := repository.ListRecurringMerchantCandidates(ctx, "user-1", since, 2)
		if err != nil {
			t.Fatalf("candidates: %v", err)
		}
		if len(candidates) != 1 || candidates[0].MerchantID != "netflix" {
			t.Fatalf("expected only netflix, got %+v", candidates)
		}
		if candidates[0].Occurrences != 2 {
			t.Errorf("occurrences = %d, want 2", candidates[0].Occurrences)
		}
		if !candidates[0].FirstSeen.Equal(now.AddDate(0, 0, -2)) {
			t.Errorf("first seen = %s", candidates[0].FirstSeen)
		}
		if !candidates[0].LastSeen.Equal(now.AddDate(0, 0, -1)) {
			t.Errorf("last seen = %s", candidates[0].LastSeen)
		}

		all, err := repository.ListRecurringMerchantCandidates(ctx, "user-1", since, 1)
		if err != nil || len(all) != 2 {
			t.Fatalf("threshold 1 should return both merchants, got %d (%v)", len(all), err)
		}
		if all[0].Occurrences < all[1].Occurrences {
			t.Error("candidates must be ordered by occurrences descending")
		}
	})

	t.Run("ListByUserAndMerchants narrows to the requested set", func(t *testing.T) {
		found, err := repository.ListByUserAndMerchants(ctx, "user-1", []domain.MerchantID{"netflix"}, since)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(found) != 2 {
			t.Fatalf("expected 2 netflix transactions, got %d", len(found))
		}
		for _, transaction := range found {
			if transaction.MerchantID != "netflix" {
				t.Fatalf("unexpected merchant %s", transaction.MerchantID)
			}
		}
	})

	t.Run("ListByUserAndMerchants without merchants loads nothing", func(t *testing.T) {
		found, err := repository.ListByUserAndMerchants(ctx, "user-1", nil, since)
		if err != nil || len(found) != 0 {
			t.Fatalf("expected nothing, got %d (%v)", len(found), err)
		}
	})
}

func TestSubscriptionFindAndSweepQueries(t *testing.T) {
	ctx := context.Background()
	repository := NewSubscriptionRepository()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

	seed := func(id string, state domain.SubscriptionState, lastChargedAt time.Time) {
		t.Helper()
		created, err := domain.NewSubscription(domain.SubscriptionID(id),
			domain.UserID("user-"+id), domain.MerchantID("merchant-"+id), "Merchant",
			state, domain.WindowMonthly, 1500, "USD", lastChargedAt, 3)
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if err := repository.Save(ctx, created); err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}

	seed("sub-overdue", domain.SubscriptionActive, now.AddDate(0, 0, -120))
	seed("sub-trial", domain.SubscriptionTrial, now.AddDate(0, 0, -90))
	seed("sub-zombie", domain.SubscriptionZombie, now.AddDate(0, 0, -200))
	seed("sub-fresh", domain.SubscriptionActive, now.AddDate(0, 0, -5))
	seed("sub-terminated", domain.SubscriptionTerminated, now.AddDate(0, 0, -200))

	pending, err := repository.ListPendingZombieTransition(ctx, now, 100)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	got := map[domain.SubscriptionID]bool{}
	for _, subscription := range pending {
		got[subscription.ID] = true
	}
	if len(got) != 2 || !got["sub-overdue"] || !got["sub-trial"] {
		t.Fatalf("expected only overdue active and trial rows, got %v", got)
	}

	limited, err := repository.ListPendingZombieTransition(ctx, now, 1)
	if err != nil || len(limited) != 1 {
		t.Fatalf("expected the limit to cap the batch, got %d (%v)", len(limited), err)
	}

	t.Run("FindByUserAndMerchant locates the pair", func(t *testing.T) {
		found, err := repository.FindByUserAndMerchant(ctx, "user-sub-overdue", "merchant-sub-overdue")
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if found.ID != "sub-overdue" {
			t.Fatalf("found %s", found.ID)
		}
	})

	t.Run("FindByUserAndMerchant reports a miss", func(t *testing.T) {
		_, err := repository.FindByUserAndMerchant(ctx, "user-nobody", "merchant-nobody")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestVirtualTokenFindAndListing(t *testing.T) {
	ctx := context.Background()
	repository := NewVirtualTokenRepository()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

	for i := range 3 {
		token, err := domain.NewVirtualToken(
			domain.VirtualTokenID(fmt.Sprintf("vtok-%02d", i)), "user-tokens",
			domain.MerchantID(fmt.Sprintf("merchant-%02d", i)), "411111******1234", 10_000, "USD", now)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		if err := repository.Save(ctx, token); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	found, err := repository.FindByUserAndMerchant(ctx, "user-tokens", "merchant-01")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found.ID != "vtok-01" {
		t.Fatalf("found %s", found.ID)
	}

	if _, err := repository.FindByUserAndMerchant(ctx, "user-nobody", "merchant-nobody"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	page, err := repository.ListByUserPage(ctx, "user-tokens", ports.Page{Limit: 2})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 2 || page[0].ID != "vtok-00" || page[1].ID != "vtok-01" {
		t.Fatalf("unexpected page: %+v", page)
	}

	if _, err := repository.ListByUserPage(ctx, "user-nobody", ports.Page{}); err != nil {
		t.Fatalf("list for an unknown user must succeed, got %v", err)
	}
}

func TestMemoryClockAdvances(t *testing.T) {
	before := time.Now()
	got := Clock{}.Now()
	if got.Before(before.Add(-time.Second)) {
		t.Fatalf("clock went backwards: %s < %s", got, before)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() *obs.Metrics {
	return obs.NewMetrics(prometheus.NewRegistry())
}

func TestMemoryOutboxRetryPaths(t *testing.T) {
	ctx := context.Background()
	transactions := NewTransactionRepository()
	outbox := NewDetectionOutbox(transactions, testMetrics(), testLogger(), 4)

	transaction := tx(t, "tx-retry", "user-1", "netflix", domain.MCCRecurringBilling, 1500, time.Now())
	if err := outbox.EnqueueWithTransaction(ctx, transaction); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	items, err := outbox.ClaimBatch(ctx, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("expected one claimed item, got %+v (%v)", items, err)
	}

	// The in-memory queue has no retry budget: Fail and RequeueStale are
	// accepted and ignored so the pipeline behaves as with MySQL.
	if err := outbox.Fail(ctx, items[0]); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if err := outbox.Complete(ctx, []int64{items[0].ID}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	requeued, err := outbox.RequeueStale(ctx, time.Hour)
	if err != nil || requeued != 0 {
		t.Fatalf("expected no requeue, got %d (%v)", requeued, err)
	}
}

func TestMemoryLifecycleQueueFail(t *testing.T) {
	ctx := context.Background()
	queue := NewLifecycleSyncQueue()

	if err := queue.Enqueue(ctx, ports.LifecycleSyncTask{EntityType: "token", EntityID: "vtok-1", Action: "freeze"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := queue.ClaimBatch(ctx, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected one task, got %+v (%v)", claimed, err)
	}
	if err := queue.Fail(ctx, claimed[0]); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if pending, err := queue.PendingCount(ctx); err != nil || pending != 1 {
		t.Fatalf("a failed task returns to pending, got %d (%v)", pending, err)
	}
	if err := queue.Fail(ctx, ports.LifecycleSyncTask{ID: 9999}); err != nil {
		t.Fatalf("failing an unknown task must be a no-op, got %v", err)
	}
}
