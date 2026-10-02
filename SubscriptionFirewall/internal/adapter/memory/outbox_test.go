package memory

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
)

func TestMemoryOutboxLifecycle(t *testing.T) {
	transactions := NewTransactionRepository()
	outbox := NewDetectionOutbox(
		transactions,
		obs.NewMetrics(prometheus.NewRegistry()),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		16,
	)
	ctx := context.Background()

	transaction := domain.Transaction{
		ID: "tx-1", UserID: "user-1", MerchantID: "netflix", MerchantName: "Netflix",
		MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD",
		AuthorizedAt: time.Now(),
	}
	if err := outbox.EnqueueWithTransaction(ctx, transaction); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := transactions.GetByID(ctx, "tx-1"); err != nil {
		t.Fatalf("transaction must be saved by outbox ingest: %v", err)
	}

	items, err := outbox.ClaimBatch(ctx, 8)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(items) != 1 || items[0].UserID != "user-1" {
		t.Fatalf("expected one claimed job for user-1, got %+v", items)
	}
	if pending, err := outbox.PendingCount(ctx); err != nil || pending != 0 {
		t.Fatalf("expected empty pending count after claim, got %d (%v)", pending, err)
	}
	if err := outbox.Complete(ctx, []int64{items[0].ID}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	emptyContext, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	empty, err := outbox.ClaimBatch(emptyContext, 8)
	if !errors.Is(err, context.DeadlineExceeded) || len(empty) != 0 {
		t.Fatalf("expected deadline on empty claim, got %+v (%v)", empty, err)
	}
}

func TestMemoryOutboxClose(t *testing.T) {
	outbox := NewDetectionOutbox(
		NewTransactionRepository(),
		obs.NewMetrics(prometheus.NewRegistry()),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		16,
	)
	ctx := context.Background()
	outbox.Close()
	outbox.Close()

	_, err := outbox.ClaimBatch(ctx, 8)
	if !errors.Is(err, ports.ErrOutboxClosed) {
		t.Fatalf("expected ErrOutboxClosed after close, got %v", err)
	}

	err = outbox.EnqueueWithTransaction(ctx, domain.Transaction{
		ID: "tx-1", UserID: "user-1", MerchantID: "netflix", MerchantName: "Netflix",
		MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD",
		AuthorizedAt: time.Now(),
	})
	if !errors.Is(err, ports.ErrOutboxClosed) {
		t.Fatalf("expected ErrOutboxClosed on enqueue after close, got %v", err)
	}
}
