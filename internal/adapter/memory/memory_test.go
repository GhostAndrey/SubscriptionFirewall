package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

func TestPageSliceSemantics(t *testing.T) {
	items := []string{"a", "b", "c", "d", "e"}

	tests := map[string]struct {
		page ports.Page
		want []string
	}{
		"unbounded returns everything": {ports.Page{}, []string{"a", "b", "c", "d", "e"}},
		"first page":                   {ports.Page{Limit: 2}, []string{"a", "b"}},
		"second page":                  {ports.Page{Limit: 2, Offset: 2}, []string{"c", "d"}},
		"page past the end":            {ports.Page{Limit: 2, Offset: 10}, []string{}},
		"offset exactly at the end":    {ports.Page{Limit: 2, Offset: 5}, []string{}},
		"partial last page":            {ports.Page{Limit: 3, Offset: 4}, []string{"e"}},
		"limit beyond the end":         {ports.Page{Limit: 100}, []string{"a", "b", "c", "d", "e"}},
		"zero limit is unbounded":      {ports.Page{Limit: 0, Offset: 1}, []string{"a", "b", "c", "d", "e"}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := pageSlice(items, test.page)
			if len(got) != len(test.want) {
				t.Fatalf("pageSlice = %v, want %v", got, test.want)
			}
			for i := range test.want {
				if got[i] != test.want[i] {
					t.Fatalf("pageSlice = %v, want %v", got, test.want)
				}
			}
		})
	}
}

func TestPageIsUnbounded(t *testing.T) {
	if (ports.Page{}).IsUnbounded() != true {
		t.Error("a zero limit must mean unbounded")
	}
	if (ports.Page{Limit: -1}).IsUnbounded() != true {
		t.Error("a negative limit must mean unbounded")
	}
	if (ports.Page{Limit: 1}).IsUnbounded() != false {
		t.Error("a positive limit must be bounded")
	}
}

func TestListByUserPageAppliesWindow(t *testing.T) {
	ctx := context.Background()
	repository := NewSubscriptionRepository()
	clock := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	for i := range 5 {
		created, err := domain.NewSubscription(
			domain.SubscriptionID(fmt.Sprintf("sub-%02d", i)), "user-page", "merchant-page", "Merchant",
			domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", clock, 3)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		if err := repository.Save(ctx, created); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	first, err := repository.ListByUserPage(ctx, "user-page", ports.Page{Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first) != 2 || first[0].ID != "sub-00" || first[1].ID != "sub-01" {
		t.Fatalf("unexpected first page: %v", ids(first))
	}

	rest, err := repository.ListByUserPage(ctx, "user-page", ports.Page{Limit: 10, Offset: 4})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(rest) != 1 || rest[0].ID != "sub-04" {
		t.Fatalf("unexpected second page: %v", ids(rest))
	}

	beyond, err := repository.ListByUserPage(ctx, "user-page", ports.Page{Limit: 2, Offset: 99})
	if err != nil {
		t.Fatalf("page beyond the end: %v", err)
	}
	if len(beyond) != 0 {
		t.Fatalf("expected an empty page, got %v", ids(beyond))
	}
}

func ids(subscriptions []*domain.Subscription) []string {
	out := make([]string, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		out = append(out, string(subscription.ID))
	}
	return out
}

func TestSubscriptionCreateIfAbsentIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repository := NewSubscriptionRepository()
	now := time.Now()

	first := newSubscription(t, "sub-a", now)
	second := newSubscription(t, "sub-b", now)

	storedFirst, err := repository.CreateIfAbsent(ctx, first)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	storedSecond, err := repository.CreateIfAbsent(ctx, second)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if storedFirst.ID != storedSecond.ID {
		t.Fatalf("expected one canonical subscription, got %s and %s", storedFirst.ID, storedSecond.ID)
	}

	listed, err := repository.ListByUserPage(ctx, "user-1", ports.Page{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected exactly one stored subscription, got %d", len(listed))
	}
}

func TestSubscriptionUpdateVersionGuards(t *testing.T) {
	ctx := context.Background()
	repository := NewSubscriptionRepository()
	seed := newSubscription(t, "sub-1", time.Now())
	if err := repository.Save(ctx, seed); err != nil {
		t.Fatalf("save: %v", err)
	}

	stale, err := repository.GetByID(ctx, "sub-1")
	if err != nil {
		t.Fatalf("get stale: %v", err)
	}
	latest, err := repository.GetByID(ctx, "sub-1")
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if err := latest.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.UpdateVersion(ctx, latest); err != nil {
		t.Fatalf("update: %v", err)
	}
	if latest.Version != 1 {
		t.Fatalf("expected version 1 after update, got %d", latest.Version)
	}

	if err := repository.UpdateVersion(ctx, stale); err == nil {
		t.Fatal("expected a stale write to be rejected")
	}

	missing := newSubscription(t, "sub-missing", time.Now())
	if err := repository.UpdateVersion(ctx, missing); !isNotFoundErr(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestVirtualTokenChargeUnderConcurrency(t *testing.T) {
	const (
		monthlyLimit = 10_000
		chargeAmount = 500
		goroutines   = 40
	)

	ctx := context.Background()
	repository := NewVirtualTokenRepository()
	now := time.Now()

	token, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", monthlyLimit, "USD", now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.Save(ctx, token); err != nil {
		t.Fatalf("save: %v", err)
	}

	var approved atomic.Int32
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := repository.Charge(ctx, "vtok-1", chargeAmount, "USD", now); err == nil {
				approved.Add(1)
			}
		}()
	}
	wg.Wait()

	if got, want := approved.Load(), int32(monthlyLimit/chargeAmount); got != want {
		t.Fatalf("approved = %d, want %d", got, want)
	}
	persisted, err := repository.GetByID(ctx, "vtok-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if persisted.SpentInPeriod != monthlyLimit {
		t.Fatalf("spent = %d, want %d", persisted.SpentInPeriod, monthlyLimit)
	}
}

func TestTokenIssuanceReservationIsExclusive(t *testing.T) {
	ctx := context.Background()
	repository := NewVirtualTokenRepository()
	now := time.Now()

	first, err := repository.ReserveTokenIssuance(ctx, "user-1", "netflix", "issue:user-1:netflix", now, now.Add(-time.Minute))
	if err != nil || !first.Reserved {
		t.Fatalf("expected the first caller to reserve, got %+v (%v)", first, err)
	}

	second, err := repository.ReserveTokenIssuance(ctx, "user-1", "netflix", "issue:user-1:netflix", now, now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if second.Reserved || second.Existing == nil || second.Existing.State != domain.VirtualTokenIssuing {
		t.Fatalf("expected the second caller to observe the held reservation, got %+v", second)
	}

	if err := repository.ReleaseTokenIssuance(ctx, first.TokenID); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := repository.GetByID(ctx, first.TokenID); !isNotFoundErr(err) {
		t.Fatalf("expected the released reservation to be gone, got %v", err)
	}
}

func TestTokenIssuanceReservationReclaimsStaleWork(t *testing.T) {
	ctx := context.Background()
	repository := NewVirtualTokenRepository()
	reservedAt := time.Now()

	abandoned := domain.NewIssuingVirtualToken("vtok-stale", "user-1", "netflix", reservedAt)
	if err := repository.Save(ctx, abandoned); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reservation, err := repository.ReserveTokenIssuance(ctx, "user-1", "netflix", "issue:user-1:netflix",
		reservedAt.Add(2*time.Minute), reservedAt.Add(time.Minute))
	if err != nil || !reservation.Reserved {
		t.Fatalf("expected the stale reservation to be reclaimed, got %+v (%v)", reservation, err)
	}
	if reservation.TokenID == abandoned.ID {
		t.Fatal("expected a fresh reservation id")
	}

	token, err := repository.CompleteTokenIssuance(ctx, reservation.TokenID, "issue:user-1:netflix",
		ports.IssuedCard{MaskedPAN: "411111******1234", MonthlyLimit: 10_000, Currency: "USD"}, time.Now())
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if token.State != domain.VirtualTokenActive {
		t.Fatalf("expected an active token, got %s", token.State)
	}
	if err := token.CompleteIssuance("411111******5678", 10_000, "USD", time.Now()); err == nil {
		t.Fatal("expected a repeated completion to be rejected")
	}
}

func TestLifecycleSyncQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	queue := NewLifecycleSyncQueue()

	if err := queue.Enqueue(ctx, ports.LifecycleSyncTask{EntityType: "token", EntityID: "vtok-1", Action: "terminate"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if pending, err := queue.PendingCount(ctx); err != nil || pending != 1 {
		t.Fatalf("pending = %d (%v)", pending, err)
	}

	claimed, err := queue.ClaimBatch(ctx, 10)
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("unexpected claim: %+v (%v)", claimed, err)
	}
	if again, err := queue.ClaimBatch(ctx, 10); err != nil || len(again) != 0 {
		t.Fatalf("expected no second claim, got %+v (%v)", again, err)
	}

	if requeued, err := queue.RequeueStale(ctx, time.Hour); err != nil || requeued != 0 {
		t.Fatalf("expected nothing requeued, got %d (%v)", requeued, err)
	}
	if requeued, err := queue.RequeueStale(ctx, -time.Minute); err != nil || requeued != 1 {
		t.Fatalf("expected the in-flight task to be reclaimed, got %d (%v)", requeued, err)
	}

	retried, err := queue.ClaimBatch(ctx, 10)
	if err != nil || len(retried) != 1 {
		t.Fatalf("expected a retryable task, got %+v (%v)", retried, err)
	}
	if err := queue.Complete(ctx, []int64{retried[0].ID}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if pending, err := queue.PendingCount(ctx); err != nil || pending != 0 {
		t.Fatalf("expected the queue to drain, pending = %d (%v)", pending, err)
	}
}

func TestAuditLogCopiesEntries(t *testing.T) {
	queue := NewAuditLog()
	if err := queue.Record(context.Background(), ports.AuditEntry{Actor: "adm", Action: "freeze"}); err != nil {
		t.Fatalf("record: %v", err)
	}
	entries := queue.Entries()
	entries[0].Action = "mutated"

	if again := queue.Entries(); again[0].Action != "freeze" {
		t.Fatal("expected Entries to return a copy")
	}
}

func newSubscription(t *testing.T, id domain.SubscriptionID, now time.Time) *domain.Subscription {
	t.Helper()

	subscription, err := domain.NewSubscription(id, "user-1", "netflix", "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 3)
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return subscription
}

func isNotFoundErr(err error) bool { return errors.Is(err, domain.ErrNotFound) }
