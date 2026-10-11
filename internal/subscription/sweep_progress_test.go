package subscription

import (
	"context"
	"errors"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

// stuckRepository hands back the same overdue row on every read and refuses to
// let it advance, which is what a row under sustained contention from another
// replica looks like to the sweep.
type stuckRepository struct {
	ports.SubscriptionRepository
	pending *domain.Subscription
	reads   int
}

func (r *stuckRepository) ListPendingZombieTransition(context.Context, time.Time, int) ([]*domain.Subscription, error) {
	r.reads++
	return []*domain.Subscription{r.pending}, nil
}

func (r *stuckRepository) GetByID(context.Context, domain.SubscriptionID) (*domain.Subscription, error) {
	cloned := *r.pending
	return &cloned, nil
}

func (r *stuckRepository) UpdateVersion(context.Context, *domain.Subscription) error {
	return ports.ErrVersionConflict
}

func TestSweepStopsWhenABatchMakesNoProgress(t *testing.T) {
	// A window boundary that is exactly overdue would be re-read forever if the
	// sweep looped on emptiness alone; the progress guard must end the pass.
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	stuck, err := domain.NewSubscription(
		"sub-stuck", "user-1", "netflix", "Netflix", domain.SubscriptionActive,
		domain.WindowMonthly, 1500, "USD", now.Add(-60*24*time.Hour), 3)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	repository := &stuckRepository{pending: stuck}
	service := NewService(repository, fixedClock{now: now})

	changed, err := service.Sweep(context.Background(), 0)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(changed) != 0 {
		t.Fatalf("expected no transitions, got %+v", changed)
	}
	if repository.reads != 1 {
		t.Errorf("pending reads = %d, want 1: a pass without progress must not re-read", repository.reads)
	}
}

func TestSweepReportsTheTransitionBeforeAnInterruptedContext(t *testing.T) {
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	repository := newSweepRepository()
	service := NewService(repository, fixedClock{now: now})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repository.cancelOnList = cancel

	changed, err := service.Sweep(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(changed) != 1 || changed[0].State != domain.SubscriptionZombie {
		t.Fatalf("expected the already-persisted zombie to be reported, got %+v", changed)
	}
}

// cancelOnListRepository cancels the sweep context after the first transition
// has been written, so the pass ends between batches.
type cancelOnListRepository struct {
	ports.SubscriptionRepository
	cancelOnList func()
	listed       int
}

func (r *cancelOnListRepository) ListPendingZombieTransition(ctx context.Context, now time.Time, limit int) ([]*domain.Subscription, error) {
	r.listed++
	if r.cancelOnList != nil && r.listed > 1 {
		r.cancelOnList()
		return nil, ctx.Err()
	}
	return r.SubscriptionRepository.ListPendingZombieTransition(ctx, now, limit)
}

func newSweepRepository() *cancelOnListRepository {
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	repository := memory.NewSubscriptionRepository()
	overdue, err := domain.NewSubscription(
		"sub-overdue", "user-1", "netflix", "Netflix", domain.SubscriptionActive,
		domain.WindowMonthly, 1500, "USD", now.Add(-90*24*time.Hour), 3)
	if err != nil {
		panic(err)
	}
	if err := repository.Save(context.Background(), overdue); err != nil {
		panic(err)
	}
	return &cancelOnListRepository{SubscriptionRepository: repository}
}
