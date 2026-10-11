package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
)

// brokenLookupRepository fails the counterpart lookup with an error that is not
// ErrNotFound, which is what a database outage looks like to the lifecycle
// service. The paired call must surface that error instead of crashing on the
// nil subscription the lookup left behind.
type brokenLookupRepository struct {
	ports.SubscriptionRepository
	err error
}

func (r *brokenLookupRepository) FindByUserAndMerchant(context.Context, domain.UserID, domain.MerchantID) (*domain.Subscription, error) {
	return nil, r.err
}

func TestChangeTokenSurvivesAFailedCounterpartLookup(t *testing.T) {
	storageErr := errors.New("connection reset by peer")
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)

	h.service = NewService(
		subscription.NewService(&brokenLookupRepository{
			SubscriptionRepository: h.subscriptions,
			err:                    storageErr,
		}, h.fixedNow),
		h.toks,
		Options{
			Queue:   h.queue,
			Audit:   h.audit,
			Metrics: h.metrics,
			Logger:  h.logger,
		},
	)

	// The freeze itself succeeded, so the token is frozen; the counterpart
	// could not be reached and must be rolled back.
	if _, err := h.service.FreezeToken(t.Context(), "adm-key", "vtok-1"); err == nil {
		t.Fatal("expected the storage failure to be reported")
	}

	token, err := h.tokens.GetByID(t.Context(), "vtok-1")
	if err != nil {
		t.Fatalf("load token: %v", err)
	}
	if token.State != domain.VirtualTokenActive {
		t.Errorf("token state = %s, want Active: a reversible action must be rolled back", token.State)
	}
}

func TestChangeTokenDefersAnIrreversibleActionOnLookupFailure(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)

	h.service = NewService(
		subscription.NewService(&brokenLookupRepository{
			SubscriptionRepository: h.subscriptions,
			err:                    errors.New("connection reset by peer"),
		}, h.fixedNow),
		h.toks,
		Options{
			Queue:   h.queue,
			Audit:   h.audit,
			Metrics: h.metrics,
			Logger:  h.logger,
		},
	)

	if _, err := h.service.TerminateToken(t.Context(), "adm-key", "vtok-1"); !errors.Is(err, ErrSyncDeferred) {
		t.Fatalf("expected ErrSyncDeferred, got %v", err)
	}

	token, err := h.tokens.GetByID(t.Context(), "vtok-1")
	if err != nil {
		t.Fatalf("load token: %v", err)
	}
	if token.State != domain.VirtualTokenTerminated {
		t.Errorf("token state = %s, want Terminated: an irreversible action must stand", token.State)
	}

	pending, err := h.queue.PendingCount(context.Background())
	if err != nil {
		t.Fatalf("pending count: %v", err)
	}
	if pending != 1 {
		t.Errorf("pending syncs = %d, want 1: the counterpart must be queued for replay", pending)
	}
}
