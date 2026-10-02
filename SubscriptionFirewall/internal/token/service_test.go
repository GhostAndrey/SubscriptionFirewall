package token

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type countedIssuer struct {
	issueCount atomic.Int32
}

func (i *countedIssuer) Issue(_ context.Context, _ domain.UserID, merchantID domain.MerchantID) (ports.IssuedCard, error) {
	i.issueCount.Add(1)
	return ports.IssuedCard{
		TokenID:      domain.VirtualTokenID("vtok-" + string(merchantID)),
		MaskedPAN:    "411111******1234",
		MonthlyLimit: 10_000,
		Currency:     "USD",
	}, nil
}

func TestEnsureTokenIsIdempotent(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	issuer := &countedIssuer{}
	service := NewService(repository, issuer, fixedClock{now: time.Now()})

	first, err := service.EnsureToken(context.Background(), "user-1", "netflix")
	if err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	second, err := service.EnsureToken(context.Background(), "user-1", "netflix")
	if err != nil {
		t.Fatalf("second ensure: %v", err)
	}

	if first.ID != second.ID {
		t.Errorf("expected same token, got %s and %s", first.ID, second.ID)
	}
	if issued := issuer.issueCount.Load(); issued != 1 {
		t.Errorf("expected issuer to be called once, got %d", issued)
	}
}

func TestEnsureTokenIssuesPerMerchant(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	service := NewService(repository, &countedIssuer{}, fixedClock{now: time.Now()})

	netflix, err := service.EnsureToken(context.Background(), "user-1", "netflix")
	if err != nil {
		t.Fatalf("ensure netflix: %v", err)
	}
	spotify, err := service.EnsureToken(context.Background(), "user-1", "spotify")
	if err != nil {
		t.Fatalf("ensure spotify: %v", err)
	}

	if netflix.ID == spotify.ID {
		t.Errorf("expected distinct tokens per merchant, both %s", netflix.ID)
	}
}

func TestAuthorizeAccumulatesAndEnforcesLimit(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	issuer := &countedIssuer{}
	now := time.Now()
	service := NewService(repository, issuer, fixedClock{now: now})

	token, err := service.EnsureToken(context.Background(), "user-1", "netflix")
	if err != nil {
		t.Fatalf("ensure token: %v", err)
	}

	if _, err := service.Authorize(context.Background(), token.ID, 6_000, "USD"); err != nil {
		t.Fatalf("first authorize: %v", err)
	}
	if _, err := service.Authorize(context.Background(), token.ID, 4_000, "USD"); err != nil {
		t.Fatalf("second authorize: %v", err)
	}
	if _, err := service.Authorize(context.Background(), token.ID, 1_000, "USD"); !errors.Is(err, domain.ErrSpendLimitExceeded) {
		t.Fatalf("expected ErrSpendLimitExceeded, got %v", err)
	}

	persisted, err := repository.GetByID(context.Background(), token.ID)
	if err != nil {
		t.Fatalf("load persisted token: %v", err)
	}
	if persisted.SpentInPeriod != 10_000 {
		t.Errorf("expected declined charge not to accumulate, got %d", persisted.SpentInPeriod)
	}
}

func TestFrozenTokenBlocksAuthorization(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	service := NewService(repository, &countedIssuer{}, fixedClock{now: time.Now()})

	token, err := service.EnsureToken(context.Background(), "user-1", "netflix")
	if err != nil {
		t.Fatalf("ensure token: %v", err)
	}
	if _, err := service.Freeze(context.Background(), token.ID); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	if _, err := service.Authorize(context.Background(), token.ID, 100, "USD"); !errors.Is(err, domain.ErrTokenNotActive) {
		t.Fatalf("expected ErrTokenNotActive, got %v", err)
	}
}

func TestConcurrentEnsureTokenIssuesOnce(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	issuer := &countedIssuer{}
	service := NewService(repository, issuer, fixedClock{now: time.Now()})

	const goroutines = 16
	var wg sync.WaitGroup
	ids := make([]domain.VirtualTokenID, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			token, err := service.EnsureToken(context.Background(), "user-1", "netflix")
			if err != nil {
				t.Errorf("ensure token: %v", err)
				return
			}
			ids[slot] = token.ID
		}(i)
	}
	wg.Wait()

	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("expected all goroutines to observe one token, got %s vs %s", id, ids[0])
		}
	}
	if issued := issuer.issueCount.Load(); issued != 1 {
		t.Errorf("expected issuer to be called once, got %d", issued)
	}
}
