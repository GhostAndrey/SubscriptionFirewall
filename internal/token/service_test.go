package token

import (
	"context"
	"errors"
	"io"
	"log/slog"
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
	keys       sync.Map
}

func (i *countedIssuer) Issue(_ context.Context, request ports.IssueRequest) (ports.IssuedCard, error) {
	i.issueCount.Add(1)
	i.keys.Store(request.IdempotencyKey, struct{}{})
	return ports.IssuedCard{
		MaskedPAN:    "411111******1234",
		MonthlyLimit: 10_000,
		Currency:     "USD",
	}, nil
}

func (i *countedIssuer) distinctKeys() int {
	count := 0
	i.keys.Range(func(any, any) bool {
		count++
		return true
	})
	return count
}

func newService(t *testing.T, repository ports.VirtualTokenRepository, issuer ports.VirtualCardIssuer, now time.Time) *Service {
	t.Helper()

	return NewService(repository, issuer, fixedClock{now: now}, Options{
		IssuanceTimeout: time.Minute,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func TestEnsureTokenIsIdempotent(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	issuer := &countedIssuer{}
	service := newService(t, repository, issuer, time.Now())

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
	service := newService(t, repository, &countedIssuer{}, time.Now())

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
	service := newService(t, repository, issuer, now)

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
	service := newService(t, repository, &countedIssuer{}, time.Now())

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

func TestConcurrentAuthorizeNeverExceedsLimit(t *testing.T) {
	const (
		monthlyLimit = 10_000
		chargeAmount = 1_000
		goroutines   = 50
	)

	repository := memory.NewVirtualTokenRepository()
	now := time.Now()
	service := newService(t, repository, &countedIssuer{}, now)

	token, err := service.EnsureToken(context.Background(), "user-1", "netflix")
	if err != nil {
		t.Fatalf("ensure token: %v", err)
	}

	var approved atomic.Int32
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := service.Authorize(context.Background(), token.ID, chargeAmount, "USD"); err == nil {
				approved.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := approved.Load(); got != monthlyLimit/chargeAmount {
		t.Fatalf("approved charges = %d, want %d", got, monthlyLimit/chargeAmount)
	}
	persisted, err := repository.GetByID(context.Background(), token.ID)
	if err != nil {
		t.Fatalf("load persisted token: %v", err)
	}
	if persisted.SpentInPeriod != monthlyLimit {
		t.Errorf("spent = %d, want %d", persisted.SpentInPeriod, monthlyLimit)
	}
}

func TestConcurrentEnsureTokenIssuesOnce(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	issuer := &countedIssuer{}
	service := newService(t, repository, issuer, time.Now())

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
	if keys := issuer.distinctKeys(); keys != 1 {
		t.Errorf("expected a single idempotency key, got %d", keys)
	}
}

func TestEnsureTokenReleasesReservationWhenIssuerFails(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	failing := &scriptedIssuer{err: errors.New("provider down")}
	service := newService(t, repository, failing, time.Now())

	if _, err := service.EnsureToken(context.Background(), "user-1", "netflix"); err == nil {
		t.Fatal("expected issuance failure")
	}

	listed, err := repository.ListByUserPage(context.Background(), "user-1", ports.Page{})
	if err != nil {
		t.Fatalf("list tokens: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("expected no token to survive a failed issuance, got %+v", listed)
	}

	working := &countedIssuer{}
	retried := newService(t, repository, working, time.Now())
	if _, err := retried.EnsureToken(context.Background(), "user-1", "netflix"); err != nil {
		t.Fatalf("retry after release: %v", err)
	}
	if issued := working.issueCount.Load(); issued != 1 {
		t.Errorf("expected the retry to issue once, got %d", issued)
	}
}

func TestEnsureTokenReclaimsStaleReservation(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	reservedAt := time.Now()

	abandoned := domain.NewIssuingVirtualToken("vtok-abandoned", "user-1", "netflix", reservedAt)
	if err := repository.Save(context.Background(), abandoned); err != nil {
		t.Fatalf("seed abandoned reservation: %v", err)
	}

	issuer := &countedIssuer{}
	service := newService(t, repository, issuer, reservedAt.Add(2*time.Minute))
	token, err := service.EnsureToken(context.Background(), "user-1", "netflix")
	if err != nil {
		t.Fatalf("issuance after staleness: %v", err)
	}
	if token.State != domain.VirtualTokenActive {
		t.Fatalf("expected an active token, got %s", token.State)
	}
	if token.ID == abandoned.ID {
		t.Error("expected the stale reservation to be replaced")
	}
	if issued := issuer.issueCount.Load(); issued != 1 {
		t.Errorf("expected exactly one issuance, got %d", issued)
	}
}

func TestEnsureTokenReportsConcurrentIssuance(t *testing.T) {
	repository := memory.NewVirtualTokenRepository()
	now := time.Now()

	pending := domain.NewIssuingVirtualToken("vtok-pending", "user-1", "netflix", now)
	if err := repository.Save(context.Background(), pending); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}

	issuer := &countedIssuer{}
	service := newService(t, repository, issuer, now)
	if _, err := service.EnsureToken(context.Background(), "user-1", "netflix"); !errors.Is(err, ports.ErrIssuanceInProgress) {
		t.Fatalf("expected ErrIssuanceInProgress, got %v", err)
	}
	if issued := issuer.issueCount.Load(); issued != 0 {
		t.Errorf("expected no provider call while a reservation is held, got %d", issued)
	}
}

type scriptedIssuer struct {
	err   error
	calls atomic.Int32
}

func (s *scriptedIssuer) Issue(context.Context, ports.IssueRequest) (ports.IssuedCard, error) {
	s.calls.Add(1)
	return ports.IssuedCard{}, s.err
}
