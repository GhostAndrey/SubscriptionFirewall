package issuer

import (
	"context"
	"errors"
	"testing"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type fakeIssuer struct {
	calls    int
	failures int // number of initial calls that fail before succeeding
	err      error
	delay    time.Duration
}

func (f *fakeIssuer) Issue(ctx context.Context, _ domain.UserID, _ domain.MerchantID) (ports.IssuedCard, error) {
	f.calls++
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return ports.IssuedCard{}, ctx.Err()
		case <-time.After(f.delay):
		}
	}
	if f.calls <= f.failures {
		return ports.IssuedCard{}, f.err
	}
	return ports.IssuedCard{}, nil
}

func TestResilientIssuerRetriesUntilSuccess(t *testing.T) {
	inner := &fakeIssuer{failures: 2, err: errors.New("provider down")}
	issuer := NewResilientIssuer(inner, ResilientConfig{Retries: 3, Backoff: time.Millisecond})

	if _, err := issuer.Issue(context.Background(), "user-1", "merch-1"); err != nil {
		t.Fatalf("Issue failed: %v", err)
	}
	if inner.calls != 3 {
		t.Fatalf("calls = %d, want 3", inner.calls)
	}
}

func TestResilientIssuerReturnsLastErrorAfterExhaustedRetries(t *testing.T) {
	boom := errors.New("provider down")
	inner := &fakeIssuer{failures: 99, err: boom}
	issuer := NewResilientIssuer(inner, ResilientConfig{Retries: 2, Backoff: time.Millisecond})

	if _, err := issuer.Issue(context.Background(), "user-1", "merch-1"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if inner.calls != 3 {
		t.Fatalf("calls = %d, want 3 (1 + 2 retries)", inner.calls)
	}
}

func TestResilientIssuerTimeoutPerAttempt(t *testing.T) {
	inner := &fakeIssuer{delay: 50 * time.Millisecond}
	issuer := NewResilientIssuer(inner, ResilientConfig{Timeout: 5 * time.Millisecond})

	if _, err := issuer.Issue(context.Background(), "user-1", "merch-1"); err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestResilientIssuerBreakerOpensAndRecovers(t *testing.T) {
	boom := errors.New("provider down")
	inner := &fakeIssuer{failures: 99, err: boom}
	issuer := NewResilientIssuer(inner, ResilientConfig{Retries: 0, BreakerThreshold: 2, BreakerCooldown: 20 * time.Millisecond})
	ctx := context.Background()

	// Two failed calls (threshold 2) open the breaker.
	for range 2 {
		if _, err := issuer.Issue(ctx, "user-1", "merch-1"); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want %v", err, boom)
		}
	}

	// While open: fail fast without reaching the provider.
	callsAfterOpen := inner.calls
	if _, err := issuer.Issue(ctx, "user-1", "merch-1"); !errors.Is(err, errBreakerOpen) {
		t.Fatalf("err = %v, want %v", err, errBreakerOpen)
	}
	if inner.calls != callsAfterOpen {
		t.Fatal("open breaker must not call the provider")
	}

	// Cooldown elapsed: single probe succeeds, breaker closes.
	inner.err = nil
	time.Sleep(25 * time.Millisecond)
	if _, err := issuer.Issue(ctx, "user-1", "merch-1"); err != nil {
		t.Fatalf("probe Issue failed: %v", err)
	}
	if _, err := issuer.Issue(ctx, "user-1", "merch-1"); err != nil {
		t.Fatalf("Issue after recovery failed: %v", err)
	}
}
