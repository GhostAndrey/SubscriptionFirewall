package domain

import (
	"errors"
	"testing"
	"time"
)

func TestAuthorizeChargeAccumulatesSpending(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	token, err := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	if err := token.AuthorizeCharge(6_000, "USD", now); err != nil {
		t.Fatalf("first charge: %v", err)
	}
	if err := token.AuthorizeCharge(4_000, "USD", now); err != nil {
		t.Fatalf("second charge: %v", err)
	}
	if snapshot := token.Snapshot(); snapshot.SpentInPeriod != 10_000 {
		t.Errorf("expected spent 10000, got %d", snapshot.SpentInPeriod)
	}
}

func TestAuthorizeChargeRejectsOverLimit(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	token, _ := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)

	err := token.AuthorizeCharge(10_001, "USD", now)
	if !errors.Is(err, ErrSpendLimitExceeded) {
		t.Fatalf("expected ErrSpendLimitExceeded, got %v", err)
	}
}

func TestFrozenTokenRejectsAuthorization(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	token, _ := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)

	if err := token.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	err := token.AuthorizeCharge(100, "USD", now)
	if !errors.Is(err, ErrTokenNotActive) {
		t.Fatalf("expected ErrTokenNotActive, got %v", err)
	}
}

func TestPeriodRollResetsSpending(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	token, _ := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", start)

	if err := token.AuthorizeCharge(9_999, "USD", start); err != nil {
		t.Fatalf("charge in first period: %v", err)
	}
	nextPeriod := start.Add(31 * 24 * time.Hour)
	if err := token.AuthorizeCharge(1, "USD", nextPeriod); err != nil {
		t.Fatalf("charge in next period: %v", err)
	}
	if snapshot := token.Snapshot(); snapshot.SpentInPeriod != 1 {
		t.Errorf("expected spending reset to 1, got %d", snapshot.SpentInPeriod)
	}
}

func TestAuthorizeChargeRejectsNonPositiveAmount(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	for _, amount := range []int64{0, -1, -10_000} {
		token, _ := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)
		if err := token.AuthorizeCharge(9_999, "USD", now); err != nil {
			t.Fatalf("prime spending: %v", err)
		}

		err := token.AuthorizeCharge(amount, "USD", now)
		if !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("amount %d: expected ErrInvalidAmount, got %v", amount, err)
		}
		if snapshot := token.Snapshot(); snapshot.SpentInPeriod != 9_999 {
			t.Errorf("amount %d: expected spending unchanged at 9999, got %d", amount, snapshot.SpentInPeriod)
		}
	}
}

func TestAuthorizeChargeRejectsInvalidAmountBeforeStateCheck(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	token, _ := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)
	if err := token.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	if err := token.AuthorizeCharge(-100, "USD", now); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("expected ErrInvalidAmount to win over frozen state, got %v", err)
	}
}

func TestApplyChargeIsolatesFailures(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	token, _ := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)
	if err := token.ApplyCharge(1_000, "USD", now); err != nil {
		t.Fatalf("prime spending: %v", err)
	}

	before := token.Snapshot()
	if err := token.ApplyCharge(0, "USD", now); err == nil {
		t.Fatal("expected zero amount to be rejected")
	}
	if err := token.ApplyCharge(-1, "USD", now); err == nil {
		t.Fatal("expected negative amount to be rejected")
	}
	after := token.Snapshot()
	if after.SpentInPeriod != before.SpentInPeriod || after.State != before.State {
		t.Fatalf("rejected charges changed the token: before=%+v after=%+v", before, after)
	}
}

func TestNewIssuingVirtualTokenStartsPending(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	token := NewIssuingVirtualToken("vtok-1", "user-1", "netflix", now)

	if token.State != VirtualTokenIssuing {
		t.Errorf("state = %s, want %s", token.State, VirtualTokenIssuing)
	}
	if token.MaskedPAN != "" || token.MonthlyLimit != 0 {
		t.Errorf("a reservation must carry no card data yet: %+v", token.Snapshot())
	}
	if !token.PeriodStarted.Equal(now) {
		t.Errorf("period started = %s, want %s", token.PeriodStarted, now)
	}
	if err := token.AuthorizeCharge(100, "USD", now); !errors.Is(err, ErrTokenNotActive) {
		t.Errorf("a reservation must not authorize charges, got %v", err)
	}
}

func TestCompleteIssuance(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	t.Run("promotes a reservation to an active card", func(t *testing.T) {
		token := NewIssuingVirtualToken("vtok-1", "user-1", "netflix", now)
		completedAt := now.Add(time.Minute)

		if err := token.CompleteIssuance("411111******1234", 25_000, "EUR", completedAt); err != nil {
			t.Fatalf("complete: %v", err)
		}
		snapshot := token.Snapshot()
		if snapshot.State != VirtualTokenActive {
			t.Errorf("state = %s, want Active", snapshot.State)
		}
		if snapshot.MaskedPAN != "411111******1234" || snapshot.MonthlyLimit != 25_000 || snapshot.Currency != "EUR" {
			t.Errorf("card data not applied: %+v", snapshot)
		}
		if !snapshot.PeriodStarted.Equal(completedAt) {
			t.Errorf("period started = %s, want %s", snapshot.PeriodStarted, completedAt)
		}
	})

	t.Run("rejects a card that is not usable", func(t *testing.T) {
		tests := map[string]struct {
			pan      string
			limit    int64
			currency string
		}{
			"blank pan":      {"   ", 10_000, "USD"},
			"zero limit":     {"411111******1234", 0, "USD"},
			"negative limit": {"411111******1234", -1, "USD"},
			"bad currency":   {"411111******1234", 10_000, "usd"},
			"no currency":    {"411111******1234", 10_000, ""},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				token := NewIssuingVirtualToken("vtok-1", "user-1", "netflix", now)
				if err := token.CompleteIssuance(test.pan, test.limit, test.currency, now); !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("expected ErrInvalidTransition, got %v", err)
				}
				if token.State != VirtualTokenIssuing {
					t.Errorf("a rejected completion must keep the reservation, got %s", token.State)
				}
			})
		}
	})

	t.Run("cannot be completed twice", func(t *testing.T) {
		token := NewIssuingVirtualToken("vtok-1", "user-1", "netflix", now)
		if err := token.CompleteIssuance("411111******1234", 10_000, "USD", now); err != nil {
			t.Fatalf("first completion: %v", err)
		}
		if err := token.CompleteIssuance("411111******5678", 10_000, "USD", now); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("expected ErrInvalidTransition, got %v", err)
		}
	})
}

func TestNewVirtualTokenRejectsUnusableCard(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		pan      string
		limit    int64
		currency string
	}{
		"blank pan":      {"", 10_000, "USD"},
		"blank pan only": {"  ", 10_000, "USD"},
		"zero limit":     {"411111******1234", 0, "USD"},
		"negative limit": {"411111******1234", -5, "USD"},
		"bad currency":   {"411111******1234", 10_000, "usd"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewVirtualToken("vtok-1", "user-1", "netflix", test.pan, test.limit, test.currency, now)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition, got %v", err)
			}
		})
	}
}

func TestTerminatedTokenCannotBeReactivated(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	token, _ := NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)

	if err := token.Terminate(); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if err := token.Reactivate(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}
