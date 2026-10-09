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
