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
