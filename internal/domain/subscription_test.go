package domain

import (
	"errors"
	"testing"
	"time"
)

func newSubscription(t *testing.T, state SubscriptionState, window BillingWindow, lastChargedAt time.Time) *Subscription {
	t.Helper()

	subscription, err := NewSubscription("sub-1", "user-1", "netflix", "Netflix",
		state, window, 1500, "USD", lastChargedAt, 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return subscription
}

func TestNewSubscriptionRejectsInvalidInput(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		state    SubscriptionState
		window   BillingWindow
		observed int
	}{
		"unknown state":    {SubscriptionState("Weird"), WindowMonthly, 3},
		"empty state":      {"", WindowMonthly, 3},
		"unknown window":   {SubscriptionActive, BillingWindow("17d"), 3},
		"empty window":     {SubscriptionActive, "", 3},
		"no observations":  {SubscriptionActive, WindowMonthly, 0},
		"negative charges": {SubscriptionActive, WindowMonthly, -1},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewSubscription("sub-1", "user-1", "netflix", "Netflix",
				test.state, test.window, 1500, "USD", now, test.observed)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition, got %v", err)
			}
		})
	}
}

func TestNewSubscriptionDerivesNextExpectedAt(t *testing.T) {
	lastChargedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	for _, window := range []BillingWindow{WindowWeekly, WindowMonthly, WindowYearly} {
		t.Run(string(window), func(t *testing.T) {
			subscription := newSubscription(t, SubscriptionActive, window, lastChargedAt)
			if want := lastChargedAt.Add(window.Duration()); !subscription.NextExpectedAt.Equal(want) {
				t.Fatalf("next expected = %s, want %s", subscription.NextExpectedAt, want)
			}
		})
	}
}

func TestSubscriptionStateIsValid(t *testing.T) {
	valid := []SubscriptionState{
		SubscriptionActive, SubscriptionTrial, SubscriptionZombie,
		SubscriptionFrozen, SubscriptionTerminated,
	}
	for _, state := range valid {
		if !state.IsValid() {
			t.Errorf("%s must be valid", state)
		}
	}
	for _, state := range []SubscriptionState{"", "active", "ZOMBIE", "Unknown"} {
		if state.IsValid() {
			t.Errorf("%q must be invalid", state)
		}
	}
}

func TestBillingWindowDuration(t *testing.T) {
	tests := map[BillingWindow]time.Duration{
		WindowWeekly:  7 * 24 * time.Hour,
		WindowMonthly: 30 * 24 * time.Hour,
		WindowYearly:  365 * 24 * time.Hour,
		"":            0,
		"17d":         0,
	}

	for window, want := range tests {
		if got := window.Duration(); got != want {
			t.Errorf("%q.Duration() = %s, want %s", window, got, want)
		}
	}
}

func TestMarkMissedTransitionsOnlyWhenWindowPassed(t *testing.T) {
	lastChargedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		state   SubscriptionState
		elapsed time.Duration
		want    SubscriptionState
	}{
		"active just overdue":         {SubscriptionActive, 31 * 24 * time.Hour, SubscriptionActive},
		"active a full window late":   {SubscriptionActive, 61 * 24 * time.Hour, SubscriptionZombie},
		"trial a full window late":    {SubscriptionTrial, 61 * 24 * time.Hour, SubscriptionZombie},
		"zombie stays zombie":         {SubscriptionZombie, 61 * 24 * time.Hour, SubscriptionZombie},
		"frozen is never zombied":     {SubscriptionFrozen, 400 * 24 * time.Hour, SubscriptionFrozen},
		"terminated is never zombied": {SubscriptionTerminated, 400 * 24 * time.Hour, SubscriptionTerminated},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			subscription := newSubscription(t, test.state, WindowMonthly, lastChargedAt)
			if err := subscription.MarkMissed(lastChargedAt.Add(test.elapsed)); err != nil {
				t.Fatalf("mark missed: %v", err)
			}
			if subscription.State != test.want {
				t.Fatalf("state = %s, want %s", subscription.State, test.want)
			}
		})
	}
}

func TestObserveActivityRules(t *testing.T) {
	lastChargedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	t.Run("resumes an active subscription", func(t *testing.T) {
		subscription := newSubscription(t, SubscriptionZombie, WindowMonthly, lastChargedAt)
		chargedAt := lastChargedAt.Add(120 * 24 * time.Hour)

		if err := subscription.ObserveActivity(chargedAt, 1800, 5); err != nil {
			t.Fatalf("observe: %v", err)
		}
		if subscription.State != SubscriptionActive {
			t.Errorf("state = %s, want Active", subscription.State)
		}
		if subscription.AverageAmount != 1800 || subscription.ObservedPayments != 5 {
			t.Errorf("amounts not applied: %d / %d", subscription.AverageAmount, subscription.ObservedPayments)
		}
		if want := chargedAt.Add(WindowMonthly.Duration()); !subscription.NextExpectedAt.Equal(want) {
			t.Errorf("next expected = %s, want %s", subscription.NextExpectedAt, want)
		}
	})

	t.Run("keeps a frozen subscription frozen", func(t *testing.T) {
		subscription := newSubscription(t, SubscriptionFrozen, WindowMonthly, lastChargedAt)

		if err := subscription.ObserveActivity(lastChargedAt.Add(30*24*time.Hour), 1600, 4); err != nil {
			t.Fatalf("observe: %v", err)
		}
		if subscription.State != SubscriptionFrozen {
			t.Errorf("state = %s, want Frozen", subscription.State)
		}
		if subscription.AverageAmount != 1600 {
			t.Errorf("amounts should still be refreshed, got %d", subscription.AverageAmount)
		}
	})

	t.Run("refuses a terminated subscription", func(t *testing.T) {
		subscription := newSubscription(t, SubscriptionTerminated, WindowMonthly, lastChargedAt)

		err := subscription.ObserveActivity(lastChargedAt.Add(30*24*time.Hour), 1600, 4)
		if !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("expected ErrInvalidTransition, got %v", err)
		}
	})
}

func TestSubscriptionLifecycleTransitions(t *testing.T) {
	lastChargedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	t.Run("freeze from every live state", func(t *testing.T) {
		for _, state := range []SubscriptionState{SubscriptionActive, SubscriptionTrial, SubscriptionZombie} {
			subscription := newSubscription(t, state, WindowMonthly, lastChargedAt)
			if err := subscription.Freeze(); err != nil {
				t.Fatalf("freeze from %s: %v", state, err)
			}
			if subscription.State != SubscriptionFrozen {
				t.Errorf("from %s: state = %s, want Frozen", state, subscription.State)
			}
		}
	})

	t.Run("freeze is idempotent", func(t *testing.T) {
		subscription := newSubscription(t, SubscriptionActive, WindowMonthly, lastChargedAt)
		for range 2 {
			if err := subscription.Freeze(); err != nil {
				t.Fatalf("freeze: %v", err)
			}
		}
		if subscription.State != SubscriptionFrozen {
			t.Errorf("state = %s", subscription.State)
		}
	})

	t.Run("freeze refuses terminal states", func(t *testing.T) {
		subscription := newSubscription(t, SubscriptionTerminated, WindowMonthly, lastChargedAt)
		if err := subscription.Freeze(); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("expected ErrInvalidTransition, got %v", err)
		}
	})

	t.Run("reactivate lifts only a frozen subscription", func(t *testing.T) {
		for _, state := range []SubscriptionState{SubscriptionFrozen, SubscriptionActive, SubscriptionTrial, SubscriptionZombie} {
			subscription := newSubscription(t, state, WindowMonthly, lastChargedAt)
			if err := subscription.Reactivate(); err != nil {
				t.Fatalf("reactivate from %s: %v", state, err)
			}
			want := state
			if state == SubscriptionFrozen {
				want = SubscriptionActive
			}
			if subscription.State != want {
				t.Errorf("from %s: state = %s, want %s", state, subscription.State, want)
			}
		}
	})

	t.Run("reactivate refuses a terminated subscription", func(t *testing.T) {
		subscription := newSubscription(t, SubscriptionTerminated, WindowMonthly, lastChargedAt)
		if err := subscription.Reactivate(); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("expected ErrInvalidTransition, got %v", err)
		}
	})

	t.Run("terminate is final and idempotent", func(t *testing.T) {
		subscription := newSubscription(t, SubscriptionActive, WindowMonthly, lastChargedAt)
		for range 2 {
			if err := subscription.Terminate(); err != nil {
				t.Fatalf("terminate: %v", err)
			}
		}
		if subscription.State != SubscriptionTerminated {
			t.Fatalf("state = %s", subscription.State)
		}
		if err := subscription.Freeze(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("freeze after terminate must fail, got %v", err)
		}
		if err := subscription.Reactivate(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("reactivate after terminate must fail, got %v", err)
		}
		if err := subscription.ObserveActivity(lastChargedAt, 1500, 3); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("observe after terminate must fail, got %v", err)
		}
	})
}

func TestMCCIsSubscriptionProne(t *testing.T) {
	prone := []MCC{MCCRecurringBilling, MCCDigitalGoodsSubscript, MCCInsuranceSubscription, MCCStreamingMedia}
	for _, mcc := range prone {
		if !mcc.IsSubscriptionProne() {
			t.Errorf("MCC %d must be subscription prone", mcc)
		}
	}
	for _, mcc := range []MCC{5411, 5814, 0, 9999, 5967} {
		if mcc.IsSubscriptionProne() {
			t.Errorf("MCC %d must not be subscription prone", mcc)
		}
	}
}
