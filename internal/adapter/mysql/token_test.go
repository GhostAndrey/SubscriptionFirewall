package mysql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

func TestVirtualTokenUpdateVersionRejectsStaleWrite(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	seeded := newTestToken(t, "vtok-version", 10_000)
	if err := repository.Save(ctx, seeded); err != nil {
		t.Fatalf("seed: %v", err)
	}

	stale, err := repository.GetByID(ctx, "vtok-version")
	if err != nil {
		t.Fatalf("get stale: %v", err)
	}
	if _, err := repository.Charge(ctx, "vtok-version", 2_500, "USD", now); err != nil {
		t.Fatalf("charge: %v", err)
	}

	// Read after the charge, so this copy is current and must be accepted.
	latest, err := repository.GetByID(ctx, "vtok-version")
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if err := latest.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.UpdateVersion(ctx, latest); err != nil {
		t.Fatalf("update latest: %v", err)
	}
	if got := latest.CurrentVersion(); got != 2 {
		t.Fatalf("version = %d, want 2: the charge and the freeze each bump it", got)
	}

	if err := stale.Freeze(); err != nil {
		t.Fatalf("freeze stale: %v", err)
	}
	if err := repository.UpdateVersion(ctx, stale); !errors.Is(err, ports.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}

	reloaded, err := repository.GetByID(ctx, "vtok-version")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.State != domain.VirtualTokenFrozen {
		t.Errorf("state = %s, want Frozen", reloaded.State)
	}
	if got := reloaded.Snapshot().SpentInPeriod; got != 2_500 {
		t.Errorf("spent = %d, want 2500: a rejected write must not clobber the charge", got)
	}
}

func TestVirtualTokenUpdateVersionReportsMissingRow(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()

	absent := newTestToken(t, "vtok-absent", 10_000)
	if err := repository.UpdateVersion(ctx, absent); !isNotFound(err) {
		t.Fatalf("expected ErrNotFound rather than a version conflict, got %v", err)
	}
}

func TestChargeInvalidatesAPendingLifecycleTransition(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	if err := repository.Save(ctx, newTestToken(t, "vtok-race", 10_000)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	read := func() *domain.VirtualToken {
		t.Helper()
		loaded, err := repository.GetByID(ctx, "vtok-race")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		return loaded
	}

	transitioning := read()
	if _, err := repository.Charge(ctx, "vtok-race", 4_000, "USD", now); err != nil {
		t.Fatalf("charge: %v", err)
	}
	if err := transitioning.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.UpdateVersion(ctx, transitioning); !errors.Is(err, ports.ErrVersionConflict) {
		t.Fatalf("expected the interleaved charge to reject the freeze, got %v", err)
	}

	retry := read()
	if err := retry.Freeze(); err != nil {
		t.Fatalf("freeze retry: %v", err)
	}
	if err := repository.UpdateVersion(ctx, retry); err != nil {
		t.Fatalf("update retry: %v", err)
	}

	final := read()
	if final.State != domain.VirtualTokenFrozen {
		t.Errorf("state = %s, want Frozen", final.State)
	}
	if got := final.Snapshot().SpentInPeriod; got != 4_000 {
		t.Errorf("spent = %d, want 4000", got)
	}
}

func TestCompleteTokenIssuanceReportsALostReservation(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	reservation, err := repository.ReserveTokenIssuance(ctx, "user-lost", "netflix", "issue:user-lost:netflix", now, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !reservation.Reserved {
		t.Fatal("expected the reservation to be granted")
	}

	// Another replica takes the abandoned reservation over, which clears the
	// issuance key this caller still holds.
	if _, err := repository.ReserveTokenIssuance(ctx, "user-lost", "netflix", "issue:user-lost:netflix", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("take over: %v", err)
	}
	if err := repository.ReleaseTokenIssuance(ctx, reservation.TokenID); err != nil {
		t.Fatalf("release: %v", err)
	}

	card := ports.IssuedCard{MaskedPAN: "411111******1234", MonthlyLimit: 10_000, Currency: "USD"}
	if _, err := repository.CompleteTokenIssuance(ctx, reservation.TokenID, "issue:user-lost:netflix", card, now); err == nil {
		t.Fatal("expected the completion of a released reservation to fail")
	} else if !isNotFound(err) {
		t.Fatalf("expected ErrNotFound for a released reservation, got %v", err)
	}
}

func TestCompleteTokenIssuanceRejectsAForeignIssuanceKey(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	reservation, err := repository.ReserveTokenIssuance(ctx, "user-foreign", "netflix", "issue:user-foreign:netflix", now, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	card := ports.IssuedCard{MaskedPAN: "411111******1234", MonthlyLimit: 10_000, Currency: "USD"}
	_, err = repository.CompleteTokenIssuance(ctx, reservation.TokenID, "issue:someone-else:netflix", card, now)
	if !errors.Is(err, ports.ErrIssuanceLost) {
		t.Fatalf("expected ErrIssuanceLost for a foreign issuance key, got %v", err)
	}

	held, err := repository.GetByID(ctx, reservation.TokenID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if held.State != domain.VirtualTokenIssuing {
		t.Errorf("state = %s, want Issuing: a rejected completion must leave the reservation held", held.State)
	}
}

func TestCompleteTokenIssuanceKeepsOperatorState(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	reservation, err := repository.ReserveTokenIssuance(ctx, "user-frozen", "netflix", "issue:user-frozen:netflix", now, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !reservation.Reserved {
		t.Fatal("expected the reservation to be granted")
	}

	frozen, err := repository.GetByID(ctx, reservation.TokenID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := frozen.Freeze(); err != nil {
		t.Fatalf("freeze an in-flight reservation: %v", err)
	}
	if err := repository.UpdateVersion(ctx, frozen); err != nil {
		t.Fatalf("persist freeze: %v", err)
	}

	completed, err := repository.CompleteTokenIssuance(ctx, reservation.TokenID, "issue:user-frozen:netflix",
		ports.IssuedCard{MaskedPAN: "411111******1234", MonthlyLimit: 10_000, Currency: "USD"}, now)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if completed.State != domain.VirtualTokenFrozen {
		t.Errorf("state = %s, want Frozen", completed.State)
	}
	if completed.MaskedPAN != "411111******1234" {
		t.Errorf("card not recorded: %+v", completed.Snapshot())
	}
}

func TestReleaseTokenIssuanceDropsAnUnprovisionedReservation(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	reservation, err := repository.ReserveTokenIssuance(ctx, "user-release", "netflix", "issue:user-release:netflix", now, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	frozen, err := repository.GetByID(ctx, reservation.TokenID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := frozen.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.UpdateVersion(ctx, frozen); err != nil {
		t.Fatalf("persist freeze: %v", err)
	}

	if err := repository.ReleaseTokenIssuance(ctx, reservation.TokenID); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := repository.GetByID(ctx, reservation.TokenID); !isNotFound(err) {
		t.Fatalf("a reservation that never got a card must be dropped, got %v", err)
	}
}

func TestLifecycleSyncClaimHandlesAFullBatch(t *testing.T) {
	db := testDB(t)
	queue := NewLifecycleSyncQueue(db)
	ctx := context.Background()
	now := time.Now().UTC()

	// A full batch is the case that used to leave the result set open and make
	// the claim UPDATE fail with "commands out of sync".
	const rounds, batch = 2, 16
	for i := range rounds * batch {
		task := ports.LifecycleSyncTask{
			EntityType: "token",
			EntityID:   fmt.Sprintf("vtok-full-batch-%02d", i),
			Action:     "freeze",
		}
		if err := queue.Enqueue(ctx, task); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	for round := range rounds {
		claimed, err := queue.ClaimBatch(ctx, batch)
		if err != nil {
			t.Fatalf("claim round %d: %v", round, err)
		}
		if len(claimed) != batch {
			t.Fatalf("claimed %d tasks in round %d, want %d", len(claimed), round, batch)
		}
		ids := make([]int64, 0, len(claimed))
		for _, task := range claimed {
			ids = append(ids, task.ID)
		}
		if err := queue.Complete(ctx, ids); err != nil {
			t.Fatalf("complete round %d: %v", round, err)
		}
	}
	_ = now
}

func TestConcurrentChargesAndFreezesKeepEveryDebit(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	const workers, amount = 8, 1_000
	if err := repository.Save(ctx, newTestToken(t, "vtok-parallel", 100_000)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The debits land first and in full, so the final balance is a fixed
	// number. Freezing afterwards may succeed or conflict depending on the
	// interleaving, but no outcome may change that balance.
	var charges sync.WaitGroup
	for worker := range workers {
		charges.Add(1)
		go func() {
			defer charges.Done()
			if _, err := repository.Charge(ctx, "vtok-parallel", amount, "USD", now); err != nil {
				t.Errorf("charge %d: %v", worker, err)
			}
		}()
	}
	charges.Wait()

	var freezes sync.WaitGroup
	for worker := range workers {
		freezes.Add(1)
		go func() {
			defer freezes.Done()
			loaded, err := repository.GetByID(ctx, "vtok-parallel")
			if err != nil {
				t.Errorf("get %d: %v", worker, err)
				return
			}
			if err := loaded.Freeze(); err != nil {
				t.Errorf("freeze %d: %v", worker, err)
				return
			}
			// Several replicas freezing at once conflict with each other; the
			// caller retries, which is what must not rewind the balance.
			if err := repository.UpdateVersion(ctx, loaded); err != nil &&
				!errors.Is(err, ports.ErrVersionConflict) {
				t.Errorf("update %d: %v", worker, err)
			}
		}()
	}
	freezes.Wait()

	final, err := repository.GetByID(ctx, "vtok-parallel")
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if want := int64(workers * amount); final.Snapshot().SpentInPeriod != want {
		t.Errorf("spent = %d, want %d: concurrent freezes must never discard a debit",
			final.Snapshot().SpentInPeriod, want)
	}
	if final.State != domain.VirtualTokenFrozen {
		t.Errorf("state = %s, want Frozen", final.State)
	}
}
