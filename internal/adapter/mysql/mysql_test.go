package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("SUBSCRIPTION_FIREWALL_TEST_DSN")
	if dsn == "" {
		t.Skip("SUBSCRIPTION_FIREWALL_TEST_DSN is not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, table := range []string{"transactions", "subscriptions", "virtual_tokens", "detection_outbox", "audit_log", "lifecycle_sync_outbox"} {
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	return db
}

func isNotFound(err error) bool      { return errors.Is(err, domain.ErrNotFound) }
func isAlreadyExists(err error) bool { return errors.Is(err, domain.ErrAlreadyExists) }

func TestTransactionRepositoryRoundTrip(t *testing.T) {
	db := testDB(t)
	repository := NewTransactionRepository(db)
	ctx := context.Background()
	authorizedAt := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	transaction := domain.Transaction{
		ID: "tx-1", UserID: "user-1", MerchantID: "netflix", MerchantName: "Netflix",
		MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD", AuthorizedAt: authorizedAt,
	}
	if err := repository.Save(ctx, transaction); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := repository.Save(ctx, transaction); err == nil {
		t.Fatal("expected duplicate save to fail with ErrAlreadyExists")
	} else if !isAlreadyExists(err) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	loaded, err := repository.GetByID(ctx, "tx-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if loaded.AmountMinor != 1500 || loaded.MerchantID != "netflix" {
		t.Errorf("unexpected loaded transaction: %+v", loaded)
	}

	listed, err := repository.ListByUserSince(ctx, "user-1", authorizedAt.Add(-time.Hour))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(listed))
	}

	if _, err := repository.GetByID(ctx, "tx-missing"); !isNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSubscriptionRepositoryRoundTrip(t *testing.T) {
	db := testDB(t)
	repository := NewSubscriptionRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	subscription, err := domain.NewSubscription("sub-1", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	subscription.VirtualTokenID = "vtok-1"
	if err := repository.Save(ctx, subscription); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := subscription.Terminate(); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if err := repository.Save(ctx, subscription); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	loaded, err := repository.GetByID(ctx, "sub-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if loaded.State != domain.SubscriptionTerminated || loaded.VirtualTokenID != "vtok-1" {
		t.Errorf("expected terminated upserted subscription, got state=%s token=%s", loaded.State, loaded.VirtualTokenID)
	}

	byMerchant, err := repository.FindByUserAndMerchant(ctx, "user-1", "netflix")
	if err != nil {
		t.Fatalf("find by user and merchant: %v", err)
	}
	if byMerchant.ID != "sub-1" {
		t.Errorf("expected sub-1, got %s", byMerchant.ID)
	}

	pending, err := repository.ListPendingZombieTransition(ctx, now, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("a terminated subscription is never pending, got %+v", pending)
	}

	if _, err := repository.GetByID(ctx, "sub-missing"); !isNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestVirtualTokenRepositoryRoundTrip(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	token, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.Save(ctx, token); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := token.AuthorizeCharge(4_000, "USD", now); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if err := token.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.Save(ctx, token); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	loaded, err := repository.GetByID(ctx, "vtok-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	snapshot := loaded.Snapshot()
	if snapshot.SpentInPeriod != 4_000 || snapshot.State != domain.VirtualTokenFrozen {
		t.Errorf("expected spent 4000 and Frozen, got spent=%d state=%s", snapshot.SpentInPeriod, snapshot.State)
	}

	byMerchant, err := repository.FindByUserAndMerchant(ctx, "user-1", "netflix")
	if err != nil {
		t.Fatalf("find by user and merchant: %v", err)
	}
	if byMerchant.ID != "vtok-1" {
		t.Errorf("expected vtok-1, got %s", byMerchant.ID)
	}
}

func TestSubscriptionUpdateVersionRejectsStaleWrite(t *testing.T) {
	db := testDB(t)
	repository := NewSubscriptionRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	subscription, err := domain.NewSubscription("sub-version", "user-1", "netflix", "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.Save(ctx, subscription); err != nil {
		t.Fatalf("save: %v", err)
	}

	stale, err := repository.GetByID(ctx, "sub-version")
	if err != nil {
		t.Fatalf("get stale: %v", err)
	}
	latest, err := repository.GetByID(ctx, "sub-version")
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if err := latest.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.UpdateVersion(ctx, latest); err != nil {
		t.Fatalf("update: %v", err)
	}
	if latest.Version != 1 {
		t.Fatalf("expected version 1 after the first update, got %d", latest.Version)
	}

	if err := stale.MarkMissed(now); err != nil {
		t.Fatalf("mark missed: %v", err)
	}
	if err := repository.UpdateVersion(ctx, stale); !errors.Is(err, ports.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}

	reloaded, err := repository.GetByID(ctx, "sub-version")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.State != domain.SubscriptionFrozen || reloaded.Version != 1 {
		t.Fatalf("stale write must not have landed: state=%s version=%d", reloaded.State, reloaded.Version)
	}

	missing, err := domain.NewSubscription("sub-missing", "user-9", "netflix", "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.UpdateVersion(ctx, missing); !isNotFound(err) {
		t.Fatalf("expected ErrNotFound for an unknown subscription, got %v", err)
	}
}

func TestTransactionRepositoryFiltersCandidateMerchants(t *testing.T) {
	db := testDB(t)
	repository := NewTransactionRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	since := now.AddDate(-1, 0, 0)

	save := func(id string, merchantID string, mcc domain.MCC, at time.Time) {
		t.Helper()
		err := repository.Save(ctx, domain.Transaction{
			ID: domain.TransactionID(id), UserID: "user-1",
			MerchantID: domain.MerchantID(merchantID), MerchantName: merchantID,
			MCC: mcc, AmountMinor: 1500, Currency: "USD", AuthorizedAt: at,
		})
		if err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}

	for i, offset := range []int{0, 30, 60, 90} {
		save("tx-netflix-"+string(rune('a'+i)), "netflix", domain.MCCRecurringBilling, now.AddDate(0, 0, -offset))
	}
	save("tx-coffee-1", "coffee", 5814, now.AddDate(0, 0, -10))
	save("tx-old", "legacy", domain.MCCRecurringBilling, now.AddDate(-2, 0, 0))

	candidates, err := repository.ListRecurringMerchantCandidates(ctx, "user-1", since, 2)
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].MerchantID != "netflix" {
		t.Fatalf("expected only netflix, got %+v", candidates)
	}
	if candidates[0].Occurrences != 4 {
		t.Errorf("occurrences = %d, want 4", candidates[0].Occurrences)
	}
	if !candidates[0].FirstSeen.Equal(now.AddDate(0, 0, -90).UTC()) {
		t.Errorf("first seen = %s", candidates[0].FirstSeen)
	}
	if !candidates[0].LastSeen.Equal(now.UTC()) {
		t.Errorf("last seen = %s", candidates[0].LastSeen)
	}

	transactions, err := repository.ListByUserAndMerchants(ctx, "user-1", []domain.MerchantID{"netflix"}, since)
	if err != nil {
		t.Fatalf("list by merchants: %v", err)
	}
	if len(transactions) != 4 {
		t.Fatalf("expected 4 netflix transactions, got %d", len(transactions))
	}
	for i := 1; i < len(transactions); i++ {
		if transactions[i].AuthorizedAt.Before(transactions[i-1].AuthorizedAt) {
			t.Fatal("expected transactions ordered by authorized_at")
		}
	}

	if empty, err := repository.ListByUserAndMerchants(ctx, "user-1", nil, since); err != nil || len(empty) != 0 {
		t.Fatalf("no merchants must load nothing, got %+v (%v)", empty, err)
	}
	other, err := repository.ListByUserAndMerchants(ctx, "user-2", []domain.MerchantID{"netflix"}, since)
	if err != nil || len(other) != 0 {
		t.Fatalf("another user must see nothing, got %+v (%v)", other, err)
	}
}

func TestSubscriptionListPendingZombieTransitionFiltersAndOrders(t *testing.T) {
	db := testDB(t)
	repository := NewSubscriptionRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

	seed := func(id domain.SubscriptionID, state domain.SubscriptionState, window domain.BillingWindow, lastChargedAt time.Time) {
		t.Helper()
		created, err := domain.NewSubscription(id, domain.UserID("user-"+string(id)),
			domain.MerchantID("merchant-"+string(id)), "Merchant", state,
			window, 1500, "USD", lastChargedAt, 3)
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if err := repository.Save(ctx, created); err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}

	// Overdue by more than its window: pending.
	seed("sub-monthly-old", domain.SubscriptionActive, domain.WindowMonthly, now.Add(-120*24*time.Hour))
	seed("sub-trial", domain.SubscriptionTrial, domain.WindowMonthly, now.Add(-90*24*time.Hour))
	seed("sub-weekly", domain.SubscriptionActive, domain.WindowWeekly, now.Add(-30*24*time.Hour))

	// Due but not yet past the window: not pending.
	seed("sub-monthly-fresh", domain.SubscriptionActive, domain.WindowMonthly, now.Add(-5*24*time.Hour))
	// Zombie rows are excluded so a sweep pass terminates.
	seed("sub-zombie", domain.SubscriptionZombie, domain.WindowMonthly, now.Add(-200*24*time.Hour))
	// Frozen rows never become zombies.
	frozen, err := domain.NewSubscription("sub-frozen", "user-frozen", "merchant-frozen", "Merchant",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now.Add(-120*24*time.Hour), 3)
	if err != nil {
		t.Fatalf("create frozen: %v", err)
	}
	if err := frozen.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := repository.Save(ctx, frozen); err != nil {
		t.Fatalf("save frozen: %v", err)
	}

	pending, err := repository.ListPendingZombieTransition(ctx, now, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	got := make([]string, 0, len(pending))
	for _, subscription := range pending {
		got = append(got, string(subscription.ID))
	}
	want := []string{"sub-monthly-old", "sub-trial", "sub-weekly"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("pending order = %v, want %v", got, want)
	}

	limited, err := repository.ListPendingZombieTransition(ctx, now, 2)
	if err != nil {
		t.Fatalf("list pending limited: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("expected the limit to cap the batch, got %d", len(limited))
	}
}

func TestVirtualTokenChargeIsAtomic(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	token, err := domain.NewVirtualToken("vtok-charge", "user-1", "netflix", "411111******1234", 10_000, "USD", now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.Save(ctx, token); err != nil {
		t.Fatalf("save: %v", err)
	}

	if _, err := repository.Charge(ctx, token.ID, 4_000, "USD", now); err != nil {
		t.Fatalf("charge 4000: %v", err)
	}
	if _, err := repository.Charge(ctx, token.ID, 6_000, "USD", now); err != nil {
		t.Fatalf("charge 6000: %v", err)
	}
	if _, err := repository.Charge(ctx, token.ID, 1, "USD", now); !errors.Is(err, domain.ErrSpendLimitExceeded) {
		t.Fatalf("expected ErrSpendLimitExceeded, got %v", err)
	}
	if _, err := repository.Charge(ctx, token.ID, -1, "USD", now); !errors.Is(err, domain.ErrInvalidAmount) {
		t.Fatalf("expected ErrInvalidAmount, got %v", err)
	}
	if _, err := repository.Charge(ctx, token.ID, 100, "EUR", now); !errors.Is(err, domain.ErrUnknownCurrency) {
		t.Fatalf("expected ErrUnknownCurrency, got %v", err)
	}
	if _, err := repository.Charge(ctx, "vtok-missing", 100, "USD", now); !isNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	persisted, err := repository.GetByID(ctx, token.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if persisted.SpentInPeriod != 10_000 {
		t.Fatalf("spent = %d, want 10000", persisted.SpentInPeriod)
	}
}

func TestVirtualTokenChargeConcurrentNeverExceedsLimit(t *testing.T) {
	const (
		monthlyLimit = 10_000
		chargeAmount = 500
		concurrency  = 20
	)

	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	token, err := domain.NewVirtualToken("vtok-race", "user-1", "netflix", "411111******1234", monthlyLimit, "USD", now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repository.Save(ctx, token); err != nil {
		t.Fatalf("save: %v", err)
	}

	var approved atomic.Int32
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := repository.Charge(ctx, token.ID, chargeAmount, "USD", now); err == nil {
				approved.Add(1)
			}
		}()
	}
	wg.Wait()

	if got, want := approved.Load(), int32(monthlyLimit/chargeAmount); got != want {
		t.Fatalf("approved charges = %d, want %d", got, want)
	}
	persisted, err := repository.GetByID(ctx, token.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if persisted.SpentInPeriod != monthlyLimit {
		t.Fatalf("spent = %d, want %d", persisted.SpentInPeriod, monthlyLimit)
	}
}

func TestSaveRejectsRedirectedRow(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	tokenRepository := NewVirtualTokenRepository(db)
	original, err := domain.NewVirtualToken("vtok-original", "user-1", "netflix", "411111******1111", 10_000, "USD", now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := tokenRepository.Save(ctx, original); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Same user and merchant, different id: the unique key would otherwise
	// absorb the write while the caller keeps the id it never persisted.
	impostor, err := domain.NewVirtualToken("vtok-impostor", "user-1", "netflix", "411111******9999", 10_000, "USD", now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := tokenRepository.Save(ctx, impostor); !isAlreadyExists(err) {
		t.Fatalf("expected ErrAlreadyExists for a redirected write, got %v", err)
	}

	persisted, err := tokenRepository.GetByID(ctx, "vtok-original")
	if err != nil {
		t.Fatalf("load original: %v", err)
	}
	if persisted.MaskedPAN != "411111******1111" {
		t.Fatalf("original token was overwritten: %q", persisted.MaskedPAN)
	}

	subscriptionRepository := NewSubscriptionRepository(db)
	ctx2 := context.Background()
	originalSub, err := domain.NewSubscription("sub-redirect-a", "user-redirect", "netflix", "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now().UTC(), 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := subscriptionRepository.Save(ctx2, originalSub); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}
	clash, err := domain.NewSubscription("sub-redirect-b", "user-redirect", "netflix", "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now().UTC(), 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := subscriptionRepository.Save(ctx2, clash); !isAlreadyExists(err) {
		t.Fatalf("expected ErrAlreadyExists for a redirected subscription write, got %v", err)
	}
	preserved, err := subscriptionRepository.GetByID(ctx2, "sub-redirect-a")
	if err != nil {
		t.Fatalf("load original subscription: %v", err)
	}
	if preserved.ID != "sub-redirect-a" {
		t.Fatalf("original subscription was replaced by %s", preserved.ID)
	}
}

func TestVirtualTokenChargeAcrossReplicasNeverExceedsLimit(t *testing.T) {
	const (
		monthlyLimit = 10_000
		chargeAmount = 500
		replicas     = 4
		perReplica   = 10
	)

	dsn := os.Getenv("SUBSCRIPTION_FIREWALL_TEST_DSN")
	if dsn == "" {
		t.Skip("SUBSCRIPTION_FIREWALL_TEST_DSN is not set")
	}
	ctx := context.Background()

	// Each replica is its own pool, as in a real multi-replica deployment.
	repositories := make([]*VirtualTokenRepository, replicas)
	for slot := range replicas {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open replica %d: %v", slot, err)
		}
		t.Cleanup(func() { db.Close() })
		repositories[slot] = NewVirtualTokenRepository(db)
	}

	if err := repositories[0].Save(ctx, newTestToken(t, "vtok-replicas", monthlyLimit)); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	var approved atomic.Int64
	var wg sync.WaitGroup
	for _, repository := range repositories {
		for range perReplica {
			wg.Add(1)
			go func(repo *VirtualTokenRepository) {
				defer wg.Done()
				if _, err := repo.Charge(ctx, "vtok-replicas", chargeAmount, "USD", time.Now().UTC()); err == nil {
					approved.Add(1)
				}
			}(repository)
		}
	}
	wg.Wait()

	want := int64(monthlyLimit / chargeAmount)
	if got := approved.Load(); got != want {
		t.Fatalf("approved charges = %d, want %d", got, want)
	}
	persisted, err := repositories[0].GetByID(ctx, "vtok-replicas")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if persisted.SpentInPeriod != monthlyLimit {
		t.Fatalf("spent = %d, want %d", persisted.SpentInPeriod, monthlyLimit)
	}
}

func newTestToken(t *testing.T, id domain.VirtualTokenID, monthlyLimit int64) *domain.VirtualToken {
	t.Helper()

	token, err := domain.NewVirtualToken(id, domain.UserID("user-"+string(id)), domain.MerchantID("merchant-"+string(id)), "411111******1234", monthlyLimit, "USD", time.Now().UTC())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	return token
}

func TestTokenIssuanceReservationIsExclusive(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	reservation, err := repository.ReserveTokenIssuance(ctx, "user-1", "netflix", "issue:user-1:netflix", now, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !reservation.Reserved {
		t.Fatal("expected the first caller to win the reservation")
	}

	other := domain.UserID("user-1")
	loser, err := repository.ReserveTokenIssuance(ctx, other, "netflix", "issue:user-1:netflix", now, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if loser.Reserved {
		t.Fatal("expected the second caller to lose the reservation")
	}
	if loser.Existing.State != domain.VirtualTokenIssuing {
		t.Fatalf("expected the held reservation to be Issuing, got %s", loser.Existing.State)
	}

	token, err := repository.CompleteTokenIssuance(ctx, reservation.TokenID, "issue:user-1:netflix",
		ports.IssuedCard{MaskedPAN: "411111******1234", MonthlyLimit: 10_000, Currency: "USD"}, now)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if token.State != domain.VirtualTokenActive || token.MonthlyLimit != 10_000 {
		t.Fatalf("unexpected completed token: %+v", token.Snapshot())
	}

	if _, err := repository.CompleteTokenIssuance(ctx, reservation.TokenID, "issue:user-1:netflix",
		ports.IssuedCard{MaskedPAN: "411111******5678", MonthlyLimit: 10_000, Currency: "USD"}, now); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("expected a replayed completion to be rejected, got %v", err)
	}
}

func TestTokenIssuanceReservationIsReclaimedAfterTimeout(t *testing.T) {
	db := testDB(t)
	repository := NewVirtualTokenRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	stale := domain.NewIssuingVirtualToken("vtok-stale", "user-1", "netflix", now.Add(-time.Hour))
	if err := repository.Save(ctx, stale); err != nil {
		t.Fatalf("seed stale reservation: %v", err)
	}

	reservation, err := repository.ReserveTokenIssuance(ctx, "user-1", "netflix", "issue:user-1:netflix", now, now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !reservation.Reserved {
		t.Fatal("expected a stale reservation to be reclaimable")
	}
	if reservation.TokenID == stale.ID {
		t.Fatal("expected the stale reservation to be replaced by a new one")
	}

	if err := repository.ReleaseTokenIssuance(ctx, reservation.TokenID); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := repository.GetByID(ctx, reservation.TokenID); !isNotFound(err) {
		t.Fatalf("expected the released reservation to be gone, got %v", err)
	}
}

func TestOutboxEnqueueWithTransactionIsAtomic(t *testing.T) {
	db := testDB(t)
	outbox := NewOutbox(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	transaction := domain.Transaction{
		ID: "tx-ob-1", UserID: "user-1", MerchantID: "netflix", MerchantName: "Netflix",
		MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD", AuthorizedAt: now,
	}
	if err := outbox.EnqueueWithTransaction(ctx, transaction); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	duplicate := transaction
	duplicate.MerchantID = "spotify"
	if err := outbox.EnqueueWithTransaction(ctx, duplicate); !isAlreadyExists(err) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	if pending, err := outbox.PendingCount(ctx); err != nil || pending != 1 {
		t.Fatalf("expected exactly 1 pending job, got %d (%v)", pending, err)
	}
}

func TestOutboxClaimCompleteAndRetry(t *testing.T) {
	db := testDB(t)
	outbox := NewOutbox(db)
	ctx := context.Background()
	now := time.Now().UTC()

	for _, id := range []string{"tx-a", "tx-b"} {
		if err := outbox.EnqueueWithTransaction(ctx, domain.Transaction{
			ID: domain.TransactionID(id), UserID: "user-1", MerchantID: "netflix", MerchantName: "Netflix",
			MCC: domain.MCCRecurringBilling, AmountMinor: 1500, Currency: "USD", AuthorizedAt: now,
		}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	items, err := outbox.ClaimBatch(ctx, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 claimed jobs, got %d", len(items))
	}
	for _, item := range items {
		if item.Attempts != 1 {
			t.Errorf("expected attempts=1 after first claim, got %d", item.Attempts)
		}
	}

	again, err := outbox.ClaimBatch(ctx, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("expected no items for second claim, got %+v (%v)", again, err)
	}

	if err := outbox.Complete(ctx, []int64{items[0].ID}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := outbox.Fail(ctx, items[1]); err != nil {
		t.Fatalf("fail: %v", err)
	}

	retried, err := outbox.ClaimBatch(ctx, 10)
	if err != nil || len(retried) != 1 {
		t.Fatalf("expected 1 retryable job, got %+v (%v)", retried, err)
	}
	if retried[0].ID != items[1].ID || retried[0].Attempts != 2 {
		t.Errorf("expected job %d with attempts=2, got %+v", items[1].ID, retried[0])
	}

	if err := outbox.Fail(ctx, retried[0]); err != nil {
		t.Fatalf("fail retried: %v", err)
	}
	requeued, err := outbox.RequeueStale(ctx, time.Hour)
	if err != nil || requeued != 0 {
		t.Fatalf("expected nothing requeued (item is pending), got %d (%v)", requeued, err)
	}
}

func TestLifecycleSyncQueueRoundTrip(t *testing.T) {
	db := testDB(t)
	queue := NewLifecycleSyncQueue(db)
	ctx := context.Background()

	if err := queue.Enqueue(ctx, ports.LifecycleSyncTask{EntityType: "token", EntityID: "vtok-1", Action: "terminate"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if pending, err := queue.PendingCount(ctx); err != nil || pending != 1 {
		t.Fatalf("expected 1 pending task, got %d (%v)", pending, err)
	}

	claimed, err := queue.ClaimBatch(ctx, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected one claimed task, got %+v (%v)", claimed, err)
	}
	if claimed[0].Attempts != 1 || claimed[0].EntityID != "vtok-1" {
		t.Fatalf("unexpected claimed task: %+v", claimed[0])
	}
	if again, err := queue.ClaimBatch(ctx, 10); err != nil || len(again) != 0 {
		t.Fatalf("expected no second claim, got %+v (%v)", again, err)
	}

	if err := queue.Fail(ctx, claimed[0]); err != nil {
		t.Fatalf("fail: %v", err)
	}
	retried, err := queue.ClaimBatch(ctx, 10)
	if err != nil || len(retried) != 1 || retried[0].Attempts != 2 {
		t.Fatalf("expected a retryable task with attempts=2, got %+v (%v)", retried, err)
	}

	requeued, err := queue.RequeueStale(ctx, -time.Minute)
	if err != nil || requeued != 1 {
		t.Fatalf("expected the in-flight task to be reclaimed, got %d (%v)", requeued, err)
	}
	if err := queue.Complete(ctx, []int64{}); err != nil {
		t.Fatalf("complete with no ids must be a no-op: %v", err)
	}
	if err := queue.Complete(ctx, []int64{retried[0].ID}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if pending, err := queue.PendingCount(ctx); err != nil || pending != 0 {
		t.Fatalf("expected the queue to drain, got %d (%v)", pending, err)
	}
}

func TestSubscriptionCreateIfAbsentConverges(t *testing.T) {
	db := testDB(t)
	repository := NewSubscriptionRepository(db)
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	first, err := domain.NewSubscription("sub-a", "user-1", "netflix", "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	second, err := domain.NewSubscription("sub-b", "user-1", "netflix", "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 4)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	storedFirst, err := repository.CreateIfAbsent(ctx, first)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	storedSecond, err := repository.CreateIfAbsent(ctx, second)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if storedFirst.ID != storedSecond.ID {
		t.Fatalf("expected both callers to see one subscription, got %s and %s", storedFirst.ID, storedSecond.ID)
	}

	listed, err := repository.ListByUser(ctx, "user-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected exactly one subscription row, got %d", len(listed))
	}
}

func TestAuditLogRecords(t *testing.T) {
	db := testDB(t)
	audit := NewAuditLog(db)
	ctx := context.Background()

	entry := ports.AuditEntry{Actor: "adm…key", Action: "freeze", EntityType: "subscription", EntityID: "sub-1"}
	if err := audit.Record(ctx, entry); err != nil {
		t.Fatalf("record: %v", err)
	}

	var actor, action, entityType, entityID string
	if err := db.QueryRowContext(ctx,
		`SELECT actor, action, entity_type, entity_id FROM audit_log WHERE entity_id = 'sub-1'`,
	).Scan(&actor, &action, &entityType, &entityID); err != nil {
		t.Fatalf("query audit row: %v", err)
	}
	if actor != entry.Actor || action != entry.Action || entityType != entry.EntityType {
		t.Errorf("unexpected audit row: %+v", ports.AuditEntry{Actor: actor, Action: action, EntityType: entityType, EntityID: entityID})
	}
}
