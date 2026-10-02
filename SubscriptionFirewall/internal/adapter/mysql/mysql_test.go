package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
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
	for _, table := range []string{"transactions", "subscriptions", "virtual_tokens", "detection_outbox", "audit_log"} {
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

	all, err := repository.ListAll(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(all))
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
