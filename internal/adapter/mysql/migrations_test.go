package mysql

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"subscriptionfirewall/internal/domain"
)

// migrationTestDB opens the integration database without truncating it, so
// tests can operate on the schema itself.
func migrationTestDB(t *testing.T) *sql.DB {
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
	return db
}

// schemaObjects describes the physical state the adapter relies on. Rolling a
// migration back must remove exactly the objects it introduced.
type schemaObjects struct {
	tables  map[string]bool
	columns map[string]map[string]bool
	indexes map[string]map[string]bool
}

func readSchema(t *testing.T, db *sql.DB) schemaObjects {
	t.Helper()

	objects := schemaObjects{
		tables:  map[string]bool{},
		columns: map[string]map[string]bool{},
		indexes: map[string]map[string]bool{},
	}

	ctx := context.Background()
	for name := range queryNames(ctx, t, db,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()`) {
		objects.tables[name] = true
		objects.columns[name] = map[string]bool{}
	}
	for pair := range queryPairs(ctx, t, db,
		`SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = DATABASE()`) {
		if objects.columns[pair.First] == nil {
			objects.columns[pair.First] = map[string]bool{}
		}
		objects.columns[pair.First][pair.Second] = true
	}
	for pair := range queryPairs(ctx, t, db,
		`SELECT table_name, index_name FROM information_schema.statistics
		 WHERE table_schema = DATABASE() AND index_name <> 'PRIMARY'`) {
		if objects.indexes[pair.First] == nil {
			objects.indexes[pair.First] = map[string]bool{}
		}
		objects.indexes[pair.First][pair.Second] = true
	}
	return objects
}

func queryNames(ctx context.Context, t *testing.T, db *sql.DB, query string) map[string]struct{} {
	t.Helper()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	result := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		result[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return result
}

type namePair struct {
	First  string
	Second string
}

func queryPairs(ctx context.Context, t *testing.T, db *sql.DB, query string) map[namePair]string {
	t.Helper()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	result := map[namePair]string{}
	for rows.Next() {
		var pair namePair
		if err := rows.Scan(&pair.First, &pair.Second); err != nil {
			t.Fatalf("scan: %v", err)
		}
		result[pair] = pair.Second
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return result
}

func assertIndex(t *testing.T, objects schemaObjects, table, index string) {
	t.Helper()

	if !objects.tables[table] {
		t.Fatalf("expected table %s to exist", table)
	}
	if !objects.indexes[table][index] {
		t.Fatalf("expected index %s on %s to exist", index, table)
	}
}

func assertNoIndex(t *testing.T, objects schemaObjects, table, index string) {
	t.Helper()

	if objects.indexes[table][index] {
		t.Fatalf("expected index %s on %s to be dropped", index, table)
	}
}

func assertSchema(t *testing.T, objects schemaObjects, table string, columns ...string) {
	t.Helper()

	if !objects.tables[table] {
		t.Fatalf("expected table %s to exist", table)
	}
	for _, column := range columns {
		if !objects.columns[table][column] {
			t.Fatalf("expected column %s.%s to exist", table, column)
		}
	}
}

func assertNoTable(t *testing.T, objects schemaObjects, table string) {
	t.Helper()

	if objects.tables[table] {
		t.Fatalf("expected table %s to be dropped", table)
	}
}

func assertNoColumn(t *testing.T, objects schemaObjects, table, column string) {
	t.Helper()

	if objects.columns[table][column] {
		t.Fatalf("expected column %s.%s to be dropped", table, column)
	}
}

func TestMigrationsApplyIdempotently(t *testing.T) {
	db := migrationTestDB(t)
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	before, err := MigrationVersion(ctx, db)
	if err != nil {
		t.Fatalf("version: %v", err)
	}

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("repeated migrate must be a no-op, got %v", err)
	}
	after, err := MigrationVersion(ctx, db)
	if err != nil {
		t.Fatalf("version after repeat: %v", err)
	}
	if before != after {
		t.Fatalf("repeated migrate changed the version from %d to %d", before, after)
	}
}

func TestMigrationsRollbackAndReapply(t *testing.T) {
	db := migrationTestDB(t)
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		if err := Migrate(context.Background(), db); err != nil {
			t.Errorf("restore schema after rollback test: %v", err)
		}
	})

	applied := readSchema(t, db)
	assertSchema(t, applied, "virtual_tokens", "issuance_key")
	assertSchema(t, applied, "subscriptions", "version")
	assertSchema(t, applied, "lifecycle_sync_outbox", "entity_type", "action")
	assertIndex(t, applied, "subscriptions", "idx_subscriptions_sweep")

	// 00007: subscriptions sweep index
	if err := Rollback(ctx, db, 6); err != nil {
		t.Fatalf("rollback to 00006: %v", err)
	}
	rolledBack := readSchema(t, db)
	assertNoIndex(t, rolledBack, "subscriptions", "idx_subscriptions_sweep")
	assertSchema(t, rolledBack, "lifecycle_sync_outbox", "entity_type", "action")

	// 00006: lifecycle_sync_outbox
	if err := Rollback(ctx, db, 5); err != nil {
		t.Fatalf("rollback to 00005: %v", err)
	}
	rolledBack = readSchema(t, db)
	assertNoTable(t, rolledBack, "lifecycle_sync_outbox")
	assertSchema(t, rolledBack, "subscriptions", "version")
	assertSchema(t, rolledBack, "virtual_tokens", "issuance_key")

	// 00005: subscriptions.version
	if err := Rollback(ctx, db, 4); err != nil {
		t.Fatalf("rollback to 00004: %v", err)
	}
	rolledBack = readSchema(t, db)
	assertNoColumn(t, rolledBack, "subscriptions", "version")
	assertSchema(t, rolledBack, "virtual_tokens", "issuance_key")

	// 00004: virtual_tokens.issuance_key
	if err := Rollback(ctx, db, 3); err != nil {
		t.Fatalf("rollback to 00003: %v", err)
	}
	rolledBack = readSchema(t, db)
	assertNoColumn(t, rolledBack, "virtual_tokens", "issuance_key")

	version, err := MigrationVersion(ctx, db)
	if err != nil {
		t.Fatalf("version after rollbacks: %v", err)
	}
	if version != 3 {
		t.Fatalf("expected version 3 after four rollbacks, got %d", version)
	}

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("reapply: %v", err)
	}
	reapplied := readSchema(t, db)
	assertSchema(t, reapplied, "virtual_tokens", "issuance_key")
	assertSchema(t, reapplied, "subscriptions", "version")
	assertSchema(t, reapplied, "lifecycle_sync_outbox", "entity_type", "action")
	assertIndex(t, reapplied, "subscriptions", "idx_subscriptions_sweep")

	restored, err := MigrationVersion(ctx, db)
	if err != nil {
		t.Fatalf("version after reapply: %v", err)
	}
	if restored != 7 {
		t.Fatalf("expected version 7 after reapplying, got %d", restored)
	}
}

func newTestSubscription(id domain.SubscriptionID, _ context.Context) *domain.Subscription {
	subscription, err := domain.NewSubscription(id, domain.UserID("user-"+string(id)), domain.MerchantID("merchant-"+string(id)), "Netflix",
		domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now().UTC(), 3)
	if err != nil {
		panic(err)
	}
	return subscription
}

func TestMigrationsRollbackPreservesData(t *testing.T) {
	db := migrationTestDB(t)
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "TRUNCATE TABLE subscriptions"); err != nil {
			t.Errorf("cleanup subscriptions: %v", err)
		}
		if err := Migrate(context.Background(), db); err != nil {
			t.Errorf("restore schema: %v", err)
		}
	})

	repository := NewSubscriptionRepository(db)
	seed := newTestSubscription("sub-migration", ctx)
	if err := repository.Save(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Dropping and re-adding columns must not affect unrelated tables.
	if err := Rollback(ctx, db, 5); err != nil {
		t.Fatalf("rollback lifecycle_sync_outbox: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("reapply: %v", err)
	}

	loaded, err := repository.GetByID(ctx, "sub-migration")
	if err != nil {
		t.Fatalf("subscription survived the rollback: %v", err)
	}
	if loaded.UserID != "user-sub-migration" {
		t.Fatalf("unexpected subscription: %+v", loaded)
	}
}
