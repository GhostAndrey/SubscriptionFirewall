package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type SubscriptionRepository struct {
	db *sql.DB
}

func NewSubscriptionRepository(db *sql.DB) *SubscriptionRepository {
	return &SubscriptionRepository{db: db}
}

// Save inserts a new subscription or replaces an existing row addressed by
// id. It is intended for seeding and tests; concurrent use-case updates go
// through UpdateVersion.
func (r *SubscriptionRepository) Save(ctx context.Context, subscription *domain.Subscription) error {
	if _, err := r.db.ExecContext(ctx, subscriptionUpsert, subscriptionArgs(subscription)...); err != nil {
		return fmt.Errorf("save subscription %s: %w", subscription.ID, err)
	}
	return verifyWrite(ctx, "subscription", string(subscription.ID), func(ctx context.Context) error {
		_, err := r.GetByID(ctx, subscription.ID)
		return err
	})
}

// CreateIfAbsent relies on the unique key over (user_id, merchant_id) so that
// two replicas detecting the same pair at the same time produce one row, and
// returns whichever row survived.
func (r *SubscriptionRepository) CreateIfAbsent(ctx context.Context, subscription *domain.Subscription) (*domain.Subscription, error) {
	if _, err := r.db.ExecContext(ctx, subscriptionInsert, subscriptionArgs(subscription)...); err != nil {
		return nil, fmt.Errorf("create subscription for user %s merchant %s: %w",
			subscription.UserID, subscription.MerchantID, err)
	}
	return r.FindByUserAndMerchant(ctx, subscription.UserID, subscription.MerchantID)
}

// UpdateVersion writes the subscription only when the row still carries the
// version the caller read, so two replicas cannot silently overwrite each
// other's lifecycle transitions.
func (r *SubscriptionRepository) UpdateVersion(ctx context.Context, subscription *domain.Subscription) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE subscriptions SET
			merchant_name = ?, virtual_token_id = ?, state = ?, billing_window = ?,
			average_amount = ?, currency = ?, last_charged_at = ?, next_expected_at = ?,
			observed_payments = ?, version = version + 1
		 WHERE id = ? AND version = ?`,
		subscription.MerchantName, subscription.VirtualTokenID, subscription.State,
		subscription.BillingWindow, subscription.AverageAmount, subscription.Currency,
		subscription.LastChargedAt.UTC(), subscription.NextExpectedAt.UTC(),
		subscription.ObservedPayments, subscription.ID, subscription.Version,
	)
	if err != nil {
		return fmt.Errorf("update subscription %s: %w", subscription.ID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read update result for subscription %s: %w", subscription.ID, err)
	}
	if affected == 0 {
		return r.classifyUpdateMiss(ctx, subscription)
	}
	subscription.Version++
	return nil
}

func (r *SubscriptionRepository) GetByID(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	row := r.db.QueryRowContext(ctx, subscriptionSelect+` WHERE id = ?`, id)
	subscription, err := scanSubscription(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("subscription %s: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get subscription %s: %w", id, err)
	}
	return subscription, nil
}

func (r *SubscriptionRepository) FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.Subscription, error) {
	row := r.db.QueryRowContext(ctx, subscriptionSelect+` WHERE user_id = ? AND merchant_id = ?`, userID, merchantID)
	subscription, err := scanSubscription(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("subscription for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("find subscription for user %s merchant %s: %w", userID, merchantID, err)
	}
	return subscription, nil
}

func (r *SubscriptionRepository) ListByUserPage(ctx context.Context, userID domain.UserID, page ports.Page) ([]*domain.Subscription, error) {
	query := subscriptionSelect + ` WHERE user_id = ? ORDER BY id`
	args := []any{userID}
	if !page.IsUnbounded() {
		query += ` LIMIT ? OFFSET ?`
		args = append(args, page.Limit, page.Offset)
	}
	return r.list(ctx, query, args...)
}

// ListPendingZombieTransition selects exactly the rows whose overdue check has
// not been evaluated yet. The per-window condition mirrors Subscription.MarkMissed
// and excluding already-zombie rows guarantees a sweep pass terminates instead
// of re-reading the same overdue rows forever. The comparison is strict so the
// query never returns a row the domain rule would refuse to transition.
func (r *SubscriptionRepository) ListPendingZombieTransition(ctx context.Context, now time.Time, limit int) ([]*domain.Subscription, error) {
	const pendingZombie = `state IN (?, ?) AND (
			(billing_window = '7d'   AND next_expected_at < DATE_SUB(?, INTERVAL 7 DAY)) OR
			(billing_window = '30d'  AND next_expected_at < DATE_SUB(?, INTERVAL 30 DAY)) OR
			(billing_window = '365d' AND next_expected_at < DATE_SUB(?, INTERVAL 365 DAY))
		) ORDER BY next_expected_at LIMIT ?`

	return r.list(ctx, subscriptionSelect+` WHERE `+pendingZombie,
		domain.SubscriptionActive, domain.SubscriptionTrial,
		now.UTC(), now.UTC(), now.UTC(), limit)
}

// classifyUpdateMiss separates a missing row from a genuine version conflict.
// Both produce zero affected rows, but only the second one is retryable, so
// collapsing them would make a deleted subscription surface as a conflict.
func (r *SubscriptionRepository) classifyUpdateMiss(ctx context.Context, subscription *domain.Subscription) error {
	var existing uint32
	err := r.db.QueryRowContext(ctx,
		`SELECT version FROM subscriptions WHERE id = ?`, subscription.ID).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("subscription %s: %w", subscription.ID, domain.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("check subscription %s after missed update: %w", subscription.ID, err)
	}
	return fmt.Errorf("subscription %s at version %d, stored version %d: %w",
		subscription.ID, subscription.Version, existing, ports.ErrVersionConflict)
}

func (r *SubscriptionRepository) list(ctx context.Context, query string, args ...any) ([]*domain.Subscription, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()

	subscriptions := make([]*domain.Subscription, 0)
	for rows.Next() {
		subscription, err := scanSubscription(rows)
		if err != nil {
			return nil, fmt.Errorf("scan subscription: %w", err)
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate subscriptions: %w", err)
	}
	return subscriptions, nil
}

const subscriptionColumns = `(id, user_id, merchant_id, merchant_name, virtual_token_id, state,
	billing_window, average_amount, currency, last_charged_at, next_expected_at, observed_payments, version)`

const subscriptionSelect = `SELECT ` + subscriptionColumns + ` FROM subscriptions`

const subscriptionInsert = `INSERT INTO subscriptions ` + subscriptionColumns + `
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE id = id`

const subscriptionUpsert = `INSERT INTO subscriptions ` + subscriptionColumns + `
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		merchant_name = VALUES(merchant_name), virtual_token_id = VALUES(virtual_token_id),
		state = VALUES(state), billing_window = VALUES(billing_window),
		average_amount = VALUES(average_amount), currency = VALUES(currency),
		last_charged_at = VALUES(last_charged_at), next_expected_at = VALUES(next_expected_at),
		observed_payments = VALUES(observed_payments), version = VALUES(version)`

func subscriptionArgs(subscription *domain.Subscription) []any {
	return []any{
		subscription.ID, subscription.UserID, subscription.MerchantID, subscription.MerchantName,
		subscription.VirtualTokenID, subscription.State, subscription.BillingWindow,
		subscription.AverageAmount, subscription.Currency, subscription.LastChargedAt.UTC(),
		subscription.NextExpectedAt.UTC(), subscription.ObservedPayments, subscription.Version,
	}
}

func scanSubscription(row rowScanner) (*domain.Subscription, error) {
	subscription := &domain.Subscription{}
	if err := row.Scan(
		&subscription.ID, &subscription.UserID, &subscription.MerchantID, &subscription.MerchantName,
		&subscription.VirtualTokenID, &subscription.State, &subscription.BillingWindow,
		&subscription.AverageAmount, &subscription.Currency, &subscription.LastChargedAt,
		&subscription.NextExpectedAt, &subscription.ObservedPayments, &subscription.Version,
	); err != nil {
		return nil, err
	}
	return subscription, nil
}

var _ ports.SubscriptionRepository = (*SubscriptionRepository)(nil)
