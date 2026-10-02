package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type TransactionRepository struct {
	db *sql.DB
}

func NewTransactionRepository(db *sql.DB) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) Save(ctx context.Context, transaction domain.Transaction) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO transactions (id, user_id, merchant_id, merchant_name, mcc, amount_minor, currency, authorized_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		transaction.ID, transaction.UserID, transaction.MerchantID, transaction.MerchantName,
		transaction.MCC, transaction.AmountMinor, transaction.Currency, transaction.AuthorizedAt.UTC(),
	)
	if isDuplicateKey(err) {
		return fmt.Errorf("transaction %s: %w", transaction.ID, domain.ErrAlreadyExists)
	}
	if err != nil {
		return fmt.Errorf("insert transaction %s: %w", transaction.ID, err)
	}
	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, id domain.TransactionID) (*domain.Transaction, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, user_id, merchant_id, merchant_name, mcc, amount_minor, currency, authorized_at
		 FROM transactions WHERE id = ?`, id)
	transaction, err := scanTransaction(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("transaction %s: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get transaction %s: %w", id, err)
	}
	return transaction, nil
}

func (r *TransactionRepository) ListByUserSince(ctx context.Context, userID domain.UserID, since time.Time) ([]domain.Transaction, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, user_id, merchant_id, merchant_name, mcc, amount_minor, currency, authorized_at
		 FROM transactions WHERE user_id = ? AND authorized_at >= ? ORDER BY authorized_at`,
		userID, since.UTC())
	if err != nil {
		return nil, fmt.Errorf("list transactions for user %s: %w", userID, err)
	}
	defer rows.Close()

	transactions := make([]domain.Transaction, 0)
	for rows.Next() {
		transaction, err := scanTransaction(rows)
		if err != nil {
			return nil, fmt.Errorf("scan transaction for user %s: %w", userID, err)
		}
		transactions = append(transactions, *transaction)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transactions for user %s: %w", userID, err)
	}
	return transactions, nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanTransaction(row rowScanner) (*domain.Transaction, error) {
	var transaction domain.Transaction
	if err := row.Scan(
		&transaction.ID, &transaction.UserID, &transaction.MerchantID, &transaction.MerchantName,
		&transaction.MCC, &transaction.AmountMinor, &transaction.Currency, &transaction.AuthorizedAt,
	); err != nil {
		return nil, err
	}
	return &transaction, nil
}

type SubscriptionRepository struct {
	db *sql.DB
}

func NewSubscriptionRepository(db *sql.DB) *SubscriptionRepository {
	return &SubscriptionRepository{db: db}
}

func (r *SubscriptionRepository) Save(ctx context.Context, subscription *domain.Subscription) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO subscriptions
			(id, user_id, merchant_id, merchant_name, virtual_token_id, state, billing_window,
			 average_amount, currency, last_charged_at, next_expected_at, observed_payments)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
			merchant_name = VALUES(merchant_name), virtual_token_id = VALUES(virtual_token_id),
			state = VALUES(state), billing_window = VALUES(billing_window),
			average_amount = VALUES(average_amount), currency = VALUES(currency),
			last_charged_at = VALUES(last_charged_at), next_expected_at = VALUES(next_expected_at),
			observed_payments = VALUES(observed_payments)`,
		subscription.ID, subscription.UserID, subscription.MerchantID, subscription.MerchantName,
		subscription.VirtualTokenID, subscription.State, subscription.BillingWindow,
		subscription.AverageAmount, subscription.Currency, subscription.LastChargedAt.UTC(),
		subscription.NextExpectedAt.UTC(), subscription.ObservedPayments,
	)
	if err != nil {
		return fmt.Errorf("save subscription %s: %w", subscription.ID, err)
	}
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

func (r *SubscriptionRepository) ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.Subscription, error) {
	return r.list(ctx, subscriptionSelect+` WHERE user_id = ? ORDER BY id`, userID)
}

func (r *SubscriptionRepository) ListAll(ctx context.Context) ([]*domain.Subscription, error) {
	return r.list(ctx, subscriptionSelect+` ORDER BY id`)
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

const subscriptionSelect = `SELECT id, user_id, merchant_id, merchant_name, virtual_token_id, state,
	billing_window, average_amount, currency, last_charged_at, next_expected_at, observed_payments
	FROM subscriptions`

func scanSubscription(row rowScanner) (*domain.Subscription, error) {
	subscription := &domain.Subscription{}
	if err := row.Scan(
		&subscription.ID, &subscription.UserID, &subscription.MerchantID, &subscription.MerchantName,
		&subscription.VirtualTokenID, &subscription.State, &subscription.BillingWindow,
		&subscription.AverageAmount, &subscription.Currency, &subscription.LastChargedAt,
		&subscription.NextExpectedAt, &subscription.ObservedPayments,
	); err != nil {
		return nil, err
	}
	return subscription, nil
}

type VirtualTokenRepository struct {
	db *sql.DB
}

func NewVirtualTokenRepository(db *sql.DB) *VirtualTokenRepository {
	return &VirtualTokenRepository{db: db}
}

func (r *VirtualTokenRepository) Save(ctx context.Context, token *domain.VirtualToken) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO virtual_tokens
			(id, user_id, merchant_id, masked_pan, monthly_limit, spent_in_period, currency, state, period_started)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
			state = VALUES(state), spent_in_period = VALUES(spent_in_period),
			monthly_limit = VALUES(monthly_limit), period_started = VALUES(period_started)`,
		token.ID, token.UserID, token.MerchantID, token.MaskedPAN, token.MonthlyLimit,
		token.SpentInPeriod, token.Currency, token.State, token.PeriodStarted.UTC(),
	)
	if err != nil {
		return fmt.Errorf("save virtual token %s: %w", token.ID, err)
	}
	return nil
}

func (r *VirtualTokenRepository) GetByID(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	row := r.db.QueryRowContext(ctx, tokenSelect+` WHERE id = ?`, id)
	token, err := scanToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("virtual token %s: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get virtual token %s: %w", id, err)
	}
	return token, nil
}

func (r *VirtualTokenRepository) FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error) {
	row := r.db.QueryRowContext(ctx, tokenSelect+` WHERE user_id = ? AND merchant_id = ?`, userID, merchantID)
	token, err := scanToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("virtual token for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("find virtual token for user %s merchant %s: %w", userID, merchantID, err)
	}
	return token, nil
}

func (r *VirtualTokenRepository) ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.VirtualToken, error) {
	rows, err := r.db.QueryContext(ctx, tokenSelect+` WHERE user_id = ? ORDER BY id`, userID)
	if err != nil {
		return nil, fmt.Errorf("list virtual tokens for user %s: %w", userID, err)
	}
	defer rows.Close()

	tokens := make([]*domain.VirtualToken, 0)
	for rows.Next() {
		token, err := scanToken(rows)
		if err != nil {
			return nil, fmt.Errorf("scan virtual token: %w", err)
		}
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate virtual tokens: %w", err)
	}
	return tokens, nil
}

const tokenSelect = `SELECT id, user_id, merchant_id, masked_pan, monthly_limit, spent_in_period,
	currency, state, period_started FROM virtual_tokens`

func scanToken(row rowScanner) (*domain.VirtualToken, error) {
	token := &domain.VirtualToken{}
	if err := row.Scan(
		&token.ID, &token.UserID, &token.MerchantID, &token.MaskedPAN, &token.MonthlyLimit,
		&token.SpentInPeriod, &token.Currency, &token.State, &token.PeriodStarted,
	); err != nil {
		return nil, err
	}
	return token, nil
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

var (
	_ ports.TransactionRepository  = (*TransactionRepository)(nil)
	_ ports.SubscriptionRepository = (*SubscriptionRepository)(nil)
	_ ports.VirtualTokenRepository = (*VirtualTokenRepository)(nil)
)
