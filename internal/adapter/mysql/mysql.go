package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/identifier"
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
	return r.scanTransactions(ctx,
		`SELECT id, user_id, merchant_id, merchant_name, mcc, amount_minor, currency, authorized_at
		 FROM transactions WHERE user_id = ? AND authorized_at >= ? ORDER BY authorized_at`,
		userID, since.UTC())
}

// ListRecurringMerchantCandidates aggregates in the database so the detector
// never has to pull a user's whole year of transactions to discover that most
// merchants are one-off purchases.
func (r *TransactionRepository) ListRecurringMerchantCandidates(
	ctx context.Context,
	userID domain.UserID,
	since time.Time,
	minOccurrences int,
) ([]ports.MerchantChargeCount, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT merchant_id, COUNT(*), MIN(authorized_at), MAX(authorized_at)
		 FROM transactions
		 WHERE user_id = ? AND authorized_at >= ?
		 GROUP BY merchant_id
		 HAVING COUNT(*) >= ?
		 ORDER BY COUNT(*) DESC, merchant_id`,
		userID, since.UTC(), minOccurrences)
	if err != nil {
		return nil, fmt.Errorf("list merchant candidates for user %s: %w", userID, err)
	}
	defer rows.Close()

	candidates := make([]ports.MerchantChargeCount, 0)
	for rows.Next() {
		var candidate ports.MerchantChargeCount
		if err := rows.Scan(&candidate.MerchantID, &candidate.Occurrences, &candidate.FirstSeen, &candidate.LastSeen); err != nil {
			return nil, fmt.Errorf("scan merchant candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate merchant candidates: %w", err)
	}
	return candidates, nil
}

func (r *TransactionRepository) ListByUserAndMerchants(
	ctx context.Context,
	userID domain.UserID,
	merchants []domain.MerchantID,
	since time.Time,
) ([]domain.Transaction, error) {
	if len(merchants) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(merchants)), ", ")
	arguments := make([]any, 0, len(merchants)+2)
	arguments = append(arguments, userID, since.UTC())
	for _, merchantID := range merchants {
		arguments = append(arguments, merchantID)
	}

	return r.scanTransactions(ctx,
		`SELECT id, user_id, merchant_id, merchant_name, mcc, amount_minor, currency, authorized_at
		 FROM transactions
		 WHERE user_id = ? AND authorized_at >= ? AND merchant_id IN (`+placeholders+`)
		 ORDER BY authorized_at`,
		arguments...)
}

func (r *TransactionRepository) scanTransactions(ctx context.Context, query string, args ...any) ([]domain.Transaction, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list transactions: %w", err)
	}
	defer rows.Close()

	transactions := make([]domain.Transaction, 0)
	for rows.Next() {
		transaction, err := scanTransaction(rows)
		if err != nil {
			return nil, fmt.Errorf("scan transaction: %w", err)
		}
		transactions = append(transactions, *transaction)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transactions: %w", err)
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

// Save inserts a new subscription or replaces an existing row addressed by
// id. It is intended for seeding and tests; concurrent use-case updates go
// through UpdateVersion.
func (r *SubscriptionRepository) Save(ctx context.Context, subscription *domain.Subscription) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO subscriptions
			(id, user_id, merchant_id, merchant_name, virtual_token_id, state, billing_window,
			 average_amount, currency, last_charged_at, next_expected_at, observed_payments, version)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
			merchant_name = VALUES(merchant_name), virtual_token_id = VALUES(virtual_token_id),
			state = VALUES(state), billing_window = VALUES(billing_window),
			average_amount = VALUES(average_amount), currency = VALUES(currency),
			last_charged_at = VALUES(last_charged_at), next_expected_at = VALUES(next_expected_at),
			observed_payments = VALUES(observed_payments), version = VALUES(version)`,
		subscription.ID, subscription.UserID, subscription.MerchantID, subscription.MerchantName,
		subscription.VirtualTokenID, subscription.State, subscription.BillingWindow,
		subscription.AverageAmount, subscription.Currency, subscription.LastChargedAt.UTC(),
		subscription.NextExpectedAt.UTC(), subscription.ObservedPayments, subscription.Version,
	)
	if err != nil {
		return fmt.Errorf("save subscription %s: %w", subscription.ID, err)
	}
	return verifyWrite(ctx, "subscription", string(subscription.ID), func(ctx context.Context) error {
		_, err := r.GetByID(ctx, subscription.ID)
		return err
	})
}

// verifyWrite reads a row back after an upsert. Both tables carry a unique key
// over (user_id, merchant_id) on top of the primary key, so a write that
// carries a fresh id for an existing pair is absorbed by the existing row
// instead of landing under the id the caller asked for. Reporting that as a
// conflict keeps the caller's id from silently pointing at another record.
func verifyWrite(ctx context.Context, entity, id string, load func(context.Context) error) error {
	err := load(ctx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return fmt.Errorf("%s %s was absorbed by an existing row with a different id: %w",
			entity, id, domain.ErrAlreadyExists)
	default:
		return fmt.Errorf("verify %s %s: %w", entity, id, err)
	}
}

// CreateIfAbsent relies on the unique key over (user_id, merchant_id) so that
// two replicas detecting the same pair at the same time produce one row, and
// returns whichever row survived.
func (r *SubscriptionRepository) CreateIfAbsent(ctx context.Context, subscription *domain.Subscription) (*domain.Subscription, error) {
	if _, err := r.db.ExecContext(ctx,
		`INSERT INTO subscriptions
			(id, user_id, merchant_id, merchant_name, virtual_token_id, state, billing_window,
			 average_amount, currency, last_charged_at, next_expected_at, observed_payments, version)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id = id`,
		subscription.ID, subscription.UserID, subscription.MerchantID, subscription.MerchantName,
		subscription.VirtualTokenID, subscription.State, subscription.BillingWindow,
		subscription.AverageAmount, subscription.Currency, subscription.LastChargedAt.UTC(),
		subscription.NextExpectedAt.UTC(), subscription.ObservedPayments, subscription.Version,
	); err != nil {
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

// ListPendingZombieTransition selects exactly the rows whose overdue check has
// not been evaluated yet. The per-window condition mirrors Subscription.MarkMissed,
// and excluding already-zombie rows guarantees a sweep pass terminates instead
// of re-reading the same overdue rows forever.
func (r *SubscriptionRepository) ListPendingZombieTransition(ctx context.Context, now time.Time, limit int) ([]*domain.Subscription, error) {
	const pendingZombie = `state IN (?, ?) AND (
			(billing_window = '7d'   AND next_expected_at <= DATE_SUB(?, INTERVAL 7 DAY)) OR
			(billing_window = '30d'  AND next_expected_at <= DATE_SUB(?, INTERVAL 30 DAY)) OR
			(billing_window = '365d' AND next_expected_at <= DATE_SUB(?, INTERVAL 365 DAY))
		) ORDER BY next_expected_at LIMIT ?`

	return r.list(ctx, subscriptionSelect+` WHERE `+pendingZombie,
		domain.SubscriptionActive, domain.SubscriptionTrial,
		now.UTC(), now.UTC(), now.UTC(), limit)
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
	billing_window, average_amount, currency, last_charged_at, next_expected_at, observed_payments, version
	FROM subscriptions`

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

type VirtualTokenRepository struct {
	db *sql.DB
}

func NewVirtualTokenRepository(db *sql.DB) *VirtualTokenRepository {
	return &VirtualTokenRepository{db: db}
}

func (r *VirtualTokenRepository) Save(ctx context.Context, token *domain.VirtualToken) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO virtual_tokens
			(id, user_id, merchant_id, masked_pan, monthly_limit, spent_in_period, currency, state, period_started, issuance_key)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
			state = VALUES(state), spent_in_period = VALUES(spent_in_period),
			monthly_limit = VALUES(monthly_limit), period_started = VALUES(period_started)`,
		token.ID, token.UserID, token.MerchantID, token.MaskedPAN, token.MonthlyLimit,
		token.SpentInPeriod, token.Currency, token.State, token.PeriodStarted.UTC(), issuanceKeyOf(token),
	)
	if err != nil {
		return fmt.Errorf("save virtual token %s: %w", token.ID, err)
	}
	return verifyWrite(ctx, "virtual token", string(token.ID), func(ctx context.Context) error {
		_, err := r.GetByID(ctx, token.ID)
		return err
	})
}

func issuanceKeyOf(token *domain.VirtualToken) any {
	if token.State == domain.VirtualTokenIssuing {
		return issuanceKeyFor(token.UserID, token.MerchantID)
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

// Charge reads the token under an exclusive row lock, applies the domain rule
// and persists the result inside one transaction. Concurrent charges from any
// replica queue on the same row, so the monthly limit cannot be exceeded by
// interleaved read-modify-write pairs.
func (r *VirtualTokenRepository) Charge(
	ctx context.Context,
	id domain.VirtualTokenID,
	amountMinor int64,
	currency string,
	now time.Time,
) (*domain.VirtualToken, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin charge transaction: %w", err)
	}
	defer tx.Rollback()

	token, err := scanToken(tx.QueryRowContext(ctx, tokenSelect+` WHERE id = ? FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("virtual token %s: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("lock virtual token %s: %w", id, err)
	}

	if err := token.ApplyCharge(amountMinor, currency, now); err != nil {
		return nil, err
	}
	if err := persistToken(ctx, tx, token); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit charge transaction: %w", err)
	}
	return token, nil
}

type tokenExec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func persistToken(ctx context.Context, executor tokenExec, token *domain.VirtualToken) error {
	if _, err := executor.ExecContext(ctx,
		`UPDATE virtual_tokens
		 SET state = ?, spent_in_period = ?, monthly_limit = ?, period_started = ?
		 WHERE id = ?`,
		token.State, token.SpentInPeriod, token.MonthlyLimit, token.PeriodStarted.UTC(), token.ID,
	); err != nil {
		return fmt.Errorf("persist virtual token %s: %w", token.ID, err)
	}
	return nil
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

// issuanceKeyFor is derived from the user and merchant pair alone, so every
// replica and every retry addresses the same logical issuance.
func issuanceKeyFor(userID domain.UserID, merchantID domain.MerchantID) string {
	return "issue:" + string(userID) + ":" + string(merchantID)
}

func (r *VirtualTokenRepository) ReserveTokenIssuance(
	ctx context.Context,
	userID domain.UserID,
	merchantID domain.MerchantID,
	issuanceKey string,
	now time.Time,
	staleBefore time.Time,
) (ports.TokenReservation, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("begin issuance reservation: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM virtual_tokens
		 WHERE user_id = ? AND merchant_id = ? AND state = ? AND period_started < ?`,
		userID, merchantID, domain.VirtualTokenIssuing, staleBefore.UTC(),
	); err != nil {
		return ports.TokenReservation{}, fmt.Errorf("clear stale issuance reservation: %w", err)
	}

	reservedToken := domain.NewIssuingVirtualToken(domain.VirtualTokenID(newTokenID()), userID, merchantID, now)
	result, err := tx.ExecContext(ctx,
		`INSERT INTO virtual_tokens
			(id, user_id, merchant_id, masked_pan, monthly_limit, spent_in_period, currency, state, period_started, issuance_key)
		 VALUES (?, ?, ?, '', 0, 0, '', ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id = id`,
		reservedToken.ID, userID, merchantID, domain.VirtualTokenIssuing, now.UTC(), issuanceKey,
	)
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("reserve virtual token issuance: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("read issuance reservation result: %w", err)
	}
	if affected == 1 {
		if err := tx.Commit(); err != nil {
			return ports.TokenReservation{}, fmt.Errorf("commit issuance reservation: %w", err)
		}
		return ports.TokenReservation{Reserved: true, TokenID: reservedToken.ID}, nil
	}

	existing, err := scanToken(tx.QueryRowContext(ctx,
		tokenSelect+` WHERE user_id = ? AND merchant_id = ? FOR UPDATE`, userID, merchantID))
	if errors.Is(err, sql.ErrNoRows) {
		return ports.TokenReservation{}, fmt.Errorf("virtual token for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
	}
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("read reserved virtual token: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ports.TokenReservation{}, fmt.Errorf("commit issuance reservation: %w", err)
	}
	return ports.TokenReservation{Reserved: false, TokenID: existing.ID, Existing: existing}, nil
}

func (r *VirtualTokenRepository) CompleteTokenIssuance(
	ctx context.Context,
	tokenID domain.VirtualTokenID,
	issuanceKey string,
	card ports.IssuedCard,
	now time.Time,
) (*domain.VirtualToken, error) {
	token, err := r.GetByID(ctx, tokenID)
	if err != nil {
		return nil, err
	}
	if err := token.CompleteIssuance(card.MaskedPAN, card.MonthlyLimit, card.Currency, now); err != nil {
		return nil, err
	}
	if _, err := r.db.ExecContext(ctx,
		`UPDATE virtual_tokens
		 SET masked_pan = ?, monthly_limit = ?, currency = ?, state = ?, period_started = ?, issuance_key = NULL
		 WHERE id = ? AND state = ? AND issuance_key = ?`,
		token.MaskedPAN, token.MonthlyLimit, token.Currency, token.State, now.UTC(),
		tokenID, domain.VirtualTokenIssuing, issuanceKey,
	); err != nil {
		return nil, fmt.Errorf("complete virtual token issuance %s: %w", tokenID, err)
	}
	return token, nil
}

func (r *VirtualTokenRepository) ReleaseTokenIssuance(ctx context.Context, tokenID domain.VirtualTokenID) error {
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM virtual_tokens WHERE id = ? AND state = ?`,
		tokenID, domain.VirtualTokenIssuing,
	); err != nil {
		return fmt.Errorf("release virtual token issuance %s: %w", tokenID, err)
	}
	return nil
}

func newTokenID() string {
	return identifier.New("vtok")
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
