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
	row := r.db.QueryRowContext(ctx, transactionSelect+` WHERE id = ?`, id)
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
		transactionSelect+` WHERE user_id = ? AND authorized_at >= ? ORDER BY authorized_at`,
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
	arguments := make([]any, 0, len(merchants)+2)
	arguments = append(arguments, userID, since.UTC())
	for _, merchantID := range merchants {
		arguments = append(arguments, merchantID)
	}

	return r.scanTransactions(ctx,
		transactionSelect+` WHERE user_id = ? AND authorized_at >= ? AND merchant_id IN (`+placeholders(len(merchants))+`)
		 ORDER BY authorized_at`,
		arguments...)
}

const transactionSelect = `SELECT id, user_id, merchant_id, merchant_name, mcc, amount_minor, currency, authorized_at
	FROM transactions`

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

var _ ports.TransactionRepository = (*TransactionRepository)(nil)
