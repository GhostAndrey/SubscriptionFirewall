package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

const maxOutboxAttempts = 8

type Outbox struct {
	db *sql.DB
}

func NewOutbox(db *sql.DB) *Outbox {
	return &Outbox{db: db}
}

func (o *Outbox) EnqueueWithTransaction(ctx context.Context, transaction domain.Transaction) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ingest transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
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

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO detection_outbox (user_id, status, attempts, created_at, updated_at)
		 VALUES (?, 'pending', 0, ?, ?)`,
		transaction.UserID, now, now); err != nil {
		return fmt.Errorf("insert detection outbox for user %s: %w", transaction.UserID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ingest transaction: %w", err)
	}
	return nil
}

func (o *Outbox) ClaimBatch(ctx context.Context, limit int) ([]ports.OutboxItem, error) {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin claim: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		`SELECT id, user_id, attempts FROM detection_outbox
		 WHERE status = 'pending' AND attempts < ?
		 ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`,
		maxOutboxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending outbox items: %w", err)
	}
	items := make([]ports.OutboxItem, 0, limit)
	for rows.Next() {
		var item ports.OutboxItem
		if err := rows.Scan(&item.ID, &item.UserID, &item.Attempts); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan outbox item: %w", err)
		}
		item.Attempts++
		items = append(items, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox items: %w", err)
	}
	if len(items) == 0 {
		return nil, tx.Commit()
	}

	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE detection_outbox SET status = 'processing', attempts = attempts + 1, updated_at = ?
		 WHERE id IN (`+placeholders(len(ids))+`)`,
		append([]any{time.Now().UTC()}, int64SliceToAny(ids)...)...); err != nil {
		return nil, fmt.Errorf("mark outbox items processing: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return items, nil
}

func (o *Outbox) Complete(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := o.db.ExecContext(ctx,
		`UPDATE detection_outbox SET status = 'done', updated_at = ?
		 WHERE id IN (`+placeholders(len(ids))+`)`,
		append([]any{time.Now().UTC()}, int64SliceToAny(ids)...)...)
	if err != nil {
		return fmt.Errorf("complete outbox items: %w", err)
	}
	return nil
}

func (o *Outbox) Fail(ctx context.Context, item ports.OutboxItem) error {
	status := "pending"
	if item.Attempts >= maxOutboxAttempts {
		status = "failed"
	}
	if _, err := o.db.ExecContext(ctx,
		`UPDATE detection_outbox SET status = ?, updated_at = ? WHERE id = ?`,
		status, time.Now().UTC(), item.ID); err != nil {
		return fmt.Errorf("fail outbox item %d: %w", item.ID, err)
	}
	return nil
}

func (o *Outbox) RequeueStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	result, err := o.db.ExecContext(ctx,
		`UPDATE detection_outbox SET status = 'pending', updated_at = ?
		 WHERE status = 'processing' AND updated_at < ?`,
		time.Now().UTC(), time.Now().UTC().Add(-olderThan))
	if err != nil {
		return 0, fmt.Errorf("requeue stale outbox items: %w", err)
	}
	requeued, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count requeued outbox items: %w", err)
	}
	return requeued, nil
}

func (o *Outbox) PendingCount(ctx context.Context) (int64, error) {
	var count int64
	if err := o.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM detection_outbox WHERE status = 'pending'`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending outbox items: %w", err)
	}
	return count, nil
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func int64SliceToAny(values []int64) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}

var _ ports.DetectionOutbox = (*Outbox)(nil)

type AuditLog struct {
	db *sql.DB
}

func NewAuditLog(db *sql.DB) *AuditLog {
	return &AuditLog{db: db}
}

func (a *AuditLog) Record(ctx context.Context, entry ports.AuditEntry) error {
	_, err := a.db.ExecContext(ctx,
		`INSERT INTO audit_log (actor, action, entity_type, entity_id, created_at) VALUES (?, ?, ?, ?, ?)`,
		entry.Actor, entry.Action, entry.EntityType, entry.EntityID, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("record audit entry: %w", err)
	}
	return nil
}

var _ ports.AuditLog = (*AuditLog)(nil)
