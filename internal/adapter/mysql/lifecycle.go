package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"subscriptionfirewall/internal/ports"
)

const maxLifecycleSyncAttempts = 8

// LifecycleSyncQueue is the durable fallback for irreversible transitions:
// when the counterpart entity cannot be updated inline, the action is stored
// here and replayed by the lifecycle syncer.
type LifecycleSyncQueue struct {
	db *sql.DB
}

func NewLifecycleSyncQueue(db *sql.DB) *LifecycleSyncQueue {
	return &LifecycleSyncQueue{db: db}
}

func (q *LifecycleSyncQueue) Enqueue(ctx context.Context, task ports.LifecycleSyncTask) error {
	now := time.Now().UTC()
	if _, err := q.db.ExecContext(ctx,
		`INSERT INTO lifecycle_sync_outbox (entity_type, entity_id, action, status, attempts, created_at, updated_at)
		 VALUES (?, ?, ?, 'pending', 0, ?, ?)`,
		task.EntityType, task.EntityID, task.Action, now, now,
	); err != nil {
		return fmt.Errorf("enqueue lifecycle sync for %s %s: %w", task.EntityType, task.EntityID, err)
	}
	return nil
}

func (q *LifecycleSyncQueue) ClaimBatch(ctx context.Context, limit int) ([]ports.LifecycleSyncTask, error) {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin lifecycle sync claim: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		`SELECT id, entity_type, entity_id, action, attempts FROM lifecycle_sync_outbox
		 WHERE status = 'pending' AND attempts < ?
		 ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`,
		maxLifecycleSyncAttempts, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select pending lifecycle syncs: %w", err)
	}

	tasks := make([]ports.LifecycleSyncTask, 0, limit)
	for rows.Next() {
		var task ports.LifecycleSyncTask
		if err := rows.Scan(&task.ID, &task.EntityType, &task.EntityID, &task.Action, &task.Attempts); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan lifecycle sync: %w", err)
		}
		task.Attempts++
		tasks = append(tasks, task)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lifecycle syncs: %w", err)
	}
	if len(tasks) == 0 {
		return nil, tx.Commit()
	}

	ids := make([]int64, len(tasks))
	for i, task := range tasks {
		ids[i] = task.ID
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE lifecycle_sync_outbox SET status = 'processing', attempts = attempts + 1, updated_at = ?
		 WHERE id IN (`+placeholders(len(ids))+`)`,
		append([]any{time.Now().UTC()}, int64SliceToAny(ids)...)...,
	); err != nil {
		return nil, fmt.Errorf("mark lifecycle syncs processing: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lifecycle sync claim: %w", err)
	}
	return tasks, nil
}

func (q *LifecycleSyncQueue) Complete(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := q.db.ExecContext(ctx,
		`UPDATE lifecycle_sync_outbox SET status = 'done', updated_at = ?
		 WHERE id IN (`+placeholders(len(ids))+`)`,
		append([]any{time.Now().UTC()}, int64SliceToAny(ids)...)...,
	); err != nil {
		return fmt.Errorf("complete lifecycle syncs: %w", err)
	}
	return nil
}

func (q *LifecycleSyncQueue) Fail(ctx context.Context, task ports.LifecycleSyncTask) error {
	status := "pending"
	if task.Attempts >= maxLifecycleSyncAttempts {
		status = "failed"
	}
	if _, err := q.db.ExecContext(ctx,
		`UPDATE lifecycle_sync_outbox SET status = ?, updated_at = ? WHERE id = ?`,
		status, time.Now().UTC(), task.ID,
	); err != nil {
		return fmt.Errorf("fail lifecycle sync %d: %w", task.ID, err)
	}
	return nil
}

func (q *LifecycleSyncQueue) RequeueStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	result, err := q.db.ExecContext(ctx,
		`UPDATE lifecycle_sync_outbox SET status = 'pending', updated_at = ?
		 WHERE status = 'processing' AND updated_at < ?`,
		time.Now().UTC(), time.Now().UTC().Add(-olderThan),
	)
	if err != nil {
		return 0, fmt.Errorf("requeue stale lifecycle syncs: %w", err)
	}
	requeued, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count requeued lifecycle syncs: %w", err)
	}
	return requeued, nil
}

func (q *LifecycleSyncQueue) PendingCount(ctx context.Context) (int64, error) {
	var count int64
	if err := q.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM lifecycle_sync_outbox WHERE status = 'pending'`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending lifecycle syncs: %w", err)
	}
	return count, nil
}

var _ ports.LifecycleSyncQueue = (*LifecycleSyncQueue)(nil)
