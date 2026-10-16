package memory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
)

const maxDetectionAttempts = 8

// queuedJob carries the retry count alongside the user so Fail can park a job
// that keeps failing instead of hot-looping it through the worker forever.
type queuedJob struct {
	userID   domain.UserID
	attempts int
}

type DetectionOutbox struct {
	transactions ports.TransactionRepository
	metrics      *obs.Metrics
	logger       *slog.Logger
	jobs         chan queuedJob

	mu     sync.Mutex
	closed bool
	nextID atomic.Int64
}

func NewDetectionOutbox(
	transactions ports.TransactionRepository,
	metrics *obs.Metrics,
	logger *slog.Logger,
	capacity int,
) *DetectionOutbox {
	return &DetectionOutbox{
		transactions: transactions,
		metrics:      metrics,
		logger:       logger,
		jobs:         make(chan queuedJob, capacity),
	}
}

// EnqueueWithTransaction stores the transaction and queues a detection job for
// its user; a replayed ingest is rejected, mirroring the primary key of the SQL
// adapter.
func (o *DetectionOutbox) EnqueueWithTransaction(ctx context.Context, transaction domain.Transaction) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ports.ErrOutboxClosed
	}
	if _, err := o.transactions.GetByID(ctx, transaction.ID); err == nil {
		return fmt.Errorf("transaction %s: %w", transaction.ID, domain.ErrAlreadyExists)
	} else if !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	if err := o.transactions.Save(ctx, transaction); err != nil {
		return err
	}

	select {
	case o.jobs <- queuedJob{userID: transaction.UserID}:
	default:
		o.metrics.CountEnqueueDropped()
		o.logger.Warn("detection queue full, user dropped", "user_id", string(transaction.UserID))
	}
	return nil
}

// ClaimBatch drains up to limit queued users, blocking until at least one is
// available or the context is cancelled.
func (o *DetectionOutbox) ClaimBatch(ctx context.Context, limit int) ([]ports.OutboxItem, error) {
	items := make([]ports.OutboxItem, 0, limit)
	for len(items) < limit {
		select {
		case job, ok := <-o.jobs:
			if !ok {
				if len(items) > 0 {
					return items, nil
				}
				return nil, ports.ErrOutboxClosed
			}
			items = append(items, o.newItem(job))
		default:
			if len(items) > 0 {
				return items, nil
			}
			return o.claimBlocking(ctx, items)
		}
	}
	return items, nil
}

// claimBlocking waits for the next queued user once the backlog is drained, so
// idle workers sleep instead of spinning on an empty channel.
func (o *DetectionOutbox) claimBlocking(ctx context.Context, items []ports.OutboxItem) ([]ports.OutboxItem, error) {
	select {
	case <-ctx.Done():
		return items, ctx.Err()
	case job, ok := <-o.jobs:
		if !ok {
			if len(items) > 0 {
				return items, nil
			}
			return nil, ports.ErrOutboxClosed
		}
		return append(items, o.newItem(job)), nil
	}
}

func (o *DetectionOutbox) newItem(job queuedJob) ports.OutboxItem {
	return ports.OutboxItem{ID: o.nextID.Add(1), UserID: job.userID, Attempts: job.attempts}
}

func (o *DetectionOutbox) Complete(_ context.Context, _ []int64) error { return nil }

// Fail requeues the user until the retry budget is exhausted, so a transient
// detection failure is retried within the process instead of being dropped on
// the floor. Once the budget is spent the job is parked and reported, matching
// the MySQL adapter's behaviour.
func (o *DetectionOutbox) Fail(_ context.Context, item ports.OutboxItem) error {
	if item.Attempts+1 >= maxDetectionAttempts {
		o.metrics.CountEnqueueDropped()
		o.logger.Warn("detection job exhausted retries, user dropped",
			"user_id", string(item.UserID), "attempts", item.Attempts)
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ports.ErrOutboxClosed
	}

	select {
	case o.jobs <- queuedJob{userID: item.UserID, attempts: item.Attempts + 1}:
	default:
		o.metrics.CountEnqueueDropped()
		o.logger.Warn("detection queue full, retry dropped", "user_id", string(item.UserID))
	}
	return nil
}

func (o *DetectionOutbox) RequeueStale(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func (o *DetectionOutbox) PendingCount(_ context.Context) (int64, error) {
	return int64(len(o.jobs)), nil
}

func (o *DetectionOutbox) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.closed = true
	close(o.jobs)
}

var _ ports.DetectionOutbox = (*DetectionOutbox)(nil)

type AuditLog struct {
	mu      sync.Mutex
	entries []ports.AuditEntry
}

func NewAuditLog() *AuditLog {
	return &AuditLog{}
}

func (a *AuditLog) Record(_ context.Context, entry ports.AuditEntry) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, entry)
	return nil
}

func (a *AuditLog) Entries() []ports.AuditEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ports.AuditEntry(nil), a.entries...)
}

var _ ports.AuditLog = (*AuditLog)(nil)
