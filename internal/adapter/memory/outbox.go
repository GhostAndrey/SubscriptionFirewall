package memory

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
)

type DetectionOutbox struct {
	transactions ports.TransactionRepository
	metrics      *obs.Metrics
	logger       *slog.Logger
	jobs         chan domain.UserID

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
		jobs:         make(chan domain.UserID, capacity),
	}
}

func (o *DetectionOutbox) EnqueueWithTransaction(ctx context.Context, transaction domain.Transaction) error {
	if err := o.transactions.Save(ctx, transaction); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ports.ErrOutboxClosed
	}
	select {
	case o.jobs <- transaction.UserID:
	default:
		o.metrics.CountEnqueueDropped()
		o.logger.Warn("detection queue full, user dropped", "user_id", string(transaction.UserID))
	}
	return nil
}

func (o *DetectionOutbox) ClaimBatch(ctx context.Context, limit int) ([]ports.OutboxItem, error) {
	items := make([]ports.OutboxItem, 0, limit)
	for {

		select {
		case userID, ok := <-o.jobs:
			if !ok {

				if len(items) > 0 {
					return items, nil
				}
				return items, ports.ErrOutboxClosed
			}
			items = append(items, o.newItem(userID))
			if len(items) == limit {
				return items, nil
			}
			continue
		default:
		}
		if len(items) > 0 {
			return items, nil
		}

		select {
		case <-ctx.Done():

			select {
			case userID, ok := <-o.jobs:
				if !ok {
					if len(items) > 0 {
						return items, nil
					}
					return items, ports.ErrOutboxClosed
				}
				items = append(items, o.newItem(userID))
			default:
				return items, ctx.Err()
			}
		case userID, ok := <-o.jobs:
			if !ok {
				if len(items) > 0 {
					return items, nil
				}
				return items, ports.ErrOutboxClosed
			}
			items = append(items, o.newItem(userID))
		}
	}
}

func (o *DetectionOutbox) newItem(userID domain.UserID) ports.OutboxItem {
	return ports.OutboxItem{ID: o.nextID.Add(1), UserID: userID}
}

func (o *DetectionOutbox) Complete(_ context.Context, _ []int64) error { return nil }

func (o *DetectionOutbox) Fail(_ context.Context, _ ports.OutboxItem) error { return nil }

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
