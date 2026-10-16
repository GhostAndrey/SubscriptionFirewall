package memory

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"subscriptionfirewall/internal/ports"
)

const maxLifecycleSyncAttempts = 8

// LifecycleSyncQueue mirrors the MySQL adapter in memory with the same claim,
// retry and stale-recovery semantics.
type LifecycleSyncQueue struct {
	mu     sync.Mutex
	tasks  map[int64]*lifecycleEntry
	nextID atomic.Int64
}

type lifecycleEntry struct {
	task      ports.LifecycleSyncTask
	status    string
	updatedAt time.Time
}

func NewLifecycleSyncQueue() *LifecycleSyncQueue {
	return &LifecycleSyncQueue{tasks: make(map[int64]*lifecycleEntry)}
}

func (q *LifecycleSyncQueue) Enqueue(_ context.Context, task ports.LifecycleSyncTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task.ID = q.nextID.Add(1)
	q.tasks[task.ID] = &lifecycleEntry{task: task, status: "pending", updatedAt: time.Now()}
	return nil
}

func (q *LifecycleSyncQueue) ClaimBatch(_ context.Context, limit int) ([]ports.LifecycleSyncTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	claimed := make([]ports.LifecycleSyncTask, 0, limit)
	for _, id := range q.idsInOrder() {
		entry := q.tasks[id]
		if entry.status != "pending" || entry.task.Attempts >= maxLifecycleSyncAttempts {
			continue
		}
		entry.task.Attempts++
		entry.status = "processing"
		entry.updatedAt = time.Now()
		claimed = append(claimed, entry.task)
		if len(claimed) == limit {
			break
		}
	}
	return claimed, nil
}

func (q *LifecycleSyncQueue) Complete(_ context.Context, ids []int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, id := range ids {
		if entry, ok := q.tasks[id]; ok {
			entry.status = "done"
			entry.updatedAt = time.Now()
		}
	}
	return nil
}

func (q *LifecycleSyncQueue) Fail(_ context.Context, task ports.LifecycleSyncTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	entry, ok := q.tasks[task.ID]
	if !ok {
		return nil
	}
	entry.status = "pending"
	if task.Attempts >= maxLifecycleSyncAttempts {
		entry.status = "failed"
	}
	entry.updatedAt = time.Now()
	return nil
}

func (q *LifecycleSyncQueue) RequeueStale(_ context.Context, olderThan time.Duration) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	var requeued int64
	for _, entry := range q.tasks {
		if entry.status == "processing" && entry.updatedAt.Before(cutoff) {
			entry.status = "pending"
			entry.updatedAt = time.Now()
			requeued++
		}
	}
	return requeued, nil
}

func (q *LifecycleSyncQueue) PendingCount(context.Context) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var pending int64
	for _, entry := range q.tasks {
		if entry.status == "pending" {
			pending++
		}
	}
	return pending, nil
}

// idsInOrder lists task ids oldest first so claims replay a backlog in the
// sequence it was created.
func (q *LifecycleSyncQueue) idsInOrder() []int64 {
	ids := make([]int64, 0, len(q.tasks))
	for id := range q.tasks {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

var _ ports.LifecycleSyncQueue = (*LifecycleSyncQueue)(nil)
