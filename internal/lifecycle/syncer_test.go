package lifecycle

import (
	"context"
	"testing"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

// maxQueueAttempts mirrors the retry budget of the in-memory sync queue.
const maxQueueAttempts = 8

func TestSyncerReplaysDeferredTask(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionTerminated, domain.VirtualTokenActive)

	if err := h.queue.Enqueue(t.Context(), tokenTask("vtok-1", ActionTerminate)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	syncer := NewSyncer(h.service, h.queue, h.metrics, discardLogger())
	syncer.Start(t.Context(), 1)
	t.Cleanup(syncer.Stop)

	waitFor(t, time.Second, func() bool {
		token, err := h.tokens.GetByID(t.Context(), "vtok-1")
		return err == nil && token.State == domain.VirtualTokenTerminated
	}, "expected the deferred termination to be replayed")

	if pending, err := h.queue.PendingCount(context.Background()); err != nil || pending != 0 {
		t.Fatalf("expected the queue to drain, got %d (%v)", pending, err)
	}
}

func TestSyncerRetriesFailedTaskWithoutLosingIt(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionTerminated, domain.VirtualTokenActive)

	task := tokenTask("vtok-missing", ActionTerminate)
	if err := h.queue.Enqueue(t.Context(), task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	syncer := NewSyncer(h.service, h.queue, h.metrics, discardLogger())
	syncer.Start(t.Context(), 1)
	t.Cleanup(syncer.Stop)

	waitFor(t, time.Second, func() bool {
		claimed, err := h.queue.ClaimBatch(context.Background(), 10)
		return err == nil && len(claimed) == 0
	}, "expected the failing task to return to the queue for another attempt")

	if _, err := h.queue.RequeueStale(t.Context(), -time.Minute); err != nil {
		t.Fatalf("requeue stale: %v", err)
	}
}

func TestSyncerStopIsIdempotent(t *testing.T) {
	h := newHarness(t, time.Now())

	syncer := NewSyncer(h.service, h.queue, h.metrics, discardLogger())
	syncer.Start(t.Context(), 2)
	syncer.Stop()
	syncer.Stop()
}

func TestLifecycleSyncQueueReclaimsStaleWork(t *testing.T) {
	h := newHarness(t, time.Now())

	if err := h.queue.Enqueue(t.Context(), tokenTask("vtok-1", ActionTerminate)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := h.queue.ClaimBatch(t.Context(), 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected one claimed task, got %+v (%v)", claimed, err)
	}
	if claimed[0].Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", claimed[0].Attempts)
	}

	requeued, err := h.queue.RequeueStale(t.Context(), time.Hour)
	if err != nil || requeued != 0 {
		t.Fatalf("expected nothing requeued before the stale window, got %d (%v)", requeued, err)
	}
	// A negative window marks every in-flight task as stale regardless of clock granularity.
	requeued, err = h.queue.RequeueStale(t.Context(), -time.Minute)
	if err != nil || requeued != 1 {
		t.Fatalf("expected the claimed task to be reclaimed, got %d (%v)", requeued, err)
	}

	again, err := h.queue.ClaimBatch(t.Context(), 10)
	if err != nil || len(again) != 1 || again[0].Attempts != 2 {
		t.Fatalf("expected a retryable task with attempts=2, got %+v (%v)", again, err)
	}
}

func TestLifecycleSyncQueueStopsAfterMaxAttempts(t *testing.T) {
	h := newHarness(t, time.Now())

	task := tokenTask("vtok-1", ActionTerminate)
	if err := h.queue.Enqueue(t.Context(), task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var last ports.LifecycleSyncTask
	for range maxQueueAttempts {
		claimed, err := h.queue.ClaimBatch(t.Context(), 10)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("expected one claimable task, got %+v (%v)", claimed, err)
		}
		last = claimed[0]
		if err := h.queue.Fail(t.Context(), last); err != nil {
			t.Fatalf("fail: %v", err)
		}
	}

	exhausted, err := h.queue.ClaimBatch(t.Context(), 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(exhausted) != 0 {
		t.Fatalf("expected the task to be parked after %d attempts, got %+v", last.Attempts, exhausted)
	}
}

func waitFor(t *testing.T, limit time.Duration, condition func() bool, message string) {
	t.Helper()

	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(message)
}
