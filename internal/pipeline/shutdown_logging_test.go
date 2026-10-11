package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/detector"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

// logRecorder captures records so a shutdown can be asserted not to report the
// closed queue as a failure.
type logRecorder struct {
	mu      sync.Mutex
	records []string
}

func (r *logRecorder) Write(payload []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, string(payload))
	return len(payload), nil
}

func (r *logRecorder) contains(needle string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.records {
		if strings.Contains(record, needle) {
			return true
		}
	}
	return false
}

func TestShutdownDoesNotReportTheClosedQueueAsAFailure(t *testing.T) {
	transactions := memory.NewTransactionRepository()
	outbox := memory.NewDetectionOutbox(transactions, obs.NewMetrics(prometheus.NewRegistry()), testLogger(), 16)
	capture := &logRecorder{}
	logger := slog.New(slog.NewTextHandler(capture, &slog.HandlerOptions{Level: slog.LevelInfo}))

	service := New(
		outbox,
		detector.New(transactions, memory.Clock{}, detector.DefaultConfig()),
		subscription.NewService(memory.NewSubscriptionRepository(), memory.Clock{}),
		token.NewService(memory.NewVirtualTokenRepository(), stubIssuer{}, memory.Clock{}, token.Options{}),
		obs.NewMetrics(prometheus.NewRegistry()),
		logger,
	)

	// The queue is closed before the workers are cancelled, so a worker that is
	// mid-claim observes the closed sentinel rather than a cancelled context.
	service.Start(context.Background(), 1)
	service.Stop()

	if capture.contains("claim detection pipeline failed") {
		t.Errorf("shutdown logged a claim failure for the closed queue:\n%s", strings.Join(capture.records, ""))
	}
}

func TestClaimTranslatesTheClosedQueueSentinel(t *testing.T) {
	transactions := memory.NewTransactionRepository()
	outbox := memory.NewDetectionOutbox(transactions, obs.NewMetrics(prometheus.NewRegistry()), testLogger(), 16)

	service := New(
		outbox,
		detector.New(transactions, memory.Clock{}, detector.DefaultConfig()),
		subscription.NewService(memory.NewSubscriptionRepository(), memory.Clock{}),
		token.NewService(memory.NewVirtualTokenRepository(), stubIssuer{}, memory.Clock{}, token.Options{}),
		obs.NewMetrics(prometheus.NewRegistry()),
		testLogger(),
	)
	outbox.Close()

	_, err := service.claimUntilOpen(context.Background(), 4)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the closed queue to read as a cancellation, got %v", err)
	}
}

func TestClaimPassesThroughStorageErrors(t *testing.T) {
	failing := &failingOutbox{err: errors.New("connection reset by peer")}

	service := New(
		failing,
		detector.New(memory.NewTransactionRepository(), memory.Clock{}, detector.DefaultConfig()),
		subscription.NewService(memory.NewSubscriptionRepository(), memory.Clock{}),
		token.NewService(memory.NewVirtualTokenRepository(), stubIssuer{}, memory.Clock{}, token.Options{}),
		obs.NewMetrics(prometheus.NewRegistry()),
		testLogger(),
	)

	_, err := service.claimUntilOpen(context.Background(), 4)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("a storage failure must stay retryable, got %v", err)
	}
}

// failingOutbox reports a storage error on every claim.
type failingOutbox struct {
	ports.DetectionOutbox
	err error
}

func (o *failingOutbox) ClaimBatch(context.Context, int) ([]ports.OutboxItem, error) {
	return nil, o.err
}

func TestStopIsSafeWithoutStart(t *testing.T) {
	transactions := memory.NewTransactionRepository()
	outbox := memory.NewDetectionOutbox(transactions, obs.NewMetrics(prometheus.NewRegistry()), testLogger(), 16)

	service := New(
		outbox,
		detector.New(transactions, memory.Clock{}, detector.DefaultConfig()),
		subscription.NewService(memory.NewSubscriptionRepository(), memory.Clock{}),
		token.NewService(memory.NewVirtualTokenRepository(), stubIssuer{}, memory.Clock{}, token.Options{}),
		obs.NewMetrics(prometheus.NewRegistry()),
		testLogger(),
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Stop()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop must return when the pipeline was never started")
	}
}
