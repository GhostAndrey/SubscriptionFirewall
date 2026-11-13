package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPanicInProcessDoesNotKillTheWorker(t *testing.T) {
	var mu sync.Mutex
	var processed []string
	record := func(value string) {
		mu.Lock()
		defer mu.Unlock()
		processed = append(processed, value)
	}
	var delivered sync.Once

	runner := New(Options[string]{
		Claim: func(ctx context.Context, _ int) ([]string, error) {
			handOut := false
			delivered.Do(func() { handOut = true })
			if !handOut {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return []string{"panic-item", "next-item"}, nil
		},
		Process: func(_ context.Context, item string) {
			if item == "panic-item" {
				panic("boom")
			}
			record(item)
		},
		OnPanic: func(_ context.Context, item string, _ any) {
			record(item)
		},
		Name:   "test",
		Logger: testLogger(),
	})
	runner.Start(context.Background(), 1)
	runner.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 2 {
		t.Fatalf("processed = %v, want both the panicked item and the next one", processed)
	}
}

func TestPanicInMaintenanceDoesNotKillTheWorker(t *testing.T) {
	runner := New(Options[string]{
		Claim: func(ctx context.Context, _ int) ([]string, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		Process:             func(context.Context, string) {},
		Maintenance:         func(context.Context) { panic("boom") },
		MaintenanceInterval: 10 * time.Millisecond,
		Name:                "test",
		Logger:              testLogger(),
	})
	runner.Start(context.Background(), 1)
	runner.Stop()
}

func TestStopLetsInFlightItemsFinishOnLiveContext(t *testing.T) {
	claimed := make(chan struct{})
	first := true
	runner := New(Options[string]{
		Claim: func(ctx context.Context, _ int) ([]string, error) {
			if first {
				first = false
				close(claimed)
				return []string{"item-1"}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		Process: func(ctx context.Context, item string) {
			if err := ctx.Err(); err != nil {
				t.Errorf("process context must survive Stop, got %v", err)
			}
			time.Sleep(50 * time.Millisecond)
		},
		Name:   "test",
		Logger: testLogger(),
	})

	runner.Start(context.Background(), 1)
	<-claimed
	runner.Stop()
}
