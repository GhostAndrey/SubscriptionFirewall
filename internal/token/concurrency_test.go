package token

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
)

// interleavingRepository performs a charge in the window between the service
// reading a token and persisting its transition. That window is exactly where a
// blind read-modify-write loses debited budget.
type interleavingRepository struct {
	*memory.VirtualTokenRepository
	charge func()
	once   sync.Once
}

func (r *interleavingRepository) UpdateVersion(ctx context.Context, token *domain.VirtualToken) error {
	r.once.Do(r.charge)
	return r.VirtualTokenRepository.UpdateVersion(ctx, token)
}

func TestFreezeKeepsAChargeCommittedMidTransition(t *testing.T) {
	ctx := t.Context()
	base := memory.NewVirtualTokenRepository()

	seeded, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", time.Now())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := base.Save(ctx, seeded); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	repository := &interleavingRepository{VirtualTokenRepository: base}
	repository.charge = func() {
		if _, err := base.Charge(ctx, "vtok-1", 4_000, "USD", time.Now()); err != nil {
			t.Errorf("interleaved charge: %v", err)
		}
	}

	service := newService(t, repository, &countedIssuer{}, time.Now())
	frozen, err := service.Freeze(ctx, "vtok-1")
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if frozen.State != domain.VirtualTokenFrozen {
		t.Errorf("state = %s, want Frozen", frozen.State)
	}

	stored, err := base.GetByID(ctx, "vtok-1")
	if err != nil {
		t.Fatalf("load token: %v", err)
	}
	if stored.Snapshot().SpentInPeriod != 4_000 {
		t.Errorf("spent = %d, want 4000: the interleaved charge must survive the freeze",
			stored.Snapshot().SpentInPeriod)
	}
}

func TestTerminateKeepsAChargeCommittedMidTransition(t *testing.T) {
	ctx := t.Context()
	base := memory.NewVirtualTokenRepository()

	seeded, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", time.Now())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := base.Save(ctx, seeded); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	repository := &interleavingRepository{VirtualTokenRepository: base}
	repository.charge = func() {
		if _, err := base.Charge(ctx, "vtok-1", 1_500, "USD", time.Now()); err != nil {
			t.Errorf("interleaved charge: %v", err)
		}
	}

	service := newService(t, repository, &countedIssuer{}, time.Now())
	if _, err := service.Terminate(ctx, "vtok-1"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	stored, err := base.GetByID(ctx, "vtok-1")
	if err != nil {
		t.Fatalf("load token: %v", err)
	}
	if stored.Snapshot().SpentInPeriod != 1_500 {
		t.Errorf("spent = %d, want 1500", stored.Snapshot().SpentInPeriod)
	}
	if stored.State != domain.VirtualTokenTerminated {
		t.Errorf("state = %s, want Terminated", stored.State)
	}
}

func TestLifecycleTransitionsNeverCrossTheMonthlyLimit(t *testing.T) {
	ctx := t.Context()
	repository := memory.NewVirtualTokenRepository()

	seeded, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 5_000, "USD", time.Now())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := repository.Save(ctx, seeded); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	service := newService(t, repository, &countedIssuer{}, time.Now())
	if _, err := service.Authorize(ctx, "vtok-1", 4_000, "USD"); err != nil {
		t.Fatalf("authorize: %v", err)
	}

	// A freeze landing next to the last affordable charge must not reopen
	// budget: the remaining headroom has to stay at most 1000.
	if _, err := service.Freeze(ctx, "vtok-1"); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if _, err := service.Reactivate(ctx, "vtok-1"); err != nil {
		t.Fatalf("reactivate: %v", err)
	}
	if _, err := service.Authorize(ctx, "vtok-1", 1_500, "USD"); err == nil {
		t.Error("expected the charge to be declined: lifecycle transitions must not reset the period spend")
	}
}

func TestKeyedMutexReleasesEntriesWhenIdle(t *testing.T) {
	var mutex keyedMutex

	unlock := mutex.lock("user-1|netflix")
	mutex.mu.Lock()
	held := len(mutex.locks)
	mutex.mu.Unlock()
	if held != 1 {
		t.Fatalf("held entries = %d, want 1 while the lock is taken", held)
	}

	unlock()
	mutex.mu.Lock()
	remaining := len(mutex.locks)
	mutex.mu.Unlock()
	if remaining != 0 {
		t.Errorf("entries = %d, want 0 once every holder left", remaining)
	}
}

func TestKeyedMutexKeepsEntryWhileAnotherHolderWaits(t *testing.T) {
	var mutex keyedMutex

	unlock := mutex.lock("user-1|netflix")
	mutex.mu.Lock()
	mutex.locks["user-1|netflix"].refs++
	mutex.mu.Unlock()

	unlock()
	mutex.mu.Lock()
	remaining := len(mutex.locks)
	mutex.mu.Unlock()
	if remaining != 1 {
		t.Errorf("entries = %d, want 1: the entry must survive while a waiter holds it", remaining)
	}
}

func TestKeyedMutexSerializesAndDoesNotLeakUnderConcurrency(t *testing.T) {
	var mutex keyedMutex
	const goroutines, keys = 32, 8

	probe := newConcurrencyProbe()
	var wg sync.WaitGroup
	for worker := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Every goroutine reuses the same small key set, so the per-key
			// lock has to serialise them. Different keys may still overlap,
			// which is why the probe is keyed.
			for round := range keys {
				key := fmt.Sprintf("user-%d|merchant-%d", worker%keys, round%keys)
				unlock := mutex.lock(key)
				probe.enter(key)
				probe.leave(key)
				unlock()
			}
		}()
	}
	wg.Wait()

	// Same key must never overlap, while distinct keys are free to run at once.
	for key, peak := range probe.peaks() {
		if peak > 1 {
			t.Errorf("key %q saw %d concurrent holders, want at most 1", key, peak)
		}
	}
	if len(probe.peaks()) == 0 {
		t.Fatal("expected the probe to observe at least one holder")
	}
	mutex.mu.Lock()
	remaining := len(mutex.locks)
	mutex.mu.Unlock()
	if remaining != 0 {
		t.Errorf("entries = %d, want 0 after every holder left", remaining)
	}
}

// concurrencyProbe records, per key, how many holders were inside the critical
// section at once, so the test can assert each lock really serialises its key.
type concurrencyProbe struct {
	mu      sync.Mutex
	current map[string]int
	highest map[string]int
}

func newConcurrencyProbe() *concurrencyProbe {
	return &concurrencyProbe{current: map[string]int{}, highest: map[string]int{}}
}

func (p *concurrencyProbe) enter(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current[key]++
	p.highest[key] = max(p.highest[key], p.current[key])
}

func (p *concurrencyProbe) leave(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current[key]--
}

func (p *concurrencyProbe) peaks() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := make(map[string]int, len(p.highest))
	for key, peak := range p.highest {
		copied[key] = peak
	}
	return copied
}
