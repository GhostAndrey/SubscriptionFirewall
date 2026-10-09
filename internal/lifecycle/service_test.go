package lifecycle

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/issuer"
	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type harness struct {
	subscriptions *memory.SubscriptionRepository
	tokens        *memory.VirtualTokenRepository
	audit         *memory.AuditLog
	queue         *memory.LifecycleSyncQueue
	subs          *subscription.Service
	toks          *token.Service
	service       *Service
	metrics       *obs.Metrics
}

func newHarness(t *testing.T, now time.Time) *harness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	subscriptions := memory.NewSubscriptionRepository()
	tokens := memory.NewVirtualTokenRepository()
	audit := memory.NewAuditLog()
	queue := memory.NewLifecycleSyncQueue()
	metrics := obs.NewMetrics(prometheus.NewRegistry())

	subs := subscription.NewService(subscriptions, fixedClock{now: now})
	toks := token.NewService(tokens, issuer.NewSimulatedCardIssuer(10_000), fixedClock{now: now}, token.Options{
		IssuanceTimeout: time.Minute,
		Logger:          logger,
	})

	return &harness{
		subscriptions: subscriptions,
		tokens:        tokens,
		audit:         audit,
		queue:         queue,
		subs:          subs,
		toks:          toks,
		metrics:       metrics,
		service: NewService(subs, toks, Options{
			Queue:   queue,
			Audit:   audit,
			Metrics: metrics,
			Logger:  logger,
		}),
	}
}

func (h *harness) seedPair(t *testing.T, subscriptionState domain.SubscriptionState, tokenState domain.VirtualTokenState) (*domain.Subscription, *domain.VirtualToken) {
	t.Helper()

	ctx := t.Context()
	virtualToken, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", h.clock())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if tokenState == domain.VirtualTokenFrozen {
		if err := virtualToken.Freeze(); err != nil {
			t.Fatalf("freeze token: %v", err)
		}
	}
	if tokenState == domain.VirtualTokenTerminated {
		if err := virtualToken.Terminate(); err != nil {
			t.Fatalf("terminate token: %v", err)
		}
	}
	if err := h.tokens.Save(ctx, virtualToken); err != nil {
		t.Fatalf("save token: %v", err)
	}

	subscription, err := domain.NewSubscription(
		"sub-1", "user-1", "netflix", "Netflix", subscriptionState,
		domain.WindowMonthly, 1500, "USD", h.clock().Add(-60*24*time.Hour), 3)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	subscription.VirtualTokenID = "vtok-1"
	if err := h.subscriptions.Save(ctx, subscription); err != nil {
		t.Fatalf("save subscription: %v", err)
	}
	return subscription, virtualToken
}

func (h *harness) clock() time.Time {
	return time.Now()
}

func (h *harness) states(t *testing.T) (domain.SubscriptionState, domain.VirtualTokenState) {
	t.Helper()

	ctx := t.Context()
	subscription, err := h.subscriptions.GetByID(ctx, "sub-1")
	if err != nil {
		t.Fatalf("load subscription: %v", err)
	}
	token, err := h.tokens.GetByID(ctx, "vtok-1")
	if err != nil {
		t.Fatalf("load token: %v", err)
	}
	return subscription.State, token.State
}

func TestFreezeSubscriptionFreezesLinkedToken(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)

	if _, err := h.service.FreezeSubscription(t.Context(), "adm-key", "sub-1"); err != nil {
		t.Fatalf("freeze subscription: %v", err)
	}
	subscriptionState, tokenState := h.states(t)
	if subscriptionState != domain.SubscriptionFrozen || tokenState != domain.VirtualTokenFrozen {
		t.Fatalf("expected both Frozen, got subscription=%s token=%s", subscriptionState, tokenState)
	}

	entries := h.audit.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected two audit entries, got %+v", entries)
	}
	if entries[0].Actor != "adm-key" || entries[1].Actor != "adm-key" {
		t.Errorf("expected the actor on both entries, got %+v", entries)
	}
}

func TestFreezeTokenFreezesLinkedSubscription(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)

	if _, err := h.service.FreezeToken(t.Context(), "adm-key", "vtok-1"); err != nil {
		t.Fatalf("freeze token: %v", err)
	}
	subscriptionState, tokenState := h.states(t)
	if subscriptionState != domain.SubscriptionFrozen || tokenState != domain.VirtualTokenFrozen {
		t.Fatalf("expected both Frozen, got subscription=%s token=%s", subscriptionState, tokenState)
	}
}

func TestFreezeSubscriptionWithoutLinkedTokenSucceeds(t *testing.T) {
	h := newHarness(t, time.Now())
	ctx := t.Context()

	subscription, err := domain.NewSubscription("sub-solo", "user-2", "spotify", "Spotify",
		domain.SubscriptionActive, domain.WindowMonthly, 900, "USD", h.clock(), 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := h.subscriptions.Save(ctx, subscription); err != nil {
		t.Fatalf("save: %v", err)
	}

	if _, err := h.service.FreezeSubscription(ctx, "adm-key", "sub-solo"); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	entries := h.audit.Entries()
	if len(entries) != 1 || entries[0].EntityType != entitySubscription {
		t.Fatalf("expected a single subscription audit entry, got %+v", entries)
	}
}

func TestReversibleActionRollsBackWhenCounterpartFails(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenTerminated)

	_, err := h.service.FreezeSubscription(t.Context(), "adm-key", "sub-1")
	if err == nil {
		t.Fatal("expected the failed freeze to be reported")
	}
	if errors.Is(err, ErrSyncDeferred) {
		t.Error("a reversible action must not be deferred")
	}

	subscriptionState, _ := h.states(t)
	if subscriptionState != domain.SubscriptionActive {
		t.Fatalf("expected the subscription to be rolled back to Active, got %s", subscriptionState)
	}
	if pending, err := h.queue.PendingCount(t.Context()); err != nil || pending != 0 {
		t.Fatalf("expected nothing to be queued for a reversible action, got %d (%v)", pending, err)
	}
}

func TestTerminateDefersCounterpartInsteadOfCompensating(t *testing.T) {
	h := newHarness(t, time.Now())
	subscription, _ := h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)
	subscription.VirtualTokenID = "vtok-missing"
	if err := h.subscriptions.Save(t.Context(), subscription); err != nil {
		t.Fatalf("save subscription: %v", err)
	}

	if _, err := h.service.TerminateSubscription(t.Context(), "adm-key", "sub-1"); !errors.Is(err, ErrSyncDeferred) {
		t.Fatalf("expected ErrSyncDeferred, got %v", err)
	}

	subscriptionState, _ := h.states(t)
	if subscriptionState != domain.SubscriptionTerminated {
		t.Fatalf("expected the subscription to stay Terminated, got %s", subscriptionState)
	}

	tasks, err := h.queue.ClaimBatch(t.Context(), 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one deferred task, got %d", len(tasks))
	}
	if tasks[0].EntityType != entityToken || tasks[0].EntityID != "vtok-missing" || tasks[0].Action != string(ActionTerminate) {
		t.Fatalf("unexpected deferred task: %+v", tasks[0])
	}
}

func TestSyncZombieFreezesCard(t *testing.T) {
	h := newHarness(t, time.Now())
	subscription, _ := h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)
	subscription.State = domain.SubscriptionZombie

	if err := h.service.SyncZombie(t.Context(), subscription); err != nil {
		t.Fatalf("sync zombie: %v", err)
	}
	if _, tokenState := h.states(t); tokenState != domain.VirtualTokenFrozen {
		t.Fatalf("expected the zombie card to be Frozen, got %s", tokenState)
	}

	entries := h.audit.Entries()
	if len(entries) != 1 || entries[0].EntityType != entityToken || entries[0].Actor != actorSystem {
		t.Fatalf("expected a system token freeze entry, got %+v", entries)
	}
}

func TestSyncZombieIgnoresNonZombie(t *testing.T) {
	h := newHarness(t, time.Now())
	subscription, _ := h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)

	if err := h.service.SyncZombie(t.Context(), subscription); err != nil {
		t.Fatalf("sync zombie: %v", err)
	}
	if _, tokenState := h.states(t); tokenState != domain.VirtualTokenActive {
		t.Fatalf("expected the card to stay Active, got %s", tokenState)
	}
}

func TestSyncZombieIgnoresMissingCard(t *testing.T) {
	h := newHarness(t, time.Now())
	subscription, _ := h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)
	subscription.State = domain.SubscriptionZombie
	subscription.VirtualTokenID = "vtok-missing"

	if err := h.service.SyncZombie(t.Context(), subscription); err != nil {
		t.Fatalf("a card that no longer exists must not fail the sweep, got %v", err)
	}
	if pending, err := h.queue.PendingCount(t.Context()); err != nil || pending != 0 {
		t.Fatalf("expected nothing to be queued, got %d (%v)", pending, err)
	}
}

func TestSyncZombieKeepsZombieWhenCardIsFrozen(t *testing.T) {
	h := newHarness(t, time.Now())
	subscription, _ := h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenFrozen)
	subscription.State = domain.SubscriptionZombie

	if err := h.service.SyncZombie(t.Context(), subscription); err != nil {
		t.Fatalf("sync zombie: %v", err)
	}
	if subscription.State != domain.SubscriptionZombie {
		t.Errorf("a zombie subscription must stay a zombie, got %s", subscription.State)
	}
}

func TestReplayTaskCompletesDeferredTermination(t *testing.T) {
	h := newHarness(t, time.Now())
	ctx := t.Context()

	created, err := domain.NewSubscription("sub-replay", "user-3", "spotify", "Spotify",
		domain.SubscriptionActive, domain.WindowMonthly, 900, "USD", h.clock(), 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	created.VirtualTokenID = "vtok-2"
	if err := h.subscriptions.Save(ctx, created); err != nil {
		t.Fatalf("save: %v", err)
	}

	// The card is not provisioned yet, so the termination cannot reach it.
	if _, err := h.service.TerminateSubscription(ctx, "adm-key", "sub-replay"); !errors.Is(err, ErrSyncDeferred) {
		t.Fatalf("expected ErrSyncDeferred, got %v", err)
	}
	tasks, err := h.queue.ClaimBatch(ctx, 10)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("expected one task, got %+v (%v)", tasks, err)
	}

	// The card appears before the next replay attempt.
	virtualToken, err := domain.NewVirtualToken("vtok-2", "user-3", "spotify", "411111******5678", 10_000, "USD", h.clock())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := h.tokens.Save(ctx, virtualToken); err != nil {
		t.Fatalf("save token: %v", err)
	}

	if err := h.service.ReplayTask(ctx, tasks[0]); err != nil {
		t.Fatalf("replay: %v", err)
	}

	token, err := h.tokens.GetByID(ctx, "vtok-2")
	if err != nil {
		t.Fatalf("load token: %v", err)
	}
	if token.State != domain.VirtualTokenTerminated {
		t.Fatalf("expected the deferred termination to land, got %s", token.State)
	}
}

func TestReplayTaskFailsWhileEntityIsStillMissing(t *testing.T) {
	h := newHarness(t, time.Now())
	ctx := t.Context()

	created, err := domain.NewSubscription("sub-replay-2", "user-4", "spotify", "Spotify",
		domain.SubscriptionActive, domain.WindowMonthly, 900, "USD", h.clock(), 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	created.VirtualTokenID = "vtok-missing"
	if err := h.subscriptions.Save(ctx, created); err != nil {
		t.Fatalf("save: %v", err)
	}

	if _, err := h.service.TerminateSubscription(ctx, "adm-key", "sub-replay-2"); !errors.Is(err, ErrSyncDeferred) {
		t.Fatalf("expected ErrSyncDeferred, got %v", err)
	}
	tasks, err := h.queue.ClaimBatch(ctx, 10)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("expected one task, got %+v (%v)", tasks, err)
	}
	if err := h.service.ReplayTask(ctx, tasks[0]); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound while the card is missing, got %v", err)
	}
}

func TestReplayTaskIsIdempotent(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenFrozen)

	if err := h.service.ReplayTask(t.Context(), tokenTask("vtok-1", ActionFreeze)); err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if err := h.service.ReplayTask(t.Context(), tokenTask("vtok-1", ActionFreeze)); err != nil {
		t.Fatalf("expected an already satisfied task to succeed, got %v", err)
	}
}

func TestChangeRejectsUnknownAction(t *testing.T) {
	h := newHarness(t, time.Now())
	h.seedPair(t, domain.SubscriptionActive, domain.VirtualTokenActive)

	if _, err := h.service.ChangeSubscription(t.Context(), "adm-key", "sub-1", Action("explode")); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestCompensateGuards(t *testing.T) {
	if _, ok := ActionTerminate.compensate(); ok {
		t.Error("terminate must not be reversible")
	}
	if action, ok := ActionFreeze.compensate(); !ok || action != ActionReactivate {
		t.Errorf("freeze must be compensated by reactivate, got %q (%v)", action, ok)
	}
	if action, ok := ActionReactivate.compensate(); !ok || action != ActionFreeze {
		t.Errorf("reactivate must be compensated by freeze, got %q (%v)", action, ok)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func tokenTask(entityID string, action Action) ports.LifecycleSyncTask {
	return ports.LifecycleSyncTask{EntityType: entityToken, EntityID: entityID, Action: string(action)}
}
