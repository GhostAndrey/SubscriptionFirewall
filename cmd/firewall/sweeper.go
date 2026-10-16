package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"subscriptionfirewall/internal/lifecycle"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/subscription"
)

// sweepLockName is the MySQL advisory lock that keeps a single replica running
// the subscription sweep. GET_LOCK is scoped to a connection, so acquire and
// release must both go through the same pinned connection.
const (
	sweepLockName       = "subscription_firewall_sweep"
	sweepReleaseTimeout = 5 * time.Second
)

// subscriptionSweeper marks overdue subscriptions as zombies on a fixed
// interval and waits for the pass in flight on shutdown instead of closing the
// database underneath it.
type subscriptionSweeper struct {
	database      *sql.DB
	subscriptions *subscription.Service
	lifecycle     *lifecycle.Service
	metrics       *obs.Metrics
	logger        *slog.Logger
	interval      time.Duration

	stopped chan struct{}
	done    chan struct{}
	once    sync.Once
	started atomic.Bool
}

func newSubscriptionSweeper(
	database *sql.DB,
	subscriptions *subscription.Service,
	lifecycleService *lifecycle.Service,
	metrics *obs.Metrics,
	logger *slog.Logger,
	interval time.Duration,
) *subscriptionSweeper {
	return &subscriptionSweeper{
		database:      database,
		subscriptions: subscriptions,
		lifecycle:     lifecycleService,
		metrics:       metrics,
		logger:        logger,
		interval:      interval,
		stopped:       make(chan struct{}),
		done:          make(chan struct{}),
	}
}

func (s *subscriptionSweeper) start(ctx context.Context) {
	if !s.started.CompareAndSwap(false, true) {
		return
	}
	go s.loop(ctx)
}

// stop waits for the pass in flight to finish. A sweeper that was never
// started has nothing to wait for.
func (s *subscriptionSweeper) stop() {
	s.once.Do(func() { close(s.stopped) })
	if !s.started.Load() {
		return
	}
	<-s.done
}

func (s *subscriptionSweeper) loop(ctx context.Context) {
	defer close(s.done)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Sweep once on start so a restart does not wait a full interval before
	// the first pass; overdue subscriptions would otherwise stay active.
	s.sweepOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopped:
			return
		case <-ticker.C:
		}
		s.sweepOnce(ctx)
	}
}

func (s *subscriptionSweeper) sweepOnce(ctx context.Context) {
	release := func() {}
	if s.database != nil {
		acquired, ok := s.acquireLock(ctx)
		if !ok {
			return
		}
		release = acquired
	}
	defer release()

	changed, err := s.subscriptions.Sweep(ctx, 0)
	if errors.Is(err, context.Canceled) {
		s.logger.Info("subscription sweep interrupted, completing the transitions already persisted")
	} else if err != nil {
		s.logger.Error("subscription sweep failed", "error", err)
		return
	}
	for _, subscription := range changed {
		s.metrics.CountSubscriptionSwept(string(subscription.State))
		s.logger.Info("subscription marked zombie",
			"subscription_id", string(subscription.ID),
			"user_id", string(subscription.UserID),
			"merchant_id", string(subscription.MerchantID),
		)
		// The card freeze runs on a detached context: the subscription is
		// already persisted as a zombie, so the counterpart must be reached
		// even though the sweep itself was interrupted.
		if err := s.lifecycle.SyncZombie(detach(ctx), subscription); err != nil {
			s.logger.Error("freeze card of zombie subscription failed",
				"subscription_id", string(subscription.ID),
				"virtual_token_id", subscription.VirtualTokenID,
				"error", err,
			)
		}
	}
}

// detach keeps the shutdown deadline off work that must still reach the
// database.
func detach(ctx context.Context) context.Context {
	if ctx.Err() == nil {
		return ctx
	}
	return context.WithoutCancel(ctx)
}

func (s *subscriptionSweeper) acquireLock(ctx context.Context) (func(), bool) {
	connection, err := s.database.Conn(ctx)
	if err != nil {
		s.logger.Error("acquire sweep lock connection failed", "error", err)
		return func() {}, false
	}

	var acquired int
	if err := connection.QueryRowContext(ctx, `SELECT GET_LOCK(?, 0)`, sweepLockName).Scan(&acquired); err != nil {
		s.logger.Error("acquire sweep lock failed", "error", err)
		s.closeLockConn(connection)
		return func() {}, false
	}
	if acquired != 1 {
		s.closeLockConn(connection)
		return func() {}, false
	}

	return func() {
		// The lock must be released even when the parent context is already
		// cancelled, otherwise the pinned connection carries it back into the
		// pool and no replica sweeps until that connection is recycled.
		releaseContext, cancel := context.WithTimeout(detach(ctx), sweepReleaseTimeout)
		defer cancel()

		if _, err := connection.ExecContext(releaseContext, `SELECT RELEASE_LOCK(?)`, sweepLockName); err != nil {
			s.logger.Error("release sweep lock failed", "error", err)
		}
		s.closeLockConn(connection)
	}, true
}

func (s *subscriptionSweeper) closeLockConn(connection *sql.Conn) {
	if err := connection.Close(); err != nil {
		s.logger.Error("return sweep lock connection failed", "error", err)
	}
}
