package issuer

import (
	"context"
	"errors"
	"sync"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

// ResilientIssuer decorates a VirtualCardIssuer with per-call timeout,
// bounded retries with backoff and a circuit breaker. When the provider
// misbehaves, the breaker fails fast instead of hanging token provisioning.
type ResilientIssuer struct {
	inner   ports.VirtualCardIssuer
	timeout time.Duration
	retries int
	backoff time.Duration
	breaker circuitBreaker
	nowFunc func() time.Time
}

type ResilientConfig struct {
	// Timeout is the per-attempt deadline; 0 disables it.
	Timeout time.Duration
	// Retries is the number of additional attempts after the first failure.
	Retries int
	// Backoff is the delay before the first retry, doubled per attempt.
	Backoff time.Duration
	// BreakerThreshold is the consecutive failures that open the breaker.
	BreakerThreshold int
	// BreakerCooldown is how long the breaker stays open before a probe.
	BreakerCooldown time.Duration
}

func NewResilientIssuer(inner ports.VirtualCardIssuer, config ResilientConfig) *ResilientIssuer {
	if config.Backoff <= 0 {
		config.Backoff = 100 * time.Millisecond
	}
	if config.BreakerThreshold <= 0 {
		config.BreakerThreshold = 5
	}
	if config.BreakerCooldown <= 0 {
		config.BreakerCooldown = 30 * time.Second
	}
	return &ResilientIssuer{
		inner:   inner,
		timeout: config.Timeout,
		retries: config.Retries,
		backoff: config.Backoff,
		breaker: circuitBreaker{
			threshold: config.BreakerThreshold,
			cooldown:  config.BreakerCooldown,
		},
		nowFunc: time.Now,
	}
}

func (r *ResilientIssuer) Issue(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (ports.IssuedCard, error) {
	if err := r.breaker.acquire(r.nowFunc()); err != nil {
		return ports.IssuedCard{}, err
	}

	var lastErr error
	for attempt := 0; attempt <= r.retries; attempt++ {
		if attempt > 0 {
			if !sleepBackoff(ctx, r.backoff<<uint(attempt-1)) {
				return ports.IssuedCard{}, ctx.Err()
			}
		}

		card, err := r.issueOnce(ctx, userID, merchantID)
		if err == nil {
			r.breaker.success()
			return card, nil
		}
		lastErr = err
		r.breaker.failure(r.nowFunc())
	}
	return ports.IssuedCard{}, lastErr
}

func (r *ResilientIssuer) issueOnce(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (ports.IssuedCard, error) {
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}
	return r.inner.Issue(ctx, userID, merchantID)
}

func sleepBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var errBreakerOpen = errors.New("card issuer unavailable: circuit breaker open")

// circuitBreaker counts consecutive failures; after the threshold it refuses
// calls for the cooldown period, then admits a single probe attempt.
type circuitBreaker struct {
	mu           sync.Mutex
	threshold    int
	cooldown     time.Duration
	failures     int
	openedAt     time.Time
	probePending bool
}

func (b *circuitBreaker) acquire(now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.failures < b.threshold {
		return nil
	}
	if now.Sub(b.openedAt) < b.cooldown {
		return errBreakerOpen
	}
	// Cooldown elapsed: allow exactly one probe; other callers keep failing fast.
	if b.probePending {
		return errBreakerOpen
	}
	b.probePending = true
	return nil
}

func (b *circuitBreaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probePending = false
}

func (b *circuitBreaker) failure(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	b.probePending = false
	if b.failures >= b.threshold {
		b.openedAt = now
	}
}

var _ ports.VirtualCardIssuer = (*ResilientIssuer)(nil)
