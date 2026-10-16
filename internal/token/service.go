package token

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

const (
	defaultIssuanceTimeout = 5 * time.Minute
	issuanceKeyPrefix      = "issue"
	updateAttempts         = 3
)

type Service struct {
	tokens          ports.VirtualTokenRepository
	issuer          ports.VirtualCardIssuer
	clock           ports.Clock
	logger          *slog.Logger
	issuanceTimeout time.Duration
	issueLocks      keyedMutex
}

type Options struct {
	// IssuanceTimeout is how long an unfinished card reservation may block
	// other attempts before it is considered abandoned. Zero selects the
	// default.
	IssuanceTimeout time.Duration
	Logger          *slog.Logger
}

func NewService(tokens ports.VirtualTokenRepository, issuer ports.VirtualCardIssuer, clock ports.Clock, options Options) *Service {
	issuanceTimeout := options.IssuanceTimeout
	if issuanceTimeout <= 0 {
		issuanceTimeout = defaultIssuanceTimeout
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		tokens:          tokens,
		issuer:          issuer,
		clock:           clock,
		logger:          logger,
		issuanceTimeout: issuanceTimeout,
	}
}

type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// lock serialises callers that share a key and releases the entry as soon as
// the last one leaves, so the map stays bounded by concurrent users.
func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedLock)
	}
	entry, ok := k.locks[key]
	if !ok {
		entry = &keyedLock{}
		k.locks[key] = entry
	}
	entry.refs++
	k.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()

		k.mu.Lock()
		defer k.mu.Unlock()
		entry.refs--
		if entry.refs == 0 {
			delete(k.locks, key)
		}
	}
}

func (s *Service) Get(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	token, err := s.tokens.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get virtual token %s: %w", id, err)
	}
	return token, nil
}

func (s *Service) ListByUser(ctx context.Context, userID domain.UserID, page ports.Page) ([]*domain.VirtualToken, error) {
	tokens, err := s.tokens.ListByUserPage(ctx, userID, page)
	if err != nil {
		return nil, fmt.Errorf("list virtual tokens for user %s: %w", userID, err)
	}
	return tokens, nil
}

// EnsureToken returns the virtual card of the user and merchant pair, issuing
// it on first use. Issuance is coordinated through a persisted reservation so
// that concurrent attempts across replicas produce exactly one card at the
// provider.
func (s *Service) EnsureToken(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error) {
	unlock := s.issueLocks.lock(string(userID) + "|" + string(merchantID))
	defer unlock()

	now := s.clock.Now()
	reservation, err := s.tokens.ReserveTokenIssuance(
		ctx, userID, merchantID,
		issuanceKey(userID, merchantID),
		now, now.Add(-s.issuanceTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("reserve virtual card for merchant %s: %w", merchantID, err)
	}
	if !reservation.Reserved {
		return resolvedReservation(reservation)
	}

	return s.issueReserved(ctx, reservation.TokenID, userID, merchantID)
}

func resolvedReservation(reservation ports.TokenReservation) (*domain.VirtualToken, error) {
	switch {
	case reservation.Existing == nil:
		return nil, fmt.Errorf("virtual card reservation for %s returned no token: %w", reservation.TokenID, domain.ErrNotFound)
	case reservation.Existing.State == domain.VirtualTokenIssuing:
		return nil, fmt.Errorf("%w: token %s", ports.ErrIssuanceInProgress, reservation.TokenID)
	default:
		return reservation.Existing, nil
	}
}

func (s *Service) issueReserved(
	ctx context.Context,
	tokenID domain.VirtualTokenID,
	userID domain.UserID,
	merchantID domain.MerchantID,
) (*domain.VirtualToken, error) {
	card, err := s.issuer.Issue(ctx, ports.IssueRequest{
		UserID:         userID,
		MerchantID:     merchantID,
		IdempotencyKey: issuanceKey(userID, merchantID),
	})
	if err != nil {
		s.releaseReservation(ctx, tokenID)
		return nil, fmt.Errorf("issue virtual card for merchant %s: %w", merchantID, err)
	}

	token, err := s.tokens.CompleteTokenIssuance(ctx, tokenID, issuanceKey(userID, merchantID), card, s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("complete virtual card %s for merchant %s: %w", tokenID, merchantID, err)
	}
	return token, nil
}

func (s *Service) releaseReservation(ctx context.Context, tokenID domain.VirtualTokenID) {
	if err := s.tokens.ReleaseTokenIssuance(ctx, tokenID); err != nil {
		s.logger.Error("release virtual card reservation failed",
			"virtual_token_id", string(tokenID),
			"error", err,
		)
	}
}

func (s *Service) Freeze(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	return s.mutate(ctx, id, (*domain.VirtualToken).Freeze)
}

func (s *Service) Reactivate(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	return s.mutate(ctx, id, (*domain.VirtualToken).Reactivate)
}

func (s *Service) Terminate(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	return s.mutate(ctx, id, (*domain.VirtualToken).Terminate)
}

func (s *Service) Authorize(ctx context.Context, id domain.VirtualTokenID, amountMinor int64, currency string) (*domain.VirtualToken, error) {
	token, err := s.tokens.Charge(ctx, id, amountMinor, currency, s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("charge virtual token %s: %w", id, err)
	}
	return token, nil
}

// mutate loads a token, applies the mutation and persists it under an
// optimistic version check. A charge that lands between the read and the write
// bumps the version, so the transition is retried against fresh data instead of
// overwriting the debited budget.
func (s *Service) mutate(ctx context.Context, id domain.VirtualTokenID, mutation func(*domain.VirtualToken) error) (*domain.VirtualToken, error) {
	var lastErr error
	for range updateAttempts {
		token, err := s.tokens.GetByID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load virtual token %s: %w", id, err)
		}
		if err := mutation(token); err != nil {
			return nil, err
		}
		if err := s.tokens.UpdateVersion(ctx, token); err != nil {
			if errors.Is(err, ports.ErrVersionConflict) {
				lastErr = err
				continue
			}
			return nil, fmt.Errorf("persist virtual token %s: %w", id, err)
		}
		return token, nil
	}
	return nil, fmt.Errorf("persist virtual token %s after %d attempts: %w", id, updateAttempts, lastErr)
}

func issuanceKey(userID domain.UserID, merchantID domain.MerchantID) string {
	return issuanceKeyPrefix + ":" + string(userID) + ":" + string(merchantID)
}
