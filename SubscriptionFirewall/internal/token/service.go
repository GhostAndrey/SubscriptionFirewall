package token

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

type Service struct {
	tokens      ports.VirtualTokenRepository
	issuer      ports.VirtualCardIssuer
	clock       ports.Clock
	provisionMu sync.Mutex
}

func NewService(tokens ports.VirtualTokenRepository, issuer ports.VirtualCardIssuer, clock ports.Clock) *Service {
	return &Service{tokens: tokens, issuer: issuer, clock: clock}
}

func (s *Service) Get(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	token, err := s.tokens.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get virtual token %s: %w", id, err)
	}
	return token, nil
}

func (s *Service) ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.VirtualToken, error) {
	tokens, err := s.tokens.ListByUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("list virtual tokens for user %s: %w", userID, err)
	}
	return tokens, nil
}

func (s *Service) EnsureToken(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error) {
	s.provisionMu.Lock()
	defer s.provisionMu.Unlock()

	existing, err := s.tokens.FindByUserAndMerchant(ctx, userID, merchantID)
	switch {
	case err == nil:
		return existing, nil
	case errors.Is(err, domain.ErrNotFound):
		return s.issue(ctx, userID, merchantID)
	default:
		return nil, fmt.Errorf("find virtual token for merchant %s: %w", merchantID, err)
	}
}

func (s *Service) issue(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error) {
	issued, err := s.issuer.Issue(ctx, userID, merchantID)
	if err != nil {
		return nil, fmt.Errorf("issue virtual card for merchant %s: %w", merchantID, err)
	}
	token, err := domain.NewVirtualToken(
		issued.TokenID,
		userID,
		merchantID,
		issued.MaskedPAN,
		issued.MonthlyLimit,
		issued.Currency,
		s.clock.Now(),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize virtual token for merchant %s: %w", merchantID, err)
	}
	if err := s.tokens.Save(ctx, token); err != nil {
		return nil, fmt.Errorf("persist virtual token %s: %w", token.ID, err)
	}
	return token, nil
}

func (s *Service) Freeze(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	token, err := s.tokens.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load virtual token %s: %w", id, err)
	}
	if err := token.Freeze(); err != nil {
		return nil, err
	}
	if err := s.tokens.Save(ctx, token); err != nil {
		return nil, fmt.Errorf("persist virtual token %s: %w", id, err)
	}
	return token, nil
}

func (s *Service) Reactivate(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	token, err := s.tokens.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load virtual token %s: %w", id, err)
	}
	if err := token.Reactivate(); err != nil {
		return nil, err
	}
	if err := s.tokens.Save(ctx, token); err != nil {
		return nil, fmt.Errorf("persist virtual token %s: %w", id, err)
	}
	return token, nil
}

func (s *Service) Terminate(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	token, err := s.tokens.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load virtual token %s: %w", id, err)
	}
	if err := token.Terminate(); err != nil {
		return nil, err
	}
	if err := s.tokens.Save(ctx, token); err != nil {
		return nil, fmt.Errorf("persist virtual token %s: %w", id, err)
	}
	return token, nil
}

func (s *Service) Authorize(ctx context.Context, id domain.VirtualTokenID, amountMinor int64, currency string) (*domain.VirtualToken, error) {
	token, err := s.tokens.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load virtual token %s: %w", id, err)
	}
	if err := token.AuthorizeCharge(amountMinor, currency, s.clock.Now()); err != nil {
		return nil, err
	}
	if err := s.tokens.Save(ctx, token); err != nil {
		return nil, fmt.Errorf("persist virtual token %s: %w", id, err)
	}
	return token, nil
}
