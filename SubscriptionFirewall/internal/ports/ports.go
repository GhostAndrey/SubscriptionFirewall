package ports

import (
	"context"
	"time"

	"subscriptionfirewall/internal/domain"
)

type Clock interface {
	Now() time.Time
}

type TransactionRepository interface {
	Save(ctx context.Context, transaction domain.Transaction) error
	ListByUserSince(ctx context.Context, userID domain.UserID, since time.Time) ([]domain.Transaction, error)
}

type SubscriptionRepository interface {
	Save(ctx context.Context, subscription *domain.Subscription) error
	GetByID(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error)
	FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.Subscription, error)
	ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.Subscription, error)
}

type VirtualTokenRepository interface {
	Save(ctx context.Context, token *domain.VirtualToken) error
	GetByID(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error)
	FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error)
	ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.VirtualToken, error)
}

type VirtualCardIssuer interface {
	Issue(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (IssuedCard, error)
}

type IssuedCard struct {
	TokenID      domain.VirtualTokenID
	MaskedPAN    string
	MonthlyLimit int64
	Currency     string
}
