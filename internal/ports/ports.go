package ports

import (
	"context"
	"errors"
	"time"

	"subscriptionfirewall/internal/domain"
)

var ErrOutboxClosed = errors.New("detection outbox is closed")

type Clock interface {
	Now() time.Time
}

type TransactionRepository interface {
	Save(ctx context.Context, transaction domain.Transaction) error
	GetByID(ctx context.Context, id domain.TransactionID) (*domain.Transaction, error)
	ListByUserSince(ctx context.Context, userID domain.UserID, since time.Time) ([]domain.Transaction, error)
}

type SubscriptionRepository interface {
	Save(ctx context.Context, subscription *domain.Subscription) error
	GetByID(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error)
	FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.Subscription, error)
	ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.Subscription, error)
	ListAll(ctx context.Context) ([]*domain.Subscription, error)
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

type OutboxItem struct {
	ID     int64
	UserID domain.UserID

	Attempts int
}

type DetectionOutbox interface {
	EnqueueWithTransaction(ctx context.Context, transaction domain.Transaction) error
	ClaimBatch(ctx context.Context, limit int) ([]OutboxItem, error)
	Complete(ctx context.Context, ids []int64) error
	Fail(ctx context.Context, item OutboxItem) error

	RequeueStale(ctx context.Context, olderThan time.Duration) (int64, error)
	PendingCount(ctx context.Context) (int64, error)
}

type AuditEntry struct {
	Actor      string
	Action     string
	EntityType string
	EntityID   string
}

type AuditLog interface {
	Record(ctx context.Context, entry AuditEntry) error
}
