package ports

import (
	"context"
	"errors"
	"time"

	"subscriptionfirewall/internal/domain"
)

var (
	ErrOutboxClosed = errors.New("detection outbox is closed")
	// ErrVersionConflict reports that a record was modified concurrently and
	// the caller must re-read it before retrying.
	ErrVersionConflict = errors.New("record was modified concurrently")
	// ErrIssuanceInProgress reports that another replica currently holds the
	// card issuance reservation for the same user and merchant. The caller
	// should retry rather than treat it as a failure.
	ErrIssuanceInProgress = errors.New("virtual card issuance already in progress")
)

type Clock interface {
	Now() time.Time
}

type TransactionRepository interface {
	Save(ctx context.Context, transaction domain.Transaction) error
	GetByID(ctx context.Context, id domain.TransactionID) (*domain.Transaction, error)
	ListByUserSince(ctx context.Context, userID domain.UserID, since time.Time) ([]domain.Transaction, error)
	// ListRecurringMerchantCandidates returns the merchants a user charged at
	// least minOccurrences times since the given instant, together with that
	// count. The detector uses it to skip merchants that cannot produce a
	// verdict before pulling their transaction history into memory.
	ListRecurringMerchantCandidates(ctx context.Context, userID domain.UserID, since time.Time, minOccurrences int) ([]MerchantChargeCount, error)
	// ListByUserAndMerchants returns the transactions of a user for the given
	// merchants since an instant.
	ListByUserAndMerchants(ctx context.Context, userID domain.UserID, merchants []domain.MerchantID, since time.Time) ([]domain.Transaction, error)
}

type MerchantChargeCount struct {
	MerchantID  domain.MerchantID
	Occurrences int
	FirstSeen   time.Time
	LastSeen    time.Time
}

type SubscriptionRepository interface {
	// Save inserts a new subscription or replaces an existing row addressed by
	// id. It is intended for seeding and tests; concurrent use-case updates go
	// through UpdateVersion.
	Save(ctx context.Context, subscription *domain.Subscription) error
	// UpdateVersion persists a subscription only if nobody else has changed it
	// since it was read, and bumps Version on success. It returns
	// ErrVersionConflict when the caller must re-read and retry.
	UpdateVersion(ctx context.Context, subscription *domain.Subscription) error
	// CreateIfAbsent stores the subscription only when the user and merchant
	// pair has none, and always returns the canonical row. Concurrent
	// detections of the same pair therefore converge on a single subscription.
	CreateIfAbsent(ctx context.Context, subscription *domain.Subscription) (*domain.Subscription, error)
	GetByID(ctx context.Context, id domain.SubscriptionID) (*domain.Subscription, error)
	FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.Subscription, error)
	ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.Subscription, error)
	// ListPendingZombieTransition returns subscriptions that are still
	// active or trial and are already past their billing window, oldest due
	// date first, capped at limit. The set is exactly the rows the sweep can
	// still act on, so a sweep pass always terminates and its cost scales with
	// the backlog rather than with the total subscription count.
	ListPendingZombieTransition(ctx context.Context, now time.Time, limit int) ([]*domain.Subscription, error)
}

type VirtualTokenRepository interface {
	Save(ctx context.Context, token *domain.VirtualToken) error
	GetByID(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error)
	FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error)
	ListByUser(ctx context.Context, userID domain.UserID) ([]*domain.VirtualToken, error)
	// Charge debits amountMinor from the token monthly budget as a single
	// atomic step. Implementations must serialize concurrent charges on the
	// same token, so that the sum of approved charges never exceeds the limit
	// regardless of how many service replicas are running.
	Charge(ctx context.Context, id domain.VirtualTokenID, amountMinor int64, currency string, now time.Time) (*domain.VirtualToken, error)
	// ReserveTokenIssuance atomically claims the right to provision a card for
	// the user and merchant pair. Reservations older than staleBefore are
	// treated as abandoned and may be taken over, so a crashed replica cannot
	// block issuance forever.
	ReserveTokenIssuance(
		ctx context.Context,
		userID domain.UserID,
		merchantID domain.MerchantID,
		issuanceKey string,
		now time.Time,
		staleBefore time.Time,
	) (TokenReservation, error)
	// CompleteTokenIssuance promotes a reservation to an active card. It fails
	// with ErrIssuanceLost when the reservation is no longer held.
	CompleteTokenIssuance(
		ctx context.Context,
		tokenID domain.VirtualTokenID,
		issuanceKey string,
		card IssuedCard,
		now time.Time,
	) (*domain.VirtualToken, error)
	// ReleaseTokenIssuance drops an unfinished reservation so that a later
	// attempt can provision the card.
	ReleaseTokenIssuance(ctx context.Context, tokenID domain.VirtualTokenID) error
}

type TokenReservation struct {
	// Reserved is true for the caller that must now call the issuer.
	Reserved bool
	TokenID  domain.VirtualTokenID
	// Existing carries the already provisioned token when Reserved is false.
	Existing *domain.VirtualToken
}

type IssueRequest struct {
	UserID     domain.UserID
	MerchantID domain.MerchantID
	// IdempotencyKey must be stable for a given user and merchant pair so the
	// provider can deduplicate retries of the same logical issuance.
	IdempotencyKey string
}

type IssuedCard struct {
	MaskedPAN    string
	MonthlyLimit int64
	Currency     string
}

type VirtualCardIssuer interface {
	Issue(ctx context.Context, request IssueRequest) (IssuedCard, error)
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

// LifecycleSyncTask is a deferred action that could not be applied to the
// entity linked to the one the caller changed. It exists for irreversible
// transitions, where compensating the first change is impossible.
type LifecycleSyncTask struct {
	ID         int64
	EntityType string
	EntityID   string
	Action     string
	Attempts   int
}

// LifecycleSyncQueue guarantees that a lifecycle action eventually reaches the
// counterpart entity, even across process crashes and replica restarts.
type LifecycleSyncQueue interface {
	Enqueue(ctx context.Context, task LifecycleSyncTask) error
	ClaimBatch(ctx context.Context, limit int) ([]LifecycleSyncTask, error)
	Complete(ctx context.Context, ids []int64) error
	Fail(ctx context.Context, task LifecycleSyncTask) error
	RequeueStale(ctx context.Context, olderThan time.Duration) (int64, error)
	PendingCount(ctx context.Context) (int64, error)
}
