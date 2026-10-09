package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/identifier"
	"subscriptionfirewall/internal/ports"
)

type TransactionRepository struct {
	mu           sync.RWMutex
	transactions map[domain.TransactionID]domain.Transaction
}

func NewTransactionRepository() *TransactionRepository {
	return &TransactionRepository{transactions: make(map[domain.TransactionID]domain.Transaction)}
}

func (r *TransactionRepository) Save(_ context.Context, transaction domain.Transaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transactions[transaction.ID] = transaction
	return nil
}

func (r *TransactionRepository) GetByID(_ context.Context, id domain.TransactionID) (*domain.Transaction, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	transaction, ok := r.transactions[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s: %w", id, domain.ErrNotFound)
	}
	return &transaction, nil
}

func (r *TransactionRepository) ListByUserSince(_ context.Context, userID domain.UserID, since time.Time) ([]domain.Transaction, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.transactionsSinceLocked(userID, since, nil), nil
}

func (r *TransactionRepository) ListRecurringMerchantCandidates(
	_ context.Context,
	userID domain.UserID,
	since time.Time,
	minOccurrences int,
) ([]ports.MerchantChargeCount, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	counts := make(map[domain.MerchantID]*ports.MerchantChargeCount)
	for _, transaction := range r.transactions {
		if transaction.UserID != userID || transaction.AuthorizedAt.Before(since) {
			continue
		}
		candidate, seen := counts[transaction.MerchantID]
		if !seen {
			candidate = &ports.MerchantChargeCount{
				MerchantID: transaction.MerchantID,
				FirstSeen:  transaction.AuthorizedAt,
				LastSeen:   transaction.AuthorizedAt,
			}
			counts[transaction.MerchantID] = candidate
		}
		candidate.Occurrences++
		if transaction.AuthorizedAt.Before(candidate.FirstSeen) {
			candidate.FirstSeen = transaction.AuthorizedAt
		}
		if transaction.AuthorizedAt.After(candidate.LastSeen) {
			candidate.LastSeen = transaction.AuthorizedAt
		}
	}

	candidates := make([]ports.MerchantChargeCount, 0, len(counts))
	for _, candidate := range counts {
		if candidate.Occurrences >= minOccurrences {
			candidates = append(candidates, *candidate)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Occurrences != candidates[j].Occurrences {
			return candidates[i].Occurrences > candidates[j].Occurrences
		}
		return candidates[i].MerchantID < candidates[j].MerchantID
	})
	return candidates, nil
}

func (r *TransactionRepository) ListByUserAndMerchants(
	_ context.Context,
	userID domain.UserID,
	merchants []domain.MerchantID,
	since time.Time,
) ([]domain.Transaction, error) {
	// An empty merchant set means "none", matching the SQL adapter. Treating
	// it as "no filter" would silently pull a user's entire history.
	if len(merchants) == 0 {
		return nil, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.transactionsSinceLocked(userID, since, merchants), nil
}

// transactionsSinceLocked filters by user and time. A nil merchant set means
// no merchant filter; callers that must narrow the set reject an empty one
// before reaching here.
func (r *TransactionRepository) transactionsSinceLocked(
	userID domain.UserID,
	since time.Time,
	merchants []domain.MerchantID,
) []domain.Transaction {
	wanted := make(map[domain.MerchantID]struct{}, len(merchants))
	for _, merchantID := range merchants {
		wanted[merchantID] = struct{}{}
	}

	filtered := make([]domain.Transaction, 0, len(r.transactions))
	for _, transaction := range r.transactions {
		if transaction.UserID != userID || transaction.AuthorizedAt.Before(since) {
			continue
		}
		if len(wanted) > 0 {
			if _, ok := wanted[transaction.MerchantID]; !ok {
				continue
			}
		}
		filtered = append(filtered, transaction)
	}
	sortTransactionsByTime(filtered)
	return filtered
}

func sortTransactionsByTime(transactions []domain.Transaction) {
	for i := 1; i < len(transactions); i++ {
		for j := i; j > 0 && transactions[j].AuthorizedAt.Before(transactions[j-1].AuthorizedAt); j-- {
			transactions[j], transactions[j-1] = transactions[j-1], transactions[j]
		}
	}
}

type SubscriptionRepository struct {
	mu            sync.RWMutex
	subscriptions map[domain.SubscriptionID]*domain.Subscription
}

func NewSubscriptionRepository() *SubscriptionRepository {
	return &SubscriptionRepository{subscriptions: make(map[domain.SubscriptionID]*domain.Subscription)}
}

func (r *SubscriptionRepository) Save(_ context.Context, subscription *domain.Subscription) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscriptions[subscription.ID] = cloneSubscription(subscription)
	return nil
}

// CreateIfAbsent reproduces the unique constraint on (user_id, merchant_id)
// that the MySQL schema enforces, so concurrent detections converge on one row.
func (r *SubscriptionRepository) CreateIfAbsent(_ context.Context, subscription *domain.Subscription) (*domain.Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, stored := range r.subscriptions {
		if stored.UserID == subscription.UserID && stored.MerchantID == subscription.MerchantID {
			return cloneSubscription(stored), nil
		}
	}
	r.subscriptions[subscription.ID] = cloneSubscription(subscription)
	return cloneSubscription(subscription), nil
}

// UpdateVersion reproduces the optimistic concurrency guarantee of the MySQL
// adapter so that tests exercising the memory storage observe the same
// conflicts as production.
func (r *SubscriptionRepository) UpdateVersion(_ context.Context, subscription *domain.Subscription) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	stored, ok := r.subscriptions[subscription.ID]
	if !ok {
		return fmt.Errorf("subscription %s: %w", subscription.ID, domain.ErrNotFound)
	}
	if stored.Version != subscription.Version {
		return fmt.Errorf("subscription %s at version %d: %w", subscription.ID, subscription.Version, ports.ErrVersionConflict)
	}
	persisted := cloneSubscription(subscription)
	persisted.Version++
	r.subscriptions[subscription.ID] = persisted
	subscription.Version = persisted.Version
	return nil
}

func (r *SubscriptionRepository) GetByID(_ context.Context, id domain.SubscriptionID) (*domain.Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subscription, ok := r.subscriptions[id]
	if !ok {
		return nil, fmt.Errorf("subscription %s: %w", id, domain.ErrNotFound)
	}
	return cloneSubscription(subscription), nil
}

func (r *SubscriptionRepository) FindByUserAndMerchant(_ context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, subscription := range r.subscriptions {
		if subscription.UserID == userID && subscription.MerchantID == merchantID {
			return cloneSubscription(subscription), nil
		}
	}
	return nil, fmt.Errorf("subscription for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
}

func (r *SubscriptionRepository) ListByUserPage(_ context.Context, userID domain.UserID, page ports.Page) ([]*domain.Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	listed := make([]*domain.Subscription, 0, len(r.subscriptions))
	for _, subscription := range r.subscriptions {
		if subscription.UserID == userID {
			listed = append(listed, cloneSubscription(subscription))
		}
	}
	sortSubscriptionsByID(listed)
	return pageSlice(listed, page), nil
}

// ListPendingZombieTransition mirrors the MySQL adapter: only rows that can
// still flip to zombie are returned, oldest due date first and capped, so a
// sweep pass always makes progress.
func (r *SubscriptionRepository) ListPendingZombieTransition(_ context.Context, now time.Time, limit int) ([]*domain.Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pending := make([]*domain.Subscription, 0, limit)
	for _, subscription := range r.subscriptions {
		if !canBecomeZombie(subscription, now) {
			continue
		}
		pending = append(pending, cloneSubscription(subscription))
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].NextExpectedAt.Equal(pending[j].NextExpectedAt) {
			return pending[i].ID < pending[j].ID
		}
		return pending[i].NextExpectedAt.Before(pending[j].NextExpectedAt)
	})
	if len(pending) > limit {
		pending = pending[:limit]
	}
	return pending, nil
}

func canBecomeZombie(subscription *domain.Subscription, now time.Time) bool {
	switch subscription.State {
	case domain.SubscriptionActive, domain.SubscriptionTrial:
	default:
		return false
	}
	return now.Sub(subscription.NextExpectedAt) > subscription.BillingWindow.Duration()
}

func sortSubscriptionsByID(subscriptions []*domain.Subscription) {
	sort.Slice(subscriptions, func(i, j int) bool {
		return subscriptions[i].ID < subscriptions[j].ID
	})
}

func cloneSubscription(subscription *domain.Subscription) *domain.Subscription {
	cloned := *subscription
	return &cloned
}

type VirtualTokenRepository struct {
	mu     sync.RWMutex
	tokens map[domain.VirtualTokenID]*domain.VirtualToken
}

func NewVirtualTokenRepository() *VirtualTokenRepository {
	return &VirtualTokenRepository{tokens: make(map[domain.VirtualTokenID]*domain.VirtualToken)}
}

func (r *VirtualTokenRepository) Save(_ context.Context, token *domain.VirtualToken) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tokens[token.ID] = cloneToken(token)
	return nil
}

func (r *VirtualTokenRepository) GetByID(_ context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	token, ok := r.tokens[id]
	if !ok {
		return nil, fmt.Errorf("virtual token %s: %w", id, domain.ErrNotFound)
	}
	return cloneToken(token), nil
}

func (r *VirtualTokenRepository) FindByUserAndMerchant(_ context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, token := range r.tokens {
		if token.UserID == userID && token.MerchantID == merchantID {
			return cloneToken(token), nil
		}
	}
	return nil, fmt.Errorf("virtual token for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
}

func (r *VirtualTokenRepository) ListByUserPage(_ context.Context, userID domain.UserID, page ports.Page) ([]*domain.VirtualToken, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	listed := make([]*domain.VirtualToken, 0, len(r.tokens))
	for _, token := range r.tokens {
		if token.UserID == userID {
			listed = append(listed, cloneToken(token))
		}
	}
	sort.Slice(listed, func(i, j int) bool { return listed[i].ID < listed[j].ID })
	return pageSlice(listed, page), nil
}

// pageSlice narrows a sorted listing to one page. Bounds are clamped so a page
// request past the end yields an empty slice rather than panicking.
func pageSlice[T any](items []T, page ports.Page) []T {
	if page.IsUnbounded() {
		return items
	}
	if page.Offset >= len(items) {
		return []T{}
	}
	return items[page.Offset:min(page.Offset+page.Limit, len(items))]
}

func cloneToken(token *domain.VirtualToken) *domain.VirtualToken {
	snapshot := token.Snapshot()
	return &domain.VirtualToken{
		ID:            snapshot.ID,
		UserID:        snapshot.UserID,
		MerchantID:    snapshot.MerchantID,
		MaskedPAN:     snapshot.MaskedPAN,
		MonthlyLimit:  snapshot.MonthlyLimit,
		SpentInPeriod: snapshot.SpentInPeriod,
		Currency:      snapshot.Currency,
		State:         snapshot.State,
		PeriodStarted: snapshot.PeriodStarted,
	}
}

// Charge mirrors the atomicity of the MySQL adapter: the repository lock spans
// the read-modify-write so concurrent charges cannot both pass the limit check.
func (r *VirtualTokenRepository) Charge(
	_ context.Context,
	id domain.VirtualTokenID,
	amountMinor int64,
	currency string,
	now time.Time,
) (*domain.VirtualToken, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stored, ok := r.tokens[id]
	if !ok {
		return nil, fmt.Errorf("virtual token %s: %w", id, domain.ErrNotFound)
	}
	token := cloneToken(stored)
	if err := token.ApplyCharge(amountMinor, currency, now); err != nil {
		return nil, err
	}
	r.tokens[id] = token
	return cloneToken(token), nil
}

func (r *VirtualTokenRepository) ReserveTokenIssuance(
	_ context.Context,
	userID domain.UserID,
	merchantID domain.MerchantID,
	_ string,
	now time.Time,
	staleBefore time.Time,
) (ports.TokenReservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.findLocked(userID, merchantID); ok {
		if existing.State != domain.VirtualTokenIssuing || existing.PeriodStarted.After(staleBefore) {
			return ports.TokenReservation{Reserved: false, TokenID: existing.ID, Existing: cloneToken(existing)}, nil
		}
		delete(r.tokens, existing.ID)
	}

	reserved := domain.NewIssuingVirtualToken(domain.VirtualTokenID(identifier.New("vtok")), userID, merchantID, now)
	r.tokens[reserved.ID] = reserved
	return ports.TokenReservation{Reserved: true, TokenID: reserved.ID}, nil
}

func (r *VirtualTokenRepository) CompleteTokenIssuance(
	_ context.Context,
	tokenID domain.VirtualTokenID,
	_ string,
	card ports.IssuedCard,
	now time.Time,
) (*domain.VirtualToken, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stored, ok := r.tokens[tokenID]
	if !ok {
		return nil, fmt.Errorf("virtual token %s: %w", tokenID, domain.ErrNotFound)
	}
	token := cloneToken(stored)
	if err := token.CompleteIssuance(card.MaskedPAN, card.MonthlyLimit, card.Currency, now); err != nil {
		return nil, err
	}
	r.tokens[tokenID] = token
	return cloneToken(token), nil
}

func (r *VirtualTokenRepository) ReleaseTokenIssuance(_ context.Context, tokenID domain.VirtualTokenID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if stored, ok := r.tokens[tokenID]; ok && stored.State == domain.VirtualTokenIssuing {
		delete(r.tokens, tokenID)
	}
	return nil
}

func (r *VirtualTokenRepository) findLocked(userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, bool) {
	for _, token := range r.tokens {
		if token.UserID == userID && token.MerchantID == merchantID {
			return token, true
		}
	}
	return nil, false
}

type Clock struct{}

func (Clock) Now() time.Time { return time.Now() }

var _ ports.TransactionRepository = (*TransactionRepository)(nil)
var _ ports.SubscriptionRepository = (*SubscriptionRepository)(nil)
var _ ports.VirtualTokenRepository = (*VirtualTokenRepository)(nil)
var _ ports.Clock = Clock{}
