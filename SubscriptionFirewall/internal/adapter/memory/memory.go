package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"subscriptionfirewall/internal/domain"
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

func (r *TransactionRepository) ListByUserSince(_ context.Context, userID domain.UserID, since time.Time) ([]domain.Transaction, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	filtered := make([]domain.Transaction, 0, len(r.transactions))
	for _, transaction := range r.transactions {
		if transaction.UserID == userID && !transaction.AuthorizedAt.Before(since) {
			filtered = append(filtered, transaction)
		}
	}
	sortTransactionsByTime(filtered)
	return filtered, nil
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

func (r *SubscriptionRepository) ListByUser(_ context.Context, userID domain.UserID) ([]*domain.Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	listed := make([]*domain.Subscription, 0, len(r.subscriptions))
	for _, subscription := range r.subscriptions {
		if subscription.UserID == userID {
			listed = append(listed, cloneSubscription(subscription))
		}
	}
	return listed, nil
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
	r.tokens[token.ID] = token
	return nil
}

func (r *VirtualTokenRepository) GetByID(_ context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	token, ok := r.tokens[id]
	if !ok {
		return nil, fmt.Errorf("virtual token %s: %w", id, domain.ErrNotFound)
	}
	return token, nil
}

func (r *VirtualTokenRepository) FindByUserAndMerchant(_ context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, token := range r.tokens {
		if token.UserID == userID && token.MerchantID == merchantID {
			return token, nil
		}
	}
	return nil, fmt.Errorf("virtual token for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
}

func (r *VirtualTokenRepository) ListByUser(_ context.Context, userID domain.UserID) ([]*domain.VirtualToken, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	listed := make([]*domain.VirtualToken, 0, len(r.tokens))
	for _, token := range r.tokens {
		if token.UserID == userID {
			listed = append(listed, token)
		}
	}
	return listed, nil
}

type Clock struct{}

func (Clock) Now() time.Time { return time.Now() }

var _ ports.TransactionRepository = (*TransactionRepository)(nil)
var _ ports.SubscriptionRepository = (*SubscriptionRepository)(nil)
var _ ports.VirtualTokenRepository = (*VirtualTokenRepository)(nil)
var _ ports.Clock = Clock{}
