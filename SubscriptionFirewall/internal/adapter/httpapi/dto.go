package httpapi

import (
	"time"

	"subscriptionfirewall/internal/domain"
)

type subscriptionView struct {
	ID             string `json:"id"`
	UserID         string `json:"user_id"`
	MerchantID     string `json:"merchant_id"`
	MerchantName   string `json:"merchant_name"`
	VirtualTokenID string `json:"virtual_token_id"`
	State          string `json:"state"`
	BillingWindow  string `json:"billing_window"`
	AverageAmount  int64  `json:"average_amount_minor"`
	Currency       string `json:"currency"`
	LastChargedAt  string `json:"last_charged_at"`
	NextExpectedAt string `json:"next_expected_at"`
}

func toSubscriptionView(subscription *domain.Subscription) subscriptionView {
	return subscriptionView{
		ID:             string(subscription.ID),
		UserID:         string(subscription.UserID),
		MerchantID:     string(subscription.MerchantID),
		MerchantName:   subscription.MerchantName,
		VirtualTokenID: subscription.VirtualTokenID,
		State:          string(subscription.State),
		BillingWindow:  string(subscription.BillingWindow),
		AverageAmount:  subscription.AverageAmount,
		Currency:       subscription.Currency,
		LastChargedAt:  subscription.LastChargedAt.Format(time.RFC3339),
		NextExpectedAt: subscription.NextExpectedAt.Format(time.RFC3339),
	}
}

type tokenView struct {
	ID            string `json:"id"`
	UserID        string `json:"user_id"`
	MerchantID    string `json:"merchant_id"`
	MaskedPAN     string `json:"masked_pan"`
	MonthlyLimit  int64  `json:"monthly_limit_minor"`
	SpentInPeriod int64  `json:"spent_in_period_minor"`
	Currency      string `json:"currency"`
	State         string `json:"state"`
}

func toTokenView(token *domain.VirtualToken) tokenView {
	snapshot := token.Snapshot()
	return tokenView{
		ID:            string(snapshot.ID),
		UserID:        string(snapshot.UserID),
		MerchantID:    string(snapshot.MerchantID),
		MaskedPAN:     snapshot.MaskedPAN,
		MonthlyLimit:  snapshot.MonthlyLimit,
		SpentInPeriod: snapshot.SpentInPeriod,
		Currency:      snapshot.Currency,
		State:         string(snapshot.State),
	}
}
