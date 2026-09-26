package domain

import "time"

type DetectedSubscription struct {
	UserID           UserID
	MerchantID       MerchantID
	MerchantName     string
	MCC              MCC
	State            SubscriptionState
	Window           BillingWindow
	AverageAmount    int64
	Currency         string
	LastChargedAt    time.Time
	ObservedPayments int
}
