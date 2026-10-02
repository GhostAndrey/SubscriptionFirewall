package domain

import "time"

type TransactionID string

type UserID string

type MerchantID string

type MCC uint16

const (
	MCCRecurringBilling      MCC = 5968
	MCCDigitalGoodsSubscript MCC = 5817
	MCCInsuranceSubscription MCC = 6300
	MCCStreamingMedia        MCC = 5815
)

func (m MCC) IsSubscriptionProne() bool {
	switch m {
	case MCCRecurringBilling, MCCDigitalGoodsSubscript, MCCInsuranceSubscription, MCCStreamingMedia:
		return true
	default:
		return false
	}
}

type Transaction struct {
	ID           TransactionID
	UserID       UserID
	MerchantID   MerchantID
	MerchantName string
	MCC          MCC
	AmountMinor  int64
	Currency     string
	AuthorizedAt time.Time
}
