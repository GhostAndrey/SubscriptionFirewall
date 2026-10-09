package detector

import (
	"context"
	"fmt"
	"sort"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

// subscriptionOccurrences is the payment count that already makes a
// subscription-prone category look recurring.
const subscriptionOccurrences = 2

type Config struct {
	AnalysisHorizon    time.Duration
	MinimumOccurrences int
	AmountTolerance    float64
	TrialDuration      time.Duration
}

func DefaultConfig() Config {
	return Config{
		AnalysisHorizon:    365 * 24 * time.Hour,
		MinimumOccurrences: 3,
		AmountTolerance:    0.05,
		TrialDuration:      14 * 24 * time.Hour,
	}
}

type Detector struct {
	transactions ports.TransactionRepository
	clock        ports.Clock
	config       Config
}

func New(transactions ports.TransactionRepository, clock ports.Clock, config Config) *Detector {
	return &Detector{transactions: transactions, clock: clock, config: config}
}

// Detect finds recurring subscriptions for a user. Candidates are narrowed in
// the storage layer first, so merchants with too few charges never have their
// history loaded into memory.
func (d *Detector) Detect(ctx context.Context, userID domain.UserID) ([]domain.DetectedSubscription, error) {
	horizonStart := d.clock.Now().Add(-d.config.AnalysisHorizon)

	candidates, err := d.transactions.ListRecurringMerchantCandidates(
		ctx, userID, horizonStart, d.minimumRequiredOccurrences())
	if err != nil {
		return nil, fmt.Errorf("list merchant candidates for user %s: %w", userID, err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	merchants := make([]domain.MerchantID, 0, len(candidates))
	for _, candidate := range candidates {
		merchants = append(merchants, candidate.MerchantID)
	}

	transactions, err := d.transactions.ListByUserAndMerchants(ctx, userID, merchants, horizonStart)
	if err != nil {
		return nil, fmt.Errorf("list candidate transactions for user %s: %w", userID, err)
	}

	byMerchant := groupByMerchant(transactions)
	detections := make([]domain.DetectedSubscription, 0, len(byMerchant))
	for merchantID, merchantTransactions := range byMerchant {
		if detection, recurring := d.detectRecurring(userID, merchantID, merchantTransactions); recurring {
			detections = append(detections, detection)
		}
	}
	sort.Slice(detections, func(i, j int) bool {
		return detections[i].LastChargedAt.After(detections[j].LastChargedAt)
	})
	return detections, nil
}

// minimumRequiredOccurrences is the lowest payment count a merchant must reach
// to be looked at at all. Subscription-prone categories need fewer payments
// than the general threshold, so the exact rule is still applied per merchant
// after the pre-filter.
func (d *Detector) minimumRequiredOccurrences() int {
	if d.config.MinimumOccurrences < subscriptionOccurrences {
		return d.config.MinimumOccurrences
	}
	return subscriptionOccurrences
}

func (d *Detector) detectRecurring(userID domain.UserID, merchantID domain.MerchantID, merchantTransactions []domain.Transaction) (domain.DetectedSubscription, bool) {
	if len(merchantTransactions) < d.requiredOccurrences(merchantTransactions[0].MCC) {
		return domain.DetectedSubscription{}, false
	}
	if !sharesSingleCurrency(merchantTransactions) {
		return domain.DetectedSubscription{}, false
	}

	medianInterval, ok := medianInterval(merchantTransactions)
	if !ok {
		return domain.DetectedSubscription{}, false
	}
	window, ok := windowFromMedianInterval(medianInterval)
	if !ok {
		return domain.DetectedSubscription{}, false
	}
	if !amountsAreStable(merchantTransactions, d.config.AmountTolerance) {
		return domain.DetectedSubscription{}, false
	}

	last := merchantTransactions[len(merchantTransactions)-1]
	first := merchantTransactions[0]
	now := d.clock.Now()

	return domain.DetectedSubscription{
		UserID:           userID,
		MerchantID:       merchantID,
		MerchantName:     last.MerchantName,
		MCC:              last.MCC,
		State:            d.classify(first.AuthorizedAt, last.AuthorizedAt, last.MCC, window, now),
		Window:           window,
		AverageAmount:    averageAmount(merchantTransactions),
		Currency:         last.Currency,
		LastChargedAt:    last.AuthorizedAt,
		ObservedPayments: len(merchantTransactions),
	}, true
}

func (d *Detector) classify(firstChargedAt, lastChargedAt time.Time, mcc domain.MCC, window domain.BillingWindow, now time.Time) domain.SubscriptionState {
	switch {
	case mcc.IsSubscriptionProne() && now.Sub(firstChargedAt) <= d.config.TrialDuration:
		return domain.SubscriptionTrial
	case now.Sub(lastChargedAt) > 2*window.Duration():
		return domain.SubscriptionZombie
	default:
		return domain.SubscriptionActive
	}
}

func (d *Detector) requiredOccurrences(mcc domain.MCC) int {
	if mcc.IsSubscriptionProne() {
		return subscriptionOccurrences
	}
	return d.config.MinimumOccurrences
}

func groupByMerchant(transactions []domain.Transaction) map[domain.MerchantID][]domain.Transaction {
	grouped := make(map[domain.MerchantID][]domain.Transaction)
	for _, transaction := range transactions {
		grouped[transaction.MerchantID] = append(grouped[transaction.MerchantID], transaction)
	}
	return grouped
}

func medianInterval(transactions []domain.Transaction) (time.Duration, bool) {
	if len(transactions) < 2 {
		return 0, false
	}
	intervals := make([]time.Duration, 0, len(transactions)-1)
	for i := 1; i < len(transactions); i++ {
		intervals = append(intervals, transactions[i].AuthorizedAt.Sub(transactions[i-1].AuthorizedAt))
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })
	return intervals[len(intervals)/2], true
}

func windowFromMedianInterval(interval time.Duration) (domain.BillingWindow, bool) {
	switch {
	case interval <= 0:
		return "", false
	case interval <= 8*24*time.Hour:
		return domain.WindowWeekly, true
	case interval <= 32*24*time.Hour:
		return domain.WindowMonthly, true
	case interval <= 370*24*time.Hour:
		return domain.WindowYearly, true
	default:
		return "", false
	}
}

func amountsAreStable(transactions []domain.Transaction, tolerance float64) bool {
	largest := transactions[0].AmountMinor
	smallest := transactions[0].AmountMinor
	for _, transaction := range transactions[1:] {
		largest = max(largest, transaction.AmountMinor)
		smallest = min(smallest, transaction.AmountMinor)
	}
	if largest <= 0 {
		return false
	}
	return float64(largest-smallest) <= tolerance*float64(largest)
}

func averageAmount(transactions []domain.Transaction) int64 {
	var total int64
	for _, transaction := range transactions {
		total += transaction.AmountMinor
	}
	return total / int64(len(transactions))
}

func sharesSingleCurrency(transactions []domain.Transaction) bool {
	for _, transaction := range transactions[1:] {
		if transaction.Currency != transactions[0].Currency {
			return false
		}
	}
	return true
}
