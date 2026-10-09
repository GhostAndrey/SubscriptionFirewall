package issuer

import (
	"context"
	"crypto/rand"
	"fmt"

	"subscriptionfirewall/internal/ports"
)

const (
	defaultMonthlyLimitMinor = 100_000
	maskedPANPrefix          = "411111******"
)

// SimulatedCardIssuer stands in for a real card provider. It is selected by
// SUBSCRIPTION_FIREWALL_ISSUER=simulated and must never be used in production,
// where a provider adapter implementing ports.VirtualCardIssuer takes over.
// The reservation flow in the token service guarantees a single call per user
// and merchant pair, so the provider can treat IdempotencyKey as unique.
type SimulatedCardIssuer struct {
	monthlyLimitMinor int64
}

func NewSimulatedCardIssuer(monthlyLimitMinor int64) *SimulatedCardIssuer {
	if monthlyLimitMinor <= 0 {
		monthlyLimitMinor = defaultMonthlyLimitMinor
	}
	return &SimulatedCardIssuer{monthlyLimitMinor: monthlyLimitMinor}
}

func (i *SimulatedCardIssuer) Issue(_ context.Context, request ports.IssueRequest) (ports.IssuedCard, error) {
	if request.UserID == "" || request.MerchantID == "" {
		return ports.IssuedCard{}, fmt.Errorf("issue request must name a user and a merchant")
	}
	suffix, err := randomDigits(4)
	if err != nil {
		return ports.IssuedCard{}, fmt.Errorf("generate PAN digits: %w", err)
	}
	return ports.IssuedCard{
		MaskedPAN:    maskedPANPrefix + suffix,
		MonthlyLimit: i.monthlyLimitMinor,
		Currency:     "USD",
	}, nil
}

func randomDigits(count int) (string, error) {
	buffer := make([]byte, count)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	digits := make([]byte, count)
	for i, value := range buffer {
		digits[i] = '0' + value%10
	}
	return string(digits), nil
}

var _ ports.VirtualCardIssuer = (*SimulatedCardIssuer)(nil)
