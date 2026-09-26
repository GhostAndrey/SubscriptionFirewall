package issuer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
)

const defaultMonthlyLimitMinor = 100_000

type SimulatedCardIssuer struct {
	monthlyLimitMinor int64
}

func NewSimulatedCardIssuer(monthlyLimitMinor int64) *SimulatedCardIssuer {
	if monthlyLimitMinor <= 0 {
		monthlyLimitMinor = defaultMonthlyLimitMinor
	}
	return &SimulatedCardIssuer{monthlyLimitMinor: monthlyLimitMinor}
}

func (i *SimulatedCardIssuer) Issue(_ context.Context, userID domain.UserID, merchantID domain.MerchantID) (ports.IssuedCard, error) {
	panDigits, err := randomHex(8)
	if err != nil {
		return ports.IssuedCard{}, fmt.Errorf("generate PAN digits: %w", err)
	}
	tokenID, err := randomHex(8)
	if err != nil {
		return ports.IssuedCard{}, fmt.Errorf("generate token id: %w", err)
	}
	return ports.IssuedCard{
		TokenID:      domain.VirtualTokenID("vtok-" + tokenID),
		MaskedPAN:    "411111******" + panDigits[len(panDigits)-4:],
		MonthlyLimit: i.monthlyLimitMinor,
		Currency:     "USD",
	}, nil
}

func randomHex(byteLength int) (string, error) {
	buffer := make([]byte, byteLength)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

var _ ports.VirtualCardIssuer = (*SimulatedCardIssuer)(nil)
