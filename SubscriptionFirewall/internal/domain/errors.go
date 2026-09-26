package domain

import "errors"

var (
	ErrNotFound               = errors.New("entity not found")
	ErrInvalidTransition      = errors.New("invalid lifecycle transition")
	ErrTokenNotActive         = errors.New("virtual token is not active")
	ErrSpendLimitExceeded     = errors.New("virtual token spend limit exceeded")
	ErrUnknownCurrency        = errors.New("currency mismatch")
	ErrMissingMerchantBinding = errors.New("virtual token is not bound to a merchant")
)
