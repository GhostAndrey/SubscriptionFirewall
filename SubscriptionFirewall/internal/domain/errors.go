package domain

import "errors"

var (
	ErrNotFound           = errors.New("entity not found")
	ErrAlreadyExists      = errors.New("entity already exists")
	ErrInvalidTransition  = errors.New("invalid lifecycle transition")
	ErrTokenNotActive     = errors.New("virtual token is not active")
	ErrSpendLimitExceeded = errors.New("virtual token spend limit exceeded")
	ErrUnknownCurrency    = errors.New("currency mismatch")
)
