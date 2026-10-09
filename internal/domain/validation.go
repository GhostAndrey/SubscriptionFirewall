package domain

import (
	"fmt"
	"strings"
)

const currencyCodeLength = 3

// IsISOCurrency reports whether value is an ISO 4217 shaped alphabetic code:
// exactly three uppercase letters.
func IsISOCurrency(value string) bool {
	if len(value) != currencyCodeLength {
		return false
	}
	for _, letter := range value {
		if letter < 'A' || letter > 'Z' {
			return false
		}
	}
	return true
}

type textRule struct {
	field    string
	value    string
	maxBytes int
	required bool
}

func (r textRule) validate() error {
	if strings.TrimSpace(r.value) == "" {
		if r.required {
			return fmt.Errorf("%s is required: %w", r.field, ErrInvalidArgument)
		}
		return nil
	}
	if len(r.value) > r.maxBytes {
		return fmt.Errorf("%s must be at most %d bytes, got %d: %w", r.field, r.maxBytes, len(r.value), ErrInvalidArgument)
	}
	return nil
}

func validateText(rules ...textRule) error {
	for _, rule := range rules {
		if err := rule.validate(); err != nil {
			return err
		}
	}
	return nil
}
