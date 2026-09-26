package masking

import "strings"

const (
	unmaskedPANPrefixLength = 6
	unmaskedPANSuffixLength = 4
	minimumPANLength        = unmaskedPANPrefixLength + unmaskedPANSuffixLength
	unmaskedTokenChunk      = 4
	minimumTokenLength      = 2 * unmaskedTokenChunk
)

func PAN(pan string) string {
	digits := strings.Map(keepDigit, pan)
	if len(digits) < minimumPANLength {
		return "****"
	}
	return digits[:unmaskedPANPrefixLength] +
		strings.Repeat("*", len(digits)-minimumPANLength) +
		digits[len(digits)-unmaskedPANSuffixLength:]
}

func Token(token string) string {
	if len(token) < minimumTokenLength {
		return "****"
	}
	return token[:unmaskedTokenChunk] + "…" + token[len(token)-unmaskedTokenChunk:]
}

func keepDigit(r rune) rune {
	if r >= '0' && r <= '9' {
		return r
	}
	return -1
}
