package identifier

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

const randomBytes = 8

// New returns a prefixed identifier such as "sub-1a2b3c4d5e6f7a8b". The
// random suffix keeps generated identifiers unguessable; the timestamp
// fallback preserves uniqueness if the system entropy source fails.
func New(prefix string) string {
	buffer := make([]byte, randomBytes)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buffer)
}
