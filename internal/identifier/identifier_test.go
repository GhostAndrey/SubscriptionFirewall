package identifier

import (
	"strings"
	"testing"
)

func TestNewKeepsPrefixAndSuffix(t *testing.T) {
	for _, prefix := range []string{"sub", "vtok"} {
		t.Run(prefix, func(t *testing.T) {
			id := New(prefix)
			if !strings.HasPrefix(id, prefix+"-") {
				t.Fatalf("id %q does not start with the prefix", id)
			}
			if len(id) != len(prefix)+1+16 {
				t.Fatalf("unexpected id %q", id)
			}
		})
	}
}

func TestNewDoesNotRepeat(t *testing.T) {
	const samples = 1000
	seen := make(map[string]struct{}, samples)
	for range samples {
		id := New("sub")
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = struct{}{}
	}
}
