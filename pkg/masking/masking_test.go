package masking

import "testing"

func TestPANMasksEverythingButPrefixAndSuffix(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"16 digit pan":   {input: "4111111111111234", want: "411111******1234"},
		"spaced pan":     {input: "4111 1111 1111 1234", want: "411111******1234"},
		"short pan":      {input: "1234", want: "****"},
		"empty pan":      {input: "", want: "****"},
		"non digit pan":  {input: "not-a-pan", want: "****"},
		"minimum length": {input: "4111111234", want: "4111111234"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := PAN(test.input); got != test.want {
				t.Errorf("PAN(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestTokenRevealsOnlyEdges(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"long token":  {input: "vtok-abcdef123456", want: "vtok…3456"},
		"short token": {input: "ab", want: "****"},
		"tiny token":  {input: "a", want: "****"},
		"empty token": {input: "", want: "****"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := Token(test.input); got != test.want {
				t.Errorf("Token(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}
