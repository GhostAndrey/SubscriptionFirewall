package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/obs"
)

func TestDecodeJSONRejectsTrailingContent(t *testing.T) {
	tests := map[string]string{
		"two objects":       `{"amount_minor":100,"currency":"USD"}{"amount_minor":200,"currency":"USD"}`,
		"object and array":  `{"amount_minor":100,"currency":"USD"}[]`,
		"object and scalar": `{"amount_minor":100,"currency":"USD"}7`,
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/tokens/vtok-1/authorize", strings.NewReader(body))
			if _, err := decodeJSON[authorizeChargeRequest](request); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

func TestDecodeJSONAcceptsTrailingWhitespace(t *testing.T) {
	body := "{\"amount_minor\":100,\"currency\":\"USD\"}\n\t  "
	request := httptest.NewRequest(http.MethodPost, "/v1/tokens/vtok-1/authorize", strings.NewReader(body))

	decoded, err := decodeJSON[authorizeChargeRequest](request)
	if err != nil {
		t.Fatalf("trailing whitespace must be accepted, got %v", err)
	}
	if decoded.AmountMinor != 100 {
		t.Fatalf("decoded = %+v", decoded)
	}
}

func TestDecodeJSONRejectsUnknownFields(t *testing.T) {
	body := `{"amount_minor":100,"currency":"USD","extra":true}`
	request := httptest.NewRequest(http.MethodPost, "/v1/tokens/vtok-1/authorize", strings.NewReader(body))

	if _, err := decodeJSON[authorizeChargeRequest](request); err == nil {
		t.Fatal("expected unknown fields to be rejected")
	}
}

func TestStatusRecorderTracksFirstStatusOnly(t *testing.T) {
	recorder := httptest.NewRecorder()
	wrapped := &statusRecorder{ResponseWriter: recorder}

	wrapped.WriteHeader(http.StatusAccepted)
	wrapped.WriteHeader(http.StatusInternalServerError)

	if got := wrapped.Status(); got != http.StatusAccepted {
		t.Fatalf("status = %d, want the first status %d", got, http.StatusAccepted)
	}
}

func TestStatusRecorderDefaultsToOK(t *testing.T) {
	wrapped := &statusRecorder{ResponseWriter: httptest.NewRecorder()}

	if got := wrapped.Status(); got != http.StatusOK {
		t.Fatalf("an untouched response must read as 200, got %d", got)
	}

	if _, err := wrapped.Write([]byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := wrapped.Status(); got != http.StatusOK {
		t.Fatalf("status after an implicit write = %d, want 200", got)
	}
}

func TestStatusRecorderUnwrapsToOriginal(t *testing.T) {
	recorder := httptest.NewRecorder()
	wrapped := &statusRecorder{ResponseWriter: recorder}

	if wrapped.Unwrap() != http.ResponseWriter(recorder) {
		t.Fatal("Unwrap must return the wrapped writer")
	}
	wrapped.Header().Set("X-Test", "value")
	if recorder.Header().Get("X-Test") != "value" {
		t.Fatal("headers must reach the underlying writer")
	}
}

func TestStatusRecorderFlushes(t *testing.T) {
	recorder := httptest.NewRecorder()
	wrapped := &statusRecorder{ResponseWriter: recorder}

	wrapped.WriteHeader(http.StatusOK)
	wrapped.Flush()

	if !recorder.Flushed {
		t.Fatal("Flush must reach the underlying writer")
	}
	if got := wrapped.Status(); got != http.StatusOK {
		t.Fatalf("status = %d, want 200", got)
	}
}

func TestObservationMiddlewareRecordsStatusAndLogs(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := obs.NewMetrics(registry)

	called := false
	handler := observationMiddleware(testLogger(), metrics, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/tokens/vtok-1/freeze", nil))

	if !called {
		t.Fatal("handler must run")
	}
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := false
	for _, family := range families {
		if family.GetName() != "subscription_firewall_http_requests_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "status" && pair.GetValue() == "201" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("expected the 201 status to be recorded")
	}
}

func TestObservationMiddlewareRecoversAndStillRecords(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := obs.NewMetrics(registry)

	handler := observationMiddleware(testLogger(), metrics,
		recoveryMiddleware(testLogger(), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("boom")
		})))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/subscriptions/sub-1", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "subscription_firewall_http_requests_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "status" && pair.GetValue() == "500" {
					return
				}
			}
		}
	}
	t.Fatal("a recovered panic must still be recorded as 500")
}

func TestAPIKeyMatchesIgnoresLengthDifferences(t *testing.T) {
	auth := newAPIKeyAuthenticator([]string{"short", "a-much-longer-key-value"})

	for _, candidate := range []string{"", "s", "short", "short-extra", "a-much-longer-key-value", "a-much-longer-key-value-x"} {
		if _, matched := auth.matches(candidate); matched != (candidate == "short" || candidate == "a-much-longer-key-value") {
			t.Errorf("matches(%q) = %v, want %v", candidate, matched, candidate == "short" || candidate == "a-much-longer-key-value")
		}
	}
}

func TestAPIKeyDigestIsFixedSize(t *testing.T) {
	short := apiKeyDigest("a")
	long := apiKeyDigest(strings.Repeat("a", 4096))

	if len(short) != len(long) {
		t.Fatalf("digest sizes differ: %d vs %d", len(short), len(long))
	}
	if string(short) == string(long) {
		t.Fatal("different inputs must not share a digest")
	}
}

func TestAPIKeyMatchesPrefersTheLastMatchingKey(t *testing.T) {
	auth := newAPIKeyAuthenticator([]string{"dup:read", "dup:manage"})

	key, matched := auth.matches("dup")
	if !matched {
		t.Fatal("expected a match")
	}
	if !key.allows(scopeManage) {
		t.Error("expected the last matching key to win, preserving prior behaviour")
	}
}
