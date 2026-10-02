package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/issuer"
	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAPIKeyMiddleware(t *testing.T) {
	const (
		validKey   = "secret-key-1"
		invalidKey = "secret-key-2"
	)

	tests := map[string]struct {
		authenticator *apiKeyAuthenticator
		headers       map[string]string
		wantStatus    int
		wantNext      bool
	}{
		"disabled authenticator lets requests through": {
			authenticator: newAPIKeyAuthenticator(nil),
			wantStatus:    http.StatusOK,
			wantNext:      true,
		},
		"blank key configuration is treated as disabled": {
			authenticator: newAPIKeyAuthenticator([]string{"", "   "}),
			wantStatus:    http.StatusOK,
			wantNext:      true,
		},
		"missing key is rejected": {
			authenticator: newAPIKeyAuthenticator([]string{validKey}),
			wantStatus:    http.StatusUnauthorized,
		},
		"wrong key is rejected": {
			authenticator: newAPIKeyAuthenticator([]string{validKey}),
			headers:       map[string]string{"X-API-Key": invalidKey},
			wantStatus:    http.StatusUnauthorized,
		},
		"valid key via X-API-Key header passes": {
			authenticator: newAPIKeyAuthenticator([]string{validKey}),
			headers:       map[string]string{"X-API-Key": validKey},
			wantStatus:    http.StatusOK,
			wantNext:      true,
		},
		"valid key via Authorization header passes": {
			authenticator: newAPIKeyAuthenticator([]string{validKey, "other-key"}),
			headers:       map[string]string{"Authorization": "Bearer other-key"},
			wantStatus:    http.StatusOK,
			wantNext:      true,
		},
		"authorization header without bearer prefix is rejected": {
			authenticator: newAPIKeyAuthenticator([]string{validKey}),
			headers:       map[string]string{"Authorization": validKey},
			wantStatus:    http.StatusUnauthorized,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			nextCalled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				nextCalled = true
				w.WriteHeader(http.StatusOK)
			})

			request := httptest.NewRequest(http.MethodGet, "/v1/users/user-1/subscriptions", nil)
			for header, value := range test.headers {
				request.Header.Set(header, value)
			}
			recorder := httptest.NewRecorder()

			test.authenticator.middleware(testLogger(), next).ServeHTTP(recorder, request)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if nextCalled != test.wantNext {
				t.Fatalf("next handler called = %v, want %v", nextCalled, test.wantNext)
			}
			if !test.wantNext && !strings.Contains(recorder.Body.String(), "unauthorized") {
				t.Fatalf("body %q does not mention unauthorized", recorder.Body.String())
			}
		})
	}
}

func TestServerRoutesRequireAPIKey(t *testing.T) {
	const apiKey = "integration-secret"

	metrics := obs.NewMetrics(prometheus.NewRegistry())
	transactionRepository := memory.NewTransactionRepository()
	apiServer := NewServer(
		Config{APIKeys: []string{apiKey}},
		transactionRepository,
		subscription.NewService(memory.NewSubscriptionRepository(), memory.Clock{}),
		token.NewService(
			memory.NewVirtualTokenRepository(),
			issuer.NewSimulatedCardIssuer(100_000),
			memory.Clock{},
		),
		memory.NewDetectionOutbox(transactionRepository, metrics, testLogger(), 16),
		memory.NewAuditLog(),
		metrics,
		testLogger(),
	)
	testServer := httptest.NewServer(apiServer.http.Handler)
	defer testServer.Close()

	tests := map[string]struct {
		path       string
		method     string
		withKey    bool
		wantStatus int
	}{
		"health check is public":               {path: "/healthz", wantStatus: http.StatusOK},
		"metrics are public":                   {path: "/metrics", wantStatus: http.StatusOK},
		"subscriptions without key":            {path: "/v1/users/user-1/subscriptions", wantStatus: http.StatusUnauthorized},
		"subscriptions with key":               {path: "/v1/users/user-1/subscriptions", withKey: true, wantStatus: http.StatusOK},
		"token freeze without key":             {path: "/v1/tokens/vtok-1/freeze", wantStatus: http.StatusUnauthorized},
		"transaction ingest without key":       {path: "/v1/transactions", method: http.MethodPost, wantStatus: http.StatusUnauthorized},
		"transaction ingest with invalid body": {path: "/v1/transactions", method: http.MethodPost, withKey: true, wantStatus: http.StatusBadRequest},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			method := test.method
			if method == "" {
				method = http.MethodGet
			}
			request, err := http.NewRequest(method, testServer.URL+test.path, nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			if test.withKey {
				request.Header.Set("X-API-Key", apiKey)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer response.Body.Close()

			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
		})
	}
}
