package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIKeyScopes(t *testing.T) {
	auth := newAPIKeyAuthenticator([]string{
		"ingest-key:ingest",
		"manage-key:manage,read",
		"admin-key",
	})

	tests := map[string]struct {
		key        string
		method     string
		path       string
		wantStatus int
		wantNext   bool
	}{
		"ingest key may ingest": {
			key: "ingest-key", method: http.MethodPost, path: "/v1/transactions",
			wantStatus: http.StatusOK, wantNext: true,
		},
		"ingest key may not read": {
			key: "ingest-key", method: http.MethodGet, path: "/v1/users/user-1/subscriptions",
			wantStatus: http.StatusForbidden,
		},
		"ingest key may not manage": {
			key: "ingest-key", method: http.MethodPost, path: "/v1/tokens/vtok-1/freeze",
			wantStatus: http.StatusForbidden,
		},
		"manage key may manage": {
			key: "manage-key", method: http.MethodPost, path: "/v1/tokens/vtok-1/freeze",
			wantStatus: http.StatusOK, wantNext: true,
		},
		"manage key may read": {
			key: "manage-key", method: http.MethodGet, path: "/v1/tokens/vtok-1",
			wantStatus: http.StatusOK, wantNext: true,
		},
		"full key may ingest": {
			key: "admin-key", method: http.MethodPost, path: "/v1/transactions",
			wantStatus: http.StatusOK, wantNext: true,
		},
		"full key may read": {
			key: "admin-key", method: http.MethodGet, path: "/v1/users/user-1/subscriptions",
			wantStatus: http.StatusOK, wantNext: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			nextCalled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				nextCalled = true
				w.WriteHeader(http.StatusOK)
			})

			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("X-API-Key", test.key)
			recorder := httptest.NewRecorder()

			auth.middleware(testLogger(), next).ServeHTTP(recorder, request)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if nextCalled != test.wantNext {
				t.Fatalf("next handler called = %v, want %v", nextCalled, test.wantNext)
			}
		})
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	limiter := newIPRateLimiter(1, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/users/user-1/subscriptions", nil)
	recorder := httptest.NewRecorder()
	limiter.middleware(next).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("first request should pass, got %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	limiter.middleware(next).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("second request should be throttled, got %d", recorder.Code)
	}

	healthRequest := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder = httptest.NewRecorder()
	limiter.middleware(next).ServeHTTP(recorder, healthRequest)
	if recorder.Code != http.StatusOK {
		t.Fatalf("healthz should bypass rate limiting, got %d", recorder.Code)
	}
}
