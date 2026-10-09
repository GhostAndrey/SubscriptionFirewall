package httpapi

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewRedisRateLimiterRequiresLimit(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})

	if limiter := newRedisRateLimiter(client, 0, 100, testLogger()); limiter != nil {
		t.Error("a disabled limit must not build a limiter")
	}
	if limiter := newRedisRateLimiter(nil, 100, 100, testLogger()); limiter != nil {
		t.Error("a nil client must not build a limiter")
	}
}

func TestNewRedisRateLimiterKeepsBurstAtLeastLimit(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})

	limiter := newRedisRateLimiter(client, 100, 10, testLogger())
	if limiter == nil {
		t.Fatal("expected a limiter")
	}
	if limiter.burst != 100 {
		t.Errorf("burst = %d, want it raised to the limit of 100", limiter.burst)
	}

	exact := newRedisRateLimiter(client, 100, 250, testLogger())
	if exact.burst != 250 {
		t.Errorf("burst = %d, want 250", exact.burst)
	}
}

func TestRedisRateLimiterFailsClosedOnAuthorizeWhenRedisIsDown(t *testing.T) {
	limiter := newRedisRateLimiter(unreachableRedis(t), 100, 200, testLogger())
	handler := limiter.middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("the handler must not run while the rate limiter is unavailable")
	}))

	for _, path := range []string{"/v1/tokens/vtok-1/authorize"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))

		if recorder.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d, want %d", path, recorder.Code, http.StatusServiceUnavailable)
		}
	}
}

func TestRedisRateLimiterFailsOpenOnReadsWhenRedisIsDown(t *testing.T) {
	reached := false
	limiter := newRedisRateLimiter(unreachableRedis(t), 100, 200, testLogger())
	handler := limiter.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/v1/users/user-1/subscriptions", "/v1/subscriptions/sub-1/freeze"} {
		reached = false
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))

		if !reached {
			t.Errorf("%s: handler must still run", path)
		}
		if recorder.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want %d", path, recorder.Code, http.StatusOK)
		}
	}
}

func TestRedisRateLimiterSkipsPublicPaths(t *testing.T) {
	reached := false
	limiter := newRedisRateLimiter(unreachableRedis(t), 100, 200, testLogger())
	handler := limiter.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if !reached {
		t.Error("public paths must bypass the limiter")
	}
	if recorder.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestRequiresHardLimit(t *testing.T) {
	tests := map[string]struct {
		method string
		path   string
		want   bool
	}{
		"authorize post":      {http.MethodPost, "/v1/tokens/vtok-1/authorize", true},
		"authorize get":       {http.MethodGet, "/v1/tokens/vtok-1/authorize", false},
		"ingest":              {http.MethodPost, "/v1/transactions", false},
		"freeze subscription": {http.MethodPost, "/v1/subscriptions/sub-1/freeze", false},
		"read tokens":         {http.MethodGet, "/v1/users/user-1/tokens", false},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			if got := requiresHardLimit(request); got != test.want {
				t.Fatalf("requiresHardLimit = %v, want %v", got, test.want)
			}
		})
	}
}

// unreachableRedis points at a closed port so every command fails fast.
func unreachableRedis(t *testing.T) *redis.Client {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:         address,
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		MaxRetries:   0,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}
