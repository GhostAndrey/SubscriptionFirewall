package httpapi

import (
	"context"
	"errors"
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

func newSystemEndpointServer(t *testing.T, readiness func(ctx context.Context) error) *httptest.Server {
	t.Helper()

	transactionRepository := memory.NewTransactionRepository()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := obs.NewMetrics(prometheus.NewRegistry())

	apiServer := NewServer(
		Config{APIKeys: []string{"test-key"}, Readiness: readiness},
		transactionRepository,
		subscription.NewService(memory.NewSubscriptionRepository(), memory.Clock{}),
		token.NewService(memory.NewVirtualTokenRepository(), issuer.NewSimulatedCardIssuer(100_000), memory.Clock{}),
		memory.NewDetectionOutbox(transactionRepository, metrics, logger, 16),
		memory.NewAuditLog(),
		metrics,
		logger,
	)
	testServer := httptest.NewServer(apiServer.http.Handler)
	t.Cleanup(testServer.Close)
	return testServer
}

func TestReadyzReportsReady(t *testing.T) {
	testServer := newSystemEndpointServer(t, nil)

	response, err := http.Get(testServer.URL + "/readyz")
	if err != nil {
		t.Fatalf("get /readyz: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestReadyzReportsUnavailableWhenCheckFails(t *testing.T) {
	testServer := newSystemEndpointServer(t, func(ctx context.Context) error {
		return errors.New("database down")
	})

	response, err := http.Get(testServer.URL + "/readyz")
	if err != nil {
		t.Fatalf("get /readyz: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestVersionEndpoint(t *testing.T) {
	testServer := newSystemEndpointServer(t, nil)

	response, err := http.Get(testServer.URL + "/version")
	if err != nil {
		t.Fatalf("get /version: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	for _, fragment := range []string{`"version"`, `"commit"`, `"build_date"`} {
		if !strings.Contains(string(body), fragment) {
			t.Fatalf("body %q does not contain %q", string(body), fragment)
		}
	}
}

func TestRequestIDGenerated(t *testing.T) {
	testServer := newSystemEndpointServer(t, nil)

	response, err := http.Get(testServer.URL + "/readyz")
	if err != nil {
		t.Fatalf("get /readyz: %v", err)
	}
	defer response.Body.Close()
	requestID := response.Header.Get("X-Request-Id")
	if len(requestID) != 32 {
		t.Fatalf("X-Request-Id = %q, want 32 hex chars", requestID)
	}
}

func TestRequestIDEchoedFromHeader(t *testing.T) {
	testServer := newSystemEndpointServer(t, nil)

	request, err := http.NewRequest(http.MethodGet, testServer.URL+"/readyz", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("X-Request-Id", "trace-123")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer response.Body.Close()
	if got := response.Header.Get("X-Request-Id"); got != "trace-123" {
		t.Fatalf("X-Request-Id = %q, want %q", got, "trace-123")
	}
}

func TestRequestIDRejectedWhenMalformed(t *testing.T) {
	testServer := newSystemEndpointServer(t, nil)

	request, err := http.NewRequest(http.MethodGet, testServer.URL+"/readyz", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("X-Request-Id", strings.Repeat("a", maxRequestIDLength+1))

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer response.Body.Close()
	got := response.Header.Get("X-Request-Id")
	if got == strings.Repeat("a", maxRequestIDLength+1) {
		t.Fatalf("oversized incoming request id was echoed")
	}
	if len(got) != 32 {
		t.Fatalf("X-Request-Id = %q, want generated 32 hex chars", got)
	}
}

func TestSystemEndpointsRequireNoAuth(t *testing.T) {
	testServer := newSystemEndpointServer(t, nil)

	for _, path := range []string{"/healthz", "/readyz", "/version"} {
		response, err := http.Get(testServer.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d, want %d", path, response.StatusCode, http.StatusOK)
		}
	}
}
