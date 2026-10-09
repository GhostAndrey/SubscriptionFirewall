package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type logCapture struct {
	records []map[string]any
}

func (c *logCapture) Write(payload []byte) (int, error) {
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(payload), &record); err != nil {
		c.records = append(c.records, map[string]any{"__unparsable": string(payload)})
		return len(payload), nil //nolint:nilerr // unparsable frames still belong in the captured record
	}
	c.records = append(c.records, record)
	return len(payload), nil
}

func (c *logCapture) last(t *testing.T) map[string]any {
	t.Helper()
	if len(c.records) == 0 {
		t.Fatal("expected at least one log record")
	}
	return c.records[len(c.records)-1]
}

func capturingLogger() (*slog.Logger, *logCapture) {
	capture := &logCapture{}
	return slog.New(NewSanitizingHandler(slog.NewJSONHandler(capture, &slog.HandlerOptions{Level: slog.LevelInfo}))), capture
}

func TestSanitizingHandlerMasksSensitiveAttributes(t *testing.T) {
	logger, capture := capturingLogger()

	logger.Info("charge",
		slog.String("pan", "4111111111111111"),
		slog.String("token", "vtok-abcdef123456"),
		slog.String("card_number", "5555444433332222"),
		slog.String("token_id", "vtok-abcdef123456"),
		slog.String("authorization", "Bearer secret-token"),
		slog.String("user_id", "user-1"),
	)

	record := capture.last(t)
	secrets := map[string]string{
		"pan":           "4111111111111111",
		"token":         "vtok-abcdef123456",
		"card_number":   "5555444433332222",
		"token_id":      "vtok-abcdef123456",
		"authorization": "Bearer secret-token",
	}
	for key, secret := range secrets {
		value, ok := record[key].(string)
		if !ok {
			t.Fatalf("attribute %s is missing or not a string: %v", key, record[key])
		}
		if strings.Contains(value, secret) {
			t.Errorf("attribute %s leaked: %q", key, value)
		}
		if value == "" {
			t.Errorf("attribute %s was dropped instead of masked", key)
		}
	}
	if record["user_id"] != "user-1" {
		t.Errorf("non-sensitive attribute must stay intact, got %v", record["user_id"])
	}
}

func TestSanitizingHandlerMasksAttrsAttachedBeforeLogging(t *testing.T) {
	logger, capture := capturingLogger()
	bound := logger.With(slog.String("pan", "4111111111111111"))

	bound.Info("with")

	value, ok := capture.last(t)["pan"].(string)
	if !ok {
		t.Fatalf("pan attribute missing: %v", capture.last(t)["pan"])
	}
	if value == "4111111111111111" {
		t.Errorf("pre-attached pan was not masked: %q", value)
	}
	if strings.Contains(value, "111111111111") {
		t.Errorf("pre-attached pan leaked its middle digits: %q", value)
	}
}

func TestSanitizingHandlerLeavesNonStringValues(t *testing.T) {
	logger, capture := capturingLogger()

	logger.Info("numeric", slog.Int64("pan", 42))

	if value := capture.last(t)["pan"]; value != float64(42) {
		t.Errorf("non-string attribute must pass through unchanged, got %v", value)
	}
}

func TestSanitizingHandlerRespectsLevel(t *testing.T) {
	handler := NewSanitizingHandler(slog.NewJSONHandler(&logCapture{}, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if handler.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("info must be disabled at warn level")
	}
	if !handler.Enabled(context.Background(), slog.LevelError) {
		t.Error("error must be enabled at warn level")
	}
}

func TestSanitizingHandlerWithGroupKeepsWorking(t *testing.T) {
	logger, capture := capturingLogger()

	logger.Info("grouped", slog.Group("payment", slog.String("amount_minor", "1500")))

	record := capture.last(t)
	group, ok := record["payment"].(map[string]any)
	if !ok {
		t.Fatalf("expected a group attribute, got %v", record["payment"])
	}
	if group["amount_minor"] != "1500" {
		t.Errorf("group content lost: %v", group)
	}
}

func TestSetupTracingDisabledWithoutEndpoint(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), "", true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if shutdown != nil {
		t.Error("expected a nil shutdown function when tracing is disabled")
	}
}

func TestMetricsCountersAndGauges(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)

	metrics.CountTransactionIngested()
	metrics.CountTransactionIngested()
	metrics.CountDetectionRun()
	metrics.CountEnqueueDropped()
	metrics.CountTokenFrozen()
	metrics.CountSubscriptionDetected(string("Active"))
	metrics.CountSubscriptionSwept(string("Zombie"))
	metrics.CountTokenAuthorization("approved")
	metrics.CountLifecycleSync("token", "freeze", "applied")
	metrics.CountLinkedActionFailure("token", "terminate")
	metrics.SetQueueDepth(7)
	metrics.SetLifecycleQueueDepth(3)

	if got := counterValue(t, registry, "subscription_firewall_transactions_ingested_total"); got != 2 {
		t.Errorf("transactions ingested = %v, want 2", got)
	}
	if got := counterValue(t, registry, "subscription_firewall_subscriptions_detected_total", map[string]string{"state": "Active"}); got != 1 {
		t.Errorf("detected = %v, want 1", got)
	}
	if got := counterValue(t, registry, "subscription_firewall_subscriptions_swept_total", map[string]string{"state": "Zombie"}); got != 1 {
		t.Errorf("swept = %v, want 1", got)
	}
	if got := counterValue(t, registry, "subscription_firewall_lifecycle_sync_total",
		map[string]string{"entity": "token", "action": "freeze", "result": "applied"}); got != 1 {
		t.Errorf("lifecycle sync = %v, want 1", got)
	}
	if got := counterValue(t, registry, "subscription_firewall_linked_action_failures_total",
		map[string]string{"entity": "token", "action": "terminate"}); got != 1 {
		t.Errorf("linked action failures = %v, want 1", got)
	}
	if got := gaugeValue(t, registry, "subscription_firewall_detection_queue_depth"); got != 7 {
		t.Errorf("queue depth = %v, want 7", got)
	}
	if got := gaugeValue(t, registry, "subscription_firewall_lifecycle_queue_depth"); got != 3 {
		t.Errorf("lifecycle queue depth = %v, want 3", got)
	}
}

func TestMetricsObserveHTTPRequest(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)

	metrics.ObserveHTTPRequest("POST", "/v1/transactions", 202, 1500*time.Microsecond)
	metrics.ObserveHTTPRequest("GET", "/v1/users/{user_id}/subscriptions", 500, 2*time.Second)

	labels := map[string]string{"method": "POST", "route": "/v1/transactions", "status": "202"}
	if got := counterValue(t, registry, "subscription_firewall_http_requests_total", labels); got != 1 {
		t.Errorf("http requests = %v, want 1", got)
	}

	count, sum := histogramValue(t, registry, "subscription_firewall_http_request_duration_seconds",
		map[string]string{"method": "GET", "route": "/v1/users/{user_id}/subscriptions"})
	if count != 1 {
		t.Errorf("histogram observations = %d, want 1", count)
	}
	if sum < 2 {
		t.Errorf("histogram sum = %v, want at least 2 seconds", sum)
	}
}

func TestMetricsBuildInfoCarriesVersion(t *testing.T) {
	registry := prometheus.NewRegistry()
	NewMetrics(registry)

	metric, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range metric {
		if family.GetName() != "subscription_firewall_build_info" {
			continue
		}
		labels := map[string]string{}
		for _, pair := range family.GetMetric()[0].GetLabel() {
			labels[pair.GetName()] = pair.GetValue()
		}
		for _, key := range []string{"version", "commit", "build_date"} {
			if _, ok := labels[key]; !ok {
				t.Errorf("build_info is missing the %s label: %v", key, labels)
			}
		}
		return
	}
	t.Fatal("build_info metric was not registered")
}
