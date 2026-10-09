package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"subscriptionfirewall/internal/ports"
)

func TestLoadConfigRejectsMalformedBoolean(t *testing.T) {
	tests := map[string]string{
		"allow no auth": "SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH",
		"otlp insecure": "SUBSCRIPTION_FIREWALL_OTLP_INSECURE",
	}

	for name, variable := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv(variable, "yes-please")
			if _, err := loadConfig(); err == nil {
				t.Fatalf("%s must reject a non-boolean value", variable)
			}
		})
	}
}

func TestEnvBoolParsesExplicitValues(t *testing.T) {
	for _, raw := range []string{"1", "t", "T", "TRUE", "true", "True"} {
		t.Setenv("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", raw)
		got, err := envBool("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", false)
		if err != nil || !got {
			t.Errorf("envBool(%q) = %v, %v; want true, nil", raw, got, err)
		}
	}
	for _, raw := range []string{"0", "f", "F", "FALSE", "false", "False"} {
		t.Setenv("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", raw)
		got, err := envBool("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", true)
		if err != nil || got {
			t.Errorf("envBool(%q) = %v, %v; want false, nil", raw, got, err)
		}
	}

	t.Setenv("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", "")
	if got, err := envBool("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", true); err != nil || !got {
		t.Errorf("unset must use the fallback, got %v, %v", got, err)
	}
}

func TestLoadConfigIssuerDefaultsToSimulated(t *testing.T) {
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.issuer != issuerSimulated {
		t.Errorf("issuer = %q, want %q", cfg.issuer, issuerSimulated)
	}
}

func TestLoadConfigRejectsUnknownIssuer(t *testing.T) {
	t.Setenv("SUBSCRIPTION_FIREWALL_ISSUER", "stripe")
	if _, err := loadConfig(); err == nil {
		t.Fatal("expected an unknown issuer to be rejected")
	}
}

func TestBuildCardIssuerRejectsUnknownProvider(t *testing.T) {
	cfg := config{issuer: "stripe"}

	if _, err := buildCardIssuer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("expected buildCardIssuer to reject an unknown provider")
	}
}

func TestBuildCardIssuerWrapsSimulatedProvider(t *testing.T) {
	cfg := config{issuer: issuerSimulated, issuerMonthlyLimitMinor: 5_000}

	cardIssuer, err := buildCardIssuer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("build issuer: %v", err)
	}
	card, err := cardIssuer.Issue(context.Background(), ports.IssueRequest{
		UserID: "user-1", MerchantID: "netflix", IdempotencyKey: "issue:user-1:netflix",
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if card.MonthlyLimit != 5_000 {
		t.Errorf("monthly limit = %d, want 5000", card.MonthlyLimit)
	}
	if !strings.HasPrefix(card.MaskedPAN, "411111") {
		t.Errorf("masked pan = %q", card.MaskedPAN)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.address != defaultAddress {
		t.Errorf("address = %q, want %q", cfg.address, defaultAddress)
	}
	if cfg.storage != storageMemory {
		t.Errorf("storage = %q, want %q once auto is resolved", cfg.storage, storageMemory)
	}
	if cfg.workerCount != defaultWorkerCount || cfg.queueCapacity != defaultIngestQueueCapacity {
		t.Errorf("workers = %d, queue = %d", cfg.workerCount, cfg.queueCapacity)
	}
	if cfg.sweepInterval != defaultSweepInterval {
		t.Errorf("sweep interval = %s, want %s", cfg.sweepInterval, defaultSweepInterval)
	}
	if cfg.rateLimit != 100 || cfg.rateBurst != 200 {
		t.Errorf("rate limit = %v, burst = %d", cfg.rateLimit, cfg.rateBurst)
	}
	if !cfg.otlpInsecure {
		t.Error("otlp insecure must default to true")
	}
	if cfg.logLevel != slog.LevelInfo {
		t.Errorf("log level = %s, want info", cfg.logLevel)
	}
}

func TestLoadConfigReadsEnvironment(t *testing.T) {
	t.Setenv("SUBSCRIPTION_FIREWALL_ADDRESS", ":9090")
	t.Setenv("SUBSCRIPTION_FIREWALL_API_KEYS", "key-a:full,key-b:ingest,read")
	t.Setenv("SUBSCRIPTION_FIREWALL_WORKERS", "9")
	t.Setenv("SUBSCRIPTION_FIREWALL_SWEEP_INTERVAL", "90s")
	t.Setenv("SUBSCRIPTION_FIREWALL_LOG_LEVEL", "debug")
	t.Setenv("SUBSCRIPTION_FIREWALL_ISSUANCE_TIMEOUT", "2m")
	t.Setenv("SUBSCRIPTION_FIREWALL_OTLP_INSECURE", "false")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.address != ":9090" {
		t.Errorf("address = %q", cfg.address)
	}
	if len(cfg.apiKeys) != 3 {
		t.Fatalf("api keys = %v, want 3 entries", cfg.apiKeys)
	}
	if cfg.workerCount != 9 {
		t.Errorf("workers = %d, want 9", cfg.workerCount)
	}
	if cfg.sweepInterval != 90*time.Second {
		t.Errorf("sweep interval = %s", cfg.sweepInterval)
	}
	if cfg.logLevel != slog.LevelDebug {
		t.Errorf("log level = %s, want debug", cfg.logLevel)
	}
	if cfg.issuanceTimeout != 2*time.Minute {
		t.Errorf("issuance timeout = %s", cfg.issuanceTimeout)
	}
	if cfg.otlpInsecure {
		t.Error("otlp insecure = true, want false")
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	tests := map[string]struct{ name, value string }{
		"zero workers":          {"SUBSCRIPTION_FIREWALL_WORKERS", "0"},
		"negative workers":      {"SUBSCRIPTION_FIREWALL_WORKERS", "-1"},
		"non numeric workers":   {"SUBSCRIPTION_FIREWALL_WORKERS", "many"},
		"zero queue capacity":   {"SUBSCRIPTION_FIREWALL_QUEUE_CAPACITY", "0"},
		"bad duration":          {"SUBSCRIPTION_FIREWALL_SWEEP_INTERVAL", "soon"},
		"zero duration":         {"SUBSCRIPTION_FIREWALL_READ_TIMEOUT", "0s"},
		"negative float":        {"SUBSCRIPTION_FIREWALL_RATE_LIMIT", "-1"},
		"bad log level":         {"SUBSCRIPTION_FIREWALL_LOG_LEVEL", "verbose"},
		"unknown storage":       {"SUBSCRIPTION_FIREWALL_STORAGE", "postgres"},
		"zero monthly limit":    {"SUBSCRIPTION_FIREWALL_ISSUER_MONTHLY_LIMIT", "0"},
		"zero issuance timeout": {"SUBSCRIPTION_FIREWALL_ISSUANCE_TIMEOUT", "0s"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv(test.name, test.value)
			if _, err := loadConfig(); err == nil {
				t.Fatalf("expected %s=%q to be rejected", test.name, test.value)
			}
		})
	}
}

func TestLoadConfigRejectsInconsistentStorage(t *testing.T) {
	t.Run("mysql without dsn", func(t *testing.T) {
		t.Setenv("SUBSCRIPTION_FIREWALL_STORAGE", storageMySQL)
		t.Setenv("SUBSCRIPTION_FIREWALL_DB_DSN", "")
		if _, err := loadConfig(); err == nil {
			t.Fatal("expected storage=mysql without a DSN to be rejected")
		}
	})

	t.Run("memory with dsn", func(t *testing.T) {
		t.Setenv("SUBSCRIPTION_FIREWALL_STORAGE", storageMemory)
		t.Setenv("SUBSCRIPTION_FIREWALL_DB_DSN", "user:pass@tcp(127.0.0.1:3306)/db?parseTime=true")
		if _, err := loadConfig(); err == nil {
			t.Fatal("expected storage=memory with a DSN to be rejected")
		}
	})
}

func TestLoadConfigResolvesStorageAutomatically(t *testing.T) {
	t.Run("dsn selects mysql", func(t *testing.T) {
		t.Setenv("SUBSCRIPTION_FIREWALL_DB_DSN", "user:pass@tcp(127.0.0.1:3306)/db?parseTime=true")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("load config: %v", err)
		}
		if cfg.storage != storageMySQL {
			t.Errorf("storage = %q, want %q", cfg.storage, storageMySQL)
		}
	})

	t.Run("no dsn selects memory", func(t *testing.T) {
		t.Setenv("SUBSCRIPTION_FIREWALL_DB_DSN", "")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("load config: %v", err)
		}
		if cfg.storage != storageMemory {
			t.Errorf("storage = %q, want %q", cfg.storage, storageMemory)
		}
	})
}

func TestLoadConfigAcceptsNonNegativeOverrides(t *testing.T) {
	t.Setenv("SUBSCRIPTION_FIREWALL_RATE_LIMIT", "0")
	t.Setenv("SUBSCRIPTION_FIREWALL_DB_MAX_IDLE_CONNS", "0")
	t.Setenv("SUBSCRIPTION_FIREWALL_ISSUER_TIMEOUT", "0s")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("zero must disable, not fail: %v", err)
	}
	if cfg.rateLimit != 0 || cfg.dbMaxIdleConns != 0 || cfg.issuerTimeout != 0 {
		t.Errorf("expected zero overrides, got %v / %d / %s", cfg.rateLimit, cfg.dbMaxIdleConns, cfg.issuerTimeout)
	}
}

func TestAddressPort(t *testing.T) {
	tests := map[string]string{
		":8080":                 ":8080",
		"127.0.0.1:9090":        ":9090",
		"0.0.0.0:80":            ":80",
		"not-an-address":        ":8080",
		"":                      ":8080",
		"[::1]:7000":            ":7000",
		"localhost:65535":       ":65535",
		"[2001:db8::1]:no-port": ":8080",
		"host:0":                ":8080",
		"host:70000":            ":8080",
		":-1":                   ":8080",
	}

	for address, want := range tests {
		t.Run(address, func(t *testing.T) {
			if got := addressPort(address); got != want {
				t.Fatalf("addressPort(%q) = %q, want %q", address, got, want)
			}
		})
	}
}

func TestEnvListSplitsAndIgnoresEmpty(t *testing.T) {
	t.Setenv("SUBSCRIPTION_FIREWALL_API_KEYS", "")
	if got := envList("SUBSCRIPTION_FIREWALL_API_KEYS"); got != nil {
		t.Errorf("empty env must yield nil, got %v", got)
	}

	t.Setenv("SUBSCRIPTION_FIREWALL_API_KEYS", "a, b ,c")
	got := envList("SUBSCRIPTION_FIREWALL_API_KEYS")
	want := []string{"a", " b ", "c"}
	if len(got) != len(want) {
		t.Fatalf("envList = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("envList = %v, want %v", got, want)
		}
	}
}

func TestConfiguredAddressFallsBackOnlyWhenUnset(t *testing.T) {
	t.Setenv("SUBSCRIPTION_FIREWALL_ADDRESS", "")
	if got := configuredAddress(); got != ":8080" {
		t.Errorf("empty env must fall back, got %q", got)
	}

	t.Setenv("SUBSCRIPTION_FIREWALL_ADDRESS", ":7000")
	if got := configuredAddress(); got != ":7000" {
		t.Errorf("set env must win, got %q", got)
	}
}
