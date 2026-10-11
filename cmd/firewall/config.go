package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultIngestQueueCapacity = 1024
	defaultWorkerCount         = 4
	defaultSweepInterval       = time.Minute
	defaultMonthlyLimitMinor   = 100_000
)

type config struct {
	address                 string
	apiKeys                 []string
	allowNoAuth             bool
	workerCount             int
	queueCapacity           int
	sweepInterval           time.Duration
	issuerMonthlyLimitMinor int64
	readTimeout             time.Duration
	writeTimeout            time.Duration
	idleTimeout             time.Duration
	shutdownTimeout         time.Duration
	dbDSN                   string
	storage                 string
	dbMaxOpenConns          int
	dbMaxIdleConns          int
	dbConnMaxLifetime       time.Duration
	logLevel                slog.Level
	rateLimit               float64
	rateBurst               int
	redisAddr               string
	redisPassword           string
	otlpEndpoint            string
	otlpInsecure            bool
	issuerTimeout           time.Duration
	issuerRetries           int
	issuerBreakerThreshold  int
	issuerBreakerCooldown   time.Duration
	issuanceTimeout         time.Duration
	issuer                  string
}

// loadConfig reads every knob from the environment. Each parser returns an error
// instead of falling back silently, so a typo is caught at startup rather than
// at the moment the misconfigured subsystem is first exercised.
func loadConfig() (config, error) {
	cfg := config{
		address:                 configuredAddress(),
		apiKeys:                 envList("SUBSCRIPTION_FIREWALL_API_KEYS"),
		workerCount:             defaultWorkerCount,
		queueCapacity:           defaultIngestQueueCapacity,
		sweepInterval:           defaultSweepInterval,
		issuerMonthlyLimitMinor: defaultMonthlyLimitMinor,
		readTimeout:             10 * time.Second,
		writeTimeout:            10 * time.Second,
		shutdownTimeout:         10 * time.Second,
		dbDSN:                   os.Getenv("SUBSCRIPTION_FIREWALL_DB_DSN"),
		storage:                 storageAuto,
		dbMaxOpenConns:          16,
		dbMaxIdleConns:          16,
		dbConnMaxLifetime:       5 * time.Minute,
		logLevel:                slog.LevelInfo,
		rateLimit:               100,
		rateBurst:               200,
		otlpInsecure:            true,
		issuerTimeout:           5 * time.Second,
		issuerRetries:           2,
		issuerBreakerThreshold:  5,
		issuerBreakerCooldown:   30 * time.Second,
		issuanceTimeout:         5 * time.Minute,
		issuer:                  issuerSimulated,
	}

	for _, read := range []func() error{
		func() error { return readBool(&cfg.allowNoAuth, "SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH") },
		func() error { return readPositiveInt(&cfg.workerCount, "SUBSCRIPTION_FIREWALL_WORKERS") },
		func() error { return readPositiveInt(&cfg.queueCapacity, "SUBSCRIPTION_FIREWALL_QUEUE_CAPACITY") },
		func() error { return readPositiveDuration(&cfg.sweepInterval, "SUBSCRIPTION_FIREWALL_SWEEP_INTERVAL") },
		func() error {
			return readPositiveInt64(&cfg.issuerMonthlyLimitMinor, "SUBSCRIPTION_FIREWALL_ISSUER_MONTHLY_LIMIT")
		},
		func() error { return readNonNegativeFloat(&cfg.rateLimit, "SUBSCRIPTION_FIREWALL_RATE_LIMIT") },
		func() error { return readPositiveInt(&cfg.rateBurst, "SUBSCRIPTION_FIREWALL_RATE_BURST") },
		func() error { return readPositiveDuration(&cfg.readTimeout, "SUBSCRIPTION_FIREWALL_READ_TIMEOUT") },
		func() error { return readPositiveDuration(&cfg.writeTimeout, "SUBSCRIPTION_FIREWALL_WRITE_TIMEOUT") },
		func() error { return readNonNegativeDuration(&cfg.idleTimeout, "SUBSCRIPTION_FIREWALL_IDLE_TIMEOUT") },
		func() error { return readPositiveDuration(&cfg.shutdownTimeout, "SUBSCRIPTION_FIREWALL_SHUTDOWN_TIMEOUT") },
		func() error { return readPositiveInt(&cfg.dbMaxOpenConns, "SUBSCRIPTION_FIREWALL_DB_MAX_OPEN_CONNS") },
		func() error { return readNonNegativeInt(&cfg.dbMaxIdleConns, "SUBSCRIPTION_FIREWALL_DB_MAX_IDLE_CONNS") },
		func() error {
			return readPositiveDuration(&cfg.dbConnMaxLifetime, "SUBSCRIPTION_FIREWALL_DB_CONN_MAX_LIFETIME")
		},
		func() error { return readEnum(&cfg.storage, "SUBSCRIPTION_FIREWALL_STORAGE", storageAuto, storageMemory, storageMySQL) },
		func() error { return readEnum(&cfg.issuer, "SUBSCRIPTION_FIREWALL_ISSUER", issuerSimulated) },
		func() error { return readLogLevel(&cfg.logLevel, "SUBSCRIPTION_FIREWALL_LOG_LEVEL") },
		func() error { return readBool(&cfg.otlpInsecure, "SUBSCRIPTION_FIREWALL_OTLP_INSECURE") },
		func() error { return readNonNegativeDuration(&cfg.issuerTimeout, "SUBSCRIPTION_FIREWALL_ISSUER_TIMEOUT") },
		func() error { return readNonNegativeInt(&cfg.issuerRetries, "SUBSCRIPTION_FIREWALL_ISSUER_RETRIES") },
		func() error {
			return readPositiveInt(&cfg.issuerBreakerThreshold, "SUBSCRIPTION_FIREWALL_ISSUER_BREAKER_THRESHOLD")
		},
		func() error {
			return readPositiveDuration(&cfg.issuerBreakerCooldown, "SUBSCRIPTION_FIREWALL_ISSUER_BREAKER_COOLDOWN")
		},
		func() error { return readPositiveDuration(&cfg.issuanceTimeout, "SUBSCRIPTION_FIREWALL_ISSUANCE_TIMEOUT") },
	} {
		if err := read(); err != nil {
			return cfg, err
		}
	}

	cfg.redisAddr = os.Getenv("SUBSCRIPTION_FIREWALL_REDIS_ADDR")
	cfg.redisPassword = os.Getenv("SUBSCRIPTION_FIREWALL_REDIS_PASSWORD")
	cfg.otlpEndpoint = os.Getenv("SUBSCRIPTION_FIREWALL_OTLP_ENDPOINT")

	if err := resolveStorage(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// resolveStorage settles the storage backend against the DSN. auto follows the
// DSN; an explicit choice must agree with it, because a mismatch would silently
// discard configured data or run replicas on separate stores.
func resolveStorage(cfg *config) error {
	switch cfg.storage {
	case storageMySQL:
		if cfg.dbDSN == "" {
			return fmt.Errorf("SUBSCRIPTION_FIREWALL_STORAGE=mysql requires SUBSCRIPTION_FIREWALL_DB_DSN")
		}
		return nil
	case storageMemory:
		if cfg.dbDSN != "" {
			return fmt.Errorf("SUBSCRIPTION_FIREWALL_STORAGE=memory conflicts with SUBSCRIPTION_FIREWALL_DB_DSN")
		}
		return nil
	}
	if cfg.dbDSN != "" {
		cfg.storage = storageMySQL
	} else {
		cfg.storage = storageMemory
	}
	return nil
}

func envList(name string) []string {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	return strings.Split(configured, ",")
}

func readBool(target *bool, name string) error {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	parsed, err := strconv.ParseBool(configured)
	if err != nil {
		return fmt.Errorf("%s must be one of 1|t|T|TRUE|true|True|0|f|F|FALSE|false|False, got %q", name, configured)
	}
	*target = parsed
	return nil
}

func readPositiveInt(target *int, name string) error {
	return readInt(target, name, false)
}

func readNonNegativeInt(target *int, name string) error {
	return readInt(target, name, true)
}

func readInt(target *int, name string, allowZero bool) error {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	parsed, err := strconv.Atoi(configured)
	bound := "a positive"
	if allowZero {
		bound = "a non-negative"
	}
	if err != nil || parsed < 0 || (!allowZero && parsed == 0) {
		return fmt.Errorf("%s must be %s integer, got %q", name, bound, configured)
	}
	*target = parsed
	return nil
}

func readPositiveInt64(target *int64, name string) error {
	return readInt64(target, name, false)
}

// readInt64 writes into target only when the variable is set, so the default
// seeded during construction survives an unset variable.
func readInt64(target *int64, name string, allowZero bool) error {	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(configured, 10, 64)
	bound := "a positive"
	if allowZero {
		bound = "a non-negative"
	}
	if err != nil || parsed < 0 || (!allowZero && parsed == 0) {
		return fmt.Errorf("%s must be %s integer, got %q", name, bound, configured)
	}
	*target = parsed
	return nil
}

func readPositiveDuration(target *time.Duration, name string) error {
	return readDuration(target, name, false)
}

func readNonNegativeDuration(target *time.Duration, name string) error {
	return readDuration(target, name, true)
}

func readDuration(target *time.Duration, name string, allowZero bool) error {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	bound := "a positive"
	if allowZero {
		bound = "a non-negative"
	}
	parsed, err := time.ParseDuration(configured)
	if err != nil || parsed < 0 || (!allowZero && parsed == 0) {
		return fmt.Errorf("%s must be %s duration, got %q", name, bound, configured)
	}
	*target = parsed
	return nil
}

func readNonNegativeFloat(target *float64, name string) error {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(configured, 64)
	if err != nil || parsed < 0 {
		return fmt.Errorf("%s must be a non-negative number, got %q", name, configured)
	}
	*target = parsed
	return nil
}

func readEnum(target *string, name string, allowed ...string) error {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	for _, candidate := range allowed {
		if configured == candidate {
			*target = configured
			return nil
		}
	}
	return fmt.Errorf("%s must be one of %s, got %q", name, strings.Join(allowed, "|"), configured)
}

func readLogLevel(target *slog.Level, name string) error {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(configured))); err != nil {
		return fmt.Errorf("%s must be one of debug|info|warn|error, got %q", name, configured)
	}
	*target = level
	return nil
}
