package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/adapter/httpapi"
	"subscriptionfirewall/internal/adapter/issuer"
	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/adapter/mysql"
	"subscriptionfirewall/internal/detector"
	"subscriptionfirewall/internal/lifecycle"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/pipeline"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/internal/subscription"
	"subscriptionfirewall/internal/token"
)

const (
	defaultAddress             = ":8080"
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

// buildCardIssuer resolves the configured card provider. Every provider is
// wrapped in the resilient decorator, so a misbehaving issuer cannot stall card
// provisioning. Selecting the provider through configuration keeps the seam
// explicit instead of hard-wiring a stub into the composition root.
func buildCardIssuer(cfg config, logger *slog.Logger) (ports.VirtualCardIssuer, error) {
	var inner ports.VirtualCardIssuer
	switch cfg.issuer {
	case issuerSimulated:
		logger.Warn("using the simulated card issuer: cards are not real and must not be used in production",
			"issuer", cfg.issuer,
			"monthly_limit_minor", cfg.issuerMonthlyLimitMinor,
		)
		inner = issuer.NewSimulatedCardIssuer(cfg.issuerMonthlyLimitMinor)
	default:
		return nil, fmt.Errorf("unknown card issuer %q: currently only %q exists", cfg.issuer, issuerSimulated)
	}

	return issuer.NewResilientIssuer(inner, issuer.ResilientConfig{
		Timeout:          cfg.issuerTimeout,
		Retries:          cfg.issuerRetries,
		BreakerThreshold: cfg.issuerBreakerThreshold,
		BreakerCooldown:  cfg.issuerBreakerCooldown,
	}), nil
}

// storage values for SUBSCRIPTION_FIREWALL_STORAGE: auto picks mysql when a
// DSN is set and memory otherwise; memory and mysql pin the choice explicitly.
const (
	storageAuto   = "auto"
	storageMemory = "memory"
	storageMySQL  = "mysql"
)

// Card providers selectable through SUBSCRIPTION_FIREWALL_ISSUER.
const issuerSimulated = "simulated"

func loadConfig() (config, error) {
	cfg := config{
		address:                 configuredAddress(),
		apiKeys:                 envList("SUBSCRIPTION_FIREWALL_API_KEYS"),
		allowNoAuth:             false,
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

	var err error
	if cfg.allowNoAuth, err = envBool("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", cfg.allowNoAuth); err != nil {
		return cfg, err
	}
	if cfg.workerCount, err = envPositiveInt("SUBSCRIPTION_FIREWALL_WORKERS", cfg.workerCount); err != nil {
		return cfg, err
	}
	if cfg.queueCapacity, err = envPositiveInt("SUBSCRIPTION_FIREWALL_QUEUE_CAPACITY", cfg.queueCapacity); err != nil {
		return cfg, err
	}
	if cfg.sweepInterval, err = envPositiveDuration("SUBSCRIPTION_FIREWALL_SWEEP_INTERVAL", cfg.sweepInterval); err != nil {
		return cfg, err
	}
	if cfg.issuerMonthlyLimitMinor, err = envPositiveInt64("SUBSCRIPTION_FIREWALL_ISSUER_MONTHLY_LIMIT", cfg.issuerMonthlyLimitMinor); err != nil {
		return cfg, err
	}
	if cfg.rateLimit, err = envNonNegativeFloat("SUBSCRIPTION_FIREWALL_RATE_LIMIT", cfg.rateLimit); err != nil {
		return cfg, err
	}
	if cfg.rateBurst, err = envPositiveInt("SUBSCRIPTION_FIREWALL_RATE_BURST", cfg.rateBurst); err != nil {
		return cfg, err
	}
	if cfg.readTimeout, err = envPositiveDuration("SUBSCRIPTION_FIREWALL_READ_TIMEOUT", cfg.readTimeout); err != nil {
		return cfg, err
	}
	if cfg.writeTimeout, err = envPositiveDuration("SUBSCRIPTION_FIREWALL_WRITE_TIMEOUT", cfg.writeTimeout); err != nil {
		return cfg, err
	}
	if cfg.shutdownTimeout, err = envPositiveDuration("SUBSCRIPTION_FIREWALL_SHUTDOWN_TIMEOUT", cfg.shutdownTimeout); err != nil {
		return cfg, err
	}
	if cfg.dbMaxOpenConns, err = envPositiveInt("SUBSCRIPTION_FIREWALL_DB_MAX_OPEN_CONNS", cfg.dbMaxOpenConns); err != nil {
		return cfg, err
	}
	if cfg.dbMaxIdleConns, err = envNonNegativeInt("SUBSCRIPTION_FIREWALL_DB_MAX_IDLE_CONNS", cfg.dbMaxIdleConns); err != nil {
		return cfg, err
	}
	if cfg.dbConnMaxLifetime, err = envPositiveDuration("SUBSCRIPTION_FIREWALL_DB_CONN_MAX_LIFETIME", cfg.dbConnMaxLifetime); err != nil {
		return cfg, err
	}
	if cfg.storage, err = envEnum("SUBSCRIPTION_FIREWALL_STORAGE", cfg.storage, storageAuto, storageMemory, storageMySQL); err != nil {
		return cfg, err
	}
	if cfg.issuer, err = envEnum("SUBSCRIPTION_FIREWALL_ISSUER", cfg.issuer, issuerSimulated); err != nil {
		return cfg, err
	}
	switch {
	case cfg.storage == storageMySQL && cfg.dbDSN == "":
		return cfg, fmt.Errorf("SUBSCRIPTION_FIREWALL_STORAGE=mysql requires SUBSCRIPTION_FIREWALL_DB_DSN")
	case cfg.storage == storageMemory && cfg.dbDSN != "":
		return cfg, fmt.Errorf("SUBSCRIPTION_FIREWALL_STORAGE=memory conflicts with SUBSCRIPTION_FIREWALL_DB_DSN")
	}
	if cfg.dbDSN != "" && cfg.storage == storageAuto {
		cfg.storage = storageMySQL
	} else if cfg.dbDSN == "" && cfg.storage == storageAuto {
		cfg.storage = storageMemory
	}
	if cfg.logLevel, err = envLogLevel("SUBSCRIPTION_FIREWALL_LOG_LEVEL", cfg.logLevel); err != nil {
		return cfg, err
	}
	cfg.redisAddr = os.Getenv("SUBSCRIPTION_FIREWALL_REDIS_ADDR")
	cfg.redisPassword = os.Getenv("SUBSCRIPTION_FIREWALL_REDIS_PASSWORD")
	cfg.otlpEndpoint = os.Getenv("SUBSCRIPTION_FIREWALL_OTLP_ENDPOINT")
	if cfg.otlpInsecure, err = envBool("SUBSCRIPTION_FIREWALL_OTLP_INSECURE", cfg.otlpInsecure); err != nil {
		return cfg, err
	}
	if cfg.issuerTimeout, err = envNonNegativeDuration("SUBSCRIPTION_FIREWALL_ISSUER_TIMEOUT", cfg.issuerTimeout); err != nil {
		return cfg, err
	}
	if cfg.issuerRetries, err = envNonNegativeInt("SUBSCRIPTION_FIREWALL_ISSUER_RETRIES", cfg.issuerRetries); err != nil {
		return cfg, err
	}
	if cfg.issuerBreakerThreshold, err = envPositiveInt("SUBSCRIPTION_FIREWALL_ISSUER_BREAKER_THRESHOLD", cfg.issuerBreakerThreshold); err != nil {
		return cfg, err
	}
	if cfg.issuerBreakerCooldown, err = envPositiveDuration("SUBSCRIPTION_FIREWALL_ISSUER_BREAKER_COOLDOWN", cfg.issuerBreakerCooldown); err != nil {
		return cfg, err
	}
	if cfg.issuanceTimeout, err = envPositiveDuration("SUBSCRIPTION_FIREWALL_ISSUANCE_TIMEOUT", cfg.issuanceTimeout); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck())
	}
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// runHealthcheck is used as the container HEALTHCHECK command: it reports the
// process ready only when /readyz answers 2xx. Kept in the binary because the
// production image (distroless) ships no shell or curl.
func runHealthcheck() int {
	target := "http://127.0.0.1" + addressPort(configuredAddress()) + "/readyz"
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %s returned %d\n", target, response.StatusCode)
		return 1
	}
	return 0
}

// addressPort extracts the listen port for the in-process healthcheck. It
// falls back to the default whenever the address cannot be understood, so a
// malformed SUBSCRIPTION_FIREWALL_ADDRESS degrades to a working probe instead
// of an unparsable URL.
func addressPort(address string) string {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return defaultPort
	}
	if parsed, err := strconv.Atoi(port); err != nil || parsed <= 0 || parsed > 65535 {
		return defaultPort
	}
	return ":" + port
}

const defaultPort = ":8080"

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if len(cfg.apiKeys) == 0 && !cfg.allowNoAuth {
		return fmt.Errorf("no api keys configured: set SUBSCRIPTION_FIREWALL_API_KEYS, or set SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH=true to start unauthenticated")
	}

	applicationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	logger := slog.New(obs.NewSanitizingHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.logLevel})))
	slog.SetDefault(logger)

	tracingShutdown, err := obs.SetupTracing(applicationContext, cfg.otlpEndpoint, cfg.otlpInsecure)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	if tracingShutdown != nil {
		defer func() {
			shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
			defer cancel()
			if err := tracingShutdown(shutdownContext); err != nil {
				logger.Error("tracer provider shutdown failed", "error", err)
			}
		}()
		logger.Info("tracing enabled", "otlp_endpoint", cfg.otlpEndpoint)
	}

	metrics := obs.NewMetrics(prometheus.DefaultRegisterer)
	clock := memory.Clock{}

	var transactionRepository ports.TransactionRepository
	var subscriptionRepository ports.SubscriptionRepository = memory.NewSubscriptionRepository()
	var tokenRepository ports.VirtualTokenRepository = memory.NewVirtualTokenRepository()
	var detectionOutbox ports.DetectionOutbox
	var lifecycleQueue ports.LifecycleSyncQueue
	var auditLog ports.AuditLog
	var db *sql.DB

	readiness := func(ctx context.Context) error { return nil }
	if cfg.storage == storageMySQL {
		var err error
		db, err = openDatabase(applicationContext, cfg.dbDSN, cfg)
		if err != nil {
			return err
		}
		defer db.Close()
		transactionRepository = mysql.NewTransactionRepository(db)
		subscriptionRepository = mysql.NewSubscriptionRepository(db)
		tokenRepository = mysql.NewVirtualTokenRepository(db)
		detectionOutbox = mysql.NewOutbox(db)
		lifecycleQueue = mysql.NewLifecycleSyncQueue(db)
		auditLog = mysql.NewAuditLog(db)
		readiness = func(ctx context.Context) error {
			return db.PingContext(ctx)
		}
		logger.Info("mysql storage connected")
	} else {
		logger.Warn("using in-memory storage: data is lost on restart and the service must run as a single replica",
			"storage", cfg.storage,
			"reason", "no database configured",
		)
		transactionRepo := memory.NewTransactionRepository()
		transactionRepository = transactionRepo
		detectionOutbox = memory.NewDetectionOutbox(transactionRepo, metrics, logger, cfg.queueCapacity)
		lifecycleQueue = memory.NewLifecycleSyncQueue()
		auditLog = memory.NewAuditLog()
	}

	detectorService := detector.New(transactionRepository, clock, detector.DefaultConfig())
	subscriptionService := subscription.NewService(subscriptionRepository, clock)
	cardIssuer, err := buildCardIssuer(cfg, logger)
	if err != nil {
		return err
	}
	tokenService := token.NewService(tokenRepository, cardIssuer, clock, token.Options{
		IssuanceTimeout: cfg.issuanceTimeout,
		Logger:          logger,
	})
	lifecycleService := lifecycle.NewService(subscriptionService, tokenService, lifecycle.Options{
		Queue:   lifecycleQueue,
		Audit:   auditLog,
		Metrics: metrics,
		Logger:  logger,
	})
	lifecycleSyncer := lifecycle.NewSyncer(lifecycleService, lifecycleQueue, metrics, logger)
	lifecycleSyncer.Start(applicationContext, 1)
	defer lifecycleSyncer.Stop()

	pipelineService := pipeline.New(detectionOutbox, detectorService, subscriptionService, tokenService, metrics, logger)
	pipelineService.Start(applicationContext, cfg.workerCount)
	defer pipelineService.Stop()

	go runSubscriptionSweeper(applicationContext, db, subscriptionService, lifecycleService, metrics, logger, cfg.sweepInterval)

	apiServer, err := httpapi.NewServer(
		httpapi.Config{
			Address:         cfg.address,
			ReadTimeout:     cfg.readTimeout,
			WriteTimeout:    cfg.writeTimeout,
			ShutdownTimeout: cfg.shutdownTimeout,
			APIKeys:         cfg.apiKeys,
			AllowNoAuth:     cfg.allowNoAuth,
			RateLimit:       cfg.rateLimit,
			RateBurst:       cfg.rateBurst,
			Clock:           clock,
			Readiness:       readiness,
			Tracing:         cfg.otlpEndpoint != "",
			RedisAddr:       cfg.redisAddr,
			RedisPassword:   cfg.redisPassword,
		},
		transactionRepository,
		subscriptionService,
		tokenService,
		lifecycleService,
		detectionOutbox,
		metrics,
		logger,
	)
	if err != nil {
		return fmt.Errorf("build http server: %w", err)
	}

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- apiServer.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		return err
	case <-applicationContext.Done():
		logger.Info("shutdown signal received")
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancelShutdown()

	if err := apiServer.Shutdown(shutdownContext); err != nil {
		logger.Error("http server shutdown failed", "error", err)
	}
	logger.Info("shutdown complete")
	return nil
}

func openDatabase(ctx context.Context, dsn string, cfg config) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(cfg.dbMaxOpenConns)
	db.SetMaxIdleConns(cfg.dbMaxIdleConns)
	db.SetConnMaxLifetime(cfg.dbConnMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	if err := mysql.Migrate(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate database: %w", err)
	}
	return db, nil
}

func runSubscriptionSweeper(ctx context.Context, db *sql.DB, subscriptions *subscription.Service, lifecycleService *lifecycle.Service, metrics *obs.Metrics, logger *slog.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		sweepOnce(ctx, db, subscriptions, lifecycleService, metrics, logger)
	}
}

func sweepOnce(ctx context.Context, db *sql.DB, subscriptions *subscription.Service, lifecycleService *lifecycle.Service, metrics *obs.Metrics, logger *slog.Logger) {
	release := func() {}
	if db != nil {
		acquired, ok := acquireSweepLock(ctx, db, logger)
		if !ok {
			return
		}
		release = acquired
	}
	defer release()

	changed, err := subscriptions.Sweep(ctx, 0)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		logger.Error("subscription sweep failed", "error", err)
		return
	}
	for _, subscription := range changed {
		metrics.CountSubscriptionSwept(string(subscription.State))
		logger.Info("subscription marked zombie",
			"subscription_id", string(subscription.ID),
			"user_id", string(subscription.UserID),
			"merchant_id", string(subscription.MerchantID),
		)
		if err := lifecycleService.SyncZombie(ctx, subscription); err != nil {
			logger.Error("freeze card of zombie subscription failed",
				"subscription_id", string(subscription.ID),
				"virtual_token_id", subscription.VirtualTokenID,
				"error", err,
			)
		}
	}
}

// sweepLockName is the MySQL advisory lock that keeps a single replica running
// the subscription sweep. GET_LOCK is scoped to a connection, so acquire and
// release must both go through the same pinned connection.
const (
	sweepLockName       = "subscription_firewall_sweep"
	sweepReleaseTimeout = 5 * time.Second
)

func acquireSweepLock(ctx context.Context, db *sql.DB, logger *slog.Logger) (release func(), ok bool) {
	conn, err := db.Conn(ctx)
	if err != nil {
		logger.Error("acquire sweep lock connection failed", "error", err)
		return func() {}, false
	}

	var acquired int
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 0)`, sweepLockName).Scan(&acquired); err != nil {
		logger.Error("acquire sweep lock failed", "error", err)
		closeSweepLockConn(conn, logger)
		return func() {}, false
	}
	if acquired != 1 {
		closeSweepLockConn(conn, logger)
		return func() {}, false
	}

	return func() {
		// The lock must be released even when the parent context is already
		// cancelled, otherwise the pinned connection carries it back into the
		// pool and no replica sweeps until that connection is recycled.
		releaseContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), sweepReleaseTimeout)
		defer cancel()

		if _, err := conn.ExecContext(releaseContext, `SELECT RELEASE_LOCK(?)`, sweepLockName); err != nil {
			logger.Error("release sweep lock failed", "error", err)
		}
		closeSweepLockConn(conn, logger)
	}, true
}

func closeSweepLockConn(conn *sql.Conn, logger *slog.Logger) {
	if err := conn.Close(); err != nil {
		logger.Error("return sweep lock connection failed", "error", err)
	}
}

// configuredAddress returns the listen address, falling back to the default
// when the variable is unset or empty.
func configuredAddress() string {
	if configured := os.Getenv("SUBSCRIPTION_FIREWALL_ADDRESS"); configured != "" {
		return configured
	}
	return defaultAddress
}

func envList(name string) []string {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	return strings.Split(configured, ",")
}

// envBool parses a boolean strictly. Unlike the numeric parsers it does not
// silently fall back, because a typo in a security-relevant switch such as
// ALLOW_NO_AUTH would otherwise go unnoticed until the service is exposed.
func envBool(name string, fallback bool) (bool, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(configured)
	if err != nil {
		return false, fmt.Errorf("%s must be one of 1|t|T|TRUE|true|True|0|f|F|FALSE|false|False, got %q", name, configured)
	}
	return parsed, nil
}

func envPositiveInt(name string, fallback int) (int, error) {
	value, err := envPositiveInt64(name, int64(fallback))
	return int(value), err
}

func envPositiveInt64(name string, fallback int64) (int64, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(configured, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, configured)
	}
	return parsed, nil
}

func envPositiveDuration(name string, fallback time.Duration) (time.Duration, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(configured)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", name, configured)
	}
	return parsed, nil
}

func envNonNegativeInt(name string, fallback int) (int, error) {
	value, err := envNonNegativeInt64(name, int64(fallback))
	return int(value), err
}

func envNonNegativeInt64(name string, fallback int64) (int64, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(configured, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, got %q", name, configured)
	}
	return parsed, nil
}

func envEnum(name, fallback string, allowed ...string) (string, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	for _, candidate := range allowed {
		if configured == candidate {
			return configured, nil
		}
	}
	return "", fmt.Errorf("%s must be one of %s, got %q", name, strings.Join(allowed, "|"), configured)
}

func envLogLevel(name string, fallback slog.Level) (slog.Level, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(configured))); err != nil {
		return 0, fmt.Errorf("%s must be one of debug|info|warn|error, got %q", name, configured)
	}
	return level, nil
}

func envNonNegativeDuration(name string, fallback time.Duration) (time.Duration, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(configured)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative duration, got %q", name, configured)
	}
	return parsed, nil
}

func envNonNegativeFloat(name string, fallback float64) (float64, error) {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(configured, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative number, got %q", name, configured)
	}
	return parsed, nil
}
