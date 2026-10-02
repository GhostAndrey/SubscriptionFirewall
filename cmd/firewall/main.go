package main

import (
	"context"
	"database/sql"
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
	"subscriptionfirewall/internal/domain"
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
}

// storage values for SUBSCRIPTION_FIREWALL_STORAGE: auto picks mysql when a
// DSN is set and memory otherwise; memory and mysql pin the choice explicitly.
const (
	storageAuto   = "auto"
	storageMemory = "memory"
	storageMySQL  = "mysql"
)

func loadConfig() (config, error) {
	cfg := config{
		address:                 envString("SUBSCRIPTION_FIREWALL_ADDRESS", defaultAddress),
		apiKeys:                 envList("SUBSCRIPTION_FIREWALL_API_KEYS"),
		allowNoAuth:             envBool("SUBSCRIPTION_FIREWALL_ALLOW_NO_AUTH", false),
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
	}

	var err error
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
	target := "http://127.0.0.1" + addressPort(envString("SUBSCRIPTION_FIREWALL_ADDRESS", defaultAddress)) + "/readyz"
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

func addressPort(address string) string {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return ":8080"
	}
	return ":" + port
}

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

	metrics := obs.NewMetrics(prometheus.DefaultRegisterer)
	clock := memory.Clock{}

	var transactionRepository ports.TransactionRepository = memory.NewTransactionRepository()
	var subscriptionRepository ports.SubscriptionRepository = memory.NewSubscriptionRepository()
	var tokenRepository ports.VirtualTokenRepository = memory.NewVirtualTokenRepository()
	var detectionOutbox ports.DetectionOutbox
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
		auditLog = mysql.NewAuditLog(db)
		readiness = func(ctx context.Context) error {
			return db.PingContext(ctx)
		}
		logger.Info("mysql storage connected")
	} else {
		logger.Warn("no database configured: using in-memory storage, data will be lost on restart")
		transactionRepo := memory.NewTransactionRepository()
		transactionRepository = transactionRepo
		detectionOutbox = memory.NewDetectionOutbox(transactionRepo, metrics, logger, cfg.queueCapacity)
		auditLog = memory.NewAuditLog()
	}

	detectorService := detector.New(transactionRepository, clock, detector.DefaultConfig())
	subscriptionService := subscription.NewService(subscriptionRepository, clock)
	tokenService := token.NewService(tokenRepository, issuer.NewSimulatedCardIssuer(cfg.issuerMonthlyLimitMinor), clock)
	pipelineService := pipeline.New(detectionOutbox, detectorService, subscriptionService, tokenService, metrics, logger)
	pipelineService.Start(applicationContext, cfg.workerCount)
	defer pipelineService.Stop()

	go runSubscriptionSweeper(applicationContext, db, subscriptionService, metrics, logger, cfg.sweepInterval)

	apiServer := httpapi.NewServer(
		httpapi.Config{
			Address:         cfg.address,
			ReadTimeout:     cfg.readTimeout,
			WriteTimeout:    cfg.writeTimeout,
			ShutdownTimeout: cfg.shutdownTimeout,
			APIKeys:         cfg.apiKeys,
			RateLimit:       cfg.rateLimit,
			RateBurst:       cfg.rateBurst,
			Readiness:       readiness,
		},
		transactionRepository,
		subscriptionService,
		tokenService,
		detectionOutbox,
		auditLog,
		metrics,
		logger,
	)

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

func runSubscriptionSweeper(ctx context.Context, db *sql.DB, subscriptions *subscription.Service, metrics *obs.Metrics, logger *slog.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		release := func() {}
		if db != nil {
			var ok bool
			release, ok = acquireSweepLock(ctx, db, logger)
			if !ok {
				continue
			}
		}

		changed, err := subscriptions.Sweep(ctx)
		release()
		if err != nil {
			logger.Error("subscription sweep failed", "error", err)
			continue
		}
		for _, subscription := range changed {
			metrics.CountSubscriptionDetected(string(domain.SubscriptionZombie))
			logger.Info("subscription marked zombie",
				"subscription_id", string(subscription.ID),
				"user_id", string(subscription.UserID),
				"merchant_id", string(subscription.MerchantID),
			)
		}
	}
}

func acquireSweepLock(ctx context.Context, db *sql.DB, logger *slog.Logger) (release func(), ok bool) {
	var acquired int
	if err := db.QueryRowContext(ctx, `SELECT GET_LOCK('subscription_firewall_sweep', 0)`).Scan(&acquired); err != nil {
		logger.Error("acquire sweep lock failed", "error", err)
		return func() {}, false
	}
	if acquired != 1 {

		return func() {}, false
	}
	return func() {
		if _, err := db.ExecContext(ctx, `SELECT RELEASE_LOCK('subscription_firewall_sweep')`); err != nil {
			logger.Error("release sweep lock failed", "error", err)
		}
	}, true
}

func envString(name, fallback string) string {
	if configured := os.Getenv(name); configured != "" {
		return configured
	}
	return fallback
}

func envList(name string) []string {
	configured := os.Getenv(name)
	if configured == "" {
		return nil
	}
	return strings.Split(configured, ",")
}

func envBool(name string, fallback bool) bool {
	configured := os.Getenv(name)
	if configured == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(configured)
	if err != nil {
		return fallback
	}
	return parsed
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
