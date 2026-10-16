package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/adapter/mysql"
	"subscriptionfirewall/internal/obs"
	"subscriptionfirewall/internal/ports"
)

type storage struct {
	transactions   ports.TransactionRepository
	subscriptions  ports.SubscriptionRepository
	tokens         ports.VirtualTokenRepository
	outbox         ports.DetectionOutbox
	lifecycleQueue ports.LifecycleSyncQueue
	audit          ports.AuditLog
	database       *sql.DB
	close          func()
}

func openStorage(
	ctx context.Context,
	cfg config,
	logger *slog.Logger,
	metrics *obs.Metrics,
) (*storage, error) {
	if cfg.storage == storageMySQL {
		return openMySQLStorage(ctx, cfg, logger)
	}

	logger.Warn("using in-memory storage: data is lost on restart and the service must run as a single replica",
		"storage", cfg.storage,
		"reason", "no database configured",
	)
	transactions := memory.NewTransactionRepository()
	return &storage{
		transactions:   transactions,
		subscriptions:  memory.NewSubscriptionRepository(),
		tokens:         memory.NewVirtualTokenRepository(),
		outbox:         memory.NewDetectionOutbox(transactions, metrics, logger, cfg.queueCapacity),
		lifecycleQueue: memory.NewLifecycleSyncQueue(),
		audit:          memory.NewAuditLog(),
		close:          func() {},
	}, nil
}

func openMySQLStorage(ctx context.Context, cfg config, logger *slog.Logger) (*storage, error) {
	database, err := openDatabase(ctx, cfg.dbDSN, cfg)
	if err != nil {
		return nil, err
	}
	logger.Info("mysql storage connected")
	return &storage{
		transactions:   mysql.NewTransactionRepository(database),
		subscriptions:  mysql.NewSubscriptionRepository(database),
		tokens:         mysql.NewVirtualTokenRepository(database),
		outbox:         mysql.NewOutbox(database),
		lifecycleQueue: mysql.NewLifecycleSyncQueue(database),
		audit:          mysql.NewAuditLog(database),
		database:       database,
		close:          func() { _ = database.Close() },
	}, nil
}

func (s *storage) readiness(ctx context.Context) error {
	if s.database == nil {
		return nil
	}
	return s.database.PingContext(ctx)
}

func openDatabase(ctx context.Context, dsn string, cfg config) (*sql.DB, error) {
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	database.SetMaxOpenConns(cfg.dbMaxOpenConns)
	database.SetMaxIdleConns(cfg.dbMaxIdleConns)
	database.SetConnMaxLifetime(cfg.dbConnMaxLifetime)

	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	if err := mysql.Migrate(ctx, database); err != nil {
		database.Close()
		return nil, fmt.Errorf("migrate database: %w", err)
	}
	return database, nil
}
