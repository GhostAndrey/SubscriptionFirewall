package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type migration struct {
	name       string
	statements []string
}

var migrations = []migration{
	{
		name: "0001_initial_schema",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS transactions (
				id            VARCHAR(64)  NOT NULL,
				user_id       VARCHAR(64)  NOT NULL,
				merchant_id   VARCHAR(64)  NOT NULL,
				merchant_name VARCHAR(255) NOT NULL,
				mcc           SMALLINT UNSIGNED NOT NULL,
				amount_minor  BIGINT       NOT NULL,
				currency      CHAR(3)      NOT NULL,
				authorized_at DATETIME(3)  NOT NULL,
				PRIMARY KEY (id),
				INDEX idx_transactions_user_time (user_id, authorized_at)
			) ENGINE=InnoDB`,
			`CREATE TABLE IF NOT EXISTS subscriptions (
				id               VARCHAR(64)  NOT NULL,
				user_id          VARCHAR(64)  NOT NULL,
				merchant_id      VARCHAR(64)  NOT NULL,
				merchant_name    VARCHAR(255) NOT NULL,
				virtual_token_id VARCHAR(64)  NOT NULL DEFAULT '',
				state            VARCHAR(16)  NOT NULL,
				billing_window   VARCHAR(8)   NOT NULL,
				average_amount   BIGINT       NOT NULL,
				currency         CHAR(3)      NOT NULL,
				last_charged_at  DATETIME(3)  NOT NULL,
				next_expected_at DATETIME(3)  NOT NULL,
				observed_payments INT         NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY uq_subscriptions_user_merchant (user_id, merchant_id),
				INDEX idx_subscriptions_user (user_id)
			) ENGINE=InnoDB`,
			`CREATE TABLE IF NOT EXISTS virtual_tokens (
				id             VARCHAR(64) NOT NULL,
				user_id        VARCHAR(64) NOT NULL,
				merchant_id    VARCHAR(64) NOT NULL,
				masked_pan     VARCHAR(32) NOT NULL,
				monthly_limit  BIGINT      NOT NULL,
				spent_in_period BIGINT     NOT NULL DEFAULT 0,
				currency       CHAR(3)     NOT NULL,
				state          VARCHAR(16) NOT NULL,
				period_started DATETIME(3) NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY uq_tokens_user_merchant (user_id, merchant_id),
				INDEX idx_tokens_user (user_id)
			) ENGINE=InnoDB`,
		},
	},
	{
		name: "0002_detection_outbox",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS detection_outbox (
				id         BIGINT AUTO_INCREMENT PRIMARY KEY,
				user_id    VARCHAR(64) NOT NULL,
				status     VARCHAR(16) NOT NULL DEFAULT 'pending',
				attempts   INT         NOT NULL DEFAULT 0,
				created_at DATETIME(3) NOT NULL,
				updated_at DATETIME(3) NOT NULL,
				INDEX idx_outbox_claim (status, attempts, id)
			) ENGINE=InnoDB`,
		},
	},
	{
		name: "0003_audit_log",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS audit_log (
				id          BIGINT AUTO_INCREMENT PRIMARY KEY,
				actor       VARCHAR(128) NOT NULL,
				action      VARCHAR(32)  NOT NULL,
				entity_type VARCHAR(32)  NOT NULL,
				entity_id   VARCHAR(64)  NOT NULL,
				created_at  DATETIME(3)  NOT NULL,
				INDEX idx_audit_entity (entity_type, entity_id)
			) ENGINE=InnoDB`,
		},
	},
}

func Migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version    VARCHAR(64) NOT NULL PRIMARY KEY,
			applied_at DATETIME(3) NOT NULL
		) ENGINE=InnoDB`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := make(map[string]bool)
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return fmt.Errorf("scan schema_migrations: %w", err)
		}
		applied[version] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema_migrations: %w", err)
	}

	for _, m := range migrations {
		if applied[m.name] {
			continue
		}
		for _, statement := range m.statements {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply migration %s: %w", m.name, err)
			}
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
			m.name, time.Now().UTC()); err != nil {
			return fmt.Errorf("record migration %s: %w", m.name, err)
		}
	}
	return nil
}
