-- +goose Up
CREATE TABLE IF NOT EXISTS transactions (
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
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS subscriptions (
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
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS virtual_tokens (
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
) ENGINE=InnoDB;

-- +goose Down
DROP TABLE IF EXISTS virtual_tokens;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS transactions;
