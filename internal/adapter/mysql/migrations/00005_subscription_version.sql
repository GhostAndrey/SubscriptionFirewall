-- +goose Up
ALTER TABLE subscriptions
    ADD COLUMN version INT UNSIGNED NOT NULL DEFAULT 0 AFTER observed_payments;

-- +goose Down
ALTER TABLE subscriptions
    DROP COLUMN version;