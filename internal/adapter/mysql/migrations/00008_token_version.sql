-- +goose Up
ALTER TABLE virtual_tokens
    ADD COLUMN version INT UNSIGNED NOT NULL DEFAULT 0 AFTER period_started;

-- +goose Down
ALTER TABLE virtual_tokens
    DROP COLUMN version;
